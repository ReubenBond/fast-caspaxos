using System.Buffers.Binary;
using System.Globalization;
using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Testing;

/// <summary>
/// Framework-neutral IRegisterService fixture on the actual Fast CASPaxos simulation
/// hosts and Clockwork scheduler/network. Keys are an ordinal immutable application
/// map in one consensus register, not a model of production per-key concurrency.
/// </summary>
public sealed class RegisterSimulationCluster : IAsyncDisposable
{
    private readonly FastCasSimulationCluster<RegisterState> _runtime;
    private readonly List<string> _trace = [];
    private readonly Dictionary<int, TaskCompletionSource<RegisterState>> _pending = [];
    private readonly List<FastCasAddress> _clients = [];
    private readonly HashSet<int> _lostReplies = [];
    private readonly Dictionary<int, TimeSpan> _replyDelays = [];
    private int _nextRequest;
    private int _contextFreeConsumerDepth;
    private bool _running;
    private bool _disposed;

    /// <summary>Creates five Fast-mode members by default (fast quorum 4, recovery quorum 3).</summary>
    public RegisterSimulationCluster(int seed, int nodeCount = 5)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(nodeCount, 1);
        NodeCount = nodeCount;
        var observer = new Observer(this);
        _runtime = new(seed, RegisterState.Empty, new FastCasSimulationOptions
        {
            ProposerModePolicy = ProposerModePolicy.StaticFast,
            CrashRestartStateModel = FastCasCrashRestartStateModel.PreserveDurableAcceptorState,
            ProposerResponseTimeout = TimeSpan.FromMilliseconds(100),
            TransportObserver = observer,
            HistoryObserver = observer
        }, DateTimeOffset.UnixEpoch);
        _runtime.CreateConsensusRegisterConfiguration(nodeCount);
        _runtime.Network.EnableDelays = true;
        _runtime.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        _runtime.Network.MaxJitter = TimeSpan.FromMilliseconds(2);
        Trace = _trace.AsReadOnly();
        Record($"cluster seed={seed} members={nodeCount} mode=fast");
    }

    public int Seed => _runtime.Seed;
    public int NodeCount { get; }
    public TimeProvider TimeProvider => _runtime.TimeProvider;
    public IReadOnlyList<string> Trace { get; }
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Runs and completes a scenario synchronously in virtual time. Await only fixture
    /// operations or work scheduled on this context. Consumer wrappers may use
    /// ConfigureAwait(false) on register operations; the scenario should capture
    /// this context. Task.Run, wall-clock delays and external I/O are not simulated.
    /// </summary>
    public void Run(Func<RegisterSimulationCluster, Task> scenario, int maxIterations = 100000)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_running) throw new InvalidOperationException("Nested simulation Run is not supported.");
        ArgumentOutOfRangeException.ThrowIfLessThan(maxIterations, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(OperationTimeout, TimeSpan.Zero);
        using var guard = _runtime.Guard.Enter();
        using var context = _runtime.SynchronizationContext.Install();
        _running = true;
        try
        {
            var task = scenario(this);
            if (!_runtime.RunUntil(() => task.IsCompleted, maxIterations))
                throw new InvalidOperationException(
                    $"Scenario stalled or escaped the deterministic scheduler. {_runtime.GetFailureDiagnostics(nameof(Run))}");
            task.GetAwaiter().GetResult();
            _runtime.AssertSafetyInvariants();
        }
        finally
        {
            _running = false;
        }
    }

    /// <summary>
    /// Creates a client pinned to a 1-based member (member 1 by default).
    /// Sticky routing avoids unnecessary speculative multi-proposer contention.
    /// </summary>
    public IRegisterService CreateClient(int? node = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var member = node ?? 1;
        ValidateNode(member);
        // Runtime reserves 0..NodeCount for its collocated clients.
        var address = _runtime.RegisterClient(checked(NodeCount + _clients.Count + 1));
        _clients.Add(address);
        foreach (var lost in _lostReplies)
            _runtime.Network.CreatePartition(FastCasAddress.Proposer(lost).NetworkAddress, address.NetworkAddress);
        foreach (var (source, delay) in _replyDelays)
            _runtime.Network.SetLinkLatency(FastCasAddress.Proposer(source).NetworkAddress,
                address.NetworkAddress, delay, TimeSpan.Zero);
        return new Client(this, member, address);
    }

    public Task DelayAsync(TimeSpan delay)
    {
        RequireContext();
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        var completion = new TaskCompletionSource();
        _runtime.TaskQueue.EnqueueAfter(() => completion.SetResult(), delay);
        return completion.Task;
    }

    /// <summary>Crashes both roles of a member. Its existing durable acceptor store is retained.</summary>
    public void CrashNode(int node)
    {
        ValidateNode(node);
        _runtime.CrashNode(FastCasAddress.Proposer(node));
        _runtime.CrashNode(FastCasAddress.Acceptor(node));
        Record($"crash node={node}");
    }

    public void RestartNode(int node)
    {
        ValidateNode(node);
        _runtime.RestartNode(FastCasAddress.Acceptor(node));
        _runtime.RestartNode(FastCasAddress.Proposer(node));
        Record($"restart node={node}");
    }

    /// <summary>Partitions all protocol links between the two physical members, both directions.</summary>
    public void Partition(int first, int second)
    {
        ValidatePair(first, second);
        foreach (var from in Addresses(first))
        foreach (var to in Addresses(second))
            _runtime.Network.CreateBidirectionalPartition(from.NetworkAddress, to.NetworkAddress);
        Record($"partition nodes={first},{second}");
    }

    /// <summary>Heals all partitions including reply loss; explicit latency overrides are retained.</summary>
    public void Heal()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _runtime.Network.HealAllPartitions();
        _lostReplies.Clear();
        Record("heal");
    }

    public void SetLinkDelay(int first, int second, TimeSpan delay)
    {
        ValidatePair(first, second);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        foreach (var from in Addresses(first))
        foreach (var to in Addresses(second))
            _runtime.Network.SetBidirectionalLinkLatency(from.NetworkAddress, to.NetworkAddress, delay, TimeSpan.Zero);
        Record($"delay nodes={first},{second} ticks={delay.Ticks}");
    }

    /// <summary>
    /// Drops replies sent by this member to every fixture client (including future clients),
    /// while leaving requests and protocol acknowledgements intact. Already sent messages
    /// retain their delivery schedule, matching Clockwork network semantics.
    /// </summary>
    public void SetReplyLoss(int node, bool enabled)
    {
        ValidateNode(node);
        if (enabled) _lostReplies.Add(node);
        else _lostReplies.Remove(node);
        foreach (var client in _clients)
        {
            var source = FastCasAddress.Proposer(node).NetworkAddress;
            if (enabled) _runtime.Network.CreatePartition(source, client.NetworkAddress);
            else _runtime.Network.HealPartition(source, client.NetworkAddress);
        }
        Record($"reply-loss node={node} enabled={enabled}");
    }

    /// <summary>Sets a fixed virtual reply delay for this member, including future clients.</summary>
    public void SetReplyDelay(int node, TimeSpan delay)
    {
        ValidateNode(node);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        _replyDelays[node] = delay;
        foreach (var client in _clients)
            _runtime.Network.SetLinkLatency(FastCasAddress.Proposer(node).NetworkAddress,
                client.NetworkAddress, delay, TimeSpan.Zero);
        Record($"reply-delay node={node} ticks={delay.Ticks}");
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        if (_running) throw new InvalidOperationException("Dispose the cluster outside Run.");
        _disposed = true;
        return _runtime.DisposeAsync();
    }

    private async ValueTask<RegisterState> ExecuteAsync(
        Client client, IOperation<RegisterState> operation, CancellationToken cancellationToken)
    {
        RequireContext();
        cancellationToken.ThrowIfCancellationRequested();
        var request = checked(++_nextRequest);
        var completion = new TaskCompletionSource<RegisterState>();
        _pending.Add(request, completion);
        // No RunContinuationsAsynchronously: completions and captured continuations belong
        // to Clockwork, never the thread pool. The timer is disposed after every operation.
        using var deadline = TimeProvider.CreateTimer(
            _ => completion.TrySetException(new RegisterUnavailableException()), null,
            OperationTimeout, Timeout.InfiniteTimeSpan);
        using var cancellation = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        try
        {
            Record($"request id={request} node={client.Node} operation={operation}");
            _runtime.SendProposal(FastCasAddress.Proposer(client.Node), operation, request, client.Address);
            return await completion.Task;
        }
        finally
        {
            _pending.Remove(request);
        }
    }

    private void OnReply(FastCasAddress proposer, int request, RegisterState state)
    {
        Record($"reply id={request} node={proposer.Ordinal} revision={state.Version}");
        if (_pending.TryGetValue(request, out var completion))
            completion.TrySetResult(state);
    }

    private ByteString NewVersion(int node)
    {
        // Request IDs are shared by reads and writes and never reset on member restart.
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt32BigEndian(bytes, node);
        BinaryPrimitives.WriteInt32BigEndian(bytes[4..], checked(_nextRequest + 1));
        return new(bytes);
    }

    private void RequireContext()
    {
        using var guard = _runtime.Guard.Enter();
        ObjectDisposedException.ThrowIf(_disposed, this);
        var context = SynchronizationContext.Current;
        if (!_running || !(context is not null && _runtime.SynchronizationContext.IsSameScheduler(context)
            || context is null && _contextFreeConsumerDepth > 0))
            throw new InvalidOperationException("Use fixture operations inside Run on its deterministic synchronization context.");
    }

    private ValueTask<T> Bridge<T>(Func<ValueTask<T>> operation)
    {
        RequireContext();
        // Internal fixture awaits always capture Clockwork, including operations
        // started by a context-free production wrapper after an earlier read.
        using var context = _runtime.SynchronizationContext.Install();
        var task = operation();
        return task.IsCompleted ? task : new SimulationValueTaskSource<T>(task, RunContextFreeConsumer).Task;
    }

    private void RunContextFreeConsumer(Action<object?> continuation, object? state)
    {
        RequireContext();
        var previous = SynchronizationContext.Current;
        _contextFreeConsumerDepth++;
        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            continuation(state);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
            _contextFreeConsumerDepth--;
        }
    }

    private void ValidateNode(int node)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfLessThan(node, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(node, NodeCount);
    }

    private void ValidatePair(int first, int second)
    {
        ValidateNode(first);
        ValidateNode(second);
        if (first == second) throw new ArgumentException("Specify two distinct physical members.");
    }

    private static FastCasAddress[] Addresses(int node) =>
        [FastCasAddress.Proposer(node), FastCasAddress.Acceptor(node)];

    private void Record(string entry) =>
        _trace.Add(string.Create(CultureInfo.InvariantCulture, $"{_runtime.Clock.CurrentTime.Ticks} {entry}"));

    private sealed class Client(RegisterSimulationCluster owner, int node, FastCasAddress address) : IRegisterService
    {
        public int Node => node;
        public FastCasAddress Address => address;

        public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default) =>
            owner.Bridge(() => ReadCoreAsync(key, cancellationToken));

        private async ValueTask<RegisterValue> ReadCoreAsync(string key, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(key);
            var state = await owner.ExecuteAsync(this, ReadOperation, cancellationToken);
            return state.Get(key);
        }

        public ValueTask<CompareExchangeResult> CompareExchangeAsync(
            string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default) =>
            owner.Bridge(() => CompareExchangeCoreAsync(key, expectedVersion, value, cancellationToken));

        private async ValueTask<CompareExchangeResult> CompareExchangeCoreAsync(
            string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(key);
            if (value.IsDefault) throw new ArgumentException("An initialized payload is required.", nameof(value));
            if (expectedVersion is { } expected && !RegisterVersion.IsValid(expected))
                throw new ArgumentException("A register version must be a token returned by this register cluster.", nameof(expectedVersion));
            owner.RequireContext();
            cancellationToken.ThrowIfCancellationRequested();
            var operation = new ConditionalWrite(key, expectedVersion, value, owner.NewVersion(node));
            try
            {
                var state = await owner.ExecuteAsync(this, operation, cancellationToken);
                var current = state.Get(key);
                var status = current.Version == operation.Version ? CompareExchangeStatus.Applied
                    : operation.MayHaveApplied ? CompareExchangeStatus.Unknown : CompareExchangeStatus.Mismatch;
                owner.Record($"result node={node} status={status}");
                return new(status, current);
            }
            catch (Exception error) when (operation.MayHaveApplied
                && error is RegisterUnavailableException or OperationCanceledException)
            {
                owner.Record($"result node={node} status=Unknown error={error.GetType().Name}");
                return new(CompareExchangeStatus.Unknown, null, error);
            }
            finally
            {
                // A request which expires while still queued must not later start a write.
                // Already dispatched accepts can still take effect: those return Unknown.
                operation.Expire();
            }
        }

        public ValueTask<RegisterKeyPage> ListKeysAsync(
            string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            owner.Bridge(() => ListKeysCoreAsync(prefix, afterKey, limit, cancellationToken));

        private async ValueTask<RegisterKeyPage> ListKeysCoreAsync(
            string prefix, string? afterKey, int limit, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(prefix);
            ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
            var state = await owner.ExecuteAsync(this, ReadOperation, cancellationToken);
            var keys = state.Keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)
                && (afterKey is null || string.CompareOrdinal(key, afterKey) > 0))
                .Take(limit == int.MaxValue ? limit : limit + 1).ToArray();
            return new(keys.Take(limit).ToArray(), keys.Length > limit ? keys[limit - 1] : null);
        }
    }

    private static readonly IOperation<RegisterState> ReadOperation = new Operation<int, RegisterState>
    {
        Name = "read",
        Input = 0,
        Apply = static (current, _) => (OperationStatus.NotApplicable, current)
    };

    private sealed class ConditionalWrite(
        string key, ByteString? expectedVersion, ByteString payload, ByteString version) : IOperation<RegisterState>
    {
        public ByteString Version => version;
        public bool MayHaveApplied { get; private set; }
        private bool _expired;

        public void Expire() => _expired = true;

        public (OperationStatus Status, RegisterState Result) Apply(RegisterState current)
        {
            var existing = current.Get(key);
            if (_expired || existing.Version == version || existing.Version != expectedVersion)
                return (OperationStatus.NotApplicable, current);
            MayHaveApplied = true;
            return (OperationStatus.Success, current.Set(key, new(payload, version)));
        }

        public override string ToString() => "compare-exchange";
    }

    private sealed class Observer(RegisterSimulationCluster owner)
        : IFastCasSimulationTransportObserver, IFastCasSimulationHistoryObserver
    {
        public void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status) =>
            owner.Record($"transport kind={kind} status={status}");

        public void OnProposalSent<TValue>(DateTimeOffset timestamp, FastCasAddress proposer,
            int requestId, IOperation<TValue> operation) where TValue : IVersionedValue<TValue> { }

        public void OnProposalCompleted<TValue>(DateTimeOffset timestamp, FastCasAddress proposer,
            int requestId, ProposeResponse<TValue> response) where TValue : IVersionedValue<TValue>
        {
            if (response.CommittedValue is RegisterState state)
                owner.OnReply(proposer, requestId, state);
        }

        public void OnLearnCommittedValue<TValue>(DateTimeOffset timestamp, FastCasAddress sourceProposer,
            FastCasAddress targetProposer, Ballot acceptedBallot, TValue value, Ballot preparedBallot)
            where TValue : IVersionedValue<TValue> =>
            owner.Record($"commit node={sourceProposer.Ordinal} fast={acceptedBallot.IsFastRoundBallot} ballot={acceptedBallot} revision={value.Version}");
    }
}
