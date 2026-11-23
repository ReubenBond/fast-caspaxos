using System.Buffers.Binary;
using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Simulation;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Invariants;
using Orleans.Testing.Simulation;

namespace Orleans.Consensus.CasPaxos.Testing;

public enum SimulatedRpcKind
{
    Read,
    Prepare,
    Accept,
    SetFloor,
    ListKeys,
}

public sealed record SimulatedRpcContext(
    string CallerId,
    string VoterId,
    SimulatedRpcKind Kind,
    string? Key,
    Ballot Ballot,
    Ballot SecondaryBallot,
    bool IsResponse);

/// <summary>
/// Clockwork-backed deterministic simulation cluster executing the real production
/// <see cref="ReplicatedRegisterService"/>, <see cref="VoterReconfigurationDriver"/>,
/// <see cref="ProposerEngine{TValue,TRoute}"/>, and <see cref="AcceptorEngine"/> over
/// Clockwork's virtual clock, scheduler, and fault-injecting network.
/// Continuously verifies per-key <see cref="FastCasAcceptorSafetyMonitor{TValue}"/> invariants,
/// client-visible linearizability (<see cref="RegisterLinearizabilityChecker"/>), and emits
/// spec §11.5 traces (<see cref="QuintTraceRecorder"/>) for replay against <c>FastCASPaxos.qnt</c>.
/// </summary>
public sealed class SimulatedReconfigurationCluster : IAsyncDisposable
{
    private readonly SimulationCluster _simulation;
    private readonly Dictionary<string, SimulatedVoter> _voters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _voterOrdinals = new(StringComparer.Ordinal);
    private readonly Dictionary<int, DeterministicBallotSource> _ballotSources = [];
    private readonly Dictionary<string, FastCasAcceptorSafetyMonitor<KeyedRegisterVersionedValue>> _monitors = new(StringComparer.Ordinal);
    private readonly SortedDictionary<long, ImmutableArray<string>> _configurations = [];
    private readonly HashSet<(string Key, long Configuration)> _completedKeyConfigs = [];
    private readonly HashSet<long> _activatedConfigurations = [1];
    private readonly Dictionary<(string Key, Ballot Ballot, ByteString Value), HashSet<string>> _acceptWitnesses = [];
    private readonly List<IAsyncDisposable> _ownedServices = [];
    private int _nextVoterOrdinal;
    private int _nextCallerOrdinal;
    private bool _running;
    private bool _disposed;

    public SimulatedReconfigurationCluster(
        int seed,
        TimeSpan? baseMessageDelay = null,
        TimeSpan? maxJitter = null)
    {
        _simulation = new SimulationCluster(seed, DateTimeOffset.UnixEpoch);
        _simulation.Network.BaseMessageDelay = baseMessageDelay ?? TimeSpan.FromMilliseconds(2);
        _simulation.Network.MaxJitter = maxJitter ?? TimeSpan.FromMilliseconds(3);
        TimeProvider = new InlineCompletionTimeProvider(_simulation.TimeProvider);
    }

    public int Seed => _simulation.Seed;
    public SimulationCluster Simulation => _simulation;
    public SimulationNetwork Network => _simulation.Network;
    public TimeProvider TimeProvider { get; }
    public long VirtualTick => _simulation.Scheduler.VirtualTime.Ticks;
    public bool EnableDelays { get; set; } = true;
    public RegisterLinearizabilityChecker Linearizability { get; } = new();
    public QuintTraceRecorder TraceRecorder { get; } = new();
    public IReadOnlyDictionary<string, FastCasAcceptorSafetyMonitor<KeyedRegisterVersionedValue>> SafetyMonitors => _monitors;

    public string AddVoter(string voterId, long initialConfiguration = 1)
    {
        ArgumentException.ThrowIfNullOrEmpty(voterId);
        ArgumentOutOfRangeException.ThrowIfLessThan(initialConfiguration, 1);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_voters.ContainsKey(voterId))
        {
            throw new InvalidOperationException($"Voter '{voterId}' already exists.");
        }

        var ordinal = ++_nextVoterOrdinal;
        var voter = new SimulatedVoter(this, voterId, ordinal, initialConfiguration);
        _voters.Add(voterId, voter);
        _voterOrdinals.Add(voterId, ordinal);
        TraceRecorder.EnsureVoterId(voterId);
        return voterId;
    }

    public ImmutableArray<VoterDescriptor> RegisterConfiguration(long configuration, params string[] voterIds) =>
        RegisterConfiguration(configuration, (IReadOnlyCollection<string>)voterIds);

    public ImmutableArray<VoterDescriptor> RegisterConfiguration(long configuration, IReadOnlyCollection<string> voterIds)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(configuration, 1);
        ArgumentNullException.ThrowIfNull(voterIds);
        ObjectDisposedException.ThrowIf(_disposed, this);

        var ordered = voterIds.OrderBy(static id => id, StringComparer.Ordinal).ToImmutableArray();
        foreach (var id in ordered)
        {
            if (!_voters.ContainsKey(id))
            {
                AddVoter(id, initialConfiguration: configuration);
            }
        }

        _configurations[configuration] = ordered;
        TraceRecorder.RegisterConfiguration(configuration, ordered);

        var addresses = ordered.Select(id => FastCasAddress.Acceptor(_voterOrdinals[id])).ToArray();
        foreach (var monitor in _monitors.Values)
        {
            monitor.RegisterConfiguration(configuration, addresses);
        }

        return [.. ordered.Select(static id => new VoterDescriptor(id, $"sim://{id}"))];
    }

    public string GetVoterNetworkAddress(string voterId) => GetVoter(voterId).NetworkAddress;

    public void CrashVoter(string voterId) => GetVoter(voterId).Crash();

    public void RestartVoter(string voterId) => GetVoter(voterId).Restart();

    /// <summary>
    /// Simulates permanent failure + disk loss of a voter (it never restarts under this identity).
    /// </summary>
    public void LoseVoter(string voterId) => GetVoter(voterId).LosePermanently();

    public bool IsVoterRunning(string voterId) => GetVoter(voterId).IsRunning;

    public Ballot GetVoterFloor(string voterId) => GetVoter(voterId).Floor;

    public KeyedAcceptorState? InspectVoterKey(string voterId, string key) => GetVoter(voterId).InspectKey(key);

    public void IsolateVoter(string voterId) => Network.IsolateNode(GetVoter(voterId).NetworkAddress);

    public void ReconnectVoter(string voterId) => Network.ReconnectNode(GetVoter(voterId).NetworkAddress);

    public void Partition(string addressA, string addressB) =>
        Network.CreateBidirectionalPartition(addressA, addressB);

    public void HealAllPartitions() => Network.HealAllPartitions();

    public void SetMessageDropRate(double dropRate) => Network.MessageDropRate = dropRate;

    public void SetNetworkDelay(TimeSpan baseDelay, TimeSpan maxJitter)
    {
        Network.BaseMessageDelay = baseDelay;
        Network.MaxJitter = maxJitter;
    }

    public Task DelayAsync(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        ObjectDisposedException.ThrowIf(_disposed, this);
        var tcs = new TaskCompletionSource();
        _simulation.SchedulerLane.EnqueueAfter(() =>
        {
            if (!_disposed) CompleteInline(() => tcs.TrySetResult());
        }, delay);
        return tcs.Task;
    }

    internal static void CompleteInline(Action complete)
    {
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            complete();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    private sealed class InlineCompletionTimeProvider(TimeProvider inner) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();
        public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;
        public override long GetTimestamp() => inner.GetTimestamp();
        public override long TimestampFrequency => inner.TimestampFrequency;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            inner.CreateTimer(
                static boxed =>
                {
                    var (cb, st) = ((TimerCallback, object?))boxed!;
                    CompleteInline(() => cb(st));
                },
                (callback, state),
                dueTime,
                period);
    }

    public IProposerBallotSource GetOrCreateBallotSource(int proposerId)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(proposerId);
        if (!_ballotSources.TryGetValue(proposerId, out var source))
        {
            source = new DeterministicBallotSource(proposerId);
            _ballotSources.Add(proposerId, source);
        }

        return source;
    }

    public TrackedRegisterClient CreateClient(
        int proposerId,
        long initialConfiguration,
        IReadOnlyCollection<string> initialVoters,
        ProposerModePolicy? mode = null,
        TimeSpan? operationTimeout = null,
        TimeSpan? attemptTimeout = null,
        TimeSpan? retryBackoff = null,
        Action<SimulatedRpcContext>? rpcObserver = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var callerId = $"client:{proposerId}:{++_nextCallerOrdinal}";
        var callerContext = new CallerContext(this, callerId, rpcObserver);
        IKeyedAcceptor Resolve(VoterDescriptor d) => callerContext.GetProxy(d.Id);

        var options = new RegisterServiceOptions
        {
            Mode = mode ?? ProposerModePolicy.Adaptive(),
            TimeProvider = TimeProvider,
            OperationTimeout = operationTimeout ?? TimeSpan.FromSeconds(5),
            AttemptTimeout = attemptTimeout ?? TimeSpan.FromMilliseconds(150),
            RetryBackoff = retryBackoff ?? TimeSpan.FromMilliseconds(10),
            InitialConfiguration = initialConfiguration,
            PeerResolver = Resolve,
        };

        ImmutableArray<AcceptorPeer> peers = [.. initialVoters
            .OrderBy(static id => id, StringComparer.Ordinal)
            .Select(id => new AcceptorPeer(id, callerContext.GetProxy(id)))];

        var service = new ReplicatedRegisterService(peers, GetOrCreateBallotSource(proposerId), options);
        _ownedServices.Add(service);
        return new TrackedRegisterClient(this, callerId, service, callerContext);
    }

    public Func<VoterDescriptor, IKeyedAcceptor> CreatePeerResolver(string callerId)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var callerContext = new CallerContext(this, callerId, rpcObserver: null);
        return descriptor => callerContext.GetProxy(descriptor.Id);
    }

    public SimulatedOperator CreateOperator(
        int proposerId,
        TimeSpan? operationTimeout = null,
        TimeSpan? attemptTimeout = null,
        TimeSpan? retryBackoff = null,
        Action<SimulatedRpcContext>? rpcObserver = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var callerId = $"operator:{proposerId}:{++_nextCallerOrdinal}";
        var callerContext = new CallerContext(this, callerId, rpcObserver);
        IKeyedAcceptor Resolve(VoterDescriptor d) => callerContext.GetProxy(d.Id);

        var options = new RegisterServiceOptions
        {
            Mode = ProposerModePolicy.StaticClassic,
            TimeProvider = TimeProvider,
            OperationTimeout = operationTimeout ?? TimeSpan.FromSeconds(10),
            AttemptTimeout = attemptTimeout ?? TimeSpan.FromMilliseconds(150),
            RetryBackoff = retryBackoff ?? TimeSpan.FromMilliseconds(10),
            PeerResolver = Resolve,
        };

        var driver = new VoterReconfigurationDriver(GetOrCreateBallotSource(proposerId), Resolve, options);
        return new SimulatedOperator(this, callerId, driver, callerContext);
    }

    /// <summary>
    /// Runs an asynchronous scenario deterministically on Clockwork's virtual clock and scheduler,
    /// then asserts all per-key safety monitors and client-visible register linearizability.
    /// </summary>
    public void Run(Func<SimulatedReconfigurationCluster, Task> scenario, int maxIterations = 500_000)
    {
        ArgumentNullException.ThrowIfNull(scenario);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_running) throw new InvalidOperationException("Nested simulation Run is not supported.");

        using var control = _simulation.Scheduler.EnterControlScope();
        using var guard = _simulation.Guard.Enter();
        using var context = _simulation.SynchronizationContext.Install();
        _running = true;
        try
        {
            var task = scenario(this);
            var result = _simulation.RunUntil(() => task.IsCompleted, CancellationToken.None, maxIterations);
            if (result.Reason != SimulationExecutionReason.ConditionMet)
            {
                throw new InvalidOperationException(
                    $"Reconfiguration simulation (seed {Seed}) stopped with reason {result.Reason} at tick {VirtualTick} before scenario completed.");
            }

            task.GetAwaiter().GetResult();
            AssertAllSafetyInvariants();
        }
        finally
        {
            _running = false;
        }
    }

    public void AssertAllSafetyInvariants()
    {
        Linearizability.AssertLinearizable();
        // Per-key FastCasAcceptorSafetyMonitor invariants are checked continuously on every state mutation.
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        if (_running) throw new InvalidOperationException("Dispose the cluster outside Run.");
        _disposed = true;
        foreach (var voter in _voters.Values)
        {
            voter.Crash();
        }

        foreach (var service in _ownedServices)
        {
            await service.DisposeAsync().ConfigureAwait(false);
        }

        await _simulation.DisposeAsync().ConfigureAwait(false);
    }

    private SimulatedVoter GetVoter(string voterId) =>
        _voters.TryGetValue(voterId, out var voter)
            ? voter
            : throw new InvalidOperationException($"Unknown simulated voter '{voterId}'.");

    private FastCasAcceptorSafetyMonitor<KeyedRegisterVersionedValue> GetOrCreateMonitor(string key)
    {
        if (!_monitors.TryGetValue(key, out var monitor))
        {
            monitor = new FastCasAcceptorSafetyMonitor<KeyedRegisterVersionedValue>();
            foreach (var (cfg, voters) in _configurations)
            {
                monitor.RegisterConfiguration(
                    cfg,
                    voters.Select(id => FastCasAddress.Acceptor(_voterOrdinals[id])).ToArray());
            }

            _monitors.Add(key, monitor);
        }

        return monitor;
    }

    private void ObserveKeyMutation(string updatedVoterId, string key)
    {
        var monitor = GetOrCreateMonitor(key);
        var snapshots = new FastCasAcceptorStateSnapshot<KeyedRegisterVersionedValue>[_voters.Count];
        var index = 0;
        foreach (var voter in _voters.Values)
        {
            snapshots[index++] = voter.CreateSnapshot(key);
        }

        monitor.Observe(
            TimeProvider.GetUtcNow(),
            FastCasAddress.Acceptor(_voterOrdinals[updatedVoterId]),
            snapshots);
    }

    private void ObserveFloorMutation(string updatedVoterId)
    {
        // Floor applies uniformly across every key tracked by safety monitors.
        foreach (var key in _monitors.Keys)
        {
            ObserveKeyMutation(updatedVoterId, key);
        }
    }

    internal void EnsureVacuousCompletionsBeforeConfigUse(string key, long config)
    {
        // If configuration c > 1 is active and key was never transferred into c,
        // it completed vacuously via the fenced classic quorum of V(c - 1).
        for (var c = 2L; c <= config; c++)
        {
            if (!_activatedConfigurations.Contains(c)) break;
            if (_completedKeyConfigs.Contains((key, c))) continue;
            _completedKeyConfigs.Add((key, c));
            TraceRecorder.RecordKeyVacuouslyComplete(VirtualTick, key, c);
        }
    }

    internal void OnAcceptRecorded(string voterId, string key, Ballot ballot, ByteString value, Ballot promised)
    {
        TraceRecorder.RecordAcceptAcknowledged(VirtualTick, key, voterId, ballot, value, promised);

        if (!_configurations.TryGetValue(ballot.Configuration, out var voters)
            || !voters.Contains(voterId))
        {
            return;
        }

        var witnessKey = (key, ballot, value);
        if (!_acceptWitnesses.TryGetValue(witnessKey, out var witnesses))
        {
            witnesses = new HashSet<string>(StringComparer.Ordinal);
            _acceptWitnesses[witnessKey] = witnesses;
        }

        witnesses.Add(voterId);
        var quorum = ballot.IsFastRoundBallot
            ? (3 * voters.Length + 3) / 4
            : voters.Length / 2 + 1;

        if (witnesses.Count >= quorum)
        {
            if (ballot.Configuration > 1 && _completedKeyConfigs.Add((key, ballot.Configuration)))
            {
                TraceRecorder.RecordKeyTransferred(VirtualTick, key, ballot.Configuration);
            }

            if (string.Equals(key, VoterConfiguration.RegisterKey, StringComparison.Ordinal))
            {
                var reg = RegisterEncoding.Decode(value);
                if (reg.IsPresent)
                {
                    var cfg = VoterConfigurationEncoding.Decode(reg.Value!.Value);
                    if (!_configurations.ContainsKey(cfg.Number))
                    {
                        RegisterConfiguration(cfg.Number, [.. cfg.Voters.Select(static v => v.Id)]);
                    }

                    if (cfg.State == VoterConfigurationState.Active)
                    {
                        _activatedConfigurations.Add(cfg.Number);
                    }
                }
            }
        }
    }

    private sealed class SimulatedVoter
    {
        private readonly SimulatedReconfigurationCluster _cluster;
        private readonly Dictionary<string, AcceptorState<ByteString?>> _durableStates = new(StringComparer.Ordinal);
        private Ballot _floor;
        private bool _permanentlyLost;

        public SimulatedVoter(
            SimulatedReconfigurationCluster cluster,
            string voterId,
            int ordinal,
            long initialConfiguration)
        {
            _cluster = cluster;
            VoterId = voterId;
            Ordinal = ordinal;
            NetworkAddress = $"voter:{voterId}";
            _floor = Ballot.Floor(initialConfiguration);
        }

        public string VoterId { get; }
        public int Ordinal { get; }
        public string NetworkAddress { get; }
        public bool IsRunning { get; private set; } = true;
        public Ballot Floor => _floor;

        public void Crash() => IsRunning = false;

        public void Restart()
        {
            if (_permanentlyLost)
            {
                throw new InvalidOperationException($"Voter '{VoterId}' was permanently lost and cannot restart under the same identity.");
            }

            IsRunning = true;
        }

        public void LosePermanently()
        {
            IsRunning = false;
            _permanentlyLost = true;
            _durableStates.Clear();
        }

        public Ballot SetFloor(Ballot targetFloor)
        {
            if (!targetFloor.IsFloorBallot)
            {
                throw new ArgumentOutOfRangeException(nameof(targetFloor));
            }

            var advanced = targetFloor > _floor;
            if (advanced)
            {
                _floor = targetFloor;
                _cluster.TraceRecorder.RecordSetFloor(_cluster.VirtualTick, VoterId, _floor.Configuration);
                _cluster.ObserveFloorMutation(VoterId);
            }

            return _floor;
        }

        public AcceptorReadResult Read(string key, AcceptedDigest? known)
        {
            if (!_durableStates.TryGetValue(key, out var raw) || raw.AcceptedValue is null)
            {
                var promised = _durableStates.TryGetValue(key, out var promisedState)
                    ? Ballot.Max(promisedState.PromisedBallot, _floor)
                    : _floor;
                return new AcceptorMissing(promised);
            }

            var state = new KeyedAcceptorState(
                key,
                Ballot.Max(raw.PromisedBallot, _floor),
                raw.AcceptedBallot,
                raw.AcceptedValue.Value);

            return known == new AcceptedDigest(state.AcceptedBallot, AcceptedDigest.Hash(state.AcceptedValue))
                ? new AcceptorUnchanged()
                : state;
        }

        public AcceptorPrepareResult<ByteString?> Prepare(string key, PrepareRequest request)
        {
            var state = LoadState(key);
            var result = AcceptorEngine.Prepare(ref state, request);
            _durableStates[key] = state;
            if (result.IsPromised)
            {
                _cluster.EnsureVacuousCompletionsBeforeConfigUse(key, request.Ballot.Configuration);
                _cluster.TraceRecorder.RecordPreparePromised(
                    _cluster.VirtualTick,
                    key,
                    VoterId,
                    request.Ballot,
                    result.AcceptedBallot,
                    result.AcceptedValue);
            }

            _cluster.ObserveKeyMutation(VoterId, key);
            return result;
        }

        public AcceptorAcceptResult Accept(string key, AcceptRequest<ByteString> request)
        {
            var state = LoadState(key);
            var result = AcceptorEngine.Accept(
                ref state,
                new AcceptRequest<ByteString?>(request.Ballot, request.Value, request.NextBallotToPrepare));
            _durableStates[key] = state;
            if (result.IsAccepted)
            {
                _cluster.OnAcceptRecorded(VoterId, key, request.Ballot, request.Value, result.PromisedBallot);
            }

            _cluster.ObserveKeyMutation(VoterId, key);
            return result;
        }

        public KeyPage ListKeys(string prefix, string? afterKey, int limit)
        {
            var entries = _durableStates
                .Where(pair => pair.Value.AcceptedValue is not null
                    && pair.Key.StartsWith(prefix, StringComparison.Ordinal)
                    && (afterKey is null || string.CompareOrdinal(pair.Key, afterKey) > 0))
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Take(limit == int.MaxValue ? limit : limit + 1)
                .Select(pair => new KeyedAcceptorState(
                    pair.Key,
                    Ballot.Max(pair.Value.PromisedBallot, _floor),
                    pair.Value.AcceptedBallot,
                    pair.Value.AcceptedValue!.Value))
                .ToArray();

            return new KeyPage(
                [.. entries.Take(limit)],
                entries.Length > limit ? entries[limit - 1].Key : null,
                _floor);
        }

        public KeyedAcceptorState? InspectKey(string key)
        {
            if (!_durableStates.TryGetValue(key, out var raw) || raw.AcceptedValue is null)
            {
                return null;
            }

            return new KeyedAcceptorState(
                key,
                Ballot.Max(raw.PromisedBallot, _floor),
                raw.AcceptedBallot,
                raw.AcceptedValue.Value);
        }

        public FastCasAcceptorStateSnapshot<KeyedRegisterVersionedValue> CreateSnapshot(string key)
        {
            if (_durableStates.TryGetValue(key, out var state))
            {
                var value = state.AcceptedValue is { } bytes
                    ? KeyedRegisterVersionedValue.FromEnvelope(bytes)
                    : KeyedRegisterVersionedValue.Absent;
                return new FastCasAcceptorStateSnapshot<KeyedRegisterVersionedValue>(
                    Acceptor: FastCasAddress.Acceptor(Ordinal),
                    IsRunning: IsRunning,
                    IsSuspended: false,
                    PromisedBallot: Ballot.Max(state.PromisedBallot, _floor),
                    AcceptedBallot: state.AcceptedBallot,
                    AcceptedValue: value,
                    FloorBallot: _floor);
            }

            return new FastCasAcceptorStateSnapshot<KeyedRegisterVersionedValue>(
                Acceptor: FastCasAddress.Acceptor(Ordinal),
                IsRunning: IsRunning,
                IsSuspended: false,
                PromisedBallot: _floor,
                AcceptedBallot: Ballot.Zero,
                AcceptedValue: KeyedRegisterVersionedValue.Absent,
                FloorBallot: _floor);
        }

        private AcceptorState<ByteString?> LoadState(string key)
        {
            if (_durableStates.TryGetValue(key, out var state))
            {
                state.FloorBallot = _floor;
                return state;
            }

            return new AcceptorState<ByteString?>
            {
                FloorBallot = _floor,
                PromisedBallot = _floor.Configuration == 1 ? Ballot.InitialFast() : _floor,
                AcceptedBallot = Ballot.Zero,
                AcceptedValue = null,
            };
        }
    }

    internal sealed class CallerContext(
        SimulatedReconfigurationCluster cluster,
        string callerId,
        Action<SimulatedRpcContext>? rpcObserver)
    {
        private readonly Dictionary<string, SimulatedNetworkAcceptor> _proxies = new(StringComparer.Ordinal);
        private readonly Dictionary<(string Key, Ballot Ballot), HashSet<string>> _receivedPromises = [];
        private readonly HashSet<(string Key, Ballot Ballot, ByteString Value, Ballot Next)> _dispatchedAccepts = [];

        public string CallerId { get; } = callerId;
        public int PrepareCount { get; private set; }
        public int AcceptCount { get; private set; }

        public void ResetCounters()
        {
            PrepareCount = 0;
            AcceptCount = 0;
        }

        public IKeyedAcceptor GetProxy(string voterId)
        {
            if (!_proxies.TryGetValue(voterId, out var proxy))
            {
                proxy = new SimulatedNetworkAcceptor(cluster, this, cluster.GetVoter(voterId));
                _proxies.Add(voterId, proxy);
            }

            return proxy;
        }

        public void NotifyRpc(SimulatedRpcContext context) => rpcObserver?.Invoke(context);

        public void RecordPrepareSent() => PrepareCount++;

        public void RecordPromiseReceived(string key, Ballot ballot, string voterId)
        {
            var mapKey = (key, ballot);
            if (!_receivedPromises.TryGetValue(mapKey, out var set))
            {
                set = new HashSet<string>(StringComparer.Ordinal);
                _receivedPromises[mapKey] = set;
            }

            set.Add(voterId);
        }

        public void RecordAcceptDispatched(string key, AcceptRequest<ByteString> request)
        {
            AcceptCount++;
            var dedupKey = (key, request.Ballot, request.Value, request.NextBallotToPrepare);
            if (!_dispatchedAccepts.Add(dedupKey))
            {
                return;
            }

            cluster.EnsureVacuousCompletionsBeforeConfigUse(key, request.Ballot.Configuration);
            var isRecovered = request.Ballot.IsClassicRoundBallot
                && _receivedPromises.TryGetValue((key, request.Ballot), out var promised)
                && promised.Count > 0;
            ImmutableArray<string> quorum = isRecovered
                ? [.. _receivedPromises[(key, request.Ballot)].OrderBy(static id => id, StringComparer.Ordinal)]
                : [];
            cluster.TraceRecorder.RecordAcceptDispatched(
                cluster.VirtualTick,
                key,
                request.Ballot,
                request.Value,
                request.NextBallotToPrepare,
                isRecovered,
                quorum);
        }
    }

    private sealed class SimulatedNetworkAcceptor(
        SimulatedReconfigurationCluster cluster,
        CallerContext caller,
        SimulatedVoter voter) : IKeyedAcceptor
    {
        public ValueTask<AcceptorReadResult> ReadAsync(
            string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default) =>
            SendRpcAsync(
                SimulatedRpcKind.Read,
                key,
                Ballot.Zero,
                Ballot.Zero,
                onBeforeSend: null,
                execute: () => voter.Read(key, known),
                onResponseDelivered: null,
                cancellationToken);

        public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
            string key, PrepareRequest request, CancellationToken cancellationToken = default) =>
            SendRpcAsync(
                SimulatedRpcKind.Prepare,
                key,
                request.Ballot,
                Ballot.Zero,
                onBeforeSend: () => caller.RecordPrepareSent(),
                execute: () => voter.Prepare(key, request),
                onResponseDelivered: result =>
                {
                    if (result.IsPromised)
                    {
                        caller.RecordPromiseReceived(key, request.Ballot, voter.VoterId);
                    }
                },
                cancellationToken);

        public ValueTask<AcceptorAcceptResult> AcceptAsync(
            string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default) =>
            SendRpcAsync(
                SimulatedRpcKind.Accept,
                key,
                request.Ballot,
                request.NextBallotToPrepare,
                onBeforeSend: () => caller.RecordAcceptDispatched(key, request),
                execute: () => voter.Accept(key, request),
                onResponseDelivered: null,
                cancellationToken);

        public ValueTask<Ballot> SetFloorAsync(Ballot targetFloor, CancellationToken cancellationToken = default) =>
            SendRpcAsync(
                SimulatedRpcKind.SetFloor,
                key: null,
                targetFloor,
                Ballot.Zero,
                onBeforeSend: null,
                execute: () => voter.SetFloor(targetFloor),
                onResponseDelivered: null,
                cancellationToken);

        public ValueTask<KeyPage> ListKeysAsync(
            string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            SendRpcAsync(
                SimulatedRpcKind.ListKeys,
                prefix,
                Ballot.Zero,
                Ballot.Zero,
                onBeforeSend: null,
                execute: () => voter.ListKeys(prefix, afterKey, limit),
                onResponseDelivered: null,
                cancellationToken);

        private ValueTask<TResult> SendRpcAsync<TResult>(
            SimulatedRpcKind kind,
            string? key,
            Ballot ballot,
            Ballot secondaryBallot,
            Action? onBeforeSend,
            Func<TResult> execute,
            Action<TResult>? onResponseDelivered,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (cluster._disposed)
            {
                return ValueTask.FromException<TResult>(new ObjectDisposedException(nameof(SimulatedReconfigurationCluster)));
            }

            onBeforeSend?.Invoke();
            caller.NotifyRpc(new SimulatedRpcContext(
                caller.CallerId, voter.VoterId, kind, key, ballot, secondaryBallot, IsResponse: false));
            cancellationToken.ThrowIfCancellationRequested();

            var tcs = new TaskCompletionSource<TResult>();
            var registration = cancellationToken.Register(() => CompleteInline(() => tcs.TrySetCanceled(cancellationToken)));

            var requestDelivery = cluster.Network.CheckDelivery(caller.CallerId, voter.NetworkAddress);
            if (requestDelivery == DeliveryStatus.Success)
            {
                var requestDelay = cluster.EnableDelays
                    ? cluster.Network.GetMessageDelay(caller.CallerId, voter.NetworkAddress)
                    : TimeSpan.Zero;

                cluster._simulation.SchedulerLane.EnqueueAfter(() =>
                {
                    if (cluster._disposed || !voter.IsRunning)
                    {
                        return;
                    }

                    TResult result;
                    try
                    {
                        result = execute();
                    }
                    catch (Exception ex)
                    {
                        registration.Dispose();
                        CompleteInline(() => tcs.TrySetException(ex));
                        return;
                    }

                    var responseDelivery = cluster.Network.CheckDelivery(voter.NetworkAddress, caller.CallerId);
                    if (responseDelivery != DeliveryStatus.Success)
                    {
                        return;
                    }

                    var responseDelay = cluster.EnableDelays
                        ? cluster.Network.GetMessageDelay(voter.NetworkAddress, caller.CallerId)
                        : TimeSpan.Zero;

                    cluster._simulation.SchedulerLane.EnqueueAfter(() =>
                    {
                        registration.Dispose();
                        if (cluster._disposed || tcs.Task.IsCompleted)
                        {
                            return;
                        }

                        onResponseDelivered?.Invoke(result);
                        caller.NotifyRpc(new SimulatedRpcContext(
                            caller.CallerId, voter.VoterId, kind, key, ballot, secondaryBallot, IsResponse: true));
                        if (cancellationToken.IsCancellationRequested)
                        {
                            CompleteInline(() => tcs.TrySetCanceled(cancellationToken));
                            return;
                        }

                        CompleteInline(() => tcs.TrySetResult(result));
                    }, responseDelay);
                }, requestDelay);
            }

            return new ValueTask<TResult>(AwaitAndDisposeAsync(tcs.Task, registration));
        }

        private static async Task<TResult> AwaitAndDisposeAsync<TResult>(
            Task<TResult> task,
            CancellationTokenRegistration registration)
        {
            try
            {
                return await task.ConfigureAwait(false);
            }
            finally
            {
                registration.Dispose();
            }
        }
    }

    private sealed class DeterministicBallotSource(int proposerId) : IProposerBallotSource
    {
        private long _configuration = 1;
        private int _round;

        public int ProposerId => proposerId;

        public Ballot Reserve(long configuration, int minimumRound)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(configuration);
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRound);
            if (configuration < _configuration)
            {
                throw new InvalidOperationException(
                    $"Cannot reserve stale configuration {configuration}; allocator is already at {_configuration}.");
            }

            if (configuration > _configuration)
            {
                _configuration = configuration;
                _round = minimumRound;
            }
            else
            {
                _round = Math.Max(checked(_round + 1), minimumRound);
            }

            return new Ballot(_configuration, _round, proposerId);
        }

        public Ballot Reserve(int minimumRound) => Reserve(1, minimumRound);
    }
}

/// <summary>
/// Wraps a real <see cref="ReplicatedRegisterService"/> connected to <see cref="SimulatedReconfigurationCluster"/>,
/// recording every client call and return into <see cref="RegisterLinearizabilityChecker"/>.
/// </summary>
public sealed class TrackedRegisterClient : IRegisterService
{
    private readonly SimulatedReconfigurationCluster _cluster;
    private readonly ReplicatedRegisterService _service;
    private readonly SimulatedReconfigurationCluster.CallerContext _caller;

    internal TrackedRegisterClient(
        SimulatedReconfigurationCluster cluster,
        string networkAddress,
        ReplicatedRegisterService service,
        SimulatedReconfigurationCluster.CallerContext caller)
    {
        _cluster = cluster;
        NetworkAddress = networkAddress;
        _service = service;
        _caller = caller;
    }

    public string NetworkAddress { get; }
    public long ActiveConfiguration => _service.ActiveConfiguration;
    public ImmutableArray<AcceptorPeer> ActivePeers => _service.ActivePeers;
    public int PrepareCount => _caller.PrepareCount;
    public int AcceptCount => _caller.AcceptCount;
    public void ResetRpcCounters() => _caller.ResetCounters();

    public async ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        var opId = _cluster.Linearizability.BeginOperation(key, RecordedRegisterOpKind.Read, _cluster.VirtualTick);
        try
        {
            var value = await _service.ReadAsync(key, cancellationToken).ConfigureAwait(false);
            _cluster.Linearizability.CompleteRead(opId, _cluster.VirtualTick, value);
            return value;
        }
        catch (Exception ex) when (ex is RegisterUnavailableException or OperationCanceledException)
        {
            _cluster.Linearizability.CompleteUnavailable(opId, _cluster.VirtualTick);
            throw;
        }
    }

    public async ValueTask<CompareExchangeResult> CompareExchangeAsync(
        string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default)
    {
        var opId = _cluster.Linearizability.BeginOperation(
            key, RecordedRegisterOpKind.CompareExchange, _cluster.VirtualTick, expectedVersion, value);
        try
        {
            var result = await _service.CompareExchangeAsync(key, expectedVersion, value, cancellationToken).ConfigureAwait(false);
            _cluster.Linearizability.CompleteCompareExchange(opId, _cluster.VirtualTick, result);
            return result;
        }
        catch (Exception ex) when (ex is RegisterUnavailableException or OperationCanceledException)
        {
            _cluster.Linearizability.CompleteUnavailable(opId, _cluster.VirtualTick);
            throw;
        }
    }

    public ValueTask<RegisterKeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
        _service.ListKeysAsync(prefix, afterKey, limit, cancellationToken);
}

/// <summary>
/// Wraps a real <see cref="VoterReconfigurationDriver"/> connected to <see cref="SimulatedReconfigurationCluster"/>.
/// </summary>
public sealed class SimulatedOperator
{
    private readonly SimulatedReconfigurationCluster _cluster;
    private readonly VoterReconfigurationDriver _driver;
    private readonly SimulatedReconfigurationCluster.CallerContext _caller;

    internal SimulatedOperator(
        SimulatedReconfigurationCluster cluster,
        string networkAddress,
        VoterReconfigurationDriver driver,
        SimulatedReconfigurationCluster.CallerContext caller)
    {
        _cluster = cluster;
        NetworkAddress = networkAddress;
        _driver = driver;
        _caller = caller;
    }

    public string NetworkAddress { get; }
    public VoterReconfigurationDriver Driver => _driver;
    public int PrepareCount => _caller.PrepareCount;
    public int AcceptCount => _caller.AcceptCount;

    public Task<VoterConfiguration> ReconfigureAsync(
        ImmutableArray<VoterDescriptor> currentVoters,
        long currentConfiguration,
        ImmutableArray<VoterDescriptor> nextVoters,
        CancellationToken cancellationToken = default) =>
        _driver.ReconfigureAsync(currentVoters, currentConfiguration, nextVoters, cancellationToken);

    public Task<VoterConfiguration> ResumeAsync(
        ImmutableArray<VoterDescriptor> seedVoters,
        long seedConfiguration,
        CancellationToken cancellationToken = default) =>
        _driver.ResumeAsync(seedVoters, seedConfiguration, cancellationToken);
}

/// <summary>
/// Versioned value adapter allowing <see cref="FastCasAcceptorSafetyMonitor{TValue}"/> to monitor
/// raw FCR2 register envelopes per key across configuration changes.
/// </summary>
public readonly record struct KeyedRegisterVersionedValue(
    ulong Revision,
    ByteString? Payload) : IVersionedValue<KeyedRegisterVersionedValue>
{
    public static KeyedRegisterVersionedValue Absent => new(0, null);

    public int Version => checked((int)Revision);

    public bool IsValidSuccessorTo(KeyedRegisterVersionedValue predecessor)
    {
        if (predecessor.Revision == 0)
        {
            return true;
        }

        if (Equals(predecessor))
        {
            return true;
        }

        return Revision > predecessor.Revision;
    }

    public static KeyedRegisterVersionedValue FromEnvelope(ByteString envelope)
    {
        var decoded = RegisterEncoding.Decode(envelope);
        if (!decoded.IsPresent)
        {
            return Absent;
        }

        var rev = BinaryPrimitives.ReadUInt64BigEndian(decoded.Version!.Value.Span);
        return new KeyedRegisterVersionedValue(rev, decoded.Value);
    }

    public override string ToString() =>
        Revision == 0
            ? "Reg(absent)"
            : $"Reg(rev={Revision}, len={Payload?.Length ?? 0})";
}
