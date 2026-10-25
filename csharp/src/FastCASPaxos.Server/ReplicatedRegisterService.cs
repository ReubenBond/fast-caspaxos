using System.Buffers.Binary;
using System.Collections.Immutable;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Server;

/// <summary>
/// A bounded, transport-independent client and server implementation over fixed acceptor membership.
/// The caller owns the peer endpoints and durable ballot source.
/// </summary>
public sealed class ReplicatedRegisterService : IRegisterService, IRegisterLearner, IAsyncDisposable
{
    private readonly ImmutableArray<AcceptorPeer> _peers;
    private readonly ImmutableArray<string> _peerIds;
    private readonly SemaphoreSlim[] _peerSlots;
    private readonly IProposerBallotSource _ballots;
    private readonly RegisterServiceOptions _options;
    private readonly ILocalAcceptorInspection? _localAcceptor;
    private readonly object _gate = new();
    private readonly Dictionary<string, KeyState> _keys = new(StringComparer.Ordinal);
    private readonly LinkedList<string> _idleKeys = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _disposed;
    private int _active;
    private Ballot _requestIncarnation;
    private long _requestSequence;

    /// <summary>Publishes quorum conclusions. Subscribers enqueue delivery and return synchronously.</summary>
    public event Action<RegisterCommit>? Committed;

    public ReplicatedRegisterService(
        ImmutableArray<AcceptorPeer> peers, IProposerBallotSource ballots, RegisterServiceOptions? options = null,
        ILocalAcceptorInspection? localAcceptor = null)
    {
        if (peers.IsDefault) throw new ArgumentNullException(nameof(peers));
        ArgumentNullException.ThrowIfNull(ballots);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ballots.ProposerId);
        _peers = peers;
        if (_peers.Length == 0) throw new ArgumentException("Membership must contain acceptors.", nameof(peers));
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (var peer in _peers)
        {
            ArgumentNullException.ThrowIfNull(peer);
            ArgumentException.ThrowIfNullOrEmpty(peer.Id);
            ArgumentNullException.ThrowIfNull(peer.Acceptor);
            if (!ids.Add(peer.Id)) throw new ArgumentException("Acceptor identities must be unique.", nameof(peers));
        }

        _peerIds = [.. _peers.Select(peer => peer.Id)];

        _ballots = ballots;
        _options = options ?? new RegisterServiceOptions();
        _localAcceptor = localAcceptor;
        ArgumentNullException.ThrowIfNull(_options.Mode);
        ArgumentNullException.ThrowIfNull(_options.TimeProvider);
        _options.Mode.Validate();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.Capacity);
        ValidateTimeout(_options.OperationTimeout, nameof(options));
        ValidateTimeout(_options.AttemptTimeout, nameof(options));
        if (_options.RetryBackoff <= TimeSpan.Zero || _options.RetryBackoff.TotalMilliseconds > uint.MaxValue / 8)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "RetryBackoff must be positive and support timer delays.");
        }
        _peerSlots = _peers.Select(_ => new SemaphoreSlim(_options.Capacity)).ToArray();
    }

    public void Learn(RegisterCommit commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        ArgumentNullException.ThrowIfNull(commit.Key);
        if (commit.AcceptedBallot.Round <= 0 || commit.AcceptedBallot.Proposer < 0
            || (!commit.PreparedBallot.IsZero && (commit.PreparedBallot <= commit.AcceptedBallot
                || commit.PreparedBallot.Proposer < 0)))
            throw new ArgumentException("A commit requires an accepted ballot and an optional higher prepared ballot.", nameof(commit));
        RegisterEncoding.Decode(commit.Value);

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_keys.TryGetValue(commit.Key, out var state))
            {
                state = new KeyState(NewEngine());
                _keys.Add(commit.Key, state);
            }
            if (state.Learned is { } pending)
            {
                if (pending.AcceptedBallot > commit.AcceptedBallot) return;
                if (pending.AcceptedBallot == commit.AcceptedBallot)
                {
                    if (pending.Value != commit.Value)
                        throw new InvalidDataException("A committed ballot must identify one value.");
                    if (pending.PreparedBallot >= commit.PreparedBallot) return;
                }
            }
            state.Learned = commit;
            if (state.Users == 0)
            {
                if (state.Idle is { } idle) _idleKeys.Remove(idle);
                state.Idle = _idleKeys.AddLast(commit.Key);
                TrimIdleKeys();
            }
        }
    }

    public async ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        using var call = Enter(cancellationToken);
        return await ExecuteAsync(key, new RegisterOperation(), call).ConfigureAwait(false);
    }

    public async ValueTask<CompareExchangeResult> CompareExchangeAsync(
        string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (value.IsDefault) throw new ArgumentException("An initialized payload is required.", nameof(value));
        if (expectedVersion is { } expected) RegisterEncoding.ValidateVersion(expected);
        using var call = Enter(cancellationToken);
        var operation = new RegisterOperation(expectedVersion, value, NewRequestIdentity());
        try
        {
            var current = await ExecuteAsync(key, operation, call).ConfigureAwait(false);
            return new CompareExchangeResult(operation.Status, current);
        }
        catch (Exception exception) when (operation.MayHaveApplied)
        {
            // Preserve the failure as well as the ambiguity: a durable accept can outlive its reply.
            return new CompareExchangeResult(CompareExchangeStatus.Unknown, null, exception);
        }
    }

    public async ValueTask<RegisterKeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        using var call = Enter(cancellationToken);
        var keys = ImmutableArray.CreateBuilder<string>();
        var cursor = afterKey;
        while (true)
        {
            var pages = await ReadInventoryQuorumAsync(prefix, cursor, Math.Min(limit, 128), call).ConfigureAwait(false);
            string? frontier = null;
            foreach (var page in pages)
            {
                if (page.ContinuationKey is { } next && (frontier is null || StringComparer.Ordinal.Compare(next, frontier) < 0))
                {
                    frontier = next;
                }
            }

            // Every selected replica covered this entire prefix range. A quorum intersects every
            // committed write quorum, so its union discovers all previously committed keys here.
            var candidates = new SortedSet<string>(StringComparer.Ordinal);
            foreach (var page in pages)
            {
                foreach (var entry in page.Entries)
                {
                    if (frontier is null || StringComparer.Ordinal.Compare(entry.Key, frontier) <= 0)
                    {
                        candidates.Add(entry.Key);
                    }
                }
            }

            foreach (var key in candidates)
            {
                var current = await ExecuteAsync(key, new RegisterOperation(), call).ConfigureAwait(false);
                if (!current.IsPresent) continue;
                if (keys.Count == limit)
                {
                    var last = keys[^1];
                    return new RegisterKeyPage(keys.DrainToImmutable(), last);
                }
                keys.Add(key);
            }

            if (frontier is null) return new RegisterKeyPage(keys.DrainToImmutable(), null);
            cursor = frontier;
        }
    }

    private async ValueTask<RegisterValue> ExecuteAsync(string key, RegisterOperation operation, Call call)
    {
        KeyState state;
        lock (_gate)
        {
            if (!_keys.TryGetValue(key, out state!))
            {
                state = new KeyState(NewEngine());
                _keys.Add(key, state);
            }
            else if (state.Idle is { } idle)
            {
                _idleKeys.Remove(idle);
                state.Idle = null;
            }
            state.Users++;
        }

        var acquired = false;
        try
        {
            await state.Gate.WaitAsync(call.Token).ConfigureAwait(false);
            acquired = true;
            call.Token.ThrowIfCancellationRequested();
            RegisterCommit? learned;
            lock (_gate)
            {
                learned = state.Learned;
                state.Learned = null;
            }
            if (learned is not null)
                state.Engine.LearnCommittedValue(learned.AcceptedBallot, learned.Value, learned.PreparedBallot);
            if (operation.Payload is null
                && await TryReadQuorumAsync(key, state.Engine, call).ConfigureAwait(false) is { } observed)
            {
                return observed;
            }
            var engine = state.Engine;
            var local = _localAcceptor is not null
                ? await _localAcceptor.InspectAsync(key, call.Token).ConfigureAwait(false) : null;
            engine.Start(operation, local);
            var retries = 0;
            while (engine.Result is null)
            {
                call.Token.ThrowIfCancellationRequested();
                var request = engine.Outgoing ?? throw new InvalidOperationException("The proposer has no pending request.");
                await DrivePhaseAsync(key, engine, request, operation, call).ConfigureAwait(false);
                if (engine.Result is null && engine.Outgoing?.Prepare is not null)
                {
                    await BackoffAsync(retries++, call.Token).ConfigureAwait(false);
                }
            }
            var current = RegisterEncoding.Decode(engine.Result.Value);
            operation.Complete(engine.Result.Value, current.Version);
            Committed?.Invoke(new(key, engine.Ballot, engine.Result.Value, engine.PreparedBallot ?? Ballot.Zero));
            return current;
        }
        catch (OperationCanceledException) when (call.Token.IsCancellationRequested)
        {
            if (acquired) state.Engine = NewEngine();
            call.ThrowCancellation();
            throw;
        }
        catch
        {
            if (acquired) state.Engine = NewEngine();
            throw;
        }
        finally
        {
            if (acquired) state.Gate.Release();
            lock (_gate)
            {
                if (--state.Users == 0)
                {
                    if (_disposed)
                    {
                        _keys.Remove(key);
                        state.Dispose();
                    }
                    else
                    {
                        state.Idle = _idleKeys.AddLast(key);
                        TrimIdleKeys();
                    }
                }
            }
        }
    }

    private void TrimIdleKeys()
    {
        if (_idleKeys.Count <= _options.Capacity) return;
        var oldest = _idleKeys.First!;
        _idleKeys.RemoveFirst();
        _keys.Remove(oldest.Value, out var evicted);
        evicted!.Dispose();
    }

    private async Task<RegisterValue?> TryReadQuorumAsync(string key, Engine engine, Call call)
    {
        using var phase = CancellationTokenSource.CreateLinkedTokenSource(call.Token);
        var timeout = Task.Delay(_options.AttemptTimeout, _options.TimeProvider, phase.Token);
        var pending = Enumerable.Range(0, _peers.Length).Select(index =>
            InvokePeerAsync(index, (peer, token) => peer.ReadAsync(key, cancellationToken: token), phase.Token)).ToList();
        var matching = new Dictionary<(Ballot Ballot, ByteString Value), int>();
        var promises = new Dictionary<(Ballot Ballot, ByteString Value, Ballot Promised), int>();
        var missing = 0;
        try
        {
            while (pending.Count > 0)
            {
                var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                call.Token.ThrowIfCancellationRequested();
                if (finished == timeout) break;
                var task = (Task<PeerResult<AcceptorReadResult>>)finished;
                pending.Remove(task);
                var reply = await task.ConfigureAwait(false);
                if (reply.Error is { } error)
                {
                    call.Errors[reply.Peer] = error;
                    continue;
                }

                switch (reply.Value)
                {
                    case AcceptorMissing:
                        if (++missing > _peers.Length / 2) return new RegisterValue(null, null);
                        break;
                    case KeyedAcceptorState accepted:
                        if (accepted.Key != key || accepted.AcceptedBallot.Round <= 0
                            || accepted.AcceptedBallot.Proposer < 0 || accepted.AcceptedValue.IsDefault)
                        {
                            throw new InvalidDataException("An accepted observation must identify the requested key, ballot, and bytes.");
                        }
                        var value = RegisterEncoding.Decode(accepted.AcceptedValue);
                        var candidate = (accepted.AcceptedBallot, accepted.AcceptedValue);
                        matching.TryGetValue(candidate, out var count);
                        matching[candidate] = ++count;
                        var promise = (accepted.AcceptedBallot, accepted.AcceptedValue, accepted.PromisedBallot);
                        promises.TryGetValue(promise, out var promised);
                        promises[promise] = ++promised;
                        // A fast ballot can contain competing values, so its certificate needs the fast quorum.
                        if (engine.HasQuorum(accepted.AcceptedBallot, count))
                        {
                            var prepared = accepted.PromisedBallot > accepted.AcceptedBallot
                                && engine.HasQuorum(accepted.PromisedBallot, promised)
                                    ? accepted.PromisedBallot : Ballot.Zero;
                            engine.LearnCommittedValue(accepted.AcceptedBallot, accepted.AcceptedValue, prepared);
                            return value;
                        }
                        break;
                    case AcceptorUnchanged:
                        throw new InvalidDataException("An unconditional acceptor read must return its accepted state.");
                    default:
                        throw new InvalidDataException("An acceptor read must return an explicit outcome.");
                }
            }
            return null;
        }
        finally
        {
            // Every certificate uses observations from this request, including when late peers ignore cancellation.
            phase.Cancel();
        }
    }

    private async Task DrivePhaseAsync(string key, Engine engine, Outbound request, RegisterOperation operation, Call call)
    {
        using var phase = CancellationTokenSource.CreateLinkedTokenSource(call.Token);
        var timeout = Task.Delay(_options.AttemptTimeout, _options.TimeProvider, phase.Token);
        var pending = new List<Task<PeerResult<ProtocolReply>>>(_peers.Length);
        try
        {
            for (var index = 0; index < _peers.Length; index++)
            {
                pending.Add(InvokePeerAsync(index, async (peer, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    if (request.Prepare is { } prepare)
                    {
                        return new ProtocolReply(await peer.PrepareAsync(key, prepare, token).ConfigureAwait(false), null);
                    }

                    var accept = request.Accept!.Value;
                    var mutation = operation.EncodedWrite is { } own && own.Equals(accept.Value);
                    if (mutation) operation.MutationDispatched();
                    var accepted = await peer.AcceptAsync(key, accept, token).ConfigureAwait(false);
                    if (mutation && !accepted.IsAccepted) operation.MutationRejected();
                    return new ProtocolReply(null, accepted);
                }, phase.Token));
            }

            while (pending.Count > 0 && engine.Result is null && ReferenceEquals(engine.Outgoing, request))
            {
                var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                call.Token.ThrowIfCancellationRequested();
                if (finished == timeout) break;
                var task = (Task<PeerResult<ProtocolReply>>)finished;
                pending.Remove(task);
                var reply = await task.ConfigureAwait(false);
                if (reply.Error is { } error)
                {
                    call.Errors[reply.Peer] = error;
                    continue;
                }

                var route = _peers[reply.Peer].Id;
                if (reply.Value!.Prepare is { } result)
                {
                    var value = result.AcceptedBallot.IsZero ? RegisterEncoding.Absent
                        : result.AcceptedValue ?? throw new InvalidDataException("An accepted ballot must include accepted bytes.");
                    // Validate at the register boundary, including rejected responses used in recovery.
                    RegisterEncoding.Decode(value);
                    if (result.IsPromised)
                        engine.HandlePreparePromised(new PreparePromise<ByteString, string>(request.Round, route, result.AcceptedBallot, value));
                    else
                        engine.HandlePrepareRejected(new PrepareRejection<ByteString, string>(
                            request.Round, route, result.AcceptedBallot, value, result.ConflictingBallot));
                }
                else if (reply.Value.Accept is { } resultAccept)
                {
                    if (resultAccept.IsAccepted)
                        engine.HandleAcceptAccepted(new AcceptAccepted<string>(request.Round, route, resultAccept.PromisedBallot));
                    else
                        engine.HandleAcceptRejected(new AcceptRejected<string>(request.Round, route, resultAccept.ConflictingBallot));
                }
            }

            if (engine.Result is null && ReferenceEquals(engine.Outgoing, request))
            {
                engine.HandleResponseTimeout(request.Round);
            }
        }
        finally
        {
            // Each response is consumed only in its request/phase. Late replies never enter an engine.
            phase.Cancel();
        }
    }

    private async Task<List<KeyPage>> ReadInventoryQuorumAsync(string prefix, string? cursor, int limit, Call call)
    {
        var retries = 0;
        while (true)
        {
            try
            {
                call.Token.ThrowIfCancellationRequested();
                using var phase = CancellationTokenSource.CreateLinkedTokenSource(call.Token);
                var timeout = Task.Delay(_options.AttemptTimeout, _options.TimeProvider, phase.Token);
                var pending = Enumerable.Range(0, _peers.Length).Select(index =>
                    InvokePeerAsync(index, (peer, token) => peer.ListKeysAsync(prefix, cursor, limit, token), phase.Token)).ToList();
                var pages = new List<KeyPage>();
                try
                {
                    while (pending.Count > 0)
                    {
                        var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                        call.Token.ThrowIfCancellationRequested();
                        if (finished == timeout) break;
                        var task = (Task<PeerResult<KeyPage>>)finished;
                        pending.Remove(task);
                        var reply = await task.ConfigureAwait(false);
                        if (reply.Error is { } error) call.Errors[reply.Peer] = error;
                        else pages.Add(reply.Value!);
                        if (pages.Count > _peers.Length / 2) return pages;
                    }
                }
                finally { phase.Cancel(); }
                await BackoffAsync(retries++, call.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (call.Token.IsCancellationRequested)
            {
                call.ThrowCancellation();
                throw;
            }
        }
    }

    private async Task<PeerResult<T>> InvokePeerAsync<T>(
        int index, Func<IKeyedAcceptor, CancellationToken, ValueTask<T>> operation, CancellationToken token)
    {
        // Even a transport which ignores cancellation can hold only a bounded number of slots.
        if (!_peerSlots[index].Wait(0, CancellationToken.None))
            return new PeerResult<T>(index, default, new RegisterBusyException());
        try
        {
            token.ThrowIfCancellationRequested();
            return new PeerResult<T>(index, await operation(_peers[index].Acceptor, token).ConfigureAwait(false), null);
        }
        catch (Exception exception)
        {
            return new PeerResult<T>(index, default, exception);
        }
        finally { _peerSlots[index].Release(); }
    }

    private Task BackoffAsync(int attempt, CancellationToken token)
    {
        var factor = 1 + ((_ballots.ProposerId % 7 + attempt % 7) % 7);
        return Task.Delay(TimeSpan.FromTicks(_options.RetryBackoff.Ticks * factor), _options.TimeProvider, token);
    }

    private Engine NewEngine() => new(_ballots, _peerIds, _options.Mode);

    private ByteString NewRequestIdentity()
    {
        lock (_gate)
        {
            // One durable incarnation separates service lifetimes sharing a proposer.
            // Individual requests consume only an in-memory sequence, independent of revisions.
            if (_requestIncarnation.IsZero) _requestIncarnation = _ballots.Reserve(1);
            var sequence = checked(++_requestSequence);
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt32BigEndian(bytes, _requestIncarnation.Proposer);
            BinaryPrimitives.WriteInt32BigEndian(bytes[4..], _requestIncarnation.Round);
            BinaryPrimitives.WriteInt64BigEndian(bytes[8..], sequence);
            return new ByteString(bytes);
        }
    }

    private Call Enter(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_active == _options.Capacity) throw new RegisterBusyException();
            var call = new Call(this, cancellationToken);
            _active++;
            return call;
        }
    }

    private void Exit()
    {
        lock (_gate)
        {
            if (--_active == 0 && _disposed) _drained.TrySetResult();
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposed) return new ValueTask(_drained.Task);
            _disposed = true;
            foreach (var key in _idleKeys)
            {
                _keys.Remove(key, out var state);
                state!.Dispose();
            }
            _idleKeys.Clear();
            if (_active == 0) _drained.TrySetResult();
        }
        _stopping.Cancel();
        return new ValueTask(_drained.Task);
    }

    private static void ValidateTimeout(TimeSpan timeout, string parameter)
    {
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(parameter, "Timeouts must be positive finite timer delays.");
    }

    private sealed class Call : IDisposable
    {
        private readonly ReplicatedRegisterService _owner;
        private readonly CancellationToken _caller;
        private readonly CancellationTokenSource _deadline;
        private readonly CancellationTokenSource _linked;
        public CancellationToken Token => _linked.Token;
        public Exception?[] Errors { get; }

        public Call(ReplicatedRegisterService owner, CancellationToken caller)
        {
            _owner = owner;
            _caller = caller;
            Errors = new Exception?[owner._peers.Length];
            _deadline = new CancellationTokenSource(owner._options.OperationTimeout, owner._options.TimeProvider);
            _linked = CancellationTokenSource.CreateLinkedTokenSource(caller, owner._stopping.Token, _deadline.Token);
        }

        public void ThrowCancellation()
        {
            _caller.ThrowIfCancellationRequested();
            _owner._stopping.Token.ThrowIfCancellationRequested();
            throw new RegisterUnavailableException(new AggregateException(Errors.OfType<Exception>()));
        }

        public void Dispose()
        {
            _linked.Dispose();
            _deadline.Dispose();
            _owner.Exit();
        }
    }

    private sealed class KeyState(Engine engine) : IDisposable
    {
        public Engine Engine = engine;
        public RegisterCommit? Learned;
        public readonly SemaphoreSlim Gate = new(1);
        public int Users;
        public LinkedListNode<string>? Idle;

        public void Dispose() => Gate.Dispose();
    }

    private sealed record PeerResult<T>(int Peer, T? Value, Exception? Error);
    private sealed record ProtocolReply(AcceptorPrepareResult<ByteString?>? Prepare, AcceptorAcceptResult? Accept);
    private sealed record Outbound(PrepareRequest? Prepare, AcceptRequest<ByteString>? Accept)
    {
        public int Round => Prepare?.Ballot.Round ?? Accept!.Value.Ballot.Round;
    }

    private sealed class RegisterOperation : IOperation<ByteString>
    {
        private readonly ByteString? _expectedVersion;
        private readonly ByteString _requestIdentity;
        private long _uncertainMutations;
        public ByteString? Payload { get; }
        public ByteString? EncodedWrite { get; private set; }
        public CompareExchangeStatus Status { get; private set; }
        public bool MayHaveApplied => Volatile.Read(ref _uncertainMutations) != 0;
        public void MutationDispatched() => Interlocked.Increment(ref _uncertainMutations);
        public void MutationRejected() => Interlocked.Decrement(ref _uncertainMutations);
        public RegisterOperation() { }
        public RegisterOperation(ByteString? expectedVersion, ByteString payload, ByteString requestIdentity)
        {
            _expectedVersion = expectedVersion;
            Payload = payload;
            _requestIdentity = requestIdentity;
        }
        public void Complete(ByteString committed, ByteString? version)
        {
            if (EncodedWrite is { } own && committed == own)
            {
                Status = CompareExchangeStatus.Applied;
                return;
            }

            // This accept quorum fences earlier attempts. Another envelope at the same or
            // lower revision proves our write never committed; only a later revision can hide it.
            var superseded = MayHaveApplied && version is { } current
                && BinaryPrimitives.ReadUInt64BigEndian(current.Span)
                    > checked((_expectedVersion is { } expected ? BinaryPrimitives.ReadUInt64BigEndian(expected.Span) : 0) + 1);
            Status = superseded ? CompareExchangeStatus.Unknown : CompareExchangeStatus.Mismatch;
        }
        public (OperationStatus Status, ByteString Result) Apply(ByteString current)
        {
            var existing = RegisterEncoding.Decode(current);
            if (Payload is null || EncodedWrite == current || existing.Version != _expectedVersion)
                return (OperationStatus.NotApplicable, current);
            EncodedWrite ??= RegisterEncoding.Encode(RegisterVersion.Next(existing.Version), _requestIdentity, Payload.Value);
            return (OperationStatus.Success, EncodedWrite.Value);
        }
    }

    private sealed class Engine : ProposerEngine<ByteString, string>
    {
        private readonly IProposerBallotSource _source;
        private Ballot _reservedBallot;
        public override int ProposerId => _source.ProposerId;
        public Outbound? Outgoing { get; private set; }
        public ByteString? Result { get; private set; }
        public Engine(IProposerBallotSource source, ImmutableArray<string> peers, ProposerModePolicy mode)
            : base(new ProposerRuntime(), RegisterEncoding.Absent, peers, mode) => _source = source;
        public void Start(RegisterOperation operation, LocalAcceptorState? local)
        {
            Result = null;
            if (local is null) StartProposal(operation);
            else StartProposal(operation, local.AcceptedBallot, local.PromisedBallot);
        }
        protected override Ballot ReserveBallot(Ballot candidate) =>
            candidate.IsFastRoundBallot ? candidate : _reservedBallot = _source.Reserve(candidate.Round);
        protected override bool CanReusePreparedBallot(Ballot ballot) =>
            base.CanReusePreparedBallot(ballot) && (ballot.IsFastRoundBallot || ballot == _reservedBallot);
        protected override void OnSendPrepare(PrepareRequest request) => Outgoing = new Outbound(request, null);
        protected override void OnSendAccept(AcceptRequest<ByteString> request) => Outgoing = new Outbound(null, request);
        protected override void OnProposalCompleted(ProposeResponse<ByteString> response) => Result = response.CommittedValue;
        protected override void OnValueCommitted(CommittedValue<ByteString> committedValue) { }
    }
}
