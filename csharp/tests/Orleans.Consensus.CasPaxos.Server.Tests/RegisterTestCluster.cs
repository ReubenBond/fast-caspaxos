using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Server.Tests;

internal static class TestSupport
{
    public static readonly TimeSpan Watchdog = TimeSpan.FromSeconds(15);
    public static readonly ByteString Payload = new([10, 20, 30]);
    public static ByteString Version(int proposer, int round)
    {
        var result = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(result, proposer);
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(4), round);
        return new(result);
    }

    // Independent wire oracle: do not use the production encoder to seed malformed or partial state.
    public static ByteString Envelope(ByteString version, ByteString payload) =>
        new([.. "FCR1"u8, 1, .. version.ToArray(), .. payload.ToArray()]);
    public static ByteString Absent => new("FCR1\0"u8);
    public static ByteString Revision(ulong revision)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, revision);
        return new(bytes);
    }
    public static ulong ReadRevision(ByteString version)
    {
        Assert.Equal(8, version.Length);
        return BinaryPrimitives.ReadUInt64BigEndian(version.ToArray());
    }
    public static ByteString EnvelopeV2(ulong revision, ByteString payload,
        int proposer = 9, int incarnation = 1, long sequence = 1)
    {
        var bytes = new byte[29 + payload.Length];
        "FCR2"u8.CopyTo(bytes);
        bytes[4] = 1;
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(5), revision);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(13), proposer);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(17), incarnation);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(21), sequence);
        payload.ToArray().CopyTo(bytes, 29);
        return new(bytes);
    }
    public static (ulong Revision, int Proposer, int Incarnation, long Sequence, ByteString Payload)
        InspectWrite(ByteString envelope)
    {
        Assert.True(envelope.Length >= 29);
        var bytes = envelope.ToArray().AsSpan();
        Assert.Equal("FCR2"u8.ToArray(), bytes[..4].ToArray());
        Assert.Equal(1, bytes[4]);
        var revision = BinaryPrimitives.ReadUInt64BigEndian(bytes[5..]);
        var proposer = BinaryPrimitives.ReadInt32BigEndian(bytes[13..]);
        var incarnation = BinaryPrimitives.ReadInt32BigEndian(bytes[17..]);
        var sequence = BinaryPrimitives.ReadInt64BigEndian(bytes[21..]);
        Assert.True(revision > 0);
        Assert.True(proposer > 0);
        Assert.True(incarnation > 0);
        Assert.True(sequence > 0);
        return (revision, proposer, incarnation, sequence, new(bytes[29..]));
    }
    public static ControlledPeer[] Peers(int count = 3) =>
        Enumerable.Range(0, count).Select(_ => new ControlledPeer()).ToArray();
    public static ImmutableArray<AcceptorPeer> Routes(ControlledPeer[] peers) =>
        [.. peers.Select((peer, index) => new AcceptorPeer($"peer-{index}", peer))];
    // Recovery-specific tests must not depend on an optimistic read entering Prepare.
    public static void RequireRecovery(ControlledPeer[] peers)
    {
        foreach (var peer in peers)
            peer.Read = (_, _, _) => ValueTask.FromException<AcceptorReadResult>(new IOException("inconclusive probe"));
    }
    public static RegisterServiceOptions Options(string mode = "classic", TimeProvider? clock = null, int capacity = 32) =>
        new()
        {
            Mode = ProposerModePolicy.Parse(mode),
            TimeProvider = clock ?? TimeProvider.System,
            Capacity = capacity,
            OperationTimeout = TimeSpan.FromSeconds(5),
            AttemptTimeout = TimeSpan.FromSeconds(1),
            RetryBackoff = TimeSpan.FromMilliseconds(1)
        };
}

internal sealed class TestBallots(int proposerId) : IProposerBallotSource, IDisposable
{
    private readonly Lock _gate = new();
    private long _configuration = 1;
    private int _round;
    public int ProposerId => proposerId;
    public bool Disposed { get; private set; }
    public ConcurrentQueue<Ballot> Reserved { get; } = new();
    public Ballot Reserve(long configuration, int minimumRound)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(configuration);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRound);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Disposed, this);
            if (configuration < _configuration)
            {
                throw new InvalidOperationException(
                    $"Cannot reserve stale configuration {configuration}; already at {_configuration}.");
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

            var ballot = new Ballot(_configuration, _round, proposerId);
            Reserved.Enqueue(ballot);
            return ballot;
        }
    }
    public Ballot Reserve(int minimumRound) => Reserve(1, minimumRound);
    public void Dispose() => Disposed = true;
}

internal sealed class MemoryAcceptor : IKeyedAcceptor, ILocalAcceptorInspection
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, AcceptorState<ByteString?>> _states = new(StringComparer.Ordinal);
    private Ballot _floor = Ballot.Floor(1);
    public ConcurrentQueue<(string Key, LocalAcceptorState? State)> Inspections { get; } = new();

    public MemoryAcceptor(long initialConfiguration = 1)
    {
        _floor = Ballot.Floor(initialConfiguration);
    }

    public ValueTask<Ballot> SetFloorAsync(Ballot targetFloor, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!targetFloor.IsFloorBallot)
            throw new ArgumentOutOfRangeException(nameof(targetFloor));
        lock (_gate)
        {
            if (targetFloor > _floor) _floor = targetFloor;
            return ValueTask.FromResult(_floor);
        }
    }

    public async ValueTask<LocalAcceptorBallots?> InspectBallotsAsync(string key, CancellationToken cancellationToken = default)
    {
        var state = await InspectAsync(key, cancellationToken);
        if (state is not null) return new(state.PromisedBallot, state.AcceptedBallot);
        lock (_gate) return _floor.Configuration > 1 ? new(_floor, Ballot.Zero) : null;
    }

    public ValueTask<LocalAcceptorState?> InspectAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            LocalAcceptorState? result = _states.TryGetValue(key, out var state)
                ? new(key, Ballot.Max(state.PromisedBallot, _floor), state.AcceptedBallot, state.AcceptedValue) : null;
            Inspections.Enqueue((key, result));
            return ValueTask.FromResult(result);
        }
    }

    public ValueTask<LocalAcceptorPage> InspectKeysAsync(string prefix = "", string? afterKey = null,
        int limit = 100, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var entries = _states.Where(pair => pair.Key.StartsWith(prefix, StringComparison.Ordinal)
                    && (afterKey is null || string.CompareOrdinal(pair.Key, afterKey) > 0))
                .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                .Take(limit == int.MaxValue ? limit : limit + 1)
                .Select(pair => new LocalAcceptorState(pair.Key, Ballot.Max(pair.Value.PromisedBallot, _floor),
                    pair.Value.AcceptedBallot, pair.Value.AcceptedValue)).ToArray();
            return ValueTask.FromResult(new LocalAcceptorPage([.. entries.Take(limit)],
                entries.Length > limit ? entries[limit - 1].Key : null));
        }
    }

    public ValueTask<AcceptorReadResult> ReadAsync(
        string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        known?.Validate();
        lock (_gate)
        {
            if (!_states.TryGetValue(key, out var raw) || raw.AcceptedValue is null)
            {
                var promised = _states.TryGetValue(key, out var promisedState)
                    ? Ballot.Max(promisedState.PromisedBallot, _floor)
                    : _floor;
                return ValueTask.FromResult<AcceptorReadResult>(new AcceptorMissing(promised));
            }

            var state = new KeyedAcceptorState(key, Ballot.Max(raw.PromisedBallot, _floor), raw.AcceptedBallot, raw.AcceptedValue.Value);
            AcceptorReadResult result = known == new AcceptedDigest(state.AcceptedBallot, AcceptedDigest.Hash(state.AcceptedValue))
                ? new AcceptorUnchanged() : state;
            return ValueTask.FromResult(result);
        }
    }

    public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var state = Get(key);
            var result = AcceptorEngine.Prepare(ref state, request);
            _states[key] = state;
            return ValueTask.FromResult(result);
        }
    }

    public ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var state = Get(key);
            var result = AcceptorEngine.Accept(ref state,
                new AcceptRequest<ByteString?>(request.Ballot, request.Value, request.NextBallotToPrepare));
            _states[key] = state;
            return ValueTask.FromResult(result);
        }
    }

    public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null,
        int limit = 100, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var entries = _states.Where(pair => pair.Value.AcceptedValue is not null
                    && pair.Key.StartsWith(prefix, StringComparison.Ordinal)
                    && (afterKey is null || string.CompareOrdinal(pair.Key, afterKey) > 0))
                .OrderBy(pair => pair.Key, StringComparer.Ordinal).Take(limit == int.MaxValue ? limit : limit + 1)
                .Select(pair => new KeyedAcceptorState(pair.Key, Ballot.Max(pair.Value.PromisedBallot, _floor),
                    pair.Value.AcceptedBallot, pair.Value.AcceptedValue!.Value)).ToArray();
            return ValueTask.FromResult(new KeyPage([.. entries.Take(limit)],
                entries.Length > limit ? entries[limit - 1].Key : null, _floor));
        }
    }

    public KeyedAcceptorState? Snapshot(string key)
    {
        lock (_gate)
        {
            var state = Get(key);
            return state.AcceptedValue is null ? null :
                new(key, Ballot.Max(state.PromisedBallot, _floor), state.AcceptedBallot, state.AcceptedValue.Value);
        }
    }
    private AcceptorState<ByteString?> Get(string key)
    {
        if (_states.TryGetValue(key, out var state))
        {
            state.FloorBallot = _floor;
            return state;
        }
        return AcceptorState.CreateInitial<ByteString?>(null, _floor.Configuration);
    }
}

internal sealed class ControlledPeer : IKeyedAcceptor, IAsyncDisposable
{
    public MemoryAcceptor Inner { get; }
    public ControlledPeer(long initialConfiguration = 1) => Inner = new(initialConfiguration);
    public ConcurrentQueue<(string Key, PrepareRequest Request)> Prepares { get; } = new();
    public ConcurrentQueue<(string Key, AcceptRequest<ByteString> Request)> Accepts { get; } = new();
    public ConcurrentQueue<(string Key, AcceptedDigest? Known)> Reads { get; } = new();
    public ConcurrentQueue<Ballot> Floors { get; } = new();
    public Func<string, AcceptedDigest?, CancellationToken, ValueTask<AcceptorReadResult>>? Read { get; set; }
    public Func<string, PrepareRequest, CancellationToken, ValueTask<AcceptorPrepareResult<ByteString?>>>? Prepare { get; set; }
    public Func<string, AcceptRequest<ByteString>, CancellationToken, ValueTask<AcceptorAcceptResult>>? Accept { get; set; }
    public Func<Ballot, CancellationToken, ValueTask<Ballot>>? SetFloor { get; set; }
    public Func<string, string?, int, CancellationToken, ValueTask<KeyPage>>? List { get; set; }
    public bool Disposed { get; private set; }

    public ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        Reads.Enqueue((key, known));
        return Read is null ? Inner.ReadAsync(key, known, cancellationToken) : Read(key, known, cancellationToken);
    }
    public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        Prepares.Enqueue((key, request));
        return Prepare is null ? Inner.PrepareAsync(key, request, cancellationToken) : Prepare(key, request, cancellationToken);
    }
    public ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        Accepts.Enqueue((key, request));
        return Accept is null ? Inner.AcceptAsync(key, request, cancellationToken) : Accept(key, request, cancellationToken);
    }
    public ValueTask<Ballot> SetFloorAsync(Ballot targetFloor, CancellationToken cancellationToken = default)
    {
        Floors.Enqueue(targetFloor);
        return SetFloor is null ? Inner.SetFloorAsync(targetFloor, cancellationToken) : SetFloor(targetFloor, cancellationToken);
    }
    public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
        List is null ? Inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken) : List(prefix, afterKey, limit, cancellationToken);
    public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    public void GoOffline()
    {
        Read = (_, _, _) => ValueTask.FromException<AcceptorReadResult>(new IOException("offline"));
        Prepare = (_, _, _) => ValueTask.FromException<AcceptorPrepareResult<ByteString?>>(new IOException("offline"));
        Accept = (_, _, _) => ValueTask.FromException<AcceptorAcceptResult>(new IOException("offline"));
        SetFloor = (_, _) => ValueTask.FromException<Ballot>(new IOException("offline"));
        List = (_, _, _, _) => ValueTask.FromException<KeyPage>(new IOException("offline"));
    }
}

// Ignores cancellation deliberately: transports can deliver requests/replies after a caller timed out.
internal sealed class Gate : IDisposable
{
    private readonly TaskCompletionSource _arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Arrived => _arrived.Task.WaitAsync(TestSupport.Watchdog);
    public bool IsReleased => _release.Task.IsCompleted;
    public Task WaitAsync() { _arrived.TrySetResult(); return _release.Task; }
    public void Release() => _release.TrySetResult();
    public void Dispose() => Release();
}

internal sealed class ManualTime : TimeProvider
{
    private readonly Lock _gate = new();
    private readonly List<ManualTimer> _timers = [];
    private readonly Channel<TimeSpan> _scheduled = Channel.CreateUnbounded<TimeSpan>();
    private long _ticks;
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    public override long GetTimestamp() { lock (_gate) return _ticks; }
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch + TimeSpan.FromTicks(GetTimestamp());
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        lock (_gate)
        {
            var timer = new ManualTimer(this, callback, state);
            _timers.Add(timer);
            timer.Change(dueTime, period);
            _scheduled.Writer.TryWrite(dueTime);
            return timer;
        }
    }
    public async Task WaitForDelayAsync(TimeSpan delay)
    {
        for (var index = 0; index < 100; index++)
            if (await _scheduled.Reader.ReadAsync().AsTask().WaitAsync(TestSupport.Watchdog) == delay) return;
        throw new InvalidOperationException("Expected timer was not scheduled within the bounded trace.");
    }
    public void Advance(TimeSpan duration)
    {
        List<ManualTimer> due;
        lock (_gate)
        {
            _ticks += duration.Ticks;
            due = _timers.Where(timer => timer.Due <= _ticks).ToList();
            foreach (var timer in due) timer.Due = timer.Period > 0 ? _ticks + timer.Period : long.MaxValue;
        }
        foreach (var timer in due) timer.Fire();
    }
    private sealed class ManualTimer(ManualTime owner, TimerCallback callback, object? state) : ITimer
    {
        public long Due = long.MaxValue;
        public long Period;
        private bool _disposed;
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._gate)
            {
                if (_disposed) return false;
                Due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : owner._ticks + dueTime.Ticks;
                Period = period == Timeout.InfiniteTimeSpan ? 0 : period.Ticks;
                return true;
            }
        }
        public void Fire() { if (!_disposed) callback(state); }
        public void Dispose() { lock (owner._gate) { _disposed = true; owner._timers.Remove(this); } }
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }
}
