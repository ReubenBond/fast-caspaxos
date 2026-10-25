using System.Globalization;
using System.Runtime.CompilerServices;

namespace FastCASPaxos.Server.Diagnostics;

public sealed record NodeEvent(
    int SchemaVersion, string NodeId, string BootId, string Sequence, DateTimeOffset Timestamp, string Kind, object Data)
{
    public string Id => $"{BootId}:{Sequence}";
}

/// <summary>
/// A disposable bounded observation ring. Publishing performs bounded in-memory work and never calls consumers.
/// Readers own cursors, so replay and live reads use the same atomic history lookup.
/// </summary>
public sealed class NodeEventJournal : IDisposable
{
    private readonly object _gate = new();
    private readonly NodeEvent?[] _events;
    private TaskCompletionSource _changed = NewSignal();
    private long _sequence;
    private bool _stopped;

    public NodeEventJournal(string nodeId, int capacity = 1024, TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        NodeId = nodeId;
        BootId = Guid.NewGuid().ToString("N");
        Clock = timeProvider ?? TimeProvider.System;
        StartedAt = Clock.GetUtcNow();
        _events = new NodeEvent[capacity];
    }

    public string NodeId { get; }
    public string BootId { get; }
    public DateTimeOffset StartedAt { get; }
    public TimeProvider Clock { get; }
    public int Capacity => _events.Length;
    public string Cursor { get { lock (_gate) return Id(_sequence); } }
    public StorageObservation Storage
    {
        get { lock (_gate) return field; }

        private set;
    } = new("starting");
    public NodeEvent? LastOperation
    {
        get { lock (_gate) return field; }

        private set;
    }

    public void Publish(string kind, object data)
    {
        lock (_gate)
        {
            if (_stopped) return;
            if (data is StorageObservation storage) Storage = storage;
            var sequence = ++_sequence;
            var observation = Event(sequence, kind, data);
            _events[(sequence - 1) % _events.Length] = observation;
            if (data is OperationObservation) LastOperation = observation;
            var changed = _changed;
            _changed = NewSignal();
            changed.TrySetResult();
        }
    }

    public async IAsyncEnumerable<NodeEvent> ReadAsync(
        string? after, TimeSpan heartbeatInterval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (heartbeatInterval <= TimeSpan.Zero || heartbeatInterval > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(heartbeatInterval));
        var cursor = 0L;
        string? reset = "initial";
        if (after is not null)
        {
            var parts = after.Split(':');
            reset = parts.Length != 2 || !Guid.TryParseExact(parts[0], "N", out _)
                || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out cursor)
                ? "invalid-cursor" : parts[0] != BootId ? "restarted" : null;
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            NodeEvent? next = null;
            Task changed;
            lock (_gate)
            {
                if (_stopped) yield break;
                if (reset is null && (cursor > _sequence || cursor < Math.Max(0, _sequence - _events.Length)))
                    reset = "history-unavailable";
                if (reset is not null)
                {
                    cursor = _sequence;
                    next = Event(cursor, "reset", new ResetObservation(reset));
                    reset = null;
                }
                else if (cursor < _sequence)
                {
                    next = _events[cursor % _events.Length]!;
                    cursor++;
                }
                changed = _changed.Task;
            }
            if (next is not null)
            {
                yield return next;
                continue;
            }
            var heartbeat = false;
            try { await changed.WaitAsync(heartbeatInterval, Clock, cancellationToken).ConfigureAwait(false); }
            catch (TimeoutException) { heartbeat = true; }
            if (heartbeat)
            {
                // Heartbeats carry the reader's cursor; they never skip unread retained observations.
                yield return Event(cursor, "heartbeat", new HeartbeatObservation(Storage.Status));
            }
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _stopped = true;
            Array.Clear(_events);
            LastOperation = null;
            _changed.TrySetResult();
        }
    }

    private string Id(long sequence) => $"{BootId}:{sequence.ToString(CultureInfo.InvariantCulture)}";
    private NodeEvent Event(long sequence, string kind, object data) =>
        new(1, NodeId, BootId, sequence.ToString(CultureInfo.InvariantCulture), Clock.GetUtcNow(), kind, data);
    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
