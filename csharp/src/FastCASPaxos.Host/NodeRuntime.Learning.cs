using System.Threading.Channels;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Grpc;

namespace FastCASPaxos.Host;

public sealed partial class NodeRuntime
{
    public async ValueTask<bool> LearnDigestAsync(RegisterCommitDigest commit, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(commit);
        commit.Validate();
        var context = DiagnosticContext.Current;
        var callId = context?.CallId ?? Guid.NewGuid().ToString("N");
        ObserveCompactLearning(commit, "inbound", "request-received", context?.NodeId, callId);
        try
        {
            var generation = GetServingRegisters();
            using var lease = generation.Admission.Enter();
            var value = await _localReads.RunAsync(
                token => _acceptor!.ResolveCommittedValueAsync(commit.Key, commit.Accepted, token),
                _options.AttemptTimeout, _clock, cancellationToken).ConfigureAwait(false);
            if (value is { } bytes)
                generation.Core.Learn(new(commit.Key, commit.Accepted.AcceptedBallot, bytes, commit.PreparedBallot));
            ObserveCompactLearning(commit, "inbound", "response-produced", context?.NodeId, callId,
                value is null ? "stale" : "committed");
            return value is not null;
        }
        catch (Exception error)
        {
            ObserveCompactLearning(commit, "inbound", "fault-observed", context?.NodeId, callId, error: error.GetType().Name);
            throw;
        }
    }

    private void ObserveCompactLearning(RegisterCommitDigest commit, string direction, string phase, string? peer,
        string callId, string? outcome = null, string? error = null) =>
        Diagnostics.Publish("protocol", new ProtocolObservation("learn-digest", direction, phase, peer, callId,
            DiagnosticContext.Current?.OperationId, KeyPreview.From(commit.Key), commit.Accepted.AcceptedBallot,
            commit.PreparedBallot, outcome, AcceptedBallot: commit.Accepted.AcceptedBallot, Error: error,
            ValueHash: Convert.ToBase64String(commit.Accepted.ValueHash.ToArray())));

    private readonly record struct LearningNotification(RegisterCommit Commit, AcceptedDigest Digest, string? OperationId);

    private sealed record LearningPeer(string Id, GrpcRegisterLearner Client, LatestLearningQueue Queue)
    {
        public Task Worker { get; set; } = Task.CompletedTask;
    }

    private sealed class LatestLearningQueue(int capacity)
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
        private readonly Channel<string> _ready = Channel.CreateBounded<string>(capacity);

        public bool TryWrite(LearningNotification notification)
        {
            var key = notification.Commit.Key;
            lock (_gate)
            {
                if (_entries.TryGetValue(key, out var entry))
                {
                    var previous = entry.Latest.Commit;
                    var incoming = notification.Commit;
                    if (incoming.AcceptedBallot < previous.AcceptedBallot) return true;
                    if (incoming.AcceptedBallot == previous.AcceptedBallot)
                    {
                        if (incoming.Value != previous.Value)
                            throw new InvalidDataException("A committed ballot identifies exactly one value.");
                        if (incoming.PreparedBallot <= previous.PreparedBallot) return true;
                    }
                    entry.Latest = notification;
                    entry.Pending = notification;
                    return true;
                }
                if (_entries.Count == capacity) return false;
                _entries.Add(key, new(notification));
                Enqueue(key);
                return true;
            }
        }

        public async ValueTask<LearningNotification> ReadAsync(CancellationToken cancellationToken)
        {
            var key = await _ready.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            lock (_gate)
            {
                var entry = _entries[key];
                var notification = entry.Pending!.Value;
                entry.Pending = null;
                return notification;
            }
        }

        public void Complete(string key)
        {
            lock (_gate)
            {
                if (_entries[key].Pending is not null) Enqueue(key);
                else _entries.Remove(key);
            }
        }

        private void Enqueue(string key)
        {
            if (!_ready.Writer.TryWrite(key))
                throw new InvalidOperationException("Each admitted learning key owns one bounded queue slot.");
        }

        private sealed class Entry(LearningNotification notification)
        {
            public LearningNotification Latest { get; set; } = notification;
            public LearningNotification? Pending { get; set; } = notification;
        }
    }
}
