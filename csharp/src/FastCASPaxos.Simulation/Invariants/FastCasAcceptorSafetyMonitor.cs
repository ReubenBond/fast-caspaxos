using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using System.Globalization;

namespace FastCASPaxos.Simulation.Invariants;

public readonly record struct FastCasAcceptorStateSnapshot<TValue>(
    FastCasAddress Acceptor,
    bool IsRunning,
    bool IsSuspended,
    Ballot PromisedBallot,
    Ballot AcceptedBallot,
    TValue AcceptedValue)
    where TValue : IVersionedValue<TValue>;

public sealed record AcceptorSafetyMonitorArtifact(
    string SchemaVersion,
    int UpdateCount,
    string? LastObservedAt,
    string? LastUpdatedAcceptor,
    IReadOnlyList<AcceptorSafetyMonitorCommitArtifact> Commits,
    IReadOnlyList<AcceptorSafetyMonitorSnapshotArtifact> FinalSnapshot);

public sealed record AcceptorSafetyMonitorCommitArtifact(
    int Sequence,
    string ObservedAt,
    string TriggeredByAcceptor,
    string Ballot,
    int Version,
    string? Value,
    IReadOnlyList<string> Witnesses);

public sealed record AcceptorSafetyMonitorSnapshotArtifact(
    string Acceptor,
    bool IsRunning,
    bool IsSuspended,
    string PromisedBallot,
    string AcceptedBallot,
    int AcceptedVersion,
    string? AcceptedValue);

public sealed class FastCasAcceptorSafetyMonitor<TValue>
    where TValue : IVersionedValue<TValue>
{
    private const string SchemaVersion = "fast-caspaxos-acceptor-safety-monitor-v1";

    private readonly Dictionary<FastCasAddress, string> _formattedAddressCache = [];
    private readonly List<ObservedCommit> _commitHistory = [];
    private readonly SnapshotComparer _snapshotComparer;
    private FastCasAcceptorStateSnapshot<TValue>[] _lastSnapshot =
        Array.Empty<FastCasAcceptorStateSnapshot<TValue>>();
    private DateTimeOffset? _lastObservedAt;
    private FastCasAddress? _lastUpdatedAcceptor;
    private AcceptorSafetyMonitorArtifact? _cachedArtifact;
    private bool _artifactDirty = true;

    public FastCasAcceptorSafetyMonitor() => _snapshotComparer = new SnapshotComparer(this);

    public int UpdateCount { get; private set; }

    /// <summary>
    /// Clears all accumulated state so that subsequent observations start from a clean baseline.
    /// Used when the simulation reinitializes all nodes to model a fresh register.
    /// </summary>
    public void Reset()
    {
        _commitHistory.Clear();
        _lastSnapshot = [];
        _lastObservedAt = null;
        _lastUpdatedAcceptor = null;
        _cachedArtifact = null;
        _artifactDirty = true;
        UpdateCount = 0;
    }

    public void Observe(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> snapshots)
    {
        ArgumentNullException.ThrowIfNull(snapshots);

        var orderedSnapshots = new FastCasAcceptorStateSnapshot<TValue>[snapshots.Count];
        for (var i = 0; i < orderedSnapshots.Length; i++)
        {
            orderedSnapshots[i] = snapshots[i];
        }

        if (orderedSnapshots.Length > 1)
        {
            Array.Sort(orderedSnapshots, _snapshotComparer);
        }

        UpdateCount++;
        _lastObservedAt = timestamp;
        _lastUpdatedAcceptor = updatedAcceptor;
        _lastSnapshot = orderedSnapshots;
        _artifactDirty = true;

        ValidateAcceptedBallotConsistency(timestamp, updatedAcceptor, _lastSnapshot);

        if (!TryResolveCommittedValue(timestamp, updatedAcceptor, _lastSnapshot, out var committed))
        {
            return;
        }

        if (_commitHistory.Count > 0)
        {
            var previous = _commitHistory[^1];
            if (EqualityComparer<TValue>.Default.Equals(previous.Value, committed.Value))
            {
                return;
            }

            if (committed.Value.Version <= previous.Value.Version)
            {
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Inferred committed value {committed.Value} at {committed.Ballot} regressed from {previous.Value} at {previous.Ballot}.");
            }

            if (!committed.Value.IsValidSuccessorTo(previous.Value))
            {
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Inferred committed value {committed.Value} at {committed.Ballot} is not a valid successor to {previous.Value} at {previous.Ballot}.");
            }
        }

        _commitHistory.Add(
            new ObservedCommit(
                timestamp,
                updatedAcceptor,
                committed.Ballot,
                committed.Value,
                committed.Witnesses));
    }

    public AcceptorSafetyMonitorArtifact CreateArtifact()
    {
        if (!_artifactDirty && _cachedArtifact is not null)
        {
            return _cachedArtifact;
        }

        var artifact = new AcceptorSafetyMonitorArtifact(
            SchemaVersion,
            UpdateCount,
            _lastObservedAt?.ToString("O", CultureInfo.InvariantCulture),
            _lastUpdatedAcceptor is { } lastUpdatedAcceptor ? GetFormattedAddress(lastUpdatedAcceptor) : null,
            new FrozenCommitList(this, _commitHistory, _commitHistory.Count),
            new FrozenSnapshotList(this, _lastSnapshot));
        _cachedArtifact = artifact;
        _artifactDirty = false;
        return artifact;
    }

    private static void ValidateAcceptedBallotConsistency(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> snapshots)
    {
        foreach (var ballotGroup in snapshots
            .Where(snapshot => !snapshot.AcceptedBallot.IsZero)
            .GroupBy(snapshot => snapshot.AcceptedBallot)
            .OrderBy(group => group.Key))
        {
            if (ballotGroup.Key.IsFastRoundBallot)
            {
                continue;
            }

            var distinctValues = ballotGroup
                .Select(snapshot => snapshot.AcceptedValue)
                .Distinct()
                .ToArray();
            if (distinctValues.Length <= 1)
            {
                continue;
            }

            var detail = string.Join(
                ", ",
                ballotGroup
                    .Select(snapshot => $"{snapshot.Acceptor}={snapshot.AcceptedValue}"));
            throw new InvalidOperationException(
                $"{CreateContextPrefix(timestamp, updatedAcceptor)} Accepted ballot {ballotGroup.Key} has conflicting values across acceptors: [{detail}].");
        }
    }

    private static bool TryResolveCommittedValue(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> snapshots,
        out ResolvedCommit committed)
    {
        var acceptorCount = snapshots.Count;
        var candidates = snapshots
            .Where(snapshot => !snapshot.AcceptedBallot.IsZero)
            .GroupBy(snapshot => snapshot.AcceptedBallot)
            .SelectMany(group => ResolveCommittedCandidates(timestamp, updatedAcceptor, group, acceptorCount))
            .OrderBy(candidate => candidate.Ballot)
            .ToArray();

        if (candidates.Length == 0)
        {
            committed = default;
            return false;
        }

        if (candidates.Length > 1)
        {
            var detail = string.Join(
                ", ",
                candidates.Select(candidate => $"{candidate.Ballot}=>{candidate.Value}"));
            throw new InvalidOperationException(
                $"{CreateContextPrefix(timestamp, updatedAcceptor)} Multiple committed ballots were inferred from the acceptor snapshot: [{detail}].");
        }

        committed = candidates[0];
        return true;
    }

    private static IReadOnlyList<ResolvedCommit> ResolveCommittedCandidates(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IGrouping<Ballot, FastCasAcceptorStateSnapshot<TValue>> ballotGroup,
        int acceptorCount)
    {
        var ballotWitnesses = ballotGroup.ToArray();
        if (ballotGroup.Key.IsClassicRoundBallot)
        {
            if (!HasQuorum(ballotGroup.Key, ballotWitnesses.Length, acceptorCount))
            {
                return Array.Empty<ResolvedCommit>();
            }

            var witnesses = new FastCasAddress[ballotWitnesses.Length];
            for (var i = 0; i < witnesses.Length; i++)
            {
                witnesses[i] = ballotWitnesses[i].Acceptor;
            }

            return
            [
                new ResolvedCommit(
                    ballotGroup.Key,
                    ballotWitnesses[0].AcceptedValue,
                    witnesses),
            ];
        }

        var committedValueGroups = ballotWitnesses
            .GroupBy(snapshot => snapshot.AcceptedValue)
            .Select(group => group.ToArray())
            .Where(group => HasQuorum(ballotGroup.Key, group.Length, acceptorCount))
            .ToArray();
        if (committedValueGroups.Length > 1)
        {
            var detail = string.Join(
                ", ",
                committedValueGroups.Select(group => $"{group[0].AcceptedValue}=>[{string.Join(", ", group.Select(snapshot => snapshot.Acceptor))}]"));
            throw new InvalidOperationException(
                $"{CreateContextPrefix(timestamp, updatedAcceptor)} Fast ballot {ballotGroup.Key} has multiple values with fast quorum: [{detail}].");
        }

        if (committedValueGroups.Length == 0)
        {
            return Array.Empty<ResolvedCommit>();
        }

        var committedWitnesses = committedValueGroups[0];
        return
        [
            new ResolvedCommit(
                ballotGroup.Key,
                committedWitnesses[0].AcceptedValue,
                [.. committedWitnesses.Select(witness => witness.Acceptor)]),
        ];
    }

    private static bool HasQuorum(Ballot ballot, int responses, int acceptorCount)
    {
        if (ballot.IsFastRoundBallot)
        {
            return 4 * responses >= 3 * acceptorCount;
        }

        return 2 * responses > acceptorCount;
    }

    private static string? FormatValue(TValue value) =>
        EqualityComparer<TValue>.Default.Equals(value, default)
            ? null
            : value.ToString();

    private string GetFormattedAddress(FastCasAddress address)
    {
        if (!_formattedAddressCache.TryGetValue(address, out var formatted))
        {
            formatted = address.ToString();
            _formattedAddressCache.Add(address, formatted);
        }

        return formatted;
    }

    private AcceptorSafetyMonitorCommitArtifact CreateCommitArtifact(int sequence, ObservedCommit commit)
    {
        var witnesses = new string[commit.Witnesses.Count];
        for (var i = 0; i < witnesses.Length; i++)
        {
            witnesses[i] = GetFormattedAddress(commit.Witnesses[i]);
        }

        return new AcceptorSafetyMonitorCommitArtifact(
            Sequence: sequence,
            ObservedAt: commit.Timestamp.ToString("O", CultureInfo.InvariantCulture),
            TriggeredByAcceptor: GetFormattedAddress(commit.UpdatedAcceptor),
            Ballot: commit.Ballot.ToString(),
            Version: commit.Value.Version,
            Value: FormatValue(commit.Value),
            Witnesses: witnesses);
    }

    private AcceptorSafetyMonitorSnapshotArtifact CreateSnapshotArtifact(
        FastCasAcceptorStateSnapshot<TValue> snapshot) =>
        new(
            Acceptor: GetFormattedAddress(snapshot.Acceptor),
            IsRunning: snapshot.IsRunning,
            IsSuspended: snapshot.IsSuspended,
            PromisedBallot: snapshot.PromisedBallot.ToString(),
            AcceptedBallot: snapshot.AcceptedBallot.ToString(),
            AcceptedVersion: snapshot.AcceptedValue.GetVersionOrDefault(),
            AcceptedValue: FormatValue(snapshot.AcceptedValue));

    private static string CreateContextPrefix(DateTimeOffset timestamp, FastCasAddress updatedAcceptor) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"Acceptor safety monitor detected an inconsistency after {updatedAcceptor} updated at {timestamp:O}.");

    private sealed class SnapshotComparer(FastCasAcceptorSafetyMonitor<TValue> owner)
        : IComparer<FastCasAcceptorStateSnapshot<TValue>>
    {
        private readonly FastCasAcceptorSafetyMonitor<TValue> _owner = owner;

        public int Compare(
            FastCasAcceptorStateSnapshot<TValue> left,
            FastCasAcceptorStateSnapshot<TValue> right) =>
            StringComparer.Ordinal.Compare(
                _owner.GetFormattedAddress(left.Acceptor),
                _owner.GetFormattedAddress(right.Acceptor));
    }

    private sealed class FrozenCommitList(
        FastCasAcceptorSafetyMonitor<TValue> owner,
        IReadOnlyList<ObservedCommit> commits,
        int count)
        : IReadOnlyList<AcceptorSafetyMonitorCommitArtifact>
    {
        private readonly FastCasAcceptorSafetyMonitor<TValue> _owner = owner;
        private readonly IReadOnlyList<ObservedCommit> _commits = commits;

        public int Count { get; } = count;

        public AcceptorSafetyMonitorCommitArtifact this[int index]
        {
            get
            {
                ArgumentOutOfRangeException.ThrowIfNegative(index);
                ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
                return _owner.CreateCommitArtifact(index + 1, _commits[index]);
            }
        }

        public IEnumerator<AcceptorSafetyMonitorCommitArtifact> GetEnumerator()
        {
            for (var i = 0; i < Count; i++)
            {
                yield return this[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class FrozenSnapshotList(
        FastCasAcceptorSafetyMonitor<TValue> owner,
        IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> snapshots)
        : IReadOnlyList<AcceptorSafetyMonitorSnapshotArtifact>
    {
        private readonly FastCasAcceptorSafetyMonitor<TValue> _owner = owner;
        private readonly IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> _snapshots = snapshots;

        public int Count => _snapshots.Count;

        public AcceptorSafetyMonitorSnapshotArtifact this[int index]
        {
            get
            {
                ArgumentOutOfRangeException.ThrowIfNegative(index);
                ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
                return _owner.CreateSnapshotArtifact(_snapshots[index]);
            }
        }

        public IEnumerator<AcceptorSafetyMonitorSnapshotArtifact> GetEnumerator()
        {
            for (var i = 0; i < Count; i++)
            {
                yield return this[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private readonly record struct ResolvedCommit(
        Ballot Ballot,
        TValue Value,
        IReadOnlyList<FastCasAddress> Witnesses);

    private readonly record struct ObservedCommit(
        DateTimeOffset Timestamp,
        FastCasAddress UpdatedAcceptor,
        Ballot Ballot,
        TValue Value,
        IReadOnlyList<FastCasAddress> Witnesses);
}
