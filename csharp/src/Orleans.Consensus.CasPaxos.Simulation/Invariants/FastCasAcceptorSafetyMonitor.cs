using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using System.Globalization;

namespace Orleans.Consensus.CasPaxos.Simulation.Invariants;

public readonly record struct FastCasAcceptorStateSnapshot<TValue>(
    FastCasAddress Acceptor,
    bool IsRunning,
    bool IsSuspended,
    Ballot PromisedBallot,
    Ballot AcceptedBallot,
    TValue AcceptedValue,
    Ballot FloorBallot = default)
    where TValue : IVersionedValue<TValue>;

public sealed record AcceptorSafetyMonitorArtifact(
    string SchemaVersion,
    int UpdateCount,
    string? LastObservedAt,
    string? LastUpdatedAcceptor,
    ImmutableArray<AcceptorSafetyMonitorCommitArtifact> Commits,
    ImmutableArray<AcceptorSafetyMonitorSnapshotArtifact> FinalSnapshot);

public sealed record AcceptorSafetyMonitorCommitArtifact(
    int Sequence,
    string ObservedAt,
    string TriggeredByAcceptor,
    string Ballot,
    int Version,
    string? Value,
    ImmutableArray<string> Witnesses);

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
    private readonly SortedDictionary<long, HashSet<FastCasAddress>> _configurations = [];
    private readonly Dictionary<FastCasAddress, long> _firstConfigByAcceptor = [];
    private readonly Dictionary<FastCasAddress, Ballot> _maxFloorByAcceptor = [];
    private readonly Dictionary<FastCasAddress, (Ballot Ballot, TValue Value)> _lastAcceptedByAcceptor = [];
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
    /// Registers the authoritative voter set <c>V(configuration)</c> for configuration-aware
    /// quorum counting, pre-join refusal checks (VR-I11), and permanent retirement validation (VR-I12).
    /// </summary>
    public void RegisterConfiguration(long configuration, IReadOnlyCollection<FastCasAddress> voters)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(configuration, 1);
        ArgumentNullException.ThrowIfNull(voters);
        if (voters.Count == 0)
        {
            throw new ArgumentException("A voter configuration must contain at least one acceptor.", nameof(voters));
        }

        var voterSet = new HashSet<FastCasAddress>(voters);
        if (_configurations.TryGetValue(configuration, out var existing))
        {
            if (!existing.SetEquals(voterSet))
            {
                throw new InvalidOperationException(
                    $"Configuration {configuration} already registered with a different voter set.");
            }

            return;
        }

        // Enforce permanent retirement (VR-I12): no retired voter may re-enter a later configuration.
        foreach (var (c1, v1) in _configurations)
        {
            foreach (var (c2, v2) in _configurations)
            {
                if (c1 < c2 && c2 < configuration)
                {
                    foreach (var voter in voterSet)
                    {
                        if (v1.Contains(voter) && !v2.Contains(voter))
                        {
                            throw new InvalidOperationException(
                                $"Permanent retirement (VR-I12) violated: voter {voter} belonged to configuration {c1}, was absent in {c2}, and re-entered {configuration}.");
                        }
                    }
                }
            }
        }

        _configurations.Add(configuration, voterSet);
        foreach (var voter in voterSet)
        {
            if (!_firstConfigByAcceptor.TryGetValue(voter, out var first) || configuration < first)
            {
                _firstConfigByAcceptor[voter] = configuration;
            }
        }
    }

    /// <summary>
    /// Clears all accumulated state so that subsequent observations start from a clean baseline.
    /// Used when the simulation reinitializes all nodes to model a fresh register.
    /// </summary>
    public void Reset()
    {
        _commitHistory.Clear();
        _configurations.Clear();
        _firstConfigByAcceptor.Clear();
        _maxFloorByAcceptor.Clear();
        _lastAcceptedByAcceptor.Clear();
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
        ReadOnlySpan<FastCasAcceptorStateSnapshot<TValue>> snapshots)
    {
        var orderedSnapshots = snapshots.ToArray();

        if (orderedSnapshots.Length > 1)
        {
            Array.Sort(orderedSnapshots, _snapshotComparer);
        }

        UpdateCount++;
        _lastObservedAt = timestamp;
        _lastUpdatedAcceptor = updatedAcceptor;
        _lastSnapshot = orderedSnapshots;
        _artifactDirty = true;

        ValidateFloorAndPreJoinInvariants(timestamp, updatedAcceptor, _lastSnapshot);
        ValidateAcceptedBallotConsistency(timestamp, updatedAcceptor, _lastSnapshot);

        var resolvedCommits = ResolveCommittedValues(timestamp, updatedAcceptor, _lastSnapshot);
        if (resolvedCommits.Length == 0)
        {
            return;
        }

        foreach (var committed in resolvedCommits)
        {
            RecordResolvedCommit(timestamp, updatedAcceptor, committed);
        }
    }

    private void RecordResolvedCommit(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        ResolvedCommit committed)
    {
        if (_commitHistory.Count > 0)
        {
            var previous = _commitHistory[^1];
            if (EqualityComparer<TValue>.Default.Equals(previous.Value, committed.Value))
            {
                return;
            }

            // Across configurations, an older configuration's quorum may remain visible in snapshots
            // after a newer configuration has already committed a successor value. Verify ancestry
            // (VR-I4) rather than treating the retained older quorum as a regression.
            if (committed.Ballot < previous.Ballot)
            {
                if (!previous.Value.IsValidSuccessorTo(committed.Value))
                {
                    throw new InvalidOperationException(
                        $"{CreateContextPrefix(timestamp, updatedAcceptor)} Earlier committed value {committed.Value} at {committed.Ballot} diverges from later committed value {previous.Value} at {previous.Ballot}.");
                }

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

    private void ValidateFloorAndPreJoinInvariants(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorStateSnapshot<TValue>[] snapshots)
    {
        foreach (var snapshot in snapshots)
        {
            if (!snapshot.FloorBallot.IsZero && !snapshot.FloorBallot.IsFloorBallot)
            {
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Acceptor {snapshot.Acceptor} reported invalid floor ballot {snapshot.FloorBallot}.");
            }

            if (_maxFloorByAcceptor.TryGetValue(snapshot.Acceptor, out var previousFloor))
            {
                if (!snapshot.FloorBallot.IsZero && snapshot.FloorBallot < previousFloor)
                {
                    throw new InvalidOperationException(
                        $"{CreateContextPrefix(timestamp, updatedAcceptor)} Floor monotonicity (VR-I7) violated on {snapshot.Acceptor}: floor regressed from {previousFloor} to {snapshot.FloorBallot}.");
                }

                var effectivePromised = Ballot.Max(snapshot.PromisedBallot, snapshot.FloorBallot);
                if (!effectivePromised.IsZero && effectivePromised < previousFloor)
                {
                    throw new InvalidOperationException(
                        $"{CreateContextPrefix(timestamp, updatedAcceptor)} Effective promise {effectivePromised} on {snapshot.Acceptor} fell below durable floor {previousFloor}.");
                }

                // If the acceptor recorded a new accepted ballot after a higher floor was durable,
                // that new acceptance must be >= previousFloor (VR-I7).
                if (!snapshot.AcceptedBallot.IsZero)
                {
                    var changed = !_lastAcceptedByAcceptor.TryGetValue(snapshot.Acceptor, out var prevAccepted)
                        || prevAccepted.Ballot != snapshot.AcceptedBallot
                        || !EqualityComparer<TValue>.Default.Equals(prevAccepted.Value, snapshot.AcceptedValue);
                    if (changed && snapshot.AcceptedBallot < previousFloor)
                    {
                        throw new InvalidOperationException(
                            $"{CreateContextPrefix(timestamp, updatedAcceptor)} Acceptor {snapshot.Acceptor} accepted ballot {snapshot.AcceptedBallot} below durable floor {previousFloor} (VR-I7).");
                    }
                }
            }

            if (!snapshot.FloorBallot.IsZero)
            {
                var updatedFloor = _maxFloorByAcceptor.TryGetValue(snapshot.Acceptor, out var existingFloor)
                    ? Ballot.Max(existingFloor, snapshot.FloorBallot)
                    : snapshot.FloorBallot;
                _maxFloorByAcceptor[snapshot.Acceptor] = updatedFloor;
            }

            if (!snapshot.AcceptedBallot.IsZero)
            {
                _lastAcceptedByAcceptor[snapshot.Acceptor] = (snapshot.AcceptedBallot, snapshot.AcceptedValue);
            }

            // No pre-join promise or acceptance (VR-I11) for voters introduced in configuration > 1.
            if (_firstConfigByAcceptor.TryGetValue(snapshot.Acceptor, out var firstConfig) && firstConfig > 1)
            {
                if (!snapshot.AcceptedBallot.IsZero && snapshot.AcceptedBallot.Configuration < firstConfig)
                {
                    throw new InvalidOperationException(
                        $"{CreateContextPrefix(timestamp, updatedAcceptor)} Pre-join acceptance (VR-I11) violated on {snapshot.Acceptor} (first config {firstConfig}): accepted {snapshot.AcceptedBallot}.");
                }

                if (!snapshot.PromisedBallot.IsZero && snapshot.PromisedBallot.Configuration < firstConfig)
                {
                    throw new InvalidOperationException(
                        $"{CreateContextPrefix(timestamp, updatedAcceptor)} Pre-join promise (VR-I11) violated on {snapshot.Acceptor} (first config {firstConfig}): promised {snapshot.PromisedBallot}.");
                }
            }
        }
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
            CreateCommitArtifacts(),
            CreateSnapshotArtifacts());
        _cachedArtifact = artifact;
        _artifactDirty = false;
        return artifact;
    }

    private static void ValidateAcceptedBallotConsistency(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorStateSnapshot<TValue>[] snapshots)
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

    private ResolvedCommit[] ResolveCommittedValues(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorStateSnapshot<TValue>[] snapshots)
    {
        var candidates = snapshots
            .Where(snapshot => !snapshot.AcceptedBallot.IsZero)
            .GroupBy(snapshot => snapshot.AcceptedBallot)
            .SelectMany(group => ResolveCommittedCandidates(timestamp, updatedAcceptor, group, snapshots.Length))
            .OrderBy(candidate => candidate.Ballot)
            .ToArray();

        if (candidates.Length <= 1)
        {
            return candidates;
        }

        // Within a single configuration, quorums intersect so at most one ballot can hold a quorum
        // simultaneously. Across distinct configurations (e.g. during hand-off or disjoint migration),
        // an earlier configuration's quorum can coexist with a newer configuration's quorum, provided
        // every higher ballot's committed value extends every lower ballot's committed value (VR-I4).
        foreach (var byConfig in candidates.GroupBy(candidate => candidate.Ballot.Configuration))
        {
            var configCandidates = byConfig.ToArray();
            if (configCandidates.Length > 1)
            {
                var detail = string.Join(
                    ", ",
                    configCandidates.Select(candidate => $"{candidate.Ballot}=>{candidate.Value}"));
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Multiple committed ballots were inferred from the acceptor snapshot: [{detail}].");
            }
        }

        for (var i = 1; i < candidates.Length; i++)
        {
            var earlier = candidates[i - 1];
            var later = candidates[i];
            if (!EqualityComparer<TValue>.Default.Equals(earlier.Value, later.Value)
                && !later.Value.IsValidSuccessorTo(earlier.Value))
            {
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Cross-configuration committed values diverged: {earlier.Ballot}=>{earlier.Value} vs {later.Ballot}=>{later.Value}.");
            }
        }

        return candidates;
    }

    private ResolvedCommit[] ResolveCommittedCandidates(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IGrouping<Ballot, FastCasAcceptorStateSnapshot<TValue>> ballotGroup,
        int defaultAcceptorCount)
    {
        int acceptorCount;
        FastCasAcceptorStateSnapshot<TValue>[] ballotWitnesses;
        if (_configurations.Count > 0)
        {
            if (!_configurations.TryGetValue(ballotGroup.Key.Configuration, out var voters))
            {
                throw new InvalidOperationException(
                    $"{CreateContextPrefix(timestamp, updatedAcceptor)} Observed accepted ballot {ballotGroup.Key} for unregistered configuration {ballotGroup.Key.Configuration}.");
            }

            acceptorCount = voters.Count;
            // Votes for ballot b count strictly from voters of V(b.Configuration) (VR-I3).
            ballotWitnesses = [.. ballotGroup.Where(snapshot => voters.Contains(snapshot.Acceptor))];
        }
        else
        {
            acceptorCount = defaultAcceptorCount;
            ballotWitnesses = [.. ballotGroup];
        }

        if (ballotWitnesses.Length == 0)
        {
            return Array.Empty<ResolvedCommit>();
        }

        if (ballotGroup.Key.IsClassicRoundBallot)
        {
            if (!HasQuorum(ballotGroup.Key, ballotWitnesses.Length, acceptorCount))
            {
                return Array.Empty<ResolvedCommit>();
            }

            var witnesses = ImmutableArray.CreateBuilder<FastCasAddress>(ballotWitnesses.Length);
            foreach (var witness in ballotWitnesses)
            {
                witnesses.Add(witness.Acceptor);
            }

            return
            [
                new ResolvedCommit(
                    ballotGroup.Key,
                    ballotWitnesses[0].AcceptedValue,
                    witnesses.MoveToImmutable()),
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

    private ImmutableArray<AcceptorSafetyMonitorCommitArtifact> CreateCommitArtifacts()
    {
        var commits = ImmutableArray.CreateBuilder<AcceptorSafetyMonitorCommitArtifact>(_commitHistory.Count);
        foreach (var commit in _commitHistory)
        {
            commits.Add(CreateCommitArtifact(commits.Count + 1, commit));
        }

        return commits.MoveToImmutable();
    }

    private ImmutableArray<AcceptorSafetyMonitorSnapshotArtifact> CreateSnapshotArtifacts()
    {
        var snapshots = ImmutableArray.CreateBuilder<AcceptorSafetyMonitorSnapshotArtifact>(_lastSnapshot.Length);
        foreach (var snapshot in _lastSnapshot)
        {
            snapshots.Add(CreateSnapshotArtifact(snapshot));
        }

        return snapshots.MoveToImmutable();
    }

    private AcceptorSafetyMonitorCommitArtifact CreateCommitArtifact(int sequence, ObservedCommit commit)
    {
        var witnesses = ImmutableArray.CreateBuilder<string>(commit.Witnesses.Length);
        foreach (var witness in commit.Witnesses)
        {
            witnesses.Add(GetFormattedAddress(witness));
        }

        return new AcceptorSafetyMonitorCommitArtifact(
            Sequence: sequence,
            ObservedAt: commit.Timestamp.ToString("O", CultureInfo.InvariantCulture),
            TriggeredByAcceptor: GetFormattedAddress(commit.UpdatedAcceptor),
            Ballot: commit.Ballot.ToString(),
            Version: commit.Value.Version,
            Value: FormatValue(commit.Value),
            Witnesses: witnesses.MoveToImmutable());
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

    private readonly record struct ResolvedCommit(
        Ballot Ballot,
        TValue Value,
        ImmutableArray<FastCasAddress> Witnesses);

    private readonly record struct ObservedCommit(
        DateTimeOffset Timestamp,
        FastCasAddress UpdatedAcceptor,
        Ballot Ballot,
        TValue Value,
        ImmutableArray<FastCasAddress> Witnesses);
}
