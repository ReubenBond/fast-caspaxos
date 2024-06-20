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

    private readonly List<ObservedCommit> _commitHistory = [];
    private IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> _lastSnapshot =
        Array.Empty<FastCasAcceptorStateSnapshot<TValue>>();
    private DateTimeOffset? _lastObservedAt;
    private FastCasAddress? _lastUpdatedAcceptor;

    public int UpdateCount { get; private set; }

    public void Observe(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> snapshots)
    {
        ArgumentNullException.ThrowIfNull(snapshots);

        UpdateCount++;
        _lastObservedAt = timestamp;
        _lastUpdatedAcceptor = updatedAcceptor;
        _lastSnapshot = [.. snapshots
            .OrderBy(snapshot => snapshot.Acceptor.ToString(), StringComparer.Ordinal)];

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

    public AcceptorSafetyMonitorArtifact CreateArtifact() =>
        new(
            SchemaVersion,
            UpdateCount,
            _lastObservedAt?.ToString("O", CultureInfo.InvariantCulture),
            _lastUpdatedAcceptor?.ToString(),
            [.. _commitHistory.Select(
                (commit, index) => new AcceptorSafetyMonitorCommitArtifact(
                    Sequence: index + 1,
                    ObservedAt: commit.Timestamp.ToString("O", CultureInfo.InvariantCulture),
                    TriggeredByAcceptor: commit.UpdatedAcceptor.ToString(),
                    Ballot: commit.Ballot.ToString(),
                    Version: commit.Value.Version,
                    Value: FormatValue(commit.Value),
                    Witnesses: [.. commit.Witnesses.Select(witness => witness.ToString())]))],
            [.. _lastSnapshot.Select(snapshot => new AcceptorSafetyMonitorSnapshotArtifact(
                Acceptor: snapshot.Acceptor.ToString(),
                IsRunning: snapshot.IsRunning,
                IsSuspended: snapshot.IsSuspended,
                PromisedBallot: snapshot.PromisedBallot.ToString(),
                AcceptedBallot: snapshot.AcceptedBallot.ToString(),
                AcceptedVersion: snapshot.AcceptedValue.GetVersionOrDefault(),
                AcceptedValue: FormatValue(snapshot.AcceptedValue)))]);

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
                .Distinct(EqualityComparer<TValue>.Default)
                .ToArray();
            if (distinctValues.Length <= 1)
            {
                continue;
            }

            var detail = string.Join(
                ", ",
                ballotGroup
                    .OrderBy(snapshot => snapshot.Acceptor.ToString(), StringComparer.Ordinal)
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
        if (ballotGroup.Key.IsClassicRoundBallot)
        {
            if (!HasQuorum(ballotGroup.Key, ballotGroup.Count(), acceptorCount))
            {
                return Array.Empty<ResolvedCommit>();
            }

            var witnesses = ballotGroup
                .OrderBy(snapshot => snapshot.Acceptor.ToString(), StringComparer.Ordinal)
                .ToArray();
            return
            [
                new ResolvedCommit(
                    ballotGroup.Key,
                    witnesses[0].AcceptedValue,
                    [.. witnesses.Select(witness => witness.Acceptor)]),
            ];
        }

        var committedValueGroups = ballotGroup
            .GroupBy(snapshot => snapshot.AcceptedValue, EqualityComparer<TValue>.Default)
            .Select(group => group
                .OrderBy(snapshot => snapshot.Acceptor.ToString(), StringComparer.Ordinal)
                .ToArray())
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
        EqualityComparer<TValue>.Default.Equals(value, default!)
            ? null
            : value.ToString();

    private static string CreateContextPrefix(DateTimeOffset timestamp, FastCasAddress updatedAcceptor) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"Acceptor safety monitor detected an inconsistency after {updatedAcceptor} updated at {timestamp:O}.");

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
