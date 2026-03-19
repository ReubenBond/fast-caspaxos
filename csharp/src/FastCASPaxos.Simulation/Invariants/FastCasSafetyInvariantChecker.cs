using System.Collections.Generic;
using FastCASPaxos.Messages;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Simulation.Invariants;

public static class FastCasSafetyInvariantChecker
{
    /// <summary>
    /// Validates safety properties across proposer commit histories and client-visible responses.
    /// Commit histories are collected externally (e.g. from <see cref="Diagnostics.ProposerDiagnostics"/> events)
    /// and passed in as a per-proposer dictionary.
    /// </summary>
    public static void AssertSafety<TValue>(
        IReadOnlyDictionary<FastCasAddress, IReadOnlyList<TValue>> commitHistories,
        IEnumerable<RoutedProposeResponse<TValue, FastCasAddress>> responses)
        where TValue : IVersionedValue<TValue>
    {
        ArgumentNullException.ThrowIfNull(commitHistories);
        ArgumentNullException.ThrowIfNull(responses);

        foreach (var (proposer, history) in commitHistories)
        {
            ValidateLinearHistory<TValue>(proposer, history);
        }

        foreach (var responseGroup in responses.GroupBy(response => response.Proposer))
        {
            TValue? previousValue = default;
            var previousVersion = 0;
            var hasPrevious = false;
            foreach (var response in responseGroup.OrderBy(response => response.Round))
            {
                var currentVersion = response.CommittedValue is { } cv ? cv.Version : 0;
                if (hasPrevious && currentVersion < previousVersion)
                {
                    throw new InvalidOperationException(
                        $"Committed value for {response.Proposer} regressed from version {previousVersion} to version {currentVersion}.");
                }

                previousValue = response.CommittedValue;
                previousVersion = currentVersion;
                hasPrevious = true;
            }
        }

        SortedDictionary<int, TValue> committedByVersion = [];
        foreach (var (proposer, history) in commitHistories)
        {
            foreach (var committed in history)
            {
                if (!committedByVersion.TryGetValue(committed.Version, out var existing))
                {
                    committedByVersion[committed.Version] = committed;
                    continue;
                }

                if (!EqualityComparer<TValue>.Default.Equals(existing, committed))
                {
                    throw new InvalidOperationException(
                        $"Committed value at version {committed.Version} does not agree across proposers: {existing} != {committed} (proposer {proposer}).");
                }
            }
        }

        var first = true;
        var previousCommitted = default(TValue)!;
        foreach (var committed in committedByVersion.Values)
        {
            if (!first && !committed.IsValidSuccessorTo(previousCommitted))
            {
                throw new InvalidOperationException(
                    $"Committed value at version {committed.Version} is not a valid successor to {previousCommitted}: {committed}.");
            }

            previousCommitted = committed;
            first = false;
        }
    }

    private static void ValidateLinearHistory<TValue>(
        FastCasAddress proposer,
        IReadOnlyList<TValue> history)
        where TValue : IVersionedValue<TValue>
    {
        var previous = default(TValue)!;
        foreach (var committed in history)
        {
            var previousVersion = previous.GetVersionOrDefault();
            if ((!committed.Equals(previous) && committed.Version == previousVersion) || committed.Version < previousVersion)
            {
                throw new InvalidOperationException(
                    $"Non-linearizable history on {proposer}: {committed} after {previous} in history [{string.Join(", ", history)}].");
            }

            previous = committed;
        }
    }
}
