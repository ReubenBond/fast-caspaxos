using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Nodes;

namespace FastCASPaxos.Simulation.Scenarios;

public sealed record ProtocolModeCase(string Name, ProposerModePolicy ModePolicy)
{
    public FastCasSimulationOptions CreateOptions() =>
        new()
        {
            ProposerModePolicy = ModePolicy,
        };

    public override string ToString() => Name;
}

public static class ProtocolOptionMatrix
{
    private static readonly ProtocolModeCase[] Modes =
    [
        new("classic", ProposerModePolicy.StaticClassic),
        new("fast", ProposerModePolicy.StaticFast),
        new("leader", ProposerModePolicy.StaticLeader),
        new("fast+leader", ProposerModePolicy.StaticFastLeader),
    ];

    private static readonly int[] Seeds = [7001, 7002, 7003];

    public static IEnumerable<object[]> AllModes()
    {
        foreach (var mode in Modes)
        {
            yield return [mode];
        }
    }

    public static IEnumerable<object[]> FastModes()
    {
        foreach (var mode in Modes.Where(mode => mode.ModePolicy.StaticFastCommitEnabled))
        {
            yield return [mode];
        }
    }

    public static IEnumerable<object[]> LeaderModes()
    {
        foreach (var mode in Modes.Where(mode => mode.ModePolicy.StaticLeaderEnabled))
        {
            yield return [mode];
        }
    }

    public static IEnumerable<object[]> ModeSeedMatrix()
    {
        foreach (var mode in Modes)
        {
            foreach (var seed in Seeds)
            {
                yield return [mode, seed];
            }
        }
    }

    public static FastCasSimulationCluster<StringValue> CreateStringCluster(ProtocolModeCase mode, int seed) =>
        new(seed, default, mode.CreateOptions(), DateTimeOffset.UnixEpoch);

    public static IReadOnlyList<StringValue> ReadAllStringValues(FastCasSimulationCluster<StringValue> cluster)
    {
        List<StringValue> values = [];
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, StringScenarioOperations.Read());
            values.Add(GetRequiredProposerHost(proposer).CachedValue);
        }

        return values;
    }

    public static IReadOnlyList<StringValue> ReadUntilStringValuesAgree(
        FastCasSimulationCluster<StringValue> cluster,
        int maxRounds = 5)
    {
        IReadOnlyList<StringValue> latest = Array.Empty<StringValue>();
        for (var round = 0; round < maxRounds; round++)
        {
            latest = ReadAllStringValues(cluster);
            if (latest.Distinct().Count() == 1)
            {
                return latest;
            }
        }

        return latest;
    }

    public static IReadOnlyList<StringValue> EnsureConvergedStringValue(
        FastCasSimulationCluster<StringValue> cluster,
        int expectedVersion,
        string expectedValue,
        int maxRounds = 5)
    {
        var values = ReadUntilStringValuesAgree(cluster, maxRounds);
        var distinct = values.Distinct().ToList();
        if (distinct.Count != 1)
        {
            throw new InvalidOperationException(
                $"Cluster did not converge within {maxRounds} reconciliation rounds. Values: {string.Join(", ", values)}");
        }

        var converged = distinct[0];
        if (converged.Version != expectedVersion)
        {
            throw new InvalidOperationException(
                $"Expected converged version {expectedVersion}, but found {converged.Version}.");
        }

        if (!string.Equals(converged.Value, expectedValue, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Expected converged value '{expectedValue}', but found '{converged.Value}'.");
        }

        return values;
    }

    public static IReadOnlyList<ProtocolModeCase> GetAllModes() => Modes;

    private static FastCasProposerHost<StringValue> GetRequiredProposerHost(FastCasSimulationNode<StringValue> proposerNode) =>
        proposerNode.Host as FastCasProposerHost<StringValue>
        ?? throw new InvalidOperationException(
            $"Expected proposer host for node '{proposerNode.Host.Address}', but found '{proposerNode.Host.GetType().Name}'.");
}
