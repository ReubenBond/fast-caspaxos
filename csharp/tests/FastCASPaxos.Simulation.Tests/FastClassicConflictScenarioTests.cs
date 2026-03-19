using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastClassicConflictScenarioTests
{
    [Fact]
    public async Task ConcurrentFastRoundConflicts_ConvergeOnSingleCommittedValue()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 666,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = true,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        _ = cluster.SendProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 1);
        _ = cluster.SendProposal(FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(1, "B"), requestId: 2);

        Assert.True(cluster.RunUntil(() => cluster.ClientResponses.Count >= 2, maxIterations: 5000));
        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.Read(), maxIterations: 5000);
        _ = cluster.RunProposal(FastCasAddress.Proposer(2), StringScenarioOperations.Read(), maxIterations: 5000);

        var first = Assert.IsType<FastCasProposerHost<StringValue>>(cluster.ProposerNodes[0].Host).CachedValue;
        var second = Assert.IsType<FastCasProposerHost<StringValue>>(cluster.ProposerNodes[1].Host).CachedValue;

        Assert.Equal(first, second);
        Assert.True(first.Value is "A" or "B");
        Assert.Equal(1, first.Version);
        cluster.AssertSafetyInvariants();
    }
}

