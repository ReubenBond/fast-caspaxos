using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastLeaderContentionScenarioTests
{
    [Fact]
    public async Task ConcurrentFastLeaderConflicts_ConvergeAndStillAllowSubsequentCommit()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 9831,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = true,
                EnableDistinguishedLeader = true,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var requests = new[]
        {
            cluster.SendProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 1),
            cluster.SendProposal(FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(1, "B"), requestId: 2),
            cluster.SendProposal(FastCasAddress.Proposer(3), StringScenarioOperations.AppendAtVersion(1, "C"), requestId: 3),
        };

        Assert.True(cluster.RunUntil(() => requests.All(requestId => cluster.TryGetResponse(requestId, out _)), maxIterations: 5000));

        var firstValues = ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster);
        var initial = Assert.Single(firstValues.Distinct());
        Assert.Equal(1, initial.Version);
        Assert.Contains(initial.Value, new[] { "A", "B", "C" });

        var response = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(2, "D"), maxIterations: 5000);

        Assert.Equal(2, response.CommittedValue.Version);
        Assert.EndsWith("D", response.CommittedValue.Value, StringComparison.Ordinal);

        var converged = Assert.Single(ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster).Distinct());
        Assert.Equal(response.CommittedValue, converged);
        cluster.AssertSafetyInvariants();
    }
}

