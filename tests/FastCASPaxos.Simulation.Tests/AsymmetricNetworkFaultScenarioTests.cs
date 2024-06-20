using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class AsymmetricNetworkFaultScenarioTests
{
    [Fact]
    public async Task ReplyPathPartition_BlocksUntilHealedThenCompletes()
    {
        await using var cluster = CreateCluster(seed: 9821);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.CreatePartition(FastCasAddress.Acceptor(acceptor), proposer);
        }

        cluster.SendProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 77);
        Assert.False(cluster.RunUntil(() => cluster.TryGetResponse(77, out _), maxIterations: 100));
        Assert.False(cluster.CanReachClassicQuorum(proposer));

        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.HealPartition(FastCasAddress.Acceptor(acceptor), proposer);
        }

        var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        Assert.Equal("A", response.CommittedValue.Value);
        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task RequestPathPartition_BlocksUntilHealedThenCompletes()
    {
        await using var cluster = CreateCluster(seed: 9822);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.CreatePartition(proposer, FastCasAddress.Acceptor(acceptor));
        }

        cluster.SendProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 88);
        Assert.False(cluster.RunUntil(() => cluster.TryGetResponse(88, out _), maxIterations: 100));
        Assert.False(cluster.CanReachClassicQuorum(proposer));

        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.HealPartition(proposer, FastCasAddress.Acceptor(acceptor));
        }

        var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        Assert.Equal("A", response.CommittedValue.Value);
        cluster.AssertSafetyInvariants();
    }

    private static FastCasSimulationCluster<StringValue> CreateCluster(int seed) =>
        new(
            seed,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

