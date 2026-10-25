using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class NetworkFaultScenarioTests
{
    [Fact]
    public async Task PartitionHealAllowsSubsequentProposalToSucceed()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        cluster.SendProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 77);
        Assert.False(cluster.RunUntil(() => cluster.TryGetResponse(77, out _), maxIterations: 100));

        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"));
        Assert.Equal("A", response.CommittedValue.Value);
    }

    [Fact]
    public async Task MessageDelaysAdvanceSimulationTimeButStillComplete()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);
        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(5));

        var start = cluster.TimeProvider.GetUtcNow();
        var response = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"));

        Assert.Equal("A", response.CommittedValue.Value);
        Assert.True(cluster.TimeProvider.GetUtcNow() > start);
    }

    [Fact]
    public async Task DroppedMessagesCanBeRetriedSafelyAfterDropRateClears()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);
        var proposer = FastCasAddress.Proposer(1);

        cluster.SetMessageDropRate(1.0);
        cluster.SendProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 88);
        Assert.False(cluster.RunUntil(() => cluster.TryGetResponse(88, out _), maxIterations: 100));

        cluster.SetMessageDropRate(0);
        var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"));

        Assert.Equal("A", response.CommittedValue.Value);
        cluster.AssertSafetyInvariants();
    }

    private static FastCasSimulationCluster<StringValue> CreateCluster() =>
        new(
            seed: 444,
            initialValue: default,
            options: new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

