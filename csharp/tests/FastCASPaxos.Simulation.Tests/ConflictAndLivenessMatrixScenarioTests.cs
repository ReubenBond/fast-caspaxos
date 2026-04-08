using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class ConflictAndLivenessMatrixScenarioTests
{
    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.LeaderModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task LeaderModes_AnotherProposerCommitsAfterLeaderCrash(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9201);
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var proposer1 = FastCasAddress.Proposer(1);
        var proposer2 = FastCasAddress.Proposer(2);

        _ = cluster.RunProposal(proposer1, StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        cluster.CrashNode(proposer1);

        _ = cluster.RunProposal(proposer2, StringScenarioOperations.AppendAtVersion(2, "B"), maxIterations: 5000);

        cluster.RestartNode(proposer1);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expectedVersion: 2, expectedValue: "AB");
        cluster.AssertSafetyInvariants();
    }

    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.FastModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task FastModes_ThreeWayConcurrentConflicts_ConvergeOnSingleCommittedValue(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9202);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var requests = new[]
        {
            cluster.SendProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), requestId: 1),
            cluster.SendProposal(FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(1, "B"), requestId: 2),
            cluster.SendProposal(FastCasAddress.Proposer(3), StringScenarioOperations.AppendAtVersion(1, "C"), requestId: 3),
        };

        Assert.True(cluster.RunUntil(() => requests.All(requestId => cluster.TryGetResponse(requestId, out _)), maxIterations: 5000));

        var values = ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster);
        var first = Assert.Single(values.Distinct());

        Assert.Equal(1, first.Version);
        Assert.Contains(first.Value, new[] { "A", "B", "C" });
        cluster.AssertSafetyInvariants();
    }

    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.AllModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task QuorumLoss_ThrowsDiagnosticsAcrossAllModes(ProtocolModeCase mode)
    {
        const int seed = 9203;
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        var error = Assert.Throws<InvalidOperationException>(() =>
            cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 50));

        Assert.Contains("seed: 9203", error.Message);
        Assert.Contains("reachableQuorumAvailable=False", error.Message);
    }
}

