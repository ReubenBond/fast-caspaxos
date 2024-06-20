using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class OptionMatrixScenarioTests
{
    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.AllModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task AppendSequence_ReachesExpectedValueAcrossAllModes(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9101);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        const string expected = "HELLO";

        for (var index = 0; index < expected.Length; index++)
        {
            _ = cluster.RunProposal(
                proposers[index % proposers.Length],
                StringScenarioOperations.AppendCharacter(expected[index]),
                maxIterations: 5000);
        }

        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expected.Length, expected);
        cluster.AssertSafetyInvariants();
    }

    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.AllModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task DuplicateWritesAndReads_RemainConsistentAcrossAllModes(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9102);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        _ = cluster.RunProposal(proposers[0], StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        _ = cluster.RunProposal(proposers[1], StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expectedVersion: 1, expectedValue: "A");

        _ = cluster.RunProposal(proposers[0], StringScenarioOperations.AppendAtVersion(2, "B"), maxIterations: 5000);
        _ = cluster.RunProposal(proposers[1], StringScenarioOperations.AppendAtVersion(3, "C"), maxIterations: 5000);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expectedVersion: 3, expectedValue: "ABC");

        cluster.AssertSafetyInvariants();
    }

    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.AllModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task MajorityPartition_BlocksUntilHealedAcrossAllModes(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9103);
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

        _ = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expectedVersion: 1, expectedValue: "A");
        cluster.AssertSafetyInvariants();
    }

    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.AllModes), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task AcceptorRestart_AllowsNewCommitsAcrossAllModes(ProtocolModeCase mode)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed: 9104);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);
        cluster.CrashNode(FastCasAddress.Acceptor(1));
        cluster.RestartNode(FastCasAddress.Acceptor(1));

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(2, "B"), maxIterations: 5000);

        var acceptor = Assert.IsType<FastCasAcceptorHost<StringValue>>(cluster.AcceptorNodes[0].Host);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expectedVersion: 2, expectedValue: "AB");
        Assert.Equal("AB", acceptor.State.AcceptedValue.Value);
        cluster.AssertSafetyInvariants();
    }
}

