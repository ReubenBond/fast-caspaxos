using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class CrashRestartScenarioTests
{
    [Fact]
    public async Task AcceptorRestart_PreservesDurableStateAndAllowsNewCommits()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"));
        cluster.CrashNode(FastCasAddress.Acceptor(1));
        cluster.RestartNode(FastCasAddress.Acceptor(1));
        var second = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(2, "B"));

        var acceptor = Assert.IsType<FastCasAcceptorHost<StringValue>>(cluster.AcceptorNodes[0].Host);
        Assert.Equal("AB", second.CommittedValue.Value);
        Assert.Equal("AB", acceptor.State.AcceptedValue.Value);
    }

    [Fact]
    public async Task ProposerRestart_LosesVolatileStateButClusterStillProgresses()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"));
        cluster.CrashNode(FastCasAddress.Proposer(1));
        cluster.RestartNode(FastCasAddress.Proposer(1));

        var response = cluster.RunProposal(FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(2, "B"));

        Assert.Equal("AB", response.CommittedValue.Value);
        cluster.AssertSafetyInvariants();
    }

    private static FastCasSimulationCluster<StringValue> CreateCluster() =>
        new(
            seed: 555,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

