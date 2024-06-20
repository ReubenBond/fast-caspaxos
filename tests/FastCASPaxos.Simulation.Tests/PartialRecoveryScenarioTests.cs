using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class PartialRecoveryScenarioTests
{
    [Fact]
    public async Task StaggeredMultiAcceptorRestart_AllowsLinearProgressWithoutHistoryGaps()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 9841,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), maxIterations: 5000);

        cluster.CrashNode(FastCasAddress.Acceptor(1));
        cluster.CrashNode(FastCasAddress.Acceptor(2));

        _ = cluster.RunProposal(FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(2, "B"), maxIterations: 5000);

        cluster.RestartNode(FastCasAddress.Acceptor(1));
        cluster.RestartNode(FastCasAddress.Acceptor(2));

        var response = cluster.RunProposal(FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(3, "C"), maxIterations: 5000);

        Assert.Equal("ABC", response.CommittedValue.Value);
        Assert.Equal(3, response.CommittedValue.Version);

        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, StringScenarioOperations.Read(), maxIterations: 5000);
            var host = Assert.IsType<FastCasProposerHost<StringValue>>(proposer.Host);
            Assert.Equal(3, host.CachedValue.Version);
            Assert.Equal("ABC", host.CachedValue.Value);
        }

        foreach (var acceptor in new[] { cluster.AcceptorNodes[0], cluster.AcceptorNodes[1] })
        {
            var host = Assert.IsType<FastCasAcceptorHost<StringValue>>(acceptor.Host);
            Assert.Equal(3, host.State.AcceptedValue.Version);
            Assert.Equal("ABC", host.State.AcceptedValue.Value);
        }

        cluster.AssertSafetyInvariants();
    }
}

