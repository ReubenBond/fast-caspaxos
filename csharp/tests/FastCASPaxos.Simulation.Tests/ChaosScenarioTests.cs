using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class ChaosScenarioTests
{
    [Fact]
    public async Task BoundedChaosSequence_PreservesSafetyAndCompletes()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 777,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);
        var random = new Random(888);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();

        for (var version = 1; version <= 8; version++)
        {
            var proposer = proposers[version % proposers.Length];
            var fault = random.Next(4);

            switch (fault)
            {
                case 0:
                    cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                    break;
                case 1:
                    cluster.SuspendNode(FastCasAddress.Acceptor(2));
                    break;
                case 2:
                    cluster.CrashNode(FastCasAddress.Acceptor(3));
                    break;
                default:
                    cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5));
                    break;
            }

            var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendAtVersion(version, version.ToString()));
            Assert.Equal(version, response.CommittedValue.Version);

            cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
            cluster.ResumeNode(FastCasAddress.Acceptor(2));
            cluster.RestartNode(FastCasAddress.Acceptor(3));
            cluster.ConfigureMessageDelays(TimeSpan.Zero, TimeSpan.Zero);
        }

        cluster.AssertSafetyInvariants();
        Assert.Equal(8, cluster.ClientResponses.Max(response => response.Value.CommittedValue.Version));
    }
}

