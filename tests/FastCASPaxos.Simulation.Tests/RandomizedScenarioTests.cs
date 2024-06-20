using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class RandomizedScenarioTests
{
    [Fact]
    public async Task RandomizedAppendMix_ConvergesToSingleDeterministicResult()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 333,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        const string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var operationRandom = new Random(444);
        var proposerRandom = new Random(555);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var expected = new List<char>(capacity: 12);

        foreach (var _ in Enumerable.Range(0, 12))
        {
            var next = charset[operationRandom.Next(charset.Length)];
            expected.Add(next);

            var response = cluster.RunProposal(
                proposers[proposerRandom.Next(proposers.Length)],
                StringScenarioOperations.AppendCharacter(next));

            Assert.Equal(expected.Count, response.CommittedValue.Version);
            Assert.Equal(new string([.. expected]), response.CommittedValue.Value);
        }

        var expectedValue = new string([.. expected]);
        foreach (var proposer in proposers)
        {
            _ = cluster.RunProposal(proposer, StringScenarioOperations.Read());
            var host = Assert.IsType<FastCasProposerHost<StringValue>>(
                cluster.ProposerNodes.Single(node => node.Host.Address == proposer).Host);
            Assert.Equal(12, host.CachedValue.Version);
            Assert.Equal(expectedValue, host.CachedValue.Value);
        }

        cluster.AssertSafetyInvariants();
    }
}

