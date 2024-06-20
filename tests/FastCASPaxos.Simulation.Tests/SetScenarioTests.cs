using FastCASPaxos.Model;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class SetScenarioTests
{
    [Fact]
    public async Task UniqueAdds_ReachesExpectedSet()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var expected = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (value, index) in new[] { "A", "B", "C" }.Select((value, index) => (value, index)))
        {
            expected.Add(value);
            var response = cluster.RunProposal(proposers[index % proposers.Length], SetScenarioOperations.Add(value));

            Assert.Equal(index + 1, response.CommittedValue.Version);
            Assert.True(response.CommittedValue.Value.SetEquals(expected));
        }

        foreach (var proposer in proposers)
        {
            _ = cluster.RunProposal(proposer, SetScenarioOperations.Read());
            var host = Assert.IsType<FastCasProposerHost<SetValue>>(
                cluster.ProposerNodes.Single(node => node.Host.Address == proposer).Host);
            Assert.Equal(3, host.CachedValue.Version);
            Assert.True(host.CachedValue.Value.SetEquals(expected));
        }

        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task DuplicateAdds_AreIdempotent()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var first = cluster.RunProposal(proposers[0], SetScenarioOperations.Add("A"));
        Assert.True(first.CommittedValue.Value.SetEquals(["A"]));

        var second = cluster.RunProposal(proposers[1], SetScenarioOperations.Add("A"));
        Assert.True(second.CommittedValue.Value.SetEquals(["A"]));

        var third = cluster.RunProposal(proposers[0], SetScenarioOperations.Add("B"));
        Assert.True(third.CommittedValue.Value.SetEquals(["A", "B"]));

        var fourth = cluster.RunProposal(proposers[1], SetScenarioOperations.Add("B"));
        Assert.True(fourth.CommittedValue.Value.SetEquals(["A", "B"]));

        foreach (var proposer in proposers)
        {
            _ = cluster.RunProposal(proposer, SetScenarioOperations.Read());
            var host = Assert.IsType<FastCasProposerHost<SetValue>>(
                cluster.ProposerNodes.Single(node => node.Host.Address == proposer).Host);
            Assert.Equal(2, host.CachedValue.Version);
            Assert.True(host.CachedValue.Value.SetEquals(["A", "B"]));
        }

        cluster.AssertSafetyInvariants();
    }

    private static FastCasSimulationCluster<SetValue> CreateCluster() =>
        new(
            seed: 222,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

