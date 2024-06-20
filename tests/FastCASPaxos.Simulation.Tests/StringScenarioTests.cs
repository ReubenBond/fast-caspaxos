using FastCASPaxos.Model;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class StringScenarioTests
{
    [Fact]
    public async Task AppendSequence_ReachesExpectedValueAcrossMultipleProposers()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        const string expected = "Hello";

        for (var index = 0; index < expected.Length; index++)
        {
            var response = cluster.RunProposal(
                proposers[index % proposers.Length],
                StringScenarioOperations.AppendCharacter(expected[index]));

            Assert.Equal(index + 1, response.CommittedValue.Version);
            Assert.Equal(expected[..(index + 1)], response.CommittedValue.Value);
        }

        foreach (var proposer in proposers)
        {
            _ = cluster.RunProposal(proposer, StringScenarioOperations.Read());
            var host = Assert.IsType<FastCasProposerHost<StringValue>>(
                cluster.ProposerNodes.Single(node => node.Host.Address == proposer).Host);
            Assert.Equal(5, host.CachedValue.Version);
            Assert.Equal(expected, host.CachedValue.Value);
        }

        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task DuplicateAndReadOperations_RemainConsistent()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var first = cluster.RunProposal(proposers[0], StringScenarioOperations.AppendAtVersion(1, "A"));
        Assert.Equal("A", first.CommittedValue.Value);

        var duplicate = cluster.RunProposal(proposers[1], StringScenarioOperations.AppendAtVersion(1, "A"));
        Assert.Equal("A", duplicate.CommittedValue.Value);

        _ = cluster.RunProposal(proposers[2], StringScenarioOperations.Read());
        var thirdHost = Assert.IsType<FastCasProposerHost<StringValue>>(
            cluster.ProposerNodes.Single(node => node.Host.Address == proposers[2]).Host);
        Assert.Equal(1, thirdHost.CachedValue.Version);
        Assert.Equal("A", thirdHost.CachedValue.Value);

        var second = cluster.RunProposal(proposers[0], StringScenarioOperations.AppendAtVersion(2, "B"));
        Assert.Equal("AB", second.CommittedValue.Value);

        var third = cluster.RunProposal(proposers[1], StringScenarioOperations.AppendAtVersion(3, "C"));
        Assert.Equal("ABC", third.CommittedValue.Value);

        foreach (var proposer in proposers)
        {
            _ = cluster.RunProposal(proposer, StringScenarioOperations.Read());
            var host = Assert.IsType<FastCasProposerHost<StringValue>>(
                cluster.ProposerNodes.Single(node => node.Host.Address == proposer).Host);
            Assert.Equal(3, host.CachedValue.Version);
            Assert.Equal("ABC", host.CachedValue.Value);
        }

        cluster.AssertSafetyInvariants();
    }

    private static FastCasSimulationCluster<StringValue> CreateCluster() =>
        new(
            seed: 111,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

