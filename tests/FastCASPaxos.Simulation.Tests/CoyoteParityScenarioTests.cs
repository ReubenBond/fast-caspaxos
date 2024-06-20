using FastCASPaxos.Model;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class CoyoteParityScenarioTests
{
    [Fact]
    public async Task LegacyStringCorpus_ReachesHelloWorld()
    {
        await using var cluster = CreateStringCluster(seed: 8801);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var operations = CoyoteParityCorpus.BuildLegacyStringOperations();
        var expected = CoyoteParityCorpus.LegacyStringExpectedValue;

        for (var index = 0; index < operations.Count; index++)
        {
            var response = cluster.RunProposal(proposers[index % proposers.Length], operations[index]);
            Assert.Equal(index + 1, response.CommittedValue.Version);
            Assert.Equal(expected[..(index + 1)], response.CommittedValue.Value);
        }

        foreach (var value in ReadBackStringValues(cluster, StringScenarioOperations.Read))
        {
            Assert.Equal(expected.Length, value.Version);
            Assert.Equal(expected, value.Value);
        }

        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task LegacySetCorpus_ReachesExpectedMembership()
    {
        await using var cluster = CreateSetCluster(seed: 8802);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = CoyoteParityCorpus.BuildLegacySetOperations();
        var expectedVersion = 0;

        for (var index = 0; index < operations.Count; index++)
        {
            var response = cluster.RunProposal(proposers[index % proposers.Length], operations[index]);
            var committed = response.CommittedValue;
            if (committed.Version > expectedVersion)
            {
                expectedVersion = committed.Version;
            }
        }

        foreach (var value in ReadBackSetValues(cluster, SetScenarioOperations.Read))
        {
            Assert.Equal(expectedVersion, value.Version);
            Assert.True(value.Value.SetEquals(expected));
        }

        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task LegacyRandomStringCorpus_ReachesDeterministicValue()
    {
        await using var cluster = CreateStringCluster(seed: 8803);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var proposerRandom = new Random(8804);
        var (operations, expected) = CoyoteParityCorpus.BuildLegacyRandomStringOperations();

        for (var index = 0; index < operations.Count; index++)
        {
            var response = cluster.RunProposal(proposers[proposerRandom.Next(proposers.Length)], operations[index]);
            Assert.Equal(index + 1, response.CommittedValue.Version);
        }

        foreach (var value in ReadBackStringValues(cluster, StringScenarioOperations.Read))
        {
            Assert.Equal(expected.Length, value.Version);
            Assert.Equal(expected, value.Value);
        }

        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task LegacyForkingStringCorpus_ResolvesToSingleCommittedChain()
    {
        await using var cluster = CreateStringCluster(seed: 8805);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = CoyoteParityCorpus.BuildLegacyForkingStringOperations();

        for (var index = 0; index < operations.Count; index++)
        {
            _ = cluster.RunProposal(proposers[index % proposers.Length], operations[index]);
        }

        foreach (var value in ReadBackStringValues(cluster, StringScenarioOperations.Read))
        {
            Assert.Equal(expected.Length, value.Version);
            Assert.Equal(expected, value.Value);
        }

        cluster.AssertSafetyInvariants();
    }

    private static IReadOnlyList<StringValue> ReadBackStringValues(
        FastCasSimulationCluster<StringValue> cluster,
        Func<IOperation<StringValue>> createReadOperation)
    {
        List<StringValue> values = [];
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, createReadOperation());
            values.Add(Assert.IsType<FastCasProposerHost<StringValue>>(proposer.Host).CachedValue);
        }

        return values;
    }

    private static IReadOnlyList<SetValue> ReadBackSetValues(
        FastCasSimulationCluster<SetValue> cluster,
        Func<IOperation<SetValue>> createReadOperation)
    {
        List<SetValue> values = [];
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, createReadOperation());
            values.Add(Assert.IsType<FastCasProposerHost<SetValue>>(proposer.Host).CachedValue);
        }

        return values;
    }

    private static FastCasSimulationCluster<StringValue> CreateStringCluster(int seed) =>
        new(
            seed,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

    private static FastCasSimulationCluster<SetValue> CreateSetCluster(int seed) =>
        new(
            seed,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

