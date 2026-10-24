using System.Collections.Immutable;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class CorpusScenarioTests
{
    [Fact]
    public async Task StringCorpusReachesHelloWorld()
    {
        await using var cluster = CreateStringCluster(seed: 8801);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var operations = TestCorpus.BuildStringOperations();
        var expected = TestCorpus.StringExpectedValue;

        for (var index = 0; index < operations.Length; index++)
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
    public async Task SetCorpusReachesExpectedMembership()
    {
        await using var cluster = CreateSetCluster(seed: 8802);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = TestCorpus.BuildSetOperations();
        var expectedVersion = 0;

        for (var index = 0; index < operations.Length; index++)
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
    public async Task RandomStringCorpusReachesDeterministicValue()
    {
        await using var cluster = CreateStringCluster(seed: 8803);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var proposerRandom = new Random(8804);
        var (operations, expected) = TestCorpus.BuildRandomStringOperations();

        for (var index = 0; index < operations.Length; index++)
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
    public async Task ForkingStringCorpusResolvesToSingleCommittedChain()
    {
        await using var cluster = CreateStringCluster(seed: 8805);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = TestCorpus.BuildForkingStringOperations();

        for (var index = 0; index < operations.Length; index++)
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

    private static ImmutableArray<StringValue> ReadBackStringValues(
        FastCasSimulationCluster<StringValue> cluster,
        Func<IOperation<StringValue>> createReadOperation)
    {
        var values = ImmutableArray.CreateBuilder<StringValue>(cluster.ProposerNodes.Length);
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, createReadOperation());
            values.Add(Assert.IsType<FastCasProposerHost<StringValue>>(proposer.Host).CachedValue);
        }

        return values.MoveToImmutable();
    }

    private static ImmutableArray<SetValue> ReadBackSetValues(
        FastCasSimulationCluster<SetValue> cluster,
        Func<IOperation<SetValue>> createReadOperation)
    {
        var values = ImmutableArray.CreateBuilder<SetValue>(cluster.ProposerNodes.Length);
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, createReadOperation());
            values.Add(Assert.IsType<FastCasProposerHost<SetValue>>(proposer.Host).CachedValue);
        }

        return values.MoveToImmutable();
    }

    private static FastCasSimulationCluster<StringValue> CreateStringCluster(int seed) =>
        new(
            seed,
            initialValue: default,
            options: new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

    private static FastCasSimulationCluster<SetValue> CreateSetCluster(int seed) =>
        new(
            seed,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}

