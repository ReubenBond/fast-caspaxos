using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class DeterminismScenarioTests
{
    [Fact]
    public async Task SameSeed_ReplaysIdenticalTrace()
    {
        var left = await CaptureTrace(seed: 9901);
        var right = await CaptureTrace(seed: 9901);

        Assert.Equal(left, right);
    }

    [Fact]
    public async Task DifferentSeeds_ChangeTimedTrace()
    {
        var left = await CaptureTrace(seed: 9901);
        var right = await CaptureTrace(seed: 9902);

        Assert.NotEqual(left, right);
    }

    private static async Task<IReadOnlyList<string>> CaptureTrace(int seed)
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);
        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(3), TimeSpan.FromMilliseconds(11));

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var proposerRandom = new Random(4401);
        var (operations, expectedValue) = CoyoteParityCorpus.BuildLegacyRandomStringOperations(seed: 4402);
        List<string> trace = [];

        foreach (var operation in operations)
        {
            var proposer = proposers[proposerRandom.Next(proposers.Length)];
            var response = cluster.RunProposal(proposer, operation);
            trace.Add($"{cluster.TimeProvider.GetUtcNow():O}|proposal|{proposer}|{response.CommittedValue}");
        }

        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, StringScenarioOperations.Read());
            var cached = Assert.IsType<FastCasProposerHost<StringValue>>(proposer.Host).CachedValue;
            Assert.Equal(expectedValue, cached.Value);
            trace.Add($"{cluster.TimeProvider.GetUtcNow():O}|read|{proposer.Host.Address}|{cached}");
        }

        cluster.AssertSafetyInvariants();
        return trace;
    }
}

