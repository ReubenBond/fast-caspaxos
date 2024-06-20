using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class SeededLivenessStressTests
{
    [Theory]
    [MemberData(nameof(ProtocolOptionMatrix.ModeSeedMatrix), MemberType = typeof(ProtocolOptionMatrix))]
    public async Task SeededTransientFaultMix_ConvergesWithReplayableOutcome(ProtocolModeCase mode, int seed)
    {
        await using var cluster = ProtocolOptionMatrix.CreateStringCluster(mode, seed);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        const string charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        var operationRandom = new Random(seed + 100);
        var proposerRandom = new Random(seed + 200);
        var faultRandom = new Random(seed + 300);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        // This scenario models replayable CAS-style appends, so each request carries the version it
        // intends to extend instead of behaving like a blind append against a stale local cache.
        List<char> expected = [];

        foreach (var iteration in Enumerable.Range(0, 8))
        {
            _ = iteration;
            var next = charset[operationRandom.Next(charset.Length)];
            expected.Add(next);
            var expectedVersion = expected.Count;

            var proposer = proposers[proposerRandom.Next(proposers.Length)];
            var fault = ApplyTransientFault(cluster, proposer, faultRandom);
            try
            {
                var response = cluster.RunProposal(
                    proposer,
                    StringScenarioOperations.AppendAtVersion(expectedVersion, next.ToString()),
                    maxIterations: 5000);
                Assert.Equal(expectedVersion, response.CommittedValue.Version);
                Assert.Equal(new string([.. expected]), response.CommittedValue.Value);
            }
            finally
            {
                ClearTransientFault(cluster, proposer, fault);
            }
        }

        var expectedValue = new string([.. expected]);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expected.Count, expectedValue);
        cluster.AssertSafetyInvariants();
    }

    private static FaultKind ApplyTransientFault(
        FastCasSimulationCluster<StringValue> cluster,
        FastCasAddress proposer,
        Random faultRandom)
    {
        var fault = (FaultKind)faultRandom.Next(3);
        switch (fault)
        {
            case FaultKind.None:
                break;
            case FaultKind.Delay:
                cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(3));
                break;
            case FaultKind.Partition:
                cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                break;
            default:
                throw new InvalidOperationException($"Unsupported transient fault '{fault}'.");
        }

        return fault;
    }

    private static void ClearTransientFault(
        FastCasSimulationCluster<StringValue> cluster,
        FastCasAddress proposer,
        FaultKind fault)
    {
        switch (fault)
        {
            case FaultKind.None:
                break;
            case FaultKind.Delay:
                cluster.ConfigureMessageDelays(TimeSpan.Zero, TimeSpan.Zero);
                break;
            case FaultKind.Partition:
                cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                break;
            default:
                throw new InvalidOperationException($"Unsupported transient fault '{fault}'.");
        }
    }

    private enum FaultKind
    {
        None,
        Delay,
        Partition,
    }
}

