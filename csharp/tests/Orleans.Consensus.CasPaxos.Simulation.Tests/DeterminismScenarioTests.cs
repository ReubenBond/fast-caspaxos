using System.Collections.Immutable;
using System.Globalization;
using Orleans.Consensus.CasPaxos.Simulation;
using Orleans.Consensus.CasPaxos.Simulation.Hosts;
using Orleans.Consensus.CasPaxos.Simulation.Scenarios;
using Orleans.Testing.Simulation;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class DeterminismScenarioTests
{
    [Fact]
    public async Task SameSeedReplaysIdenticalTrace()
    {
        var left = await CaptureTrace(seed: 9901);
        var right = await CaptureTrace(seed: 9901);

        Assert.Equal<string>(left, right);
    }

    [Fact]
    public async Task DifferentSeedsChangeTimedTrace()
    {
        var left = await CaptureTrace(seed: 9901);
        var right = await CaptureTrace(seed: 9902);

        Assert.NotEqual<string>(left, right);
    }

    [Fact]
    public async Task SameSeedReplaysTraceUnderFaultsAndDifferentSeedsChangeIt()
    {
        var left = await CaptureFaultInjectedTrace(seed: 9903);

        Assert.Equal<string>(left, await CaptureFaultInjectedTrace(seed: 9903));
        Assert.NotEqual<string>(left, await CaptureFaultInjectedTrace(seed: 9904));
    }

    /// <summary>
    /// Drives proposals through seeded message drops and jitter while a partition, a suspension, a crash and restart,
    /// and a per-link latency override take turns, recording every response, fault, and transport outcome.
    /// </summary>
    private static async Task<ImmutableArray<string>> CaptureFaultInjectedTrace(int seed)
    {
        var transport = new TransportTrace();
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed,
            initialValue: default,
            options: new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
                TransportObserver = transport,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);
        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(4));
        cluster.SetMessageDropRate(0.1);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        List<string> trace = [];

        for (var version = 1; version <= 8; version++)
        {
            var proposer = proposers[version % proposers.Length];
            switch (version % 4)
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
                    cluster.ConfigureBidirectionalLinkLatency(proposer, FastCasAddress.Acceptor(4), TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(10));
                    break;
            }

            // A dropped request or reply leaves a proposal without a response, which the trace records like any other outcome.
            var requestId = cluster.SendProposal(proposer, StringScenarioOperations.AppendAtVersion(version, version.ToString(CultureInfo.InvariantCulture)));
            trace.Add(cluster.RunUntil(() => cluster.TryGetResponse(requestId, out _))
                && cluster.TryGetResponse(requestId, out var response)
                    ? $"{cluster.TimeProvider.GetUtcNow():O}|response|{proposer}|{response}"
                    : $"{cluster.TimeProvider.GetUtcNow():O}|no-response|{proposer}");

            cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
            cluster.ResumeNode(FastCasAddress.Acceptor(2));
            cluster.RestartNode(FastCasAddress.Acceptor(3));
            cluster.ClearBidirectionalLinkLatency(proposer, FastCasAddress.Acceptor(4));
        }

        cluster.AssertSafetyInvariants();
        trace.AddRange(cluster.FaultHistory);
        trace.AddRange(transport.Describe());
        return [.. trace];
    }

    private static async Task<ImmutableArray<string>> CaptureTrace(int seed)
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed,
            initialValue: default,
            options: new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);
        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(3), TimeSpan.FromMilliseconds(11));

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var proposerRandom = new Random(4401);
        var (operations, expectedValue) = TestCorpus.BuildRandomStringOperations(seed: 4402);
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
        return [.. trace];
    }

    private sealed class TransportTrace : IFastCasSimulationTransportObserver
    {
        private readonly SortedDictionary<string, int> _counts = new(StringComparer.Ordinal);

        public void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status)
        {
            var key = $"{kind}|{status}";
            _counts[key] = _counts.GetValueOrDefault(key) + 1;
        }

        public IEnumerable<string> Describe() => _counts.Select(entry => $"transport|{entry.Key}|{entry.Value}");
    }
}

