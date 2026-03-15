using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationLatencyAwareTimingTests
{
    [Fact]
    public async Task CreateConfiguration_WithoutConfiguredLatency_PreservesFixedFallbackTiming()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var timing = cluster.Options.GetProposerTimingSnapshot();

        Assert.Equal("fixed-defaults", timing.Source);
        Assert.Equal(TimeSpan.Zero, timing.ExpectedOneWayLatency);
        Assert.Equal(TimeSpan.Zero, timing.ExpectedRoundTripLatency);
        Assert.True(timing.RetryBackoffEnabled);
        Assert.Equal(2, timing.RetryStartAfterFailures);
        Assert.Equal(TimeSpan.FromMilliseconds(4), timing.RetryBaseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(64), timing.RetryMaxDelay);
        Assert.Equal(0.5, timing.RetryJitterRatio);
        Assert.Equal(TimeSpan.FromSeconds(1), timing.ResponseTimeout);
    }

    [Fact]
    public async Task ConfigureMessageDelays_DerivesLatencyAwareProposerTimingFromExpectedRtt()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(5));
        var timing = cluster.Options.GetProposerTimingSnapshot();

        Assert.Equal("latency-aware-derived", timing.Source);
        Assert.Equal(TimeSpan.FromMilliseconds(45), timing.ExpectedOneWayLatency);
        Assert.Equal(TimeSpan.FromMilliseconds(90), timing.ExpectedRoundTripLatency);
        Assert.True(timing.RetryBackoffEnabled);
        Assert.Equal(0, timing.RetryStartAfterFailures);
        Assert.Equal(TimeSpan.FromMilliseconds(270), timing.RetryBaseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(1800), timing.RetryMaxDelay);
        Assert.Equal(0.5, timing.RetryJitterRatio);
        Assert.Equal(TimeSpan.FromMilliseconds(360), timing.ResponseTimeout);
    }

    [Fact]
    public async Task ConfigureMessageDelays_PreservesExplicitProposerTimingOverrides()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 777,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
                ProposerRetryBackoff = new FastCasSimulationProposerRetryBackoffOptions
                {
                    StartAfterFailures = 1,
                    BaseDelay = TimeSpan.FromMilliseconds(250),
                    MaxDelay = TimeSpan.FromMilliseconds(750),
                    JitterRatio = 0.25,
                },
                ProposerResponseTimeout = TimeSpan.FromMilliseconds(500),
            },
            startDateTime: DateTimeOffset.UnixEpoch);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(5));
        var timing = cluster.Options.GetProposerTimingSnapshot();

        Assert.Equal("explicit-overrides", timing.Source);
        Assert.Equal(TimeSpan.FromMilliseconds(45), timing.ExpectedOneWayLatency);
        Assert.Equal(TimeSpan.FromMilliseconds(90), timing.ExpectedRoundTripLatency);
        Assert.Equal(1, timing.RetryStartAfterFailures);
        Assert.Equal(TimeSpan.FromMilliseconds(250), timing.RetryBaseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(750), timing.RetryMaxDelay);
        Assert.Equal(0.25, timing.RetryJitterRatio);
        Assert.Equal(TimeSpan.FromMilliseconds(500), timing.ResponseTimeout);
    }

    private static FastCasSimulationCluster<StringValue> CreateCluster() =>
        new(
            seed: 444,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}
