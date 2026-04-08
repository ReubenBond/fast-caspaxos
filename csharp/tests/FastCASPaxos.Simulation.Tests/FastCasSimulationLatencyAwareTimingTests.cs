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
        Assert.Equal(TimeSpan.FromMilliseconds(50), timing.RetryBaseDelay);
        Assert.Equal(TimeSpan.FromSeconds(10), timing.RetryMaxDelay);
        Assert.Equal(0.5, timing.RetryJitterRatio);
        Assert.Equal(TimeSpan.FromSeconds(1), timing.ResponseTimeout);
    }

    [Fact]
    public async Task ConfigureMessageDelays_PreservesFixedTimingWhenLatencyAwareDisabled()
    {
        await using var cluster = CreateCluster();
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(5));
        var timing = cluster.Options.GetProposerTimingSnapshot();

        Assert.Equal("fixed-defaults", timing.Source);
        Assert.Equal(TimeSpan.FromMilliseconds(45), timing.ExpectedOneWayLatency);
        Assert.Equal(TimeSpan.FromMilliseconds(90), timing.ExpectedRoundTripLatency);
        Assert.True(timing.RetryBackoffEnabled);
        Assert.Equal(2, timing.RetryStartAfterFailures);
        Assert.Equal(TimeSpan.FromMilliseconds(50), timing.RetryBaseDelay);
        Assert.Equal(TimeSpan.FromSeconds(10), timing.RetryMaxDelay);
        Assert.Equal(0.5, timing.RetryJitterRatio);
        Assert.Equal(TimeSpan.FromSeconds(1), timing.ResponseTimeout);
    }

    [Fact]
    public async Task ConfigureMessageDelays_PreservesExplicitProposerTimingOverrides()
    {
        await using var cluster = new FastCasSimulationCluster<StringValue>(
            seed: 777,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
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

        Assert.Equal("fixed-defaults", timing.Source);
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
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);
}
