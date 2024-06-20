using Clockwork;
using FastCASPaxos.Simulation;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationProposerRetryBackoffOptionsTests
{
    [Fact]
    public void GetRetryDelay_DoesNotDelayBeforeThresholdIsExceeded()
    {
        var options = new FastCasSimulationProposerRetryBackoffOptions
        {
            StartAfterFailures = 2,
            BaseDelay = TimeSpan.FromMilliseconds(4),
            MaxDelay = TimeSpan.FromMilliseconds(64),
        };
        var random = new SimulationRandom(1234);

        Assert.Equal(TimeSpan.Zero, options.GetRetryDelay(0, random));
        Assert.Equal(TimeSpan.Zero, options.GetRetryDelay(1, random));
        Assert.Equal(TimeSpan.Zero, options.GetRetryDelay(2, random));
    }

    [Fact]
    public void GetRetryDelay_UsesDeterministicJitterAndHonorsMaxDelay()
    {
        var options = new FastCasSimulationProposerRetryBackoffOptions
        {
            StartAfterFailures = 2,
            BaseDelay = TimeSpan.FromMilliseconds(4),
            MaxDelay = TimeSpan.FromMilliseconds(20),
        };

        var thirdRetry = options.GetRetryDelay(3, new SimulationRandom(99));
        var fourthRetry = options.GetRetryDelay(4, new SimulationRandom(99));
        var cappedRetry = options.GetRetryDelay(20, new SimulationRandom(99));

        Assert.InRange(thirdRetry, TimeSpan.FromMilliseconds(4), TimeSpan.FromMilliseconds(8));
        Assert.InRange(fourthRetry, TimeSpan.FromMilliseconds(8), TimeSpan.FromMilliseconds(16));
        Assert.InRange(cappedRetry, TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(20));
        Assert.Equal(thirdRetry, options.GetRetryDelay(3, new SimulationRandom(99)));
        Assert.Equal(fourthRetry, options.GetRetryDelay(4, new SimulationRandom(99)));
    }
}
