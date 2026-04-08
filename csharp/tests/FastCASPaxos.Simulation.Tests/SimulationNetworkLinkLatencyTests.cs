using Clockwork;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class SimulationNetworkLinkLatencyTests
{
    [Fact]
    public void GetMessageDelay_WithPerLinkOverride_UsesOverride()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        network.MaxJitter = TimeSpan.Zero;

        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        var delay = network.GetMessageDelay("A", "B");
        Assert.Equal(TimeSpan.FromMilliseconds(50), delay);
    }

    [Fact]
    public void GetMessageDelay_WithoutPerLinkOverride_UsesGlobalDefault()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);
        network.MaxJitter = TimeSpan.Zero;

        // No per-link override set for A -> C
        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        var delay = network.GetMessageDelay("A", "C");
        Assert.Equal(TimeSpan.FromMilliseconds(10), delay);
    }

    [Fact]
    public void GetMessageDelay_DirectionalOverride_OnlyAppliesToConfiguredDirection()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        network.MaxJitter = TimeSpan.Zero;

        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        // A -> B uses override
        Assert.Equal(TimeSpan.FromMilliseconds(50), network.GetMessageDelay("A", "B"));

        // B -> A falls back to global default
        Assert.Equal(TimeSpan.FromMilliseconds(1), network.GetMessageDelay("B", "A"));
    }

    [Fact]
    public void SetBidirectionalLinkLatency_SetsBothDirections()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        network.MaxJitter = TimeSpan.Zero;

        network.SetBidirectionalLinkLatency("A", "B", TimeSpan.FromMilliseconds(80), TimeSpan.Zero);

        Assert.Equal(TimeSpan.FromMilliseconds(80), network.GetMessageDelay("A", "B"));
        Assert.Equal(TimeSpan.FromMilliseconds(80), network.GetMessageDelay("B", "A"));
    }

    [Fact]
    public void ClearLinkLatency_RestoresGlobalDefault()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(5);
        network.MaxJitter = TimeSpan.Zero;

        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(100), TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromMilliseconds(100), network.GetMessageDelay("A", "B"));

        var removed = network.ClearLinkLatency("A", "B");
        Assert.True(removed);
        Assert.Equal(TimeSpan.FromMilliseconds(5), network.GetMessageDelay("A", "B"));
    }

    [Fact]
    public void ClearLinkLatency_ReturnsFalseWhenNoOverrideExists()
    {
        var network = CreateNetwork();
        Assert.False(network.ClearLinkLatency("X", "Y"));
    }

    [Fact]
    public void ClearAllLinkLatencies_RemovesAllOverrides()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(2);
        network.MaxJitter = TimeSpan.Zero;

        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(50), TimeSpan.Zero);
        network.SetLinkLatency("C", "D", TimeSpan.FromMilliseconds(100), TimeSpan.Zero);

        network.ClearAllLinkLatencies();

        Assert.Equal(TimeSpan.FromMilliseconds(2), network.GetMessageDelay("A", "B"));
        Assert.Equal(TimeSpan.FromMilliseconds(2), network.GetMessageDelay("C", "D"));
        Assert.Empty(network.GetAllLinkLatencies());
    }

    [Fact]
    public void GetMessageDelay_LoopbackAlwaysZero_EvenWithPerLinkOverride()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);

        network.SetLinkLatency("A", "A", TimeSpan.FromMilliseconds(999), TimeSpan.Zero);

        Assert.Equal(TimeSpan.Zero, network.GetMessageDelay("A", "A"));
    }

    [Fact]
    public void GetMessageDelay_DelaysDisabled_ReturnsZeroEvenWithOverride()
    {
        var network = CreateNetwork();
        network.EnableDelays = false;
        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        Assert.Equal(TimeSpan.Zero, network.GetMessageDelay("A", "B"));
    }

    [Fact]
    public void GetMessageDelay_PerLinkJitter_IsBounded()
    {
        var network = CreateNetwork();
        network.EnableDelays = true;

        var baseDelay = TimeSpan.FromMilliseconds(10);
        var maxJitter = TimeSpan.FromMilliseconds(5);
        network.SetLinkLatency("A", "B", baseDelay, maxJitter);

        for (var i = 0; i < 100; i++)
        {
            var delay = network.GetMessageDelay("A", "B");
            Assert.True(delay >= baseDelay, $"Delay {delay} was less than base {baseDelay}");
            Assert.True(delay <= baseDelay + maxJitter, $"Delay {delay} exceeded base+jitter {baseDelay + maxJitter}");
        }
    }

    [Fact]
    public void GetLinkLatency_ReturnsNullForUnconfiguredLink()
    {
        var network = CreateNetwork();
        Assert.Null(network.GetLinkLatency("A", "B"));
    }

    [Fact]
    public void GetLinkLatency_ReturnsConfiguredLatency()
    {
        var network = CreateNetwork();
        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(42), TimeSpan.FromMilliseconds(7));

        var latency = network.GetLinkLatency("A", "B");
        Assert.NotNull(latency);
        Assert.Equal(TimeSpan.FromMilliseconds(42), latency.Value.BaseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(7), latency.Value.MaxJitter);
    }

    [Fact]
    public void GetAllLinkLatencies_ReturnsSnapshot()
    {
        var network = CreateNetwork();
        network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(10), TimeSpan.Zero);
        network.SetBidirectionalLinkLatency("C", "D", TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(5));

        var all = network.GetAllLinkLatencies();
        Assert.Equal(3, all.Count);
        Assert.True(all.ContainsKey(("A", "B")));
        Assert.True(all.ContainsKey(("C", "D")));
        Assert.True(all.ContainsKey(("D", "C")));
    }

    [Fact]
    public void SetLinkLatency_ThrowsOnNegativeBaseDelay()
    {
        var network = CreateNetwork();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            network.SetLinkLatency("A", "B", TimeSpan.FromMilliseconds(-1), TimeSpan.Zero));
    }

    [Fact]
    public void SetLinkLatency_ThrowsOnNegativeMaxJitter()
    {
        var network = CreateNetwork();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            network.SetLinkLatency("A", "B", TimeSpan.Zero, TimeSpan.FromMilliseconds(-1)));
    }

    private static SimulationNetwork CreateNetwork(int seed = 42) =>
        new(() => [], new SimulationRandom(seed));
}
