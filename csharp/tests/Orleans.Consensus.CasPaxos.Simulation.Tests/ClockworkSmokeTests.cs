using Orleans.Testing.Simulation;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class ClockworkSmokeTests
{
    [Fact]
    public void SimulationRandomWithSameSeedProducesSameSequence()
    {
        var left = new SimulationRandom(seed: 12345);
        var right = new SimulationRandom(seed: 12345);

        var leftValues = Enumerable.Range(0, 8).Select(_ => left.Next()).ToArray();
        var rightValues = Enumerable.Range(0, 8).Select(_ => right.Next()).ToArray();

        Assert.Equal(leftValues, rightValues);
    }

    [Fact]
    public async Task SchedulerLaneExecutesDelayedWorkAfterVirtualTimeAdvances()
    {
        await using var cluster = new SimulationCluster(seed: 1, startDateTime: DateTimeOffset.UnixEpoch);
        var lane = cluster.SchedulerLane;
        var executed = new List<int>();

        lane.Enqueue(() => executed.Add(1));
        lane.EnqueueAfter(() => executed.Add(2), TimeSpan.FromSeconds(5));

        Assert.Equal(1, lane.RunUntilIdle(CancellationToken.None));
        Assert.Equal([1], executed);

        cluster.RunFor(TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.Equal([1, 2], executed);
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(5), cluster.TimeProvider.GetUtcNow());
    }

    [Fact]
    public async Task SimulationNetworkBlocksAndHealsBidirectionalPartitions()
    {
        await using var cluster = new SimulationCluster(seed: 777, startDateTime: DateTimeOffset.UnixEpoch);
        _ = cluster.AddNode("node-1");
        _ = cluster.AddNode("node-2");
        var network = cluster.Network;

        Assert.True(network.CanDeliver("node-1", "node-2"));
        Assert.True(network.CanDeliver("node-2", "node-1"));

        network.CreateBidirectionalPartition("node-1", "node-2");

        Assert.Equal(DeliveryStatus.Partitioned, network.CheckDelivery("node-1", "node-2"));
        Assert.Equal(DeliveryStatus.Partitioned, network.CheckDelivery("node-2", "node-1"));

        network.HealBidirectionalPartition("node-1", "node-2");

        Assert.True(network.CanDeliver("node-1", "node-2"));
        Assert.True(network.CanDeliver("node-2", "node-1"));
    }

    [Fact]
    public async Task SimulationClusterRunUntilCompletesScheduledTask()
    {
        await using var cluster = new SimulationCluster(seed: 9001, startDateTime: DateTimeOffset.UnixEpoch);
        var executed = false;
        var task = new Task(() => executed = true);

        task.Start(cluster.TaskScheduler);

        var result = cluster.RunUntil(() => executed, CancellationToken.None, maxIterations: 10);

        Assert.Equal(SimulationExecutionReason.ConditionMet, result.Reason);
        Assert.True(executed);
    }

    [Fact]
    public async Task SimulationClusterRunUntilIdleAdvancesTimeForNodeWork()
    {
        await using var cluster = new SimulationCluster(seed: 42, startDateTime: DateTimeOffset.UnixEpoch);
        var node = cluster.AddNode("node-1");
        var executed = false;
        var startTime = cluster.TimeProvider.GetUtcNow();

        node.Context.SchedulerLane.EnqueueAfter(() => executed = true, TimeSpan.FromSeconds(3));

        var result = cluster.RunUntilIdle(CancellationToken.None, maxIterations: 1000);

        Assert.Equal(SimulationExecutionReason.Idle, result.Reason);
        Assert.True(executed);
        Assert.True(cluster.TimeProvider.GetUtcNow() >= startTime + TimeSpan.FromSeconds(3));
    }
}
