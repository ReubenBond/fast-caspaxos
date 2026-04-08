using Clockwork;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class ClockworkSmokeTests
{
    [Fact]
    public void SimulationRandom_WithSameSeed_ProducesSameSequence()
    {
        var left = new SimulationRandom(seed: 12345);
        var right = new SimulationRandom(seed: 12345);

#pragma warning disable CA5394
        var leftValues = Enumerable.Range(0, 8).Select(_ => left.Next()).ToArray();
        var rightValues = Enumerable.Range(0, 8).Select(_ => right.Next()).ToArray();
#pragma warning restore CA5394

        Assert.Equal(leftValues, rightValues);
    }

    [Fact]
    public void SimulationTaskQueue_ExecutesDelayedWorkAfterClockAdvance()
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var queue = new SimulationTaskQueue(clock, new SingleThreadedGuard());
        var executed = new List<int>();

        queue.Enqueue(new ScheduledActionItem(() => executed.Add(1)));
        queue.EnqueueAfter(() => executed.Add(2), TimeSpan.FromSeconds(5));

        Assert.Equal(1, queue.RunUntilIdle());
        Assert.Equal([1], executed);

        clock.Advance(TimeSpan.FromSeconds(5));

        Assert.True(queue.RunOnce());
        Assert.Equal([1, 2], executed);
    }

    [Fact]
    public async Task SimulationNetwork_BlocksAndHealsBidirectionalPartitions()
    {
        await using var cluster = new TestSimulationCluster(seed: 777);
        _ = cluster.AddNode("node-1");
        _ = cluster.AddNode("node-2");
        var network = new SimulationNetwork(() => cluster.Nodes, cluster.Random.Fork());

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
    public async Task SimulationCluster_RunUntil_CompletesScheduledTask()
    {
        await using var cluster = new TestSimulationCluster(seed: 9001);
        var executed = false;
        var task = new Task(() => executed = true);

        task.Start(cluster.TaskScheduler);

        var completed = cluster.RunUntil(() => executed, maxIterations: 10);

        Assert.True(completed);
        Assert.True(executed);
    }

    [Fact]
    public async Task SimulationCluster_RunUntilIdle_AdvancesTimeForNodeWork()
    {
        await using var cluster = new TestSimulationCluster(seed: 42);
        var node = cluster.AddNode("node-1");
        var executed = false;
        var startTime = cluster.TimeProvider.GetUtcNow();

        node.Context.TaskQueue.EnqueueAfter(() => executed = true, TimeSpan.FromSeconds(3));

        var iterations = cluster.RunUntilIdle(maxIterations: 1000);

        Assert.True(iterations < 1000);
        Assert.True(executed);
        Assert.True(cluster.TimeProvider.GetUtcNow() >= startTime + TimeSpan.FromSeconds(3));
    }

    private sealed class TestSimulationCluster(int seed) : SimulationCluster<TestSimulationNode>(seed, startDateTime: DateTimeOffset.UnixEpoch)
    {
        public TestSimulationNode AddNode(string address)
        {
            var node = new TestSimulationNode(this, address);
            RegisterNode(node);
            return node;
        }

        protected override ValueTask DisposeAsyncCore() => ValueTask.CompletedTask;
    }

    private sealed class TestSimulationNode(TestSimulationCluster cluster, string address) : SimulationNode
    {
        public override SimulationNodeContext Context { get; } =
            new(cluster.Clock, cluster.Guard, cluster.CreateDerivedRandom(), cluster.TaskQueue);

        public override string NetworkAddress { get; } = address;

        public override bool IsInitialized => true;
    }
}

