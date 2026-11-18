using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Transport;
using Orleans.Testing.Simulation;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class FastCasSimulationTransportTests
{
    [Fact]
    public async Task SendEnqueuesMessageOnDestinationLane()
    {
        await using var cluster = CreateCluster();
        var transport = CreateTransport(cluster, out var targetEndpoint);
        var source = FastCasAddress.Proposer(1);
        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            targetEndpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        var status = transport.Send(message);

        Assert.Equal(DeliveryStatus.Success, status);
        Assert.Empty(targetEndpoint.Received);
        Assert.Equal(1, targetEndpoint.Lane.RunUntilIdle(CancellationToken.None));
        Assert.Equal(message, Assert.Single(targetEndpoint.Received));
    }

    [Fact]
    public async Task SendRespectsPartitionsAndHealing()
    {
        await using var cluster = CreateCluster();
        var transport = CreateTransport(cluster, out var targetEndpoint);
        var source = FastCasAddress.Proposer(1);
        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            targetEndpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        transport.Network.CreateBidirectionalPartition(source.NetworkAddress, targetEndpoint.Address.NetworkAddress);
        Assert.Equal(DeliveryStatus.Partitioned, transport.Send(message));
        Assert.Empty(targetEndpoint.Received);

        transport.Network.HealBidirectionalPartition(source.NetworkAddress, targetEndpoint.Address.NetworkAddress);
        Assert.Equal(DeliveryStatus.Success, transport.Send(message));
        Assert.Equal(1, targetEndpoint.Lane.RunUntilIdle(CancellationToken.None));
        Assert.Single(targetEndpoint.Received);
    }

    [Fact]
    public async Task SendReturnsDroppedWhenNetworkDropsMessage()
    {
        await using var cluster = CreateCluster();
        var transport = CreateTransport(cluster, out var targetEndpoint);
        var source = FastCasAddress.Proposer(1);
        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            targetEndpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        transport.Network.MessageDropRate = 1.0;

        Assert.Equal(DeliveryStatus.Dropped, transport.Send(message));
        Assert.Empty(targetEndpoint.Received);
        Assert.Equal(0, targetEndpoint.Lane.RunUntilIdle(CancellationToken.None));
    }

    [Fact]
    public async Task SendUsesPerLinkLatencyOverrideForDelay()
    {
        await using var cluster = CreateCluster();
        var transport = CreateTransport(cluster, out var endpoint);
        transport.EnableDelays = true;
        transport.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        transport.Network.MaxJitter = TimeSpan.Zero;
        var source = FastCasAddress.Proposer(1);

        // Set a large per-link latency
        transport.Network.SetLinkLatency(source.NetworkAddress, endpoint.Address.NetworkAddress, TimeSpan.FromMilliseconds(100), TimeSpan.Zero);

        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            endpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        var status = transport.Send(message);
        Assert.Equal(DeliveryStatus.Success, status);

        // Advance by less than the per-link latency: the message has not arrived yet.
        cluster.RunFor(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Empty(endpoint.Received);

        // Advance past the per-link latency: the message is delivered when it falls due.
        cluster.RunFor(TimeSpan.FromMilliseconds(60), CancellationToken.None);
        Assert.Single(endpoint.Received);
    }

    [Fact]
    public async Task SendWithDelaysDisabledDeliversImmediatelyEvenWithLinkLatencyOverride()
    {
        await using var cluster = CreateCluster();
        var transport = CreateTransport(cluster, out var endpoint);
        var source = FastCasAddress.Proposer(1);
        transport.Network.SetLinkLatency(source.NetworkAddress, endpoint.Address.NetworkAddress, TimeSpan.FromMilliseconds(50), TimeSpan.Zero);

        Assert.Equal(
            DeliveryStatus.Success,
            transport.Send(new FastCasPrepareRequestMessage<TestValue>(source, endpoint.Address, new PrepareRequest(Ballot.InitialClassic(1)))));

        Assert.Equal(1, endpoint.Lane.RunUntilIdle(CancellationToken.None));
        Assert.Single(endpoint.Received);
        Assert.Equal(DateTimeOffset.UnixEpoch, cluster.TimeProvider.GetUtcNow());
    }

    private static SimulationCluster CreateCluster() => new(seed: 1234, startDateTime: DateTimeOffset.UnixEpoch);

    private static FastCasSimulationTransport<TestValue> CreateTransport(SimulationCluster cluster, out TestEndpoint endpoint)
    {
        var transport = new FastCasSimulationTransport<TestValue>(cluster.Network);
        endpoint = new TestEndpoint(FastCasAddress.Acceptor(1), cluster.SchedulerLane);
        transport.RegisterEndpoint(endpoint.Registration);
        return transport;
    }

    private sealed class TestEndpoint(FastCasAddress address, SimulationSchedulerLane lane)
    {
        public FastCasAddress Address { get; } = address;

        public SimulationSchedulerLane Lane { get; } = lane;

        public List<IFastCasTransportMessage<TestValue>> Received { get; } = [];

        public FastCasTransportEndpointRegistration<TestValue> Registration =>
            new(Address, Lane, message => Received.Add(message));
    }

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));
    }
}
