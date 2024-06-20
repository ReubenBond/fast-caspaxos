using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Transport;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationTransportTests
{
    [Fact]
    public void Send_EnqueuesMessageOnDestinationQueue()
    {
        var transport = CreateTransport(out var targetEndpoint);
        var source = FastCasAddress.Proposer(1);
        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            targetEndpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        var status = transport.Send(message);

        Assert.Equal(DeliveryStatus.Success, status);
        Assert.Empty(targetEndpoint.Received);
        Assert.Equal(1, targetEndpoint.Queue.RunUntilIdle());
        Assert.Equal(message, Assert.Single(targetEndpoint.Received));
    }

    [Fact]
    public void Send_RespectsPartitionsAndHealing()
    {
        var transport = CreateTransport(out var targetEndpoint);
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
        Assert.Equal(1, targetEndpoint.Queue.RunUntilIdle());
        Assert.Single(targetEndpoint.Received);
    }

    [Fact]
    public void Send_ReturnsDroppedWhenNetworkDropsMessage()
    {
        var transport = CreateTransport(out var targetEndpoint);
        var source = FastCasAddress.Proposer(1);
        var message = new FastCasPrepareRequestMessage<TestValue>(
            source,
            targetEndpoint.Address,
            new PrepareRequest(Ballot.InitialClassic(1)));

        transport.Network.MessageDropRate = 1.0;

        Assert.Equal(DeliveryStatus.Dropped, transport.Send(message));
        Assert.Empty(targetEndpoint.Received);
        Assert.Equal(0, targetEndpoint.Queue.RunUntilIdle());
    }

    private static FastCasSimulationTransport<TestValue> CreateTransport(out TestEndpoint endpoint)
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var queue = new SimulationTaskQueue(clock, guard);
        var network = new SimulationNetwork(() => [], new SimulationRandom(1234));
        var transport = new FastCasSimulationTransport<TestValue>(network);

        endpoint = new TestEndpoint(FastCasAddress.Acceptor(1), queue);
        transport.RegisterEndpoint(endpoint.Registration);
        return transport;
    }

    private sealed class TestEndpoint(FastCasAddress address, SimulationTaskQueue queue)
    {
        public FastCasAddress Address { get; } = address;

        public SimulationTaskQueue Queue { get; } = queue;

        public List<IFastCasTransportMessage<TestValue>> Received { get; } = [];

        public FastCasTransportEndpointRegistration<TestValue> Registration =>
            new(Address, Queue, message => Received.Add(message));
    }

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));
    }
}

