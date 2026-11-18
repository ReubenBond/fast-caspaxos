using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Testing.Simulation;

namespace Orleans.Consensus.CasPaxos.Simulation.Transport;

public sealed class FastCasTransportEndpointRegistration<TValue>
{
    private readonly Action<IFastCasTransportMessage<TValue>> _deliver;

    public FastCasTransportEndpointRegistration(
        FastCasAddress address,
        SimulationSchedulerLane deliveryLane,
        Action<IFastCasTransportMessage<TValue>> deliver)
    {
        ArgumentNullException.ThrowIfNull(deliveryLane);
        ArgumentNullException.ThrowIfNull(deliver);

        Address = address;
        DeliveryLane = deliveryLane;
        _deliver = deliver;
    }

    public FastCasAddress Address { get; }

    public SimulationSchedulerLane DeliveryLane { get; }

    public void Deliver(IFastCasTransportMessage<TValue> message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _deliver(message);
    }
}
