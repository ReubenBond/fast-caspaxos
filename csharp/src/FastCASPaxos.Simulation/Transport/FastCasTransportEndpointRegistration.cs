using Clockwork;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Simulation.Transport;

public sealed class FastCasTransportEndpointRegistration<TValue>
{
    private readonly Action<IFastCasTransportMessage<TValue>> _deliver;

    public FastCasTransportEndpointRegistration(
        FastCasAddress address,
        SimulationTaskQueue deliveryQueue,
        Action<IFastCasTransportMessage<TValue>> deliver)
    {
        ArgumentNullException.ThrowIfNull(deliveryQueue);
        ArgumentNullException.ThrowIfNull(deliver);

        Address = address;
        DeliveryQueue = deliveryQueue;
        _deliver = deliver;
    }

    public FastCasAddress Address { get; }

    public SimulationTaskQueue DeliveryQueue { get; }

    public void Deliver(IFastCasTransportMessage<TValue> message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _deliver(message);
    }
}
