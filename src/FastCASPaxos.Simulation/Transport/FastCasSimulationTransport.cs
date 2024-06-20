using Clockwork;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Simulation.Transport;

public sealed class FastCasSimulationTransport<TValue>
{
    private readonly Dictionary<FastCasAddress, FastCasTransportEndpointRegistration<TValue>> _endpoints = [];
    private readonly IFastCasSimulationTransportObserver? _observer;

    public FastCasSimulationTransport(
        SimulationNetwork network,
        IFastCasSimulationTransportObserver? observer = null)
    {
        Network = network ?? throw new ArgumentNullException(nameof(network));
        _observer = observer;
    }

    public SimulationNetwork Network { get; }

    public IReadOnlyCollection<FastCasAddress> RegisteredAddresses => _endpoints.Keys;

    public void RegisterEndpoint(FastCasTransportEndpointRegistration<TValue> endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        if (!_endpoints.TryAdd(endpoint.Address, endpoint))
        {
            throw new InvalidOperationException($"A FastCASPaxos simulation endpoint is already registered for '{endpoint.Address}'.");
        }
    }

    public bool UnregisterEndpoint(FastCasAddress address) => _endpoints.Remove(address);

    public DeliveryStatus Send(IFastCasTransportMessage<TValue> message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (!_endpoints.TryGetValue(message.To, out var endpoint))
        {
            throw new InvalidOperationException($"Unknown FastCASPaxos simulation endpoint '{message.To}'.");
        }

        var deliveryStatus = Network.CheckDelivery(message.From.NetworkAddress, message.To.NetworkAddress);
        _observer?.OnTransportMessage(GetMessageKind(message), deliveryStatus);
        if (deliveryStatus != DeliveryStatus.Success)
        {
            return deliveryStatus;
        }

        endpoint.DeliveryQueue.EnqueueAfter(() => endpoint.Deliver(message), Network.GetMessageDelay());
        return DeliveryStatus.Success;
    }

    private static FastCasTransportMessageKind GetMessageKind(IFastCasTransportMessage<TValue> message) =>
        message switch
        {
            FastCasProposeRequestMessage<TValue> => FastCasTransportMessageKind.ProposeRequest,
            FastCasPrepareRequestMessage<TValue> => FastCasTransportMessageKind.PrepareRequest,
            FastCasPreparePromiseMessage<TValue> => FastCasTransportMessageKind.PreparePromise,
            FastCasPrepareRejectionMessage<TValue> => FastCasTransportMessageKind.PrepareRejection,
            FastCasAcceptRequestMessage<TValue> => FastCasTransportMessageKind.AcceptRequest,
            FastCasAcceptAcceptedMessage<TValue> => FastCasTransportMessageKind.AcceptAccepted,
            FastCasAcceptRejectedMessage<TValue> => FastCasTransportMessageKind.AcceptRejected,
            FastCasProposeResponseMessage<TValue> => FastCasTransportMessageKind.ProposeResponse,
            _ => throw new InvalidOperationException(
                $"Unknown FastCASPaxos simulation transport message type '{message.GetType().Name}'."),
        };
}
