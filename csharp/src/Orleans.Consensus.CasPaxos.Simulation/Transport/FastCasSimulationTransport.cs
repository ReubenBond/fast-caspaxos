using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Testing.Simulation;

namespace Orleans.Consensus.CasPaxos.Simulation.Transport;

public sealed class FastCasSimulationTransport<TValue>(
    SimulationNetwork network,
    IFastCasSimulationTransportObserver? observer = null)
{
    private readonly Dictionary<FastCasAddress, FastCasTransportEndpointRegistration<TValue>> _endpoints = [];
    private readonly IFastCasSimulationTransportObserver? _observer = observer;

    public SimulationNetwork Network { get; } = network ?? throw new ArgumentNullException(nameof(network));

    /// <summary>
    /// Gets or sets whether delivery waits for the network's message delay. While disabled, every delivered message,
    /// including one on a link with a latency override, is due immediately.
    /// </summary>
    public bool EnableDelays { get; set; }

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

        if (!_endpoints.TryGetValue(message.Destination, out var endpoint))
        {
            throw new InvalidOperationException($"Unknown FastCASPaxos simulation endpoint '{message.Destination}'.");
        }

        var deliveryStatus = Network.CheckDelivery(message.From.NetworkAddress, message.Destination.NetworkAddress);
        _observer?.OnTransportMessage(GetMessageKind(message), deliveryStatus);
        if (deliveryStatus != DeliveryStatus.Success)
        {
            return deliveryStatus;
        }

        var delay = EnableDelays
            ? Network.GetMessageDelay(message.From.NetworkAddress, message.Destination.NetworkAddress)
            : TimeSpan.Zero;
        endpoint.DeliveryLane.EnqueueAfter(() => endpoint.Deliver(message), delay);
        return DeliveryStatus.Success;
    }

    private static FastCasTransportMessageKind GetMessageKind(IFastCasTransportMessage<TValue> message) =>
        message switch
        {
            FastCasProposeRequestMessage<TValue> => FastCasTransportMessageKind.ProposeRequest,
            FastCasForwardProposeRequestMessage<TValue> => FastCasTransportMessageKind.ForwardProposeRequest,
            FastCasPrepareRequestMessage<TValue> => FastCasTransportMessageKind.PrepareRequest,
            FastCasPreparePromiseMessage<TValue> => FastCasTransportMessageKind.PreparePromise,
            FastCasPrepareRejectionMessage<TValue> => FastCasTransportMessageKind.PrepareRejection,
            FastCasAcceptRequestMessage<TValue> => FastCasTransportMessageKind.AcceptRequest,
            FastCasAcceptAcceptedMessage<TValue> => FastCasTransportMessageKind.AcceptAccepted,
            FastCasAcceptRejectedMessage<TValue> => FastCasTransportMessageKind.AcceptRejected,
            FastCasProposeResponseMessage<TValue> => FastCasTransportMessageKind.ProposeResponse,
            FastCasForwardProposeResponseMessage<TValue> => FastCasTransportMessageKind.ForwardProposeResponse,
            FastCasLearnCommittedValueMessage<TValue> => FastCasTransportMessageKind.LearnCommittedValue,
            _ => throw new InvalidOperationException(
                $"Unknown FastCASPaxos simulation transport message type '{message.GetType().Name}'."),
        };
}
