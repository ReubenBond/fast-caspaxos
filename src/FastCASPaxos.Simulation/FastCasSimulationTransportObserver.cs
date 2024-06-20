using Clockwork;

namespace FastCASPaxos.Simulation;

public interface IFastCasSimulationTransportObserver
{
    void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status);
}

public enum FastCasTransportMessageKind
{
    ProposeRequest,
    PrepareRequest,
    PreparePromise,
    PrepareRejection,
    AcceptRequest,
    AcceptAccepted,
    AcceptRejected,
    ProposeResponse,
}
