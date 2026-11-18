using Orleans.Testing.Simulation;

namespace Orleans.Consensus.CasPaxos.Simulation;

public interface IFastCasSimulationTransportObserver
{
    void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status);
}

public enum FastCasTransportMessageKind
{
    ProposeRequest,
    ForwardProposeRequest,
    PrepareRequest,
    PreparePromise,
    PrepareRejection,
    AcceptRequest,
    AcceptAccepted,
    AcceptRejected,
    ProposeResponse,
    ForwardProposeResponse,
    LearnCommittedValue,
}
