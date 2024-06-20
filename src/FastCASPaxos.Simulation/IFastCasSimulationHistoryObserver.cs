using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation;

public interface IFastCasSimulationHistoryObserver
{
    void OnProposalSent<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        IOperation<TValue> operation)
        where TValue : Contracts.IVersionedValue<TValue>;

    void OnProposalCompleted<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        ProposeResponse<TValue> response)
        where TValue : Contracts.IVersionedValue<TValue>;
}
