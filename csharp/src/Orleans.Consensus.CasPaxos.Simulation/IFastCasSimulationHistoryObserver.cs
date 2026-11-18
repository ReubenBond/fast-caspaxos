using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Simulation;

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

    void OnLearnCommittedValue<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress sourceProposer,
        FastCasAddress targetProposer,
        Ballot acceptedBallot,
        TValue value,
        Ballot preparedBallot)
        where TValue : Contracts.IVersionedValue<TValue>;
}
