using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Invariants;

namespace Orleans.Consensus.CasPaxos.Simulation;

public interface IFastCasSimulationAcceptorStateObserver
{
    void OnAcceptorStateObserved<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorSafetyMonitor<TValue> monitor)
        where TValue : IVersionedValue<TValue>;
}
