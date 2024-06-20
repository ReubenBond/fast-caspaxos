using FastCASPaxos.Messages;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Invariants;

namespace FastCASPaxos.Simulation;

public interface IFastCasSimulationAcceptorStateObserver
{
    void OnAcceptorStateObserved<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorSafetyMonitor<TValue> monitor)
        where TValue : IVersionedValue<TValue>;
}
