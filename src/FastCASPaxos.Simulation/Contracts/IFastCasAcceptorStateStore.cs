
namespace FastCASPaxos.Simulation.Contracts;

public interface IFastCasAcceptorStateStore<TValue>
{
    bool TryLoad(FastCasNodeId nodeId, out FastCasAcceptorDurableState<TValue> state);

    void Save(FastCasNodeId nodeId, FastCasAcceptorDurableState<TValue> state);
}
