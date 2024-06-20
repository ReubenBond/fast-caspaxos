
namespace FastCASPaxos.Simulation.Contracts;

public sealed class InMemoryFastCasAcceptorStateStore<TValue> : IFastCasAcceptorStateStore<TValue>
{
    private readonly Dictionary<FastCasNodeId, FastCasAcceptorDurableState<TValue>> _states = [];

    public bool TryLoad(FastCasNodeId nodeId, out FastCasAcceptorDurableState<TValue> state) =>
        _states.TryGetValue(nodeId, out state!);

    public void Save(FastCasNodeId nodeId, FastCasAcceptorDurableState<TValue> state)
    {
        ArgumentNullException.ThrowIfNull(state);
        _states[nodeId] = state;
    }
}
