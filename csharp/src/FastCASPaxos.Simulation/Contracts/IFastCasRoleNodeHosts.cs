using Clockwork;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Simulation.Contracts;

public interface IFastCasProposerNodeHost<TValue> : IFastCasNodeHost<TValue>
{
    Ballot Ballot { get; }

    Ballot? PreparedBallot { get; }

    TValue CachedValue { get; }

    bool HasPendingClientRequests { get; }

    void BindSimulationContext(SimulationNodeContext simulationContext);

    void ConfigureCommittedValueBroadcaster(
        Action<FastCasAddress, Ballot, TValue, Ballot>? committedValueBroadcaster);
}

public interface IFastCasAcceptorNodeHost<TValue> : IFastCasNodeHost<TValue>
{
    AcceptorState<TValue> State { get; }

    void ReinitializeAsFreshRegister();
}
