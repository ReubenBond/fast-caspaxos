using System.Collections.Immutable;
using Clockwork;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Transport;

namespace FastCASPaxos.Simulation.Hosts;

/// <summary>
/// Combines proposer, acceptor, learner, and request-coordination behavior for a single
/// register replica while reusing the existing proposer/acceptor host implementations.
/// </summary>
public sealed class FastCasConsensusRegister<TValue>
{
    private readonly ProposerEndpointHost _proposerEndpoint;
    private readonly AcceptorEndpointHost _acceptorEndpoint;

    public FastCasConsensusRegister(
        int ordinal,
        ImmutableArray<FastCasAddress> acceptors,
        FastCasSimulationTransport<TValue> transport,
        TValue initialValue,
        IFastCasAcceptorStateStore<TValue>? stateStore = null,
        Action<FastCasAddress>? onAcceptorStateSaved = null,
        FastCasSimulationOptions? options = null)
    {
        if (acceptors.IsDefault) throw new ArgumentNullException(nameof(acceptors));

        ProposerHost = new FastCasProposerHost<TValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, ordinal),
            acceptors,
            transport,
            initialValue,
            options);
        AcceptorHost = new FastCasAcceptorHost<TValue>(
            new FastCasNodeId(FastCasParticipantRole.Acceptor, ordinal),
            transport,
            initialValue,
            stateStore,
            onAcceptorStateSaved,
            options);

        _proposerEndpoint = new ProposerEndpointHost(this);
        _acceptorEndpoint = new AcceptorEndpointHost(this);
    }

    public int Ordinal => ProposerHost.NodeId.Ordinal;

    public FastCasAddress ProposerAddress => ProposerHost.Address;

    public FastCasAddress AcceptorAddress => AcceptorHost.Address;

    public FastCasProposerHost<TValue> ProposerHost { get; }

    public FastCasAcceptorHost<TValue> AcceptorHost { get; }

    public IFastCasProposerNodeHost<TValue> ProposerEndpoint => _proposerEndpoint;

    public IFastCasAcceptorNodeHost<TValue> AcceptorEndpoint => _acceptorEndpoint;

    public void ConfigureCommittedValueBroadcaster(
        Action<FastCasAddress, Ballot, TValue, Ballot>? committedValueBroadcaster) =>
        ProposerHost.ConfigureCommittedValueBroadcaster(committedValueBroadcaster);

    public void Crash()
    {
        ProposerHost.Crash();
        AcceptorHost.Crash();
    }

    public void Restart()
    {
        AcceptorHost.Restart();
        ProposerHost.Restart();
    }

    public void ReinitializeAsFreshRegister()
    {
        AcceptorHost.ReinitializeAsFreshRegister();
        ProposerHost.Restart();
    }

    private sealed class ProposerEndpointHost(FastCasConsensusRegister<TValue> register) : IFastCasProposerNodeHost<TValue>
    {
        public FastCasNodeId NodeId => register.ProposerHost.NodeId;

        public FastCasAddress Address => register.ProposerHost.Address;

        public FastCasCrashRestartStateModel CrashRestartStateModel => register.ProposerHost.CrashRestartStateModel;

        public bool IsRunning => register.ProposerHost.IsRunning;

        public Ballot Ballot => register.ProposerHost.Ballot;

        public Ballot? PreparedBallot => register.ProposerHost.PreparedBallot;

        public TValue CachedValue => register.ProposerHost.CachedValue;

        public bool HasPendingClientRequests => register.ProposerHost.HasPendingClientRequests;

        public void Deliver(IFastCasTransportMessage<TValue> message) => register.ProposerHost.Deliver(message);

        public void Crash() => register.ProposerHost.Crash();

        public void Restart() => register.ProposerHost.Restart();

        public void BindSimulationContext(SimulationNodeContext simulationContext) =>
            register.ProposerHost.BindSimulationContext(simulationContext);

        public void ConfigureCommittedValueBroadcaster(
            Action<FastCasAddress, Ballot, TValue, Ballot>? committedValueBroadcaster) =>
            register.ProposerHost.ConfigureCommittedValueBroadcaster(committedValueBroadcaster);
    }

    private sealed class AcceptorEndpointHost(FastCasConsensusRegister<TValue> register) : IFastCasAcceptorNodeHost<TValue>
    {
        public FastCasNodeId NodeId => register.AcceptorHost.NodeId;

        public FastCasAddress Address => register.AcceptorHost.Address;

        public FastCasCrashRestartStateModel CrashRestartStateModel => register.AcceptorHost.CrashRestartStateModel;

        public bool IsRunning => register.AcceptorHost.IsRunning;

        public AcceptorState<TValue> State => register.AcceptorHost.State;

        public void Deliver(IFastCasTransportMessage<TValue> message) => register.AcceptorHost.Deliver(message);

        public void Crash() => register.AcceptorHost.Crash();

        public void Restart() => register.AcceptorHost.Restart();

        public void ReinitializeAsFreshRegister() => register.AcceptorHost.ReinitializeAsFreshRegister();
    }
}
