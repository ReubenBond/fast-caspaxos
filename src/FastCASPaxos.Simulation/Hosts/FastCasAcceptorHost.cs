using FastCASPaxos.Messages;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Transport;

namespace FastCASPaxos.Simulation.Hosts;

/// <summary>
/// Adapts the transport-agnostic <see cref="AcceptorEngine{TValue,TRoute}"/> to the simulation transport.
/// Accepted state is durably stored after every prepare or accept so a restart can reload the latest promised and accepted ballots.
/// </summary>
public sealed class FastCasAcceptorHost<TValue> : IFastCasNodeHost<TValue>
{
    private readonly FastCasSimulationTransport<TValue> _transport;
    private readonly IFastCasAcceptorStateStore<TValue> _stateStore;
    private readonly FastCasSimulationOptions _options;
    private readonly Action<FastCasAddress>? _onStateSaved;
    private HostAcceptorEngine _engine;

    public FastCasAcceptorHost(
        FastCasNodeId nodeId,
        FastCasSimulationTransport<TValue> transport,
        IFastCasAcceptorStateStore<TValue>? stateStore = null,
        Action<FastCasAddress>? onStateSaved = null,
        FastCasSimulationOptions? options = null)
    {
        if (nodeId.Role != FastCasParticipantRole.Acceptor)
        {
            throw new ArgumentException("FastCasAcceptorHost requires an acceptor node id.", nameof(nodeId));
        }

        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _stateStore = stateStore ?? new InMemoryFastCasAcceptorStateStore<TValue>();
        _options = options ?? new FastCasSimulationOptions();
        _onStateSaved = onStateSaved;

        NodeId = nodeId;
        Address = nodeId.Address;
        CrashRestartStateModel = _options.CrashRestartStateModel;
        _engine = CreateEngine();
    }

    public FastCasNodeId NodeId { get; }

    public FastCasAddress Address { get; }

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; }

    public bool IsRunning { get; private set; } = true;

    public AcceptorState<TValue> State => _engine.GetState();

    /// <summary>
    /// Delivers a transport message into the acceptor state machine.
    /// </summary>
    public void Deliver(IFastCasTransportMessage<TValue> message)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (!IsRunning)
        {
            return;
        }

        switch (message)
        {
            case FastCasPrepareRequestMessage<TValue> prepareRequest:
                _engine.SetResponseTarget(prepareRequest.From);
                _engine.Prepare(prepareRequest.Payload);
                SaveState();
                break;
            case FastCasAcceptRequestMessage<TValue> acceptRequest:
                _engine.SetResponseTarget(acceptRequest.From);
                _engine.Accept(acceptRequest.Payload);
                SaveState();
                break;
            default:
                throw new InvalidOperationException($"Acceptor host '{Address}' cannot handle message type '{message.GetType().Name}'.");
        }
    }

    /// <summary>
    /// Stops the acceptor from handling new messages. In-flight messages are dropped until the node is restarted.
    /// </summary>
    public void Crash()
    {
        IsRunning = false;
    }

    /// <summary>
    /// Reloads the durable acceptor state and resumes message handling.
    /// </summary>
    public void Restart()
    {
        _engine = CreateEngine();
        IsRunning = true;
    }

    private HostAcceptorEngine CreateEngine()
    {
        if (_stateStore.TryLoad(NodeId, out var durableState))
        {
            return new HostAcceptorEngine(
                this,
                Address,
                new AcceptorState<TValue>
                {
                    PromisedBallot = durableState.PromisedBallot,
                    AcceptedBallot = durableState.AcceptedBallot,
                    AcceptedValue = durableState.AcceptedValue,
                });
        }

        return new HostAcceptorEngine(this, Address);
    }

    private void SaveState()
    {
        var state = _engine.GetState();
        _stateStore.Save(
            NodeId,
            new FastCasAcceptorDurableState<TValue>
            {
                PromisedBallot = state.PromisedBallot,
                AcceptedBallot = state.AcceptedBallot,
                AcceptedValue = state.AcceptedValue,
            });
        _onStateSaved?.Invoke(Address);
    }

    private sealed class HostAcceptorEngine(FastCasAcceptorHost<TValue> host, FastCasAddress acceptor, AcceptorState<TValue>? state = null) : AcceptorEngine<TValue, FastCasAddress>(acceptor, state)
    {
        private readonly FastCasAcceptorHost<TValue> _host = host;
        private FastCasAddress _responseTarget;

        public void SetResponseTarget(FastCasAddress target) => _responseTarget = target;

        protected override void OnPreparePromised(PreparePromise<TValue, FastCasAddress> result) =>
            _host._transport.Send(new FastCasPreparePromiseMessage<TValue>(_host.Address, _responseTarget, result));

        protected override void OnPrepareRejected(PrepareRejection<TValue, FastCasAddress> result) =>
            _host._transport.Send(new FastCasPrepareRejectionMessage<TValue>(_host.Address, _responseTarget, result));

        protected override void OnAcceptAccepted(AcceptAccepted<FastCasAddress> result) =>
            _host._transport.Send(new FastCasAcceptAcceptedMessage<TValue>(_host.Address, _responseTarget, result));

        protected override void OnAcceptRejected(AcceptRejected<FastCasAddress> result) =>
            _host._transport.Send(new FastCasAcceptRejectedMessage<TValue>(_host.Address, _responseTarget, result));
    }
}
