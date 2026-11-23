using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Transport;

namespace Orleans.Consensus.CasPaxos.Simulation.Hosts;

/// <summary>
/// Adapts the transport-agnostic <see cref="AcceptorEngine"/> state machine to the simulation transport.
/// Accepted state is durably stored after every prepare or accept so a restart can reload the latest promised and accepted ballots.
/// </summary>
public sealed class FastCasAcceptorHost<TValue> : IFastCasAcceptorNodeHost<TValue>
{
    private readonly FastCasSimulationTransport<TValue> _transport;
    private readonly TValue _initialValue;
    private readonly long _initialConfiguration;
    private readonly IFastCasAcceptorStateStore<TValue> _stateStore;
    private readonly FastCasSimulationOptions _options;
    private readonly Action<FastCasAddress>? _onStateSaved;
    private AcceptorState<TValue> _state;

    public FastCasAcceptorHost(
        FastCasNodeId nodeId,
        FastCasSimulationTransport<TValue> transport,
        TValue initialValue,
        IFastCasAcceptorStateStore<TValue>? stateStore = null,
        Action<FastCasAddress>? onStateSaved = null,
        FastCasSimulationOptions? options = null,
        long initialConfiguration = 1)
    {
        if (nodeId.Role != FastCasParticipantRole.Acceptor)
        {
            throw new ArgumentException("FastCasAcceptorHost requires an acceptor node id.", nameof(nodeId));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(initialConfiguration, 1);

        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _initialValue = initialValue;
        _initialConfiguration = initialConfiguration;
        _stateStore = stateStore ?? new InMemoryFastCasAcceptorStateStore<TValue>();
        _options = options ?? new FastCasSimulationOptions();
        _onStateSaved = onStateSaved;

        NodeId = nodeId;
        Address = nodeId.Address;
        CrashRestartStateModel = _options.CrashRestartStateModel;
        _state = CreateState();
    }

    public FastCasNodeId NodeId { get; }

    public FastCasAddress Address { get; }

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; }

    public bool IsRunning { get; private set; } = true;

    public AcceptorState<TValue> State => _state;

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
                SendPrepareResponse(
                    prepareRequest.From,
                    prepareRequest.Payload,
                    AcceptorEngine.Prepare(ref _state, prepareRequest.Payload));
                SaveState();
                break;
            case FastCasAcceptRequestMessage<TValue> acceptRequest:
                SendAcceptResponse(
                    acceptRequest.From,
                    acceptRequest.Payload,
                    AcceptorEngine.Accept(ref _state, acceptRequest.Payload));
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
        _state = CreateState();
        IsRunning = true;
    }

    public Ballot SetFloor(Ballot targetFloor)
    {
        if (!IsRunning)
        {
            throw new InvalidOperationException($"Acceptor host '{Address}' is not running.");
        }

        var result = AcceptorEngine.SetFloor(ref _state, targetFloor);
        SaveState();
        return result;
    }

    /// <summary>
    /// Resets the acceptor to pristine state as if it were a brand-new node that has never
    /// participated in any protocol round. The durable state store is cleared so that a
    /// subsequent <see cref="Restart"/> also starts fresh.
    /// </summary>
    public void ReinitializeAsFreshRegister()
    {
        _stateStore.Clear(NodeId);
        _state = AcceptorState.CreateInitial<TValue>(_initialValue, _initialConfiguration);
        IsRunning = true;
    }

    private AcceptorState<TValue> CreateState()
    {
        if (_stateStore.TryLoad(NodeId, out var durableState))
        {
            return new AcceptorState<TValue>
            {
                FloorBallot = durableState.FloorBallot.IsZero
                    ? Ballot.Floor(_initialConfiguration)
                    : durableState.FloorBallot,
                PromisedBallot = durableState.PromisedBallot,
                AcceptedBallot = durableState.AcceptedBallot,
                AcceptedValue = durableState.AcceptedValue,
            };
        }

        return AcceptorState.CreateInitial<TValue>(_initialValue, _initialConfiguration);
    }

    private void SaveState()
    {
        _stateStore.Save(
            NodeId,
            new FastCasAcceptorDurableState<TValue>
            {
                FloorBallot = _state.FloorBallot,
                PromisedBallot = _state.PromisedBallot,
                AcceptedBallot = _state.AcceptedBallot,
                AcceptedValue = _state.AcceptedValue,
            });
        _onStateSaved?.Invoke(Address);
    }

    private void SendPrepareResponse(
        FastCasAddress responseTarget,
        PrepareRequest request,
        AcceptorPrepareResult<TValue> result)
    {
        if (result.IsPromised)
        {
            _transport.Send(
                new FastCasPreparePromiseMessage<TValue>(
                    Address,
                    responseTarget,
                    new PreparePromise<TValue, FastCasAddress>(
                        request.Ballot.Round,
                        Address,
                        result.AcceptedBallot,
                        result.AcceptedValue)));
            return;
        }

        _transport.Send(
            new FastCasPrepareRejectionMessage<TValue>(
                Address,
                responseTarget,
                new PrepareRejection<TValue, FastCasAddress>(
                    request.Ballot.Round,
                    Address,
                    result.AcceptedBallot,
                    result.AcceptedValue,
                    result.ConflictingBallot)));
    }

    private void SendAcceptResponse(
        FastCasAddress responseTarget,
        AcceptRequest<TValue> request,
        AcceptorAcceptResult result)
    {
        if (result.IsAccepted)
        {
            _transport.Send(
                new FastCasAcceptAcceptedMessage<TValue>(
                    Address,
                    responseTarget,
                    new AcceptAccepted<FastCasAddress>(
                        request.Ballot.Round,
                        Address,
                        result.PromisedBallot)));
            return;
        }

        _transport.Send(
            new FastCasAcceptRejectedMessage<TValue>(
                Address,
                responseTarget,
                new AcceptRejected<FastCasAddress>(
                    request.Ballot.Round,
                    Address,
                    result.ConflictingBallot)));
    }
}
