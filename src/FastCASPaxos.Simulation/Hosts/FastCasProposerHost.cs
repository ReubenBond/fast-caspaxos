using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Diagnostics;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Transport;

namespace FastCASPaxos.Simulation.Hosts;

/// <summary>
/// Adapts the transport-agnostic <see cref="ProposerEngine{TValue,TRoute}"/> to the simulation transport.
/// The host owns only volatile proposer state; a crash drops in-flight messages and a restart recreates the proposer engine from scratch.
/// </summary>
public sealed class FastCasProposerHost<TValue> : IFastCasNodeHost<TValue>
{
    private readonly record struct LearnedCommit(
        Ballot AcceptedBallot,
        TValue Value,
        Ballot PreparedBallot);

    private readonly FastCasSimulationTransport<TValue> _transport;
    private readonly List<FastCasAddress> _acceptors;
    private readonly FastCasSimulationOptions _options;
    private HostProposerEngine _engine;
    private SimulationNodeContext? _simulationContext;
    private Action<FastCasAddress, Ballot, TValue, Ballot>? _committedValueBroadcaster;
    private LearnedCommit? _pendingLearnedCommit;
    private int? _activeRequestId;
    private int _proposalGeneration;
    private int _prepareSendCount;
    private bool _sentAcceptBeforeFirstPrepare;

    public FastCasProposerHost(
        FastCasNodeId nodeId,
        IEnumerable<FastCasAddress> acceptors,
        FastCasSimulationTransport<TValue> transport,
        FastCasSimulationOptions? options = null)
    {
        if (nodeId.Role != FastCasParticipantRole.Proposer)
        {
            throw new ArgumentException("FastCasProposerHost requires a proposer node id.", nameof(nodeId));
        }

        ArgumentNullException.ThrowIfNull(acceptors);

        NodeId = nodeId;
        Address = nodeId.Address;
        _acceptors = [.. acceptors];
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _options = options ?? new FastCasSimulationOptions();
        CrashRestartStateModel = _options.CrashRestartStateModel;
        _options.ProposerRetryBackoff.Validate();
        _engine = CreateEngine();
    }

    public FastCasNodeId NodeId { get; }

    public FastCasAddress Address { get; }

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; }

    public bool IsRunning { get; private set; } = true;

    public Ballot Ballot => _engine.Ballot;

    public Ballot? PreparedBallot => _engine.PreparedBallot;

    public TValue CachedValue => _engine.CachedValue;

    public bool HasPendingClientRequests =>
        _activeRequestId is not null
        || _engine.HasPendingProposal;

    public void BindSimulationContext(SimulationNodeContext simulationContext)
    {
        ArgumentNullException.ThrowIfNull(simulationContext);
        if (_simulationContext is not null
            && !ReferenceEquals(_simulationContext, simulationContext))
        {
            throw new InvalidOperationException(
                $"Proposer host '{Address}' is already bound to a different simulation context.");
        }

        _simulationContext = simulationContext;
    }

    public void ConfigureCommittedValueBroadcaster(
        Action<FastCasAddress, Ballot, TValue, Ballot>? committedValueBroadcaster) =>
        _committedValueBroadcaster = committedValueBroadcaster;

    /// <summary>
    /// Delivers a transport message into the proposer state machine.
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
            case FastCasProposeRequestMessage<TValue> proposeRequest:
                StartProposal(proposeRequest);
                break;
            case FastCasPreparePromiseMessage<TValue> preparePromise:
                _engine.HandlePreparePromised(preparePromise.Payload);
                break;
            case FastCasPrepareRejectionMessage<TValue> prepareRejection:
                _engine.HandlePrepareRejected(prepareRejection.Payload);
                break;
            case FastCasAcceptAcceptedMessage<TValue> acceptAccepted:
                _engine.HandleAcceptAccepted(acceptAccepted.Payload);
                break;
            case FastCasAcceptRejectedMessage<TValue> acceptRejected:
                _engine.HandleAcceptRejected(acceptRejected.Payload);
                break;
            case FastCasLearnCommittedValueMessage<TValue> learnedCommit:
                LearnCommittedValue(
                    learnedCommit.AcceptedBallot,
                    learnedCommit.Value,
                    learnedCommit.PreparedBallot);
                break;
            default:
                throw new InvalidOperationException($"Proposer host '{Address}' cannot handle message type '{message.GetType().Name}'.");
        }
    }

    /// <summary>
    /// Stops the proposer from handling new messages. Any already-sent messages are effectively dropped because the host no longer processes them.
    /// </summary>
    public void Crash()
    {
        ClearPendingClientRequests();
        _pendingLearnedCommit = null;
        ResetPendingRetryState();
        IsRunning = false;
    }

    /// <summary>
    /// Recreates the proposer engine from volatile state only.
    /// </summary>
    public void Restart()
    {
        ClearPendingClientRequests();
        _pendingLearnedCommit = null;
        ResetPendingRetryState();
        _engine = CreateEngine();
        IsRunning = true;
    }

    public void LearnCommittedValue(Ballot acceptedBallot, TValue value, Ballot preparedBallot = default)
    {
        if (!IsRunning)
        {
            throw new InvalidOperationException(
                $"Proposer host '{Address}' cannot learn committed state while it is not running.");
        }

        if (HasPendingClientRequests)
        {
            QueueLearnedCommit(acceptedBallot, value, preparedBallot);
            return;
        }

        _engine.LearnCommittedValue(acceptedBallot, value, preparedBallot);
    }

    private HostProposerEngine CreateEngine() =>
        new(
            this,
            Address,
            NodeId.Ordinal,
            _acceptors,
            _options.EnableDistinguishedLeader,
            _options.EnableFastCommit);

    private SimulationNodeContext SimulationContext =>
        _simulationContext
        ?? throw new InvalidOperationException(
            $"Proposer host '{Address}' is missing a bound simulation context.");

    private void StartNewProposal()
    {
        _proposalGeneration++;
        _prepareSendCount = 0;
        _sentAcceptBeforeFirstPrepare = false;
    }

    private int ActiveRequestId =>
        _activeRequestId
        ?? throw new InvalidOperationException(
            $"Proposer host '{Address}' has no active client request.");

    private void ResetPendingRetryState()
    {
        _proposalGeneration++;
        _prepareSendCount = 0;
        _sentAcceptBeforeFirstPrepare = false;
    }

    private void ClearPendingClientRequests()
    {
        _activeRequestId = null;
    }

    private void StartProposal(FastCasProposeRequestMessage<TValue> proposeRequest)
    {
        _activeRequestId = proposeRequest.RequestId;
        StartNewProposal();
        _engine.StartProposal(AttachCaller(proposeRequest.From, proposeRequest.Payload));
    }

    private void OnAcceptStarted()
    {
        if (_prepareSendCount == 0)
        {
            _sentAcceptBeforeFirstPrepare = true;
        }
    }

    private void OnProposalCompleted()
    {
        ResetPendingRetryState();
        _activeRequestId = null;
        ApplyPendingLearnedCommit();
    }

    private void BroadcastCommittedValue(Ballot acceptedBallot, TValue value, Ballot preparedBallot)
    {
        _committedValueBroadcaster?.Invoke(Address, acceptedBallot, value, preparedBallot);
    }

    private void QueueLearnedCommit(Ballot acceptedBallot, TValue value, Ballot preparedBallot)
    {
        if (acceptedBallot < _engine.Ballot)
        {
            return;
        }

        if (_pendingLearnedCommit is
            {
                AcceptedBallot: var pendingAcceptedBallot,
                PreparedBallot: var pendingPreparedBallot
            }
            && (acceptedBallot < pendingAcceptedBallot
                || (acceptedBallot == pendingAcceptedBallot && preparedBallot <= pendingPreparedBallot)))
        {
            return;
        }

        _pendingLearnedCommit = new LearnedCommit(acceptedBallot, value, preparedBallot);
    }

    private void ApplyPendingLearnedCommit()
    {
        if (HasPendingClientRequests || _pendingLearnedCommit is not { } learnedCommit)
        {
            return;
        }

        _pendingLearnedCommit = null;
        _engine.LearnCommittedValue(
            learnedCommit.AcceptedBallot,
            learnedCommit.Value,
            learnedCommit.PreparedBallot);
    }

    private void SendPrepare(
        HostProposerEngine engine,
        FastCasAddress proposer,
        PrepareRequest request)
    {
        var delay = GetRetryDelay();
        if (delay == TimeSpan.Zero)
        {
            SendPrepareNow(proposer, request);
            return;
        }

        var proposalGeneration = _proposalGeneration;
        SimulationContext.TaskQueue.EnqueueAfter(
            () =>
            {
                if (!IsRunning
                    || proposalGeneration != _proposalGeneration
                    || !ReferenceEquals(_engine, engine))
                {
                    return;
                }

                SendPrepareNow(proposer, request);
            },
            delay);
    }

    private void SendAccept(
        FastCasAddress proposer,
        AcceptRequest<TValue> request)
    {
        OnAcceptStarted();
        foreach (var acceptor in _acceptors)
        {
            _transport.Send(new FastCasAcceptRequestMessage<TValue>(proposer, acceptor, request));
        }

        ScheduleResponseTimeout();
    }

    private void SendPrepareNow(
        FastCasAddress proposer,
        PrepareRequest request)
    {
        foreach (var acceptor in _acceptors)
        {
            _transport.Send(new FastCasPrepareRequestMessage<TValue>(proposer, acceptor, request));
        }

        ScheduleResponseTimeout();
    }

    private void ScheduleResponseTimeout()
    {
        var timeout = _options.ProposerResponseTimeout;
        if (timeout <= TimeSpan.Zero || _simulationContext is null)
        {
            return;
        }

        var proposalGeneration = _proposalGeneration;
        var engine = _engine;
        var round = engine.Ballot.Round;
        _simulationContext.TaskQueue.EnqueueAfter(
            () =>
            {
                if (!IsRunning
                    || proposalGeneration != _proposalGeneration
                    || !ReferenceEquals(_engine, engine))
                {
                    return;
                }

                engine.HandleResponseTimeout(round);
            },
            timeout);
    }

    private TimeSpan GetRetryDelay()
    {
        _prepareSendCount++;
        var consecutiveFailedRetries = _sentAcceptBeforeFirstPrepare
            ? _prepareSendCount
            : _prepareSendCount - 1;
        if (consecutiveFailedRetries <= 0)
        {
            return TimeSpan.Zero;
        }

        return _options.ProposerRetryBackoff.GetRetryDelay(
            consecutiveFailedRetries,
            SimulationContext.Random);
    }

    private static IOperation<TValue> AttachCaller(FastCasAddress caller, IOperation<TValue> operation)
    {
        if (operation is IRoutedOperation<FastCasAddress> routedOperation)
        {
            if (!EqualityComparer<FastCasAddress>.Default.Equals(routedOperation.Caller, caller))
            {
                throw new InvalidOperationException(
                    $"Operation '{operation}' is already routed to caller '{routedOperation.Caller}', but transport delivered it from '{caller}'.");
            }

            return operation;
        }

        return new RoutedOperation<TValue, FastCasAddress>(caller, operation);
    }

    private sealed class HostProposerEngine : ValueTrackingProposerEngine<TValue, FastCasAddress>
    {
        private readonly FastCasProposerHost<TValue> _host;
        private readonly FastCasAddress _proposer;
        private readonly int _proposerId;

        public HostProposerEngine(
            FastCasProposerHost<TValue> host,
            FastCasAddress proposer,
            int proposerId,
            IEnumerable<FastCasAddress> acceptors,
            bool enableDistinguishedLeader,
            bool enableFastCommit)
            : base(
                new ProposerRuntime(host._options.CreateProposerDiagnostics?.Invoke(host.NodeId)),
                acceptors,
                enableDistinguishedLeader,
                enableFastCommit)
        {
            _host = host;
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(proposerId, 0);
            _proposer = proposer;
            _proposerId = proposerId;
        }

        public override int ProposerId => _proposerId;

        protected override void OnSendPrepare(PrepareRequest request)
            => _host.SendPrepare(this, _proposer, request);

        protected override void OnSendAccept(AcceptRequest<TValue> request)
            => _host.SendAccept(_proposer, request);

        protected override void OnProposalCompleted(ProposeResponse<TValue> response)
        {
            if (CurrentOperation is not IRoutedOperation<FastCasAddress> routedOperation)
            {
                throw new InvalidOperationException(
                    $"Proposer '{_proposer}' cannot route a completed proposal because CurrentOperation '{CurrentOperation}' does not expose a caller.");
            }

            _host._transport.Send(new FastCasProposeResponseMessage<TValue>(
                _proposer,
                routedOperation.Caller,
                _host.ActiveRequestId,
                response));
            _host.OnProposalCompleted();
        }

        protected override void OnValueCommitted(CommittedValue<TValue> committedValue)
        {
            _host.BroadcastCommittedValue(
                Ballot,
                committedValue.Value,
                PreparedBallot ?? Ballot.Zero);
        }
    }
}
