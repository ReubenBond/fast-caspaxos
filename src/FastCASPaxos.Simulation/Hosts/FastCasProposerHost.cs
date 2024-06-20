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
    private readonly FastCasSimulationTransport<TValue> _transport;
    private readonly List<FastCasAddress> _acceptors;
    private readonly FastCasSimulationOptions _options;
    private HostProposerEngine _engine;
    private SimulationNodeContext? _simulationContext;
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

    public TValue CachedValue => _engine.CachedValue;

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
                StartNewProposal();
                _engine.StartProposal(AttachCaller(proposeRequest.From, proposeRequest.Payload));
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
            default:
                throw new InvalidOperationException($"Proposer host '{Address}' cannot handle message type '{message.GetType().Name}'.");
        }
    }

    /// <summary>
    /// Stops the proposer from handling new messages. Any already-sent messages are effectively dropped because the host no longer processes them.
    /// </summary>
    public void Crash()
    {
        ResetPendingRetryState();
        IsRunning = false;
    }

    /// <summary>
    /// Recreates the proposer engine from volatile state only.
    /// </summary>
    public void Restart()
    {
        ResetPendingRetryState();
        _engine = CreateEngine();
        IsRunning = true;
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

    private void ResetPendingRetryState()
    {
        _proposalGeneration++;
        _prepareSendCount = 0;
        _sentAcceptBeforeFirstPrepare = false;
    }

    private void OnAcceptStarted()
    {
        if (_prepareSendCount == 0)
        {
            _sentAcceptBeforeFirstPrepare = true;
        }
    }

    private void OnProposalCompleted() => ResetPendingRetryState();

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
    }

    private void SendPrepareNow(
        FastCasAddress proposer,
        PrepareRequest request)
    {
        foreach (var acceptor in _acceptors)
        {
            _transport.Send(new FastCasPrepareRequestMessage<TValue>(proposer, acceptor, request));
        }
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

            _host._transport.Send(new FastCasProposeResponseMessage<TValue>(_proposer, routedOperation.Caller, response));
            _host.OnProposalCompleted();
        }

        protected override void OnValueCommitted(CommittedValue<TValue> committedValue)
        {
            // Value committed is tracked at the protocol level; the simulation host doesn't need
            // additional action here since the proposer host's CachedValue is updated by the engine.
        }
    }
}
