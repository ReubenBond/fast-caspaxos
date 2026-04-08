using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

/// <summary>
/// Combines coordinator, proposer, acceptor, and learner-facing behavior for a single register replica
/// while reusing the existing transport-agnostic proposer and acceptor engines.
/// </summary>
public sealed class ConsensusRegister<TValue, TRoute>
    where TRoute : notnull
{
    private readonly TRoute _proposer;
    private readonly TRoute _acceptor;
    private readonly Queue<PendingRequest> _requestQueue = new();
    private readonly Queue<LocalMessage> _localMessages = new();
    private readonly Queue<ConsensusRegisterOutputMessage<TValue, TRoute>> _outbox = new();
    private readonly RegisterProposerEngine _proposerEngine;
    private AcceptorState<TValue> _acceptorState;
    private PendingRequest? _activeRequest;
    private LearnedCommit? _pendingLearnedCommit;
    private bool _draining;

    public ConsensusRegister(
        TRoute proposer,
        TRoute acceptor,
        int proposerId,
        TValue initialValue,
        IEnumerable<TRoute> acceptors,
        ProposerModePolicy modePolicy,
        AcceptorState<TValue>? acceptorState = null,
        ProposerRuntime? proposerRuntime = null)
    {
        ArgumentNullException.ThrowIfNull(acceptors);
        ArgumentNullException.ThrowIfNull(modePolicy);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(proposerId, 0);

        _proposer = proposer;
        _acceptor = acceptor;
        var acceptorList = acceptors.Distinct().ToList();
        if (!acceptorList.Contains(acceptor))
        {
            acceptorList.Add(acceptor);
        }

        Proposer = proposer;
        Acceptor = acceptor;
        ProposerId = proposerId;
        Acceptors = acceptorList;

        _proposerEngine = new RegisterProposerEngine(
            this,
            proposerId,
            proposerRuntime ?? new ProposerRuntime(),
            initialValue,
            acceptorList,
            modePolicy);
        ModePolicy = _proposerEngine.ModePolicy;

        _acceptorState = acceptorState ?? AcceptorState<TValue>.CreateInitial(initialValue);
    }

    public TRoute Proposer { get; }

    public TRoute Acceptor { get; }

    public int ProposerId { get; }

    public IReadOnlyList<TRoute> Acceptors { get; }

    public ProposerModePolicy ModePolicy { get; }

    public Ballot ProposerBallot => _proposerEngine.Ballot;

    public Ballot? ProposerPreparedBallot => _proposerEngine.PreparedBallot;

    /// <summary>
    /// Exposes the proposer's single value slot. Interpret it via <see cref="ProposerValueStatus"/>.
    /// </summary>
    public TValue ProposerCurrentValue => _proposerEngine.CurrentValue;

    /// <summary>
    /// Exposes the stable cached proposer value when one exists. During <see cref="ProposerValueStatus.PendingAccept"/>,
    /// the register no longer retains a separate cached proposer value.
    /// </summary>
    public TValue ProposerCachedValue => _proposerEngine.CurrentValueStatus switch
    {
        ProposerValueStatus.Cached => _proposerEngine.CurrentValue,
        ProposerValueStatus.None => _acceptorState.AcceptedValue,
        ProposerValueStatus.PendingAccept => throw new InvalidOperationException(
            "ConsensusRegister does not retain a separate cached proposer value while an accept value is pending. Use ProposerCurrentValue and ProposerValueStatus instead."),
        _ => throw new InvalidOperationException(
            $"ConsensusRegister observed unknown proposer value state '{_proposerEngine.CurrentValueStatus}'."),
    };

    /// <summary>
    /// Exposes the in-flight accept value, or the initial/default value when no accept is pending.
    /// </summary>
    public TValue ProposerPendingAcceptValue =>
        _proposerEngine.CurrentValueStatus == ProposerValueStatus.PendingAccept
            ? _proposerEngine.CurrentValue
            : _proposerEngine.InitialValue;

    public ProposerValueStatus ProposerValueStatus => _proposerEngine.CurrentValueStatus;

    public bool ProposerHasPendingProposal => _proposerEngine.HasPendingProposal;

    public bool HasPendingRequests =>
        _activeRequest is not null
        || _proposerEngine.HasPendingProposal
        || _requestQueue.Count > 0
        || _localMessages.Count > 0;

    public AcceptorState<TValue> AcceptorState => _acceptorState;

    public void SubmitClientRequest(TRoute from, int requestId, IOperation<TValue> operation) =>
        SubmitClientRequest(new ConsensusRegisterClientRequest<TValue, TRoute>(from, requestId, operation));

    public void SubmitClientRequest(ConsensusRegisterClientRequest<TValue, TRoute> request)
    {
        ArgumentNullException.ThrowIfNull(request.Operation);
        EnqueueOrStart(new PendingRequest(
            request.RequestId,
            request.From,
            request.From,
            request.Operation,
            IsForwardedExecution: false));
    }

    public void DeliverForwardedRequest(ConsensusRegisterForwardedRequest<TValue, TRoute> request)
    {
        ArgumentNullException.ThrowIfNull(request.Operation);
        EnqueueOrStart(new PendingRequest(
            request.RequestId,
            request.From,
            request.OriginalCaller,
            request.Operation,
            IsForwardedExecution: true));
    }

    public void DeliverPrepare(TRoute from, PrepareRequest request)
    {
        HandlePrepare(from, request);
        DrainWorkLoop();
    }

    public void DeliverAccept(TRoute from, AcceptRequest<TValue> request)
    {
        HandleAccept(from, request);
        DrainWorkLoop();
    }

    public void DeliverPreparePromise(PreparePromise<TValue, TRoute> message)
    {
        _proposerEngine.HandlePreparePromised(message);
        DrainWorkLoop();
    }

    public void DeliverPrepareRejection(PrepareRejection<TValue, TRoute> message)
    {
        _proposerEngine.HandlePrepareRejected(message);
        DrainWorkLoop();
    }

    public void DeliverAcceptAccepted(AcceptAccepted<TRoute> message)
    {
        _proposerEngine.HandleAcceptAccepted(message);
        DrainWorkLoop();
    }

    public void DeliverAcceptRejected(AcceptRejected<TRoute> message)
    {
        _proposerEngine.HandleAcceptRejected(message);
        DrainWorkLoop();
    }

    public void LearnCommittedValue(Ballot acceptedBallot, TValue value, Ballot preparedBallot = default)
    {
        if (HasPendingRequests)
        {
            QueueLearnedCommit(acceptedBallot, value, preparedBallot);
        }
        else
        {
            _proposerEngine.LearnCommittedValue(acceptedBallot, value, preparedBallot);
        }

        DrainWorkLoop();
    }

    public bool HandleResponseTimeout(int round)
    {
        var retried = _proposerEngine.HandleResponseTimeout(round);
        DrainWorkLoop();
        return retried;
    }

    public List<ConsensusRegisterOutputMessage<TValue, TRoute>> DrainMessages()
    {
        List<ConsensusRegisterOutputMessage<TValue, TRoute>> messages = [];
        while (_outbox.TryDequeue(out var message))
        {
            messages.Add(message);
        }

        return messages;
    }

    private void EnqueueOrStart(PendingRequest request)
    {
        if (_activeRequest is not null || _proposerEngine.HasPendingProposal)
        {
            _requestQueue.Enqueue(request);
        }
        else
        {
            StartActiveRequest(request);
        }

        DrainWorkLoop();
    }

    private void StartActiveRequest(PendingRequest request)
    {
        _activeRequest = request;
        _proposerEngine.StartProposal(request.Operation);
    }

    private void QueueLearnedCommit(Ballot acceptedBallot, TValue value, Ballot preparedBallot)
    {
        if (acceptedBallot < _proposerEngine.Ballot)
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

    private void ApplyPendingLearnedCommitIfIdle()
    {
        if (_activeRequest is not null
            || _proposerEngine.HasPendingProposal
            || _requestQueue.Count > 0
            || _pendingLearnedCommit is not { } learnedCommit)
        {
            return;
        }

        _pendingLearnedCommit = null;
        _proposerEngine.LearnCommittedValue(
            learnedCommit.AcceptedBallot,
            learnedCommit.Value,
            learnedCommit.PreparedBallot);
    }

    private void DrainWorkLoop()
    {
        if (_draining)
        {
            return;
        }

        _draining = true;
        try
        {
            while (true)
            {
                while (_localMessages.TryDequeue(out var localMessage))
                {
                    switch (localMessage)
                    {
                        case LocalPrepareRequest prepare:
                            HandlePrepare(prepare.From, prepare.Payload);
                            break;
                        case LocalAcceptRequest accept:
                            HandleAccept(accept.From, accept.Payload);
                            break;
                        case LocalPreparePromise preparePromise:
                            _proposerEngine.HandlePreparePromised(preparePromise.Payload);
                            break;
                        case LocalPrepareRejection prepareRejection:
                            _proposerEngine.HandlePrepareRejected(prepareRejection.Payload);
                            break;
                        case LocalAcceptAccepted acceptAccepted:
                            _proposerEngine.HandleAcceptAccepted(acceptAccepted.Payload);
                            break;
                        case LocalAcceptRejected acceptRejected:
                            _proposerEngine.HandleAcceptRejected(acceptRejected.Payload);
                            break;
                    }
                }

                if (_activeRequest is not null && !_proposerEngine.HasPendingProposal)
                {
                    _activeRequest = null;
                    ApplyPendingLearnedCommitIfIdle();
                    if (_activeRequest is null && _requestQueue.TryDequeue(out var next))
                    {
                        StartActiveRequest(next);
                        continue;
                    }
                }

                break;
            }
        }
        finally
        {
            _draining = false;
        }
    }

    private void QueueProposerMessage(PrepareRequest request)
    {
        foreach (var acceptor in Acceptors)
        {
            if (EqualityComparer<TRoute>.Default.Equals(acceptor, _acceptor))
            {
                _localMessages.Enqueue(new LocalPrepareRequest(_proposer, request));
            }
            else
            {
                _outbox.Enqueue(new PrepareRequestOutputMessage<TValue, TRoute>(_proposer, acceptor, request));
            }
        }
    }

    private void QueueProposerMessage(AcceptRequest<TValue> request)
    {
        foreach (var acceptor in Acceptors)
        {
            if (EqualityComparer<TRoute>.Default.Equals(acceptor, _acceptor))
            {
                _localMessages.Enqueue(new LocalAcceptRequest(_proposer, request));
            }
            else
            {
                _outbox.Enqueue(new AcceptRequestOutputMessage<TValue, TRoute>(_proposer, acceptor, request));
            }
        }
    }

    private void HandlePrepare(TRoute from, PrepareRequest request)
    {
        var result = AcceptorEngine.Prepare(ref _acceptorState, request);
        QueueAcceptorMessage(from, request, result);
    }

    private void HandleAccept(TRoute from, AcceptRequest<TValue> request)
    {
        var result = AcceptorEngine.Accept(ref _acceptorState, request);
        QueueAcceptorMessage(from, request, result);
    }

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        PrepareRequest request,
        AcceptorPrepareResult<TValue> result)
    {
        if (result.IsPromised)
        {
            QueueAcceptorMessage(
                responseTarget,
                new PreparePromise<TValue, TRoute>(
                    request.Ballot.Round,
                    _acceptor,
                    result.AcceptedBallot,
                    result.AcceptedValue));
        }
        else
        {
            QueueAcceptorMessage(
                responseTarget,
                new PrepareRejection<TValue, TRoute>(
                    request.Ballot.Round,
                    _acceptor,
                    result.AcceptedBallot,
                    result.AcceptedValue,
                    result.ConflictingBallot));
        }
    }

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        AcceptRequest<TValue> request,
        AcceptorAcceptResult result)
    {
        if (result.IsAccepted)
        {
            QueueAcceptorMessage(
                responseTarget,
                new AcceptAccepted<TRoute>(
                    request.Ballot.Round,
                    _acceptor,
                    result.PromisedBallot));
        }
        else
        {
            QueueAcceptorMessage(
                responseTarget,
                new AcceptRejected<TRoute>(
                    request.Ballot.Round,
                    _acceptor,
                    result.ConflictingBallot));
        }
    }

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        PreparePromise<TValue, TRoute> result) =>
        QueueAcceptorMessage(
            responseTarget,
            result,
            static payload => new LocalPreparePromise(payload),
            static (from, to, payload) => new PreparePromiseOutputMessage<TValue, TRoute>(from, to, payload));

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        PrepareRejection<TValue, TRoute> result) =>
        QueueAcceptorMessage(
            responseTarget,
            result,
            static payload => new LocalPrepareRejection(payload),
            static (from, to, payload) => new PrepareRejectionOutputMessage<TValue, TRoute>(from, to, payload));

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        AcceptAccepted<TRoute> result) =>
        QueueAcceptorMessage(
            responseTarget,
            result,
            static payload => new LocalAcceptAccepted(payload),
            static (from, to, payload) => new AcceptAcceptedOutputMessage<TValue, TRoute>(from, to, payload));

    private void QueueAcceptorMessage(
        TRoute responseTarget,
        AcceptRejected<TRoute> result) =>
        QueueAcceptorMessage(
            responseTarget,
            result,
            static payload => new LocalAcceptRejected(payload),
            static (from, to, payload) => new AcceptRejectedOutputMessage<TValue, TRoute>(from, to, payload));

    private void QueueAcceptorMessage<TPayload>(
        TRoute responseTarget,
        TPayload payload,
        Func<TPayload, LocalMessage> localFactory,
        Func<TRoute, TRoute, TPayload, ConsensusRegisterOutputMessage<TValue, TRoute>> outputFactory)
    {
        if (EqualityComparer<TRoute>.Default.Equals(responseTarget, _proposer))
        {
            _localMessages.Enqueue(localFactory(payload));
        }
        else
        {
            _outbox.Enqueue(outputFactory(_acceptor, responseTarget, payload));
        }
    }

    private void QueueProposalCompleted(ProposeResponse<TValue> response)
    {
        if (_activeRequest is not { } activeRequest)
        {
            throw new InvalidOperationException("ConsensusRegister has no active request to complete.");
        }

        if (activeRequest.IsForwardedExecution)
        {
            _outbox.Enqueue(new ForwardProposeResponseOutputMessage<TValue, TRoute>(
                activeRequest.ReplyTo,
                activeRequest.OriginalCaller,
                activeRequest.RequestId,
                _proposerEngine.Ballot,
                _proposerEngine.PreparedBallot ?? Ballot.Zero,
                response));
            return;
        }

        _outbox.Enqueue(new ProposeResponseOutputMessage<TValue, TRoute>(
            activeRequest.ReplyTo,
            activeRequest.RequestId,
            response));
    }

    private void QueueCommittedValue(TValue value)
    {
        _outbox.Enqueue(new LearnCommittedValueOutputMessage<TValue, TRoute>(
            _proposerEngine.Ballot,
            value,
            _proposerEngine.PreparedBallot ?? Ballot.Zero));
    }

    private readonly record struct PendingRequest(
        int RequestId,
        TRoute ReplyTo,
        TRoute OriginalCaller,
        IOperation<TValue> Operation,
        bool IsForwardedExecution);

    private readonly record struct LearnedCommit(
        Ballot AcceptedBallot,
        TValue Value,
        Ballot PreparedBallot);

    private abstract record LocalMessage;

    private sealed record LocalPrepareRequest(
        TRoute From,
        PrepareRequest Payload) : LocalMessage;

    private sealed record LocalAcceptRequest(
        TRoute From,
        AcceptRequest<TValue> Payload) : LocalMessage;

    private sealed record LocalPreparePromise(
        PreparePromise<TValue, TRoute> Payload) : LocalMessage;

    private sealed record LocalPrepareRejection(
        PrepareRejection<TValue, TRoute> Payload) : LocalMessage;

    private sealed record LocalAcceptAccepted(
        AcceptAccepted<TRoute> Payload) : LocalMessage;

    private sealed record LocalAcceptRejected(
        AcceptRejected<TRoute> Payload) : LocalMessage;

    private sealed class RegisterProposerEngine(
        ConsensusRegister<TValue, TRoute> owner,
        int proposerId,
        ProposerRuntime runtime,
        TValue initialValue,
        IEnumerable<TRoute> acceptors,
        ProposerModePolicy modePolicy) : ProposerEngine<TValue, TRoute>(runtime, initialValue, acceptors, modePolicy)
    {
        private readonly ConsensusRegister<TValue, TRoute> _owner = owner;
        private readonly int _proposerId = proposerId;

        public override int ProposerId => _proposerId;

        public TValue InitialValue => _initialValue;

        protected override void OnSendPrepare(PrepareRequest request) => _owner.QueueProposerMessage(request);

        protected override void OnSendAccept(AcceptRequest<TValue> request) => _owner.QueueProposerMessage(request);

        protected override void OnProposalCompleted(ProposeResponse<TValue> response) => _owner.QueueProposalCompleted(response);

        protected override void OnValueCommitted(CommittedValue<TValue> committedValue) => _owner.QueueCommittedValue(committedValue.Value);
    }

}
