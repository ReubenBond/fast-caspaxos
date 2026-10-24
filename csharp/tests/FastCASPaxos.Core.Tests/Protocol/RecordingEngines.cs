using System.Collections.Immutable;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Core.Tests.Protocol;

/// <summary>
/// Recording wrapper over the static <see cref="AcceptorEngine"/> that captures response payloads
/// for test assertions.
/// </summary>
public sealed class RecordingAcceptorEngine<TValue, TRoute>
{
    private AcceptorState<TValue> _state;

    public RecordingAcceptorEngine(
        TRoute acceptor,
        Ballot promisedBallot,
        Ballot acceptedBallot,
        TValue acceptedValue)
    {
        Acceptor = acceptor;
        _state = new AcceptorState<TValue>
        {
            PromisedBallot = promisedBallot,
            AcceptedBallot = acceptedBallot,
            AcceptedValue = acceptedValue,
        };
    }

    public RecordingAcceptorEngine(TRoute acceptor, TValue initialValue)
        : this(acceptor, Ballot.InitialFast(), Ballot.Zero, initialValue) { }

    public TRoute Acceptor { get; }

    public Ballot PromisedBallot => _state.PromisedBallot;

    public Ballot AcceptedBallot => _state.AcceptedBallot;

    public TValue AcceptedValue => _state.AcceptedValue;

    public AcceptorPrepareResult<TValue>? LastPrepareResult { get; private set; }

    public AcceptorAcceptResult? LastAcceptResult { get; private set; }

    public PreparePromise<TValue, TRoute>? LastPreparePromise { get; private set; }
    public PrepareRejection<TValue, TRoute>? LastPrepareRejection { get; private set; }
    public AcceptAccepted<TRoute>? LastAcceptAccepted { get; private set; }
    public AcceptRejected<TRoute>? LastAcceptRejected { get; private set; }

    public AcceptorState<TValue> GetState() => _state;

    public void ResetRecording()
    {
        LastPrepareResult = null;
        LastAcceptResult = null;
        LastPreparePromise = null;
        LastPrepareRejection = null;
        LastAcceptAccepted = null;
        LastAcceptRejected = null;
    }

    public void Prepare(PrepareRequest request)
    {
        var result = AcceptorEngine.Prepare(ref _state, request);
        LastPrepareResult = result;
        if (result.IsPromised)
        {
            LastPreparePromise = new PreparePromise<TValue, TRoute>(
                request.Ballot.Round,
                Acceptor,
                result.AcceptedBallot,
                result.AcceptedValue);
            return;
        }

        LastPrepareRejection = new PrepareRejection<TValue, TRoute>(
            request.Ballot.Round,
            Acceptor,
            result.AcceptedBallot,
            result.AcceptedValue,
            result.ConflictingBallot);
    }

    public void Accept(AcceptRequest<TValue> request)
    {
        var result = AcceptorEngine.Accept(ref _state, request);
        LastAcceptResult = result;
        if (result.IsAccepted)
        {
            LastAcceptAccepted = new AcceptAccepted<TRoute>(
                request.Ballot.Round,
                Acceptor,
                result.PromisedBallot);
            return;
        }

        LastAcceptRejected = new AcceptRejected<TRoute>(
            request.Ballot.Round,
            Acceptor,
            result.ConflictingBallot);
    }
}

/// <summary>
/// Recording subclass of <see cref="ProposerEngine{TValue,TRoute}"/> that captures callback
/// invocations for test assertions.
/// </summary>
public sealed class RecordingProposerEngine<TValue, TRoute> : ValueTrackingProposerEngine<TValue, TRoute>
    where TRoute : notnull
{
    private readonly int _proposerId;

    public RecordingProposerEngine(
        TRoute proposer,
        int proposerId,
        TValue initialValue,
        ImmutableArray<TRoute> acceptors,
        ProposerModePolicy modePolicy)
        : base(new ProposerRuntime(), initialValue, acceptors, modePolicy)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(proposerId, 0);
        _proposerId = proposerId;
    }

    public override int ProposerId => _proposerId;

    public PrepareRequest? LastSendPrepare { get; private set; }
    public AcceptRequest<TValue>? LastSendAccept { get; private set; }
    public ProposeResponse<TValue>? LastProposalCompleted { get; private set; }
    public IOperation<TValue>? CurrentOperationDuringCompletion { get; private set; }
    public CommittedValue<TValue>? LastValueCommitted { get; private set; }

    /// <summary>
    /// True if any callback was invoked since the last <see cref="ResetRecording"/>.
    /// Replaces the old <c>HasWork</c> property on <c>ProposerEngineResult</c>.
    /// </summary>
    public bool HasWork =>
        LastSendPrepare is not null
        || LastSendAccept is not null
        || LastProposalCompleted is not null
        || LastValueCommitted is not null;

    public bool HasCurrentOperation => CurrentOperation is not null;

    public void ResetRecording()
    {
        LastSendPrepare = null;
        LastSendAccept = null;
        LastProposalCompleted = null;
        CurrentOperationDuringCompletion = null;
        LastValueCommitted = null;
    }

    protected override void OnSendPrepare(PrepareRequest request) => LastSendPrepare = request;
    protected override void OnSendAccept(AcceptRequest<TValue> request) => LastSendAccept = request;
    protected override void OnProposalCompleted(ProposeResponse<TValue> response)
    {
        CurrentOperationDuringCompletion = CurrentOperation;
        LastProposalCompleted = response;
    }
    protected override void OnValueCommitted(CommittedValue<TValue> committedValue) => LastValueCommitted = committedValue;
}

/// <summary>
/// Recording subclass of <see cref="OperationDriver{TValue,TRoute}"/> that captures callback
/// invocations for test assertions.
/// </summary>
public sealed class RecordingOperationDriver<TValue, TRoute> : OperationDriver<TValue, TRoute>
    where TRoute : notnull
{
    public RecordingOperationDriver(TRoute caller, ImmutableArray<TRoute> proposers)
        : base(caller, proposers) { }

    public List<ScheduledProposal<TValue, TRoute>> SentProposals { get; } = [];
    public OperationDriverCompletion<TValue, TRoute>? LastCompletion { get; private set; }

    public void ResetRecording()
    {
        SentProposals.Clear();
        LastCompletion = null;
    }

    protected override void OnSendProposal(ScheduledProposal<TValue, TRoute> proposal) => SentProposals.Add(proposal);
    protected override void OnCompleted(OperationDriverCompletion<TValue, TRoute> completion) => LastCompletion = completion;
}
