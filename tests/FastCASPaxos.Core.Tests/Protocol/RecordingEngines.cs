using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Core.Tests.Protocol;

/// <summary>
/// Recording subclass of <see cref="AcceptorEngine{TValue,TRoute}"/> that captures callback
/// invocations for test assertions.
/// </summary>
public sealed class RecordingAcceptorEngine<TValue, TRoute> : AcceptorEngine<TValue, TRoute>
{
    public RecordingAcceptorEngine(TRoute acceptor, AcceptorState<TValue>? state = null)
        : base(acceptor, state) { }

    public PreparePromise<TValue, TRoute>? LastPreparePromise { get; private set; }
    public PrepareRejection<TValue, TRoute>? LastPrepareRejection { get; private set; }
    public AcceptAccepted<TRoute>? LastAcceptAccepted { get; private set; }
    public AcceptRejected<TRoute>? LastAcceptRejected { get; private set; }

    public void ResetRecording()
    {
        LastPreparePromise = null;
        LastPrepareRejection = null;
        LastAcceptAccepted = null;
        LastAcceptRejected = null;
    }

    protected override void OnPreparePromised(PreparePromise<TValue, TRoute> result) => LastPreparePromise = result;
    protected override void OnPrepareRejected(PrepareRejection<TValue, TRoute> result) => LastPrepareRejection = result;
    protected override void OnAcceptAccepted(AcceptAccepted<TRoute> result) => LastAcceptAccepted = result;
    protected override void OnAcceptRejected(AcceptRejected<TRoute> result) => LastAcceptRejected = result;
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
        IEnumerable<TRoute> acceptors,
        bool enableDistinguishedLeader,
        bool enableFastCommit)
        : base(new ProposerRuntime(), acceptors, enableDistinguishedLeader, enableFastCommit)
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
    public RecordingOperationDriver(TRoute caller, IEnumerable<TRoute> proposers)
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
