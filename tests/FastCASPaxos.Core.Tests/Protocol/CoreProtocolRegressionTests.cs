using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class CoreProtocolRegressionTests
{
    [Fact]
    public void AcceptorEngine_Accept_PiggybacksFuturePrepare()
    {
        var engine = new RecordingAcceptorEngine<TestValue, string>("a1");
        var ballot = Ballot.InitialClassic(1);
        var nextBallot = ballot.NextRound(1);

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(ballot,
            Value: new TestValue(1, "A"),
            NextBallotToPrepare: nextBallot));

        Assert.NotNull(engine.LastAcceptAccepted);
        var accepted = engine.LastAcceptAccepted!.Value;
        Assert.Equal(nextBallot, accepted.PromisedBallot);
        Assert.Equal(ballot, engine.AcceptedBallot);
        Assert.Equal(nextBallot, engine.PromisedBallot);
        Assert.Equal(new TestValue(1, "A"), engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_RejectsSameBallotDifferentValue()
    {
        var engine = new RecordingAcceptorEngine<TestValue, string>("a1");
        var ballot = Ballot.InitialClassic(1);

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(ballot,
            Value: new TestValue(1, "A"),
            NextBallotToPrepare: null));
        Assert.NotNull(engine.LastAcceptAccepted);

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(ballot,
            Value: new TestValue(1, "B"),
            NextBallotToPrepare: null));

        Assert.NotNull(engine.LastAcceptRejected);
        var rejected = engine.LastAcceptRejected!.Value;
        Assert.Equal(ballot, rejected.ConflictingBallot);
        Assert.Equal(new TestValue(1, "A"), engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_TreatsSameBallotSameValueAsIdempotent()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var promisedBallot = acceptedBallot.NextRound(2);
        var acceptedValue = new TestValue(1, "A");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = promisedBallot,
                AcceptedBallot = acceptedBallot,
                AcceptedValue = acceptedValue,
            });

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: acceptedBallot,
            Value: acceptedValue,
            NextBallotToPrepare: promisedBallot.NextRound(3)));

        Assert.NotNull(engine.LastAcceptAccepted);
        var accepted = engine.LastAcceptAccepted!.Value;
        Assert.Equal(promisedBallot, accepted.PromisedBallot);
        Assert.Equal(promisedBallot, engine.PromisedBallot);
        Assert.Equal(acceptedBallot, engine.AcceptedBallot);
        Assert.Equal(acceptedValue, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_AllowsFastRoundWithoutVersionAdvance()
    {
        var ballot = Ballot.InitialFast();
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = ballot,
                AcceptedBallot = ballot,
                AcceptedValue = new TestValue(1, "A"),
            });

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: ballot.NextRound(proposer: 0),
            Value: new TestValue(1, "A"),
            NextBallotToPrepare: null));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(ballot, engine.PromisedBallot);
        Assert.Equal(ballot.NextRound(proposer: 0), engine.AcceptedBallot);
        Assert.Equal(new TestValue(1, "A"), engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_AllowsClassicRoundConflictRepairAtSameVersion()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var repairBallot = acceptedBallot.NextRound(2);
        var repairedValue = new TestValue(1, "B");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = acceptedBallot,
                AcceptedBallot = acceptedBallot,
                AcceptedValue = new TestValue(1, "A"),
            });

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: repairBallot,
            Value: repairedValue,
            NextBallotToPrepare: null));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(repairBallot, engine.AcceptedBallot);
        Assert.Equal(repairedValue, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_AllowsClassicRoundNonSuccessor()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var repairBallot = acceptedBallot.NextRound(2);
        var repairedValue = new TestValue(2, "Z");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = acceptedBallot,
                AcceptedBallot = acceptedBallot,
                AcceptedValue = new TestValue(1, "A"),
            });

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: repairBallot,
            Value: repairedValue,
            NextBallotToPrepare: null));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(repairBallot, engine.AcceptedBallot);
        Assert.Equal(repairedValue, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_AllowsFastRoundNonSuccessorWhenBallotAdvances()
    {
        var acceptedBallot = Ballot.InitialFast();
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = acceptedBallot,
                AcceptedBallot = acceptedBallot,
                AcceptedValue = new TestValue(1, "A"),
            });

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: acceptedBallot.NextRound(proposer: 0),
            Value: new TestValue(2, "Z"),
            NextBallotToPrepare: null));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(acceptedBallot.NextRound(proposer: 0), engine.AcceptedBallot);
        Assert.Equal(new TestValue(2, "Z"), engine.AcceptedValue);
    }

    [Fact]
    public void ProposerEngine_StartProposal_CompletesClassicCommit()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(1, "A");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a3"));
        var accept = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(1, "A"));

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a1", accept.Ballot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a2", accept.Ballot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a3", accept.Ballot));
        Assert.NotNull(engine.LastValueCommitted);
        Assert.Equal(new TestValue(1, "A"), engine.LastValueCommitted!.Value.Value);
        Assert.Equal(ProposerValueStatus.Cached, engine.CurrentValueStatus);
        Assert.Equal(new TestValue(1, "A"), engine.CurrentValue);
    }

    [Fact]
    public void ProposerEngine_Completion_KeepsCurrentOperationAvailableUntilCallbackReturns()
    {
        var engine = CreateClassicProposer();
        var operation = new RoutedOperation<TestValue, string>("client", AppendOperation(0, "noop"));
        var proposal = operation;

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a3"));

        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Same(operation, engine.CurrentOperationDuringCompletion);
        Assert.False(engine.HasCurrentOperation);
    }

    [Fact]
    public void ProposerEngine_HandleAcceptResponse_RetriesPrepareUsingHighestRejectedBallot()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(1, "A");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a3"));
        var accept = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(1, "A"));
        var highestConflict = new Ballot(2, 4);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a1", new Ballot(1, 2)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a2", highestConflict));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a3", new Ballot(2, 3)));
        var retryPrepare = AssertPrepare(engine, highestConflict.NextRound(proposer: 1));

        Assert.Equal(highestConflict.NextRound(proposer: 1), retryPrepare.Ballot);
    }

    [Fact]
    public void ProposerEngine_PrepareRecovery_RebasesCurrentRequestWithoutRepairAccept()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(2, "B");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        var acceptedValue = new TestValue(1, "A");

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1", ballot: Ballot.InitialClassic(1), value: acceptedValue));
        Assert.False(engine.HasWork);
        Assert.Equal(acceptedValue, engine.CachedValue);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2", ballot: Ballot.InitialClassic(1), value: acceptedValue));
        Assert.False(engine.HasWork);
        Assert.Equal(acceptedValue, engine.CachedValue);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a3", ballot: Ballot.InitialClassic(1), value: acceptedValue));
        var recoveryAccept = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(2, "AB"));

        Assert.Equal(new TestValue(2, "AB"), recoveryAccept.Value);
        Assert.Equal(ProposerValueStatus.PendingAccept, engine.CurrentValueStatus);
        Assert.Equal(new TestValue(2, "AB"), engine.CurrentValue);
        Assert.Equal(acceptedValue, engine.CachedValue);
        Assert.Equal(new TestValue(2, "AB"), engine.PendingAcceptValue);
        Assert.Null(engine.LastValueCommitted);
    }

    [Fact]
    public void ProposerEngine_HasQuorum_UsesFastAndClassicThresholds()
    {
        var engine = CreateFastProposer();

        Assert.False(engine.HasQuorum(Ballot.InitialClassic(1), responses: 2));
        Assert.True(engine.HasQuorum(Ballot.InitialClassic(1), responses: 3));
        Assert.False(engine.HasQuorum(Ballot.InitialFast(), responses: 3));
        Assert.True(engine.HasQuorum(Ballot.InitialFast(), responses: 4));
    }

    [Fact]
    public void ProposerEngine_StartProposal_InitialFastBallotSkipsPrepareAndCommits()
    {
        var engine = CreateFastProposer();
        var proposal = AppendOperation(1, "A");
        var preparedBallot = Ballot.InitialFast().NextRound(proposer: 0);

        engine.ResetRecording();
        engine.StartProposal(proposal);

        Assert.Null(engine.LastSendPrepare);
        var accept = AssertAccept(engine, Ballot.InitialFast(), new TestValue(1, "A"), preparedBallot);
        DriveFastAcceptQuorum(engine, accept.Ballot.Round, accept.Ballot, preparedNextBallot: preparedBallot);

        Assert.NotNull(engine.LastProposalCompleted);
        Assert.NotNull(engine.LastValueCommitted);
        Assert.Equal(new TestValue(1, "A"), engine.LastProposalCompleted!.Value.CommittedValue);
        Assert.Equal(new TestValue(1, "A"), engine.LastValueCommitted!.Value.Value);
        Assert.Equal(preparedBallot, engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngine_StartProposal_ReusesPreparedBallotAfterPreparedNextBallotQuorum()
    {
        var engine = CreateFastProposer();
        var firstProposal = AppendOperation(1, "A");
        var initialFastBallot = Ballot.InitialFast();
        var preparedBallot = initialFastBallot.NextRound(proposer: 0);

        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        Assert.Null(engine.LastSendPrepare);
        var firstAccept = AssertAccept(engine, initialFastBallot, new TestValue(1, "A"), preparedBallot);
        DriveFastAcceptQuorum(engine, firstAccept.Ballot.Round, firstAccept.Ballot, preparedNextBallot: preparedBallot);

        Assert.Equal(preparedBallot, engine.PreparedBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);
        var secondAccept = AssertAccept(engine, preparedBallot, new TestValue(2, "AB"), preparedBallot.NextRound(proposer: 0));

        Assert.Equal(preparedBallot, secondAccept.Ballot);
        Assert.Null(engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_WhenOperationCannotApply_RetriesPrepare()
    {
        var engine = CreateClassicProposer();
        var initialProposal = AppendOperation(1, "A");

        engine.ResetRecording();
        engine.StartProposal(initialProposal);
        var initialPrepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(initialPrepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(initialPrepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(initialPrepare.Ballot.Round, "a3"));
        var initialAccept = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(1, "A"));

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a1", initialAccept.Ballot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a2", initialAccept.Ballot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a3", initialAccept.Ballot));

        var failingProposal = AppendOperation(3, "C");
        engine.ResetRecording();
        engine.StartProposal(failingProposal);
        var firstRetryPrepare = AssertPrepare(engine, new Ballot(2, 1));
        var recoveredValue = new TestValue(1, "A");

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(firstRetryPrepare.Ballot.Round, "a1", ballot: initialAccept.Ballot, value: recoveredValue));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(firstRetryPrepare.Ballot.Round, "a2", ballot: initialAccept.Ballot, value: recoveredValue));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(firstRetryPrepare.Ballot.Round, "a3", ballot: initialAccept.Ballot, value: recoveredValue));
        var secondRetryPrepare = AssertPrepare(engine, new Ballot(3, 1));

        Assert.Equal(new Ballot(3, 1), secondRetryPrepare.Ballot);
        Assert.Equal(recoveredValue, engine.CachedValue);
    }

    [Fact]
    public void ProposerEngine_FastConflict_RecoversWithoutExtraClassicRetryWhenNoFastValueCanStillWin()
    {
        var engine = CreateFastProposer();
        var initialFastBallot = Ballot.InitialFast();
        var secondFastBallot = initialFastBallot.NextRound(proposer: 0);
        var thirdFastBallot = secondFastBallot.NextRound(proposer: 0);

        var firstProposal = AppendOperation(1, "A");
        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        Assert.Null(engine.LastSendPrepare);
        var firstAccept = AssertAccept(engine, initialFastBallot, new TestValue(1, "A"), secondFastBallot);
        DriveFastAcceptQuorum(engine, firstAccept.Ballot.Round, firstAccept.Ballot, preparedNextBallot: secondFastBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);
        var secondAccept = AssertAccept(engine, secondFastBallot, new TestValue(2, "AB"), thirdFastBallot);
        DriveFastAcceptQuorum(engine, secondAccept.Ballot.Round, secondAccept.Ballot, preparedNextBallot: thirdFastBallot, matchingPreparedResponses: 3);

        var thirdProposal = AppendOperation(3, "C");
        engine.ResetRecording();
        engine.StartProposal(thirdProposal);
        var thirdPrepare = AssertPrepare(engine, thirdFastBallot);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a1", ballot: thirdFastBallot, value: new TestValue(2, "AB")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a2", ballot: thirdFastBallot, value: new TestValue(2, "AB")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a3", ballot: thirdFastBallot, value: new TestValue(2, "AX")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a4", ballot: thirdFastBallot, value: new TestValue(2, "AX")));
        _ = AssertAccept(engine, thirdFastBallot, new TestValue(3, "ABC"), nextBallotToPrepare: new Ballot(4, 0));
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_FastSplitContinuesCurrentRequestWhenNoFastValueCanStillWin()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(3, "C");
        var fastBallot = Ballot.InitialFast().NextRound(proposer: 0);

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var initialPrepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var ax = new TestValue(2, "AX");
        var ab = new TestValue(2, "AB");
        var ac = new TestValue(2, "AC");

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(initialPrepare.Ballot.Round, "a1", ballot: fastBallot, value: ax));
        Assert.False(engine.HasWork);
        Assert.Equal(ax, engine.CachedValue);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(initialPrepare.Ballot.Round, "a2", ballot: fastBallot, value: ab));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(initialPrepare.Ballot.Round, "a3", ballot: fastBallot, value: ac));
        var recoveryAccept = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(3, "AXC"));

        Assert.Equal(new TestValue(3, "AXC"), recoveryAccept.Value);
    }

    [Fact]
    public void OperationDriver_SchedulesPerProposerAndComputesCompletion()
    {
        var driver = new RecordingOperationDriver<TestValue, string>("client", ["p1", "p2"]);
        driver.Enqueue("p1", AppendOperation(1, "A"));
        driver.Enqueue("p2", AppendOperation(1, "B"));
        driver.Enqueue("p2", AppendOperation(2, "C"));

        driver.ResetRecording();
        driver.StartReadyRequests();
        Assert.Equal(2, driver.SentProposals.Count);

        var firstP1Request = Assert.Single(driver.SentProposals, s => s.Proposer == "p1").Operation;
        var firstP2Request = Assert.Single(driver.SentProposals, s => s.Proposer == "p2").Operation;

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>("p1", new ProposeResponse<TestValue>(0, new TestValue(1, "A"))));
        Assert.Null(driver.LastCompletion);
        Assert.Empty(driver.SentProposals);

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>("p2", new ProposeResponse<TestValue>(0, new TestValue(1, "B"))));
        Assert.Null(driver.LastCompletion);
        var secondP2Scheduled = Assert.Single(driver.SentProposals);
        Assert.Equal("p2", secondP2Scheduled.Proposer);

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>(
            "p2",
            new ProposeResponse<TestValue>(1, new TestValue(2, "BC"))));

        Assert.NotNull(driver.LastCompletion);
        Assert.False(driver.LastCompletion!.FinalValuesAgree);
        Assert.Equal(new TestValue(1, "A"), driver.LastCompletion.FinalValues["p1"]);
        Assert.Equal(new TestValue(2, "BC"), driver.LastCompletion.FinalValues["p2"]);
    }

    private static RecordingProposerEngine<TestValue, string> CreateClassicProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: false, enableFastCommit: false);

    private static RecordingProposerEngine<TestValue, string> CreateFastProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: false, enableFastCommit: true);

    private static IOperation<TestValue> AppendOperation(int expectedVersion, string suffix) =>
        new Operation<TestValue, TestValue>
        {
            Input = new TestValue(expectedVersion, suffix),
            Name = $"Append '{suffix}' at version {expectedVersion}",
            Apply = static (current, input) =>
            {
                if (input.Version == current.Version + 1)
                {
                    var currentValue = current.Value ?? string.Empty;
                    return (OperationStatus.Success, new TestValue(input.Version, currentValue + input.Value));
                }

                if (current.Version >= input.Version)
                {
                    return (OperationStatus.NotApplicable, current);
                }

                return (OperationStatus.Failed, current);
            },
        };

    private static PreparePromise<TestValue, string> SuccessPrepare(int round, string acceptor, Ballot ballot = default, TestValue value = default) =>
        new(round, acceptor, ballot, value);

    private static AcceptAccepted<string> SuccessAccept(int round, string acceptor, Ballot ballot, Ballot? preparedNextBallot = null) =>
        new(round, acceptor, preparedNextBallot ?? Ballot.Zero);

    private static AcceptRejected<string> RejectedAccept(int round, string acceptor, Ballot ballot) =>
        new(round, acceptor, ballot);

    private static PrepareRequest AssertPrepare(RecordingProposerEngine<TestValue, string> engine, Ballot ballot)
    {
        Assert.NotNull(engine.LastSendPrepare);
        var prepare = engine.LastSendPrepare!.Value;
        Assert.Equal(ballot, prepare.Ballot);
        return prepare;
    }

    private static AcceptRequest<TestValue> AssertAccept(
        RecordingProposerEngine<TestValue, string> engine,
        Ballot ballot,
        TestValue value,
        Ballot? nextBallotToPrepare = null)
    {
        Assert.NotNull(engine.LastSendAccept);
        var accept = engine.LastSendAccept!.Value;
        Assert.Equal(ballot, accept.Ballot);
        Assert.Equal(value, accept.Value);
        Assert.Equal(nextBallotToPrepare, accept.NextBallotToPrepare);
        return accept;
    }

    private static AcceptRequest<TestValue> DriveFastPrepareQuorum(
        RecordingProposerEngine<TestValue, string> engine,
        int round,
        TestValue value,
        Ballot ballot)
    {
        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(round, "a1", ballot, value));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(round, "a2", ballot, value));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(round, "a3", ballot, value));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(round, "a4", ballot, value));
        Assert.NotNull(engine.LastSendAccept);
        return engine.LastSendAccept!.Value;
    }

    private static void DriveFastAcceptQuorum(
        RecordingProposerEngine<TestValue, string> engine,
        int round,
        Ballot ballot,
        Ballot preparedNextBallot,
        int matchingPreparedResponses = 4)
    {
        for (var i = 1; i <= 4; i++)
        {
            Ballot? responsePreparedBallot = i <= matchingPreparedResponses ? preparedNextBallot : null;
            engine.ResetRecording();
            engine.HandleAcceptAccepted(SuccessAccept(round, $"a{i}", ballot, responsePreparedBallot));
            if (i < 4)
            {
                Assert.False(engine.HasWork);
            }
        }
    }

    private readonly record struct TestValue(int Version, string Value)
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
