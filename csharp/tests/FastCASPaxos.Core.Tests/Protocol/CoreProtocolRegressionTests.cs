using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class CoreProtocolRegressionTests
{
    [Theory]
    [InlineData(true, 9, 99, false)]
    [InlineData(true, 10, 1, false)]
    [InlineData(true, 10, 2, true)]
    [InlineData(true, 10, 3, true)]
    [InlineData(true, 11, 1, true)]
    [InlineData(false, 9, 99, false)]
    [InlineData(false, 10, 1, false)]
    [InlineData(false, 10, 2, true)]
    [InlineData(false, 10, 3, true)]
    [InlineData(false, 11, 1, true)]
    public void AcceptorEnginePrepareUsesEffectiveBallotFloor(
        bool acceptedIsFloor, int round, int proposer, bool shouldPromise)
    {
        var floor = new Ballot(10, 2);
        var lower = new Ballot(5, 1);
        var acceptedBallot = acceptedIsFloor ? floor : lower;
        var promisedBallot = acceptedIsFloor ? lower : floor;
        var value = new TestValue(7, "accepted");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "A", promisedBallot, acceptedBallot, value);
        var request = new PrepareRequest(new Ballot(round, proposer));

        engine.Prepare(request);

        Assert.Equal(shouldPromise, engine.LastPrepareResult!.Value.IsPromised);
        Assert.Equal(shouldPromise ? Ballot.Zero : floor, engine.LastPrepareResult!.Value.ConflictingBallot);
        Assert.Equal(acceptedBallot, engine.LastPrepareResult!.Value.AcceptedBallot);
        Assert.Equal(value, engine.LastPrepareResult!.Value.AcceptedValue);
        Assert.Equal(shouldPromise ? request.Ballot : promisedBallot, engine.PromisedBallot);
        Assert.Equal(acceptedBallot, engine.AcceptedBallot);
        Assert.Equal(value, engine.AcceptedValue);
    }

    [Theory]
    [InlineData(true, 9, 99, false)]
    [InlineData(true, 10, 1, false)]
    [InlineData(true, 10, 2, false)]
    [InlineData(true, 10, 3, true)]
    [InlineData(true, 11, 1, true)]
    [InlineData(false, 9, 99, false)]
    [InlineData(false, 10, 1, false)]
    [InlineData(false, 10, 2, true)]
    [InlineData(false, 10, 3, true)]
    [InlineData(false, 11, 1, true)]
    public void AcceptorEngineAcceptUsesEffectiveBallotFloor(
        bool acceptedIsFloor, int round, int proposer, bool shouldAccept)
    {
        var floor = new Ballot(10, 2);
        var lower = new Ballot(5, 1);
        var acceptedBallot = acceptedIsFloor ? floor : lower;
        var promisedBallot = acceptedIsFloor ? lower : floor;
        var value = new TestValue(7, "accepted");
        var replacement = new TestValue(8, "replacement");
        var futurePromise = new Ballot(20, 4);
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "A", promisedBallot, acceptedBallot, value);
        var request = new AcceptRequest<TestValue>(new Ballot(round, proposer), replacement, futurePromise);

        engine.Accept(request);

        Assert.Equal(shouldAccept, engine.LastAcceptResult!.Value.IsAccepted);
        Assert.Equal(shouldAccept ? Ballot.Zero : floor, engine.LastAcceptResult!.Value.ConflictingBallot);
        Assert.Equal(shouldAccept ? futurePromise : promisedBallot, engine.PromisedBallot);
        Assert.Equal(shouldAccept ? request.Ballot : acceptedBallot, engine.AcceptedBallot);
        Assert.Equal(shouldAccept ? replacement : value, engine.AcceptedValue);
    }

    [Theory]
    [InlineData(10, 2, 7, "accepted", true)]
    [InlineData(10, 1, 7, "accepted", false)]
    [InlineData(10, 2, 8, "accepted", false)]
    [InlineData(10, 2, 7, "different", false)]
    public void AcceptorEngineAcceptIdempotenceRequiresExactBallotAndEntireValue(
        int round, int proposer, int version, string payload, bool shouldAccept)
    {
        var acceptedBallot = new Ballot(10, 2);
        var promisedBallot = new Ballot(11, 3);
        var value = new TestValue(7, "accepted");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "A", promisedBallot, acceptedBallot, value);

        engine.Accept(new AcceptRequest<TestValue>(
            new Ballot(round, proposer), new TestValue(version, payload), new Ballot(20, 4)));

        Assert.Equal(shouldAccept, engine.LastAcceptResult!.Value.IsAccepted);
        Assert.Equal(shouldAccept ? Ballot.Zero : promisedBallot, engine.LastAcceptResult!.Value.ConflictingBallot);
        Assert.Equal(shouldAccept ? promisedBallot : Ballot.Zero, engine.LastAcceptResult!.Value.PromisedBallot);
        Assert.Equal(promisedBallot, engine.PromisedBallot);
        Assert.Equal(acceptedBallot, engine.AcceptedBallot);
        Assert.Equal(value, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngineDelayedAcceptCannotEraseChosenValue()
    {
        var a = new RecordingAcceptorEngine<TestValue, string>("A", default);
        var b = new RecordingAcceptorEngine<TestValue, string>("B", default);
        var c = new RecordingAcceptorEngine<TestValue, string>("C", default);
        var oldBallot = new Ballot(5, 1);
        var newBallot = new Ballot(10, 2);
        var newValue = new TestValue(1, "new");

        foreach (var acceptor in new[] { a, c })
        {
            acceptor.Prepare(new PrepareRequest(oldBallot));
            Assert.True(acceptor.LastPrepareResult!.Value.IsPromised);
        }

        foreach (var acceptor in new[] { b, c })
        {
            acceptor.Prepare(new PrepareRequest(newBallot));
            Assert.True(acceptor.LastPrepareResult!.Value.IsPromised);
        }

        // A missed the newer prepare, but A+B can still choose the newer value.
        foreach (var acceptor in new[] { a, b })
        {
            acceptor.Accept(new AcceptRequest<TestValue>(newBallot, newValue, Ballot.Zero));
            Assert.True(acceptor.LastAcceptResult!.Value.IsAccepted);
            Assert.Equal(newBallot, acceptor.AcceptedBallot);
            Assert.Equal(newValue, acceptor.AcceptedValue);
        }

        Assert.Equal(oldBallot, a.PromisedBallot);
        a.ResetRecording();
        a.Accept(new AcceptRequest<TestValue>(oldBallot, new TestValue(1, "old"), Ballot.Zero));

        Assert.False(a.LastAcceptResult!.Value.IsAccepted);
        Assert.Equal(newBallot, a.LastAcceptRejected!.Value.ConflictingBallot);
        Assert.Equal(newBallot, a.AcceptedBallot);
        Assert.Equal(newValue, a.AcceptedValue);
        Assert.Equal(oldBallot, a.PromisedBallot);

        // Real rejection responses move a fresh proposer to precisely (11,3).
        var recovery = CreateThreeNodeProposer(3);
        recovery.StartProposal(ReadOperation());
        var initialPrepare = AssertPrepare(recovery, new Ballot(1, 3));
        RoutePrepare(recovery, initialPrepare, a);
        RoutePrepare(recovery, initialPrepare, c);
        var recoveryPrepare = AssertPrepare(recovery, new Ballot(11, 3));

        recovery.ResetRecording();
        RoutePrepare(recovery, recoveryPrepare, a);
        Assert.Null(recovery.LastSendAccept);
        RoutePrepare(recovery, recoveryPrepare, c);
        var identityAccept = AssertAccept(recovery, recoveryPrepare.Ballot, newValue);
        AssertNotCompleted(recovery);

        RouteAccept(recovery, identityAccept, a);
        AssertNotCompleted(recovery);
        RouteAccept(recovery, identityAccept, c);
        AssertCompleted(recovery, newValue);
    }

    [Fact]
    public void AcceptorEngineAcceptPiggybacksFuturePrepare()
    {
        var engine = new RecordingAcceptorEngine<TestValue, string>("a1", default);
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
    public void AcceptorEngineAcceptRejectsSameBallotDifferentValue()
    {
        var engine = new RecordingAcceptorEngine<TestValue, string>("a1", default);
        var ballot = Ballot.InitialClassic(1);

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(ballot,
            Value: new TestValue(1, "A"),
            NextBallotToPrepare: Ballot.Zero));
        Assert.NotNull(engine.LastAcceptAccepted);

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(ballot,
            Value: new TestValue(1, "B"),
            NextBallotToPrepare: Ballot.Zero));

        Assert.NotNull(engine.LastAcceptRejected);
        var rejected = engine.LastAcceptRejected!.Value;
        Assert.Equal(ballot, rejected.ConflictingBallot);
        Assert.Equal(new TestValue(1, "A"), engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngineAcceptTreatsSameBallotSameValueAsIdempotent()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var promisedBallot = acceptedBallot.NextRound(2);
        var acceptedValue = new TestValue(1, "A");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            promisedBallot,
            acceptedBallot,
            acceptedValue);

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
    public void AcceptorEngineAcceptAllowsFastRoundWithoutVersionAdvance()
    {
        var ballot = Ballot.InitialFast();
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            ballot,
            ballot,
            new TestValue(1, "A"));

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: ballot.NextRound(proposer: 0),
            Value: new TestValue(1, "A"),
            NextBallotToPrepare: Ballot.Zero));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(ballot, engine.PromisedBallot);
        Assert.Equal(ballot.NextRound(proposer: 0), engine.AcceptedBallot);
        Assert.Equal(new TestValue(1, "A"), engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngineAcceptAllowsClassicRoundConflictRepairAtSameVersion()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var repairBallot = acceptedBallot.NextRound(2);
        var repairedValue = new TestValue(1, "B");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            acceptedBallot,
            acceptedBallot,
            new TestValue(1, "A"));

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: repairBallot,
            Value: repairedValue,
            NextBallotToPrepare: Ballot.Zero));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(repairBallot, engine.AcceptedBallot);
        Assert.Equal(repairedValue, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngineAcceptAllowsClassicRoundNonSuccessor()
    {
        var acceptedBallot = Ballot.InitialClassic(1);
        var repairBallot = acceptedBallot.NextRound(2);
        var repairedValue = new TestValue(2, "Z");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            acceptedBallot,
            acceptedBallot,
            new TestValue(1, "A"));

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: repairBallot,
            Value: repairedValue,
            NextBallotToPrepare: Ballot.Zero));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(repairBallot, engine.AcceptedBallot);
        Assert.Equal(repairedValue, engine.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngineAcceptAllowsFastRoundNonSuccessorWhenBallotAdvances()
    {
        var acceptedBallot = Ballot.InitialFast();
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            acceptedBallot,
            acceptedBallot,
            new TestValue(1, "A"));

        engine.ResetRecording();
        engine.Accept(new AcceptRequest<TestValue>(Ballot: acceptedBallot.NextRound(proposer: 0),
            Value: new TestValue(2, "Z"),
            NextBallotToPrepare: Ballot.Zero));

        Assert.NotNull(engine.LastAcceptAccepted);
        Assert.Equal(acceptedBallot, engine.PromisedBallot);
        Assert.Equal(acceptedBallot.NextRound(proposer: 0), engine.AcceptedBallot);
        Assert.Equal(new TestValue(2, "Z"), engine.AcceptedValue);
    }

    [Fact]
    public void ProposerEngineStartProposalCompletesClassicCommit()
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
    public void ProposerEngineCompletionKeepsCurrentOperationAvailableUntilCallbackReturns()
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

        var accept = AssertAccept(engine, prepare.Ballot, default);
        AssertNotCompleted(engine);
        Assert.True(engine.HasCurrentOperation);
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a1", accept.Ballot));
        AssertNotCompleted(engine);
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a2", accept.Ballot));
        AssertNotCompleted(engine);
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a3", accept.Ballot));

        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Equal(default(TestValue), engine.LastProposalCompleted!.Value.CommittedValue);
        Assert.Same(operation, engine.CurrentOperationDuringCompletion);
        Assert.False(engine.HasCurrentOperation);
    }

    [Fact]
    public void ProposerEngineHandleAcceptResponseRetriesPrepareUsingHighestRejectedBallot()
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
    public void ProposerEnginePrepareRecoveryRebasesCurrentRequestWithoutRepairAccept()
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

        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a1", recoveryAccept.Ballot));
        AssertNotCompleted(engine);
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a2", recoveryAccept.Ballot));
        AssertNotCompleted(engine);
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a3", recoveryAccept.Ballot));
        AssertCompleted(engine, new TestValue(2, "AB"));
    }

    [Fact]
    public void ProposerEngineHasQuorumUsesFastAndClassicThresholds()
    {
        var engine = CreateFastProposer();

        Assert.False(engine.HasQuorum(Ballot.InitialClassic(1), responses: 2));
        Assert.True(engine.HasQuorum(Ballot.InitialClassic(1), responses: 3));
        Assert.False(engine.HasQuorum(Ballot.InitialFast(), responses: 3));
        Assert.True(engine.HasQuorum(Ballot.InitialFast(), responses: 4));
    }

    [Fact]
    public void ProposerEngineStartProposalInitialFastBallotSkipsPrepareAndCommits()
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
    public void ProposerEngineStartProposalReusesPreparedBallotAfterPreparedNextBallotQuorum()
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
    public void ProposerEngineLearnCommittedValueReusesLearnedPreparedNextFastBallot()
    {
        var engine = CreateFastProposer();
        var acceptedBallot = Ballot.InitialFast();
        var preparedBallot = acceptedBallot.NextRound(proposer: 0);

        engine.LearnCommittedValue(acceptedBallot, new TestValue(1, "A"), preparedBallot);

        Assert.Equal(acceptedBallot, engine.Ballot);
        Assert.Equal(new TestValue(1, "A"), engine.CachedValue);
        Assert.Equal(preparedBallot, engine.PreparedBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);

        Assert.Null(engine.LastSendPrepare);
        var secondAccept = AssertAccept(
            engine,
            preparedBallot,
            new TestValue(2, "AB"),
            preparedBallot.NextRound(proposer: 0));
        Assert.Equal(preparedBallot, secondAccept.Ballot);
        Assert.Null(engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngineLearnCommittedValueIgnoresStaleAcceptedAndPreparedBallots()
    {
        var engine = CreateFastProposer();
        var firstAcceptedBallot = Ballot.InitialFast();
        var secondAcceptedBallot = firstAcceptedBallot.NextRound(proposer: 0);
        var thirdPreparedBallot = secondAcceptedBallot.NextRound(proposer: 0);
        var ignoredPreparedBallot = thirdPreparedBallot.NextRound(proposer: 0);

        engine.LearnCommittedValue(secondAcceptedBallot, new TestValue(2, "AB"), thirdPreparedBallot);
        engine.LearnCommittedValue(firstAcceptedBallot, new TestValue(1, "A"), ignoredPreparedBallot);
        engine.LearnCommittedValue(secondAcceptedBallot, new TestValue(2, "AB"), Ballot.Zero);

        Assert.Equal(secondAcceptedBallot, engine.Ballot);
        Assert.Equal(new TestValue(2, "AB"), engine.CachedValue);
        Assert.Equal(thirdPreparedBallot, engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngineLearnCommittedValuePrefersHigherPreparedBallotAlreadyCached()
    {
        var engine = CreateFastProposer();
        var firstAcceptedBallot = new Ballot(1, 1);
        var higherPreparedBallot = new Ballot(2, 2);
        var secondAcceptedBallot = new Ballot(1, 2);
        var lowerPreparedBallot = new Ballot(2, 1);

        engine.LearnCommittedValue(firstAcceptedBallot, new TestValue(1, "A"), higherPreparedBallot);
        engine.LearnCommittedValue(secondAcceptedBallot, new TestValue(2, "AB"), lowerPreparedBallot);

        Assert.Equal(secondAcceptedBallot, engine.Ballot);
        Assert.Equal(new TestValue(2, "AB"), engine.CachedValue);
        Assert.Equal(higherPreparedBallot, engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngineLearnCommittedValueIgnoresRequestWhileOperationIsInFlight()
    {
        var engine = CreateFastProposer();
        var initialFastBallot = Ballot.InitialFast();
        var preparedBallot = initialFastBallot.NextRound(proposer: 0);

        engine.ResetRecording();
        engine.StartProposal(AppendOperation(1, "A"));
        var accept = AssertAccept(engine, initialFastBallot, new TestValue(1, "A"), preparedBallot);

        var exception = Record.Exception(() => engine.LearnCommittedValue(
            preparedBallot,
            new TestValue(99, "STALE"),
            preparedBallot.NextRound(proposer: 0)));

        Assert.Null(exception);
        Assert.Equal(initialFastBallot, engine.Ballot);
        Assert.Equal(ProposerValueStatus.PendingAccept, engine.CurrentValueStatus);
        Assert.Equal(accept.Value, engine.CurrentValue);

        DriveFastAcceptQuorum(engine, accept.Ballot.Round, accept.Ballot, preparedBallot);

        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Equal(new TestValue(1, "A"), engine.CachedValue);
        Assert.Equal(preparedBallot, engine.PreparedBallot);
    }

    [Fact]
    public void ProposerEngineBeginOperationDoesNotReusePreparedClassicBallotOwnedByAnotherProposer()
    {
        var engine = new RecordingProposerEngine<TestValue, string>(
            "p2",
            proposerId: 2,
            initialValue: default,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            ProposerModePolicy.StaticClassic);
        var acceptedBallot = Ballot.InitialClassic(1);
        var foreignPreparedBallot = acceptedBallot.NextRound(proposer: 1);

        engine.LearnCommittedValue(acceptedBallot, new TestValue(1, "A"), foreignPreparedBallot);

        Assert.Equal(acceptedBallot, engine.Ballot);
        Assert.Equal(new TestValue(1, "A"), engine.CachedValue);
        Assert.Equal(foreignPreparedBallot, engine.PreparedBallot);

        engine.ResetRecording();
        engine.StartProposal(AppendOperation(2, "B"));

        Assert.NotNull(engine.LastSendPrepare);
        var prepare = engine.LastSendPrepare!.Value;
        Assert.Equal(Ballot.InitialClassic(2).NextRound(2), prepare.Ballot);
    }

    [Fact]
    public void ProposerEngineHandlePrepareResponseWhenOperationCannotApplyRetriesPrepare()
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
    public void ProposerEngineFastConflictRecoversWithoutExtraClassicRetryWhenNoFastValueCanStillWin()
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
    public void ProposerEngineHandlePrepareResponseFastSplitContinuesCurrentRequestWhenNoFastValueCanStillWin()
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
    public void OperationDriverSchedulesPerProposerAndComputesCompletion()
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

    [Fact]
    public void ProposerEngineAdaptiveLearnedClassicLeaderHintEnablesForwarding()
    {
        var engine = CreateAdaptiveProposer();
        var leaderBallot = Ballot.InitialClassic(2);

        engine.LearnCommittedValue(leaderBallot, new TestValue(1, "A"));

        Assert.Equal(leaderBallot, engine.Ballot);
        Assert.Equal(2, engine.LeaderHintProposerId);
        Assert.True(engine.ShouldForwardToLeader);
        Assert.False(engine.ShouldAttemptLeaderTakeover());
    }

    [Fact]
    public void ProposerEngineAdaptiveLeaderMissThresholdSwitchesFromForwardingToTakeover()
    {
        var engine = CreateAdaptiveProposer(new AdaptiveProposerPolicy
        {
            LeaderMissThreshold = 2,
        });
        engine.LearnCommittedValue(Ballot.InitialClassic(2), new TestValue(1, "A"));

        engine.RecordLeaderForwardFailure();
        Assert.True(engine.ShouldForwardToLeader);
        Assert.False(engine.ShouldAttemptLeaderTakeover());

        engine.RecordLeaderForwardFailure();
        Assert.False(engine.ShouldForwardToLeader);
        Assert.True(engine.ShouldAttemptLeaderTakeover());
    }

    [Fact]
    public void ProposerEngineAdaptiveModeStateIsIsolatedPerEngine()
    {
        var sharedPolicy = ProposerModePolicy.Adaptive();
        var engine1 = new RecordingProposerEngine<TestValue, string>(
            "p1",
            proposerId: 1,
            initialValue: default,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            sharedPolicy);
        var engine2 = new RecordingProposerEngine<TestValue, string>(
            "p2",
            proposerId: 2,
            initialValue: default,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            sharedPolicy);

        engine1.LearnCommittedValue(Ballot.InitialClassic(3), new TestValue(1, "A"));

        Assert.NotSame(sharedPolicy, engine1.ModePolicy);
        Assert.NotSame(engine1.ModePolicy, engine2.ModePolicy);
        Assert.Null(sharedPolicy.LeaderHintProposerId);
        Assert.Equal(3, engine1.LeaderHintProposerId);
        Assert.Null(engine2.LeaderHintProposerId);
    }

    [Fact]
    public void ProposerEngineAdaptiveFastConflictBecomesSelfLedClassic()
    {
        var engine = CreateAdaptiveProposer();

        engine.ResetRecording();
        engine.StartProposal(AppendOperation(1, "A"));
        var initialAccept = AssertAccept(
            engine,
            Ballot.InitialFast(),
            new TestValue(1, "A"),
            Ballot.InitialFast().NextRound(proposer: 0));

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(initialAccept.Ballot.Round, "a1", new Ballot(1, 2)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(initialAccept.Ballot.Round, "a2", new Ballot(2, 2)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(initialAccept.Ballot.Round, "a3", new Ballot(2, 3)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(initialAccept.Ballot.Round, "a4", new Ballot(3, 2)));
        Assert.NotNull(engine.LastSendPrepare);
        var retryPrepare = engine.LastSendPrepare!.Value;

        Assert.False(retryPrepare.Ballot.IsFastRoundBallot);
        Assert.Equal(1, retryPrepare.Ballot.Proposer);
        Assert.Equal(1, engine.LeaderHintProposerId);
        Assert.True(engine.ConflictDebt > 0);
    }

    [Fact]
    public void ProposerEngineAdaptiveLearnedFastPreparedBallotReopensFastPath()
    {
        var engine = CreateAdaptiveProposer(new AdaptiveProposerPolicy
        {
            StartInFastMode = false,
            FastReopenThreshold = 0,
        });
        var acceptedFastBallot = Ballot.InitialFast();
        var preparedFastBallot = acceptedFastBallot.NextRound(proposer: 0);

        engine.LearnCommittedValue(acceptedFastBallot, new TestValue(1, "A"), preparedFastBallot);

        Assert.Null(engine.LeaderHintProposerId);
        Assert.Equal(preparedFastBallot, engine.PreparedBallot);

        engine.ResetRecording();
        engine.StartProposal(AppendOperation(2, "B"));
        var accept = AssertAccept(
            engine,
            preparedFastBallot,
            new TestValue(2, "AB"),
            preparedFastBallot.NextRound(proposer: 0));

        Assert.Equal(preparedFastBallot, accept.Ballot);
    }

    [Fact]
    public void ProposerEngineReadPartialAcceptedValueRequiresIdentityQuorumBeforeLaterRead()
    {
        var previous = new TestValue(7, "previous");
        var x = new TestValue(8, "X");
        var previousBallot = new Ballot(1, 1);
        var partialBallot = new Ballot(1, 2);
        var a = new RecordingAcceptorEngine<TestValue, string>("A", partialBallot, partialBallot, x);
        var b = new RecordingAcceptorEngine<TestValue, string>("B", previousBallot, previousBallot, previous);
        var c = new RecordingAcceptorEngine<TestValue, string>("C", previousBallot, previousBallot, previous);
        var r1 = CreateThreeNodeProposer(3);

        r1.StartProposal(ReadOperation());
        var prepare = AssertPrepare(r1, new Ballot(1, 3));
        RoutePrepare(r1, prepare, a);
        Assert.Null(r1.LastSendAccept);
        AssertNotCompleted(r1);
        RoutePrepare(r1, prepare, b);

        var identityAccept = AssertAccept(r1, prepare.Ballot, x);
        Assert.Equal(ProposerValueStatus.PendingAccept, r1.CurrentValueStatus);
        AssertNotCompleted(r1);
        Assert.Equal(previous, b.AcceptedValue);
        Assert.Equal(previous, c.AcceptedValue);

        RouteAccept(r1, identityAccept, a);
        AssertNotCompleted(r1);
        RouteAccept(r1, identityAccept, b);
        AssertCompleted(r1, x);
        Assert.Equal(x, b.AcceptedValue);
        Assert.Equal(previous, c.AcceptedValue);

        // The next read's prepare quorum excludes A, the only original holder of X.
        var r2 = CreateThreeNodeProposer(4);
        r2.StartProposal(ReadOperation());
        var secondPrepare = AssertPrepare(r2, new Ballot(1, 4));
        RoutePrepare(r2, secondPrepare, b);
        Assert.Null(r2.LastSendAccept);
        AssertNotCompleted(r2);
        RoutePrepare(r2, secondPrepare, c);
        var secondAccept = AssertAccept(r2, secondPrepare.Ballot, x);
        AssertNotCompleted(r2);
        RouteAccept(r2, secondAccept, b);
        AssertNotCompleted(r2);
        RouteAccept(r2, secondAccept, c);

        AssertCompleted(r2, x);
        Assert.Equal(x, c.AcceptedValue);
    }

    [Fact]
    public void ProposerEngineReadStalePiggybackedBallotRepreparesAndReturnsNewerValue()
    {
        var a = new RecordingAcceptorEngine<TestValue, string>("A", default);
        var b = new RecordingAcceptorEngine<TestValue, string>("B", default);
        var reader = CreateThreeNodeProposer(1, ProposerModePolicy.StaticLeader);
        var cached = new TestValue(1, "A");
        var newer = new TestValue(2, "AB");

        // Establish a real reusable promise, rather than injecting PreparedBallot.
        reader.StartProposal(AppendOperation(1, "A"));
        var firstPrepare = AssertPrepare(reader, new Ballot(1, 1));
        RoutePrepare(reader, firstPrepare, a);
        RoutePrepare(reader, firstPrepare, b);
        var reusableBallot = new Ballot(2, 1);
        var firstAccept = AssertAccept(reader, firstPrepare.Ballot, cached, reusableBallot);
        RouteAccept(reader, firstAccept, a);
        RouteAccept(reader, firstAccept, b);
        AssertCompleted(reader, cached);
        Assert.Equal(reusableBallot, reader.PreparedBallot);

        // Another proposer overtakes that promise and chooses a newer value.
        var writer = CreateThreeNodeProposer(2);
        writer.StartProposal(AppendOperation(2, "B"));
        var initialWriterPrepare = AssertPrepare(writer, new Ballot(1, 2));
        RoutePrepare(writer, initialWriterPrepare, a);
        RoutePrepare(writer, initialWriterPrepare, b);
        var writerPrepare = AssertPrepare(writer, new Ballot(2, 2));
        RoutePrepare(writer, writerPrepare, a);
        RoutePrepare(writer, writerPrepare, b);
        var writerAccept = AssertAccept(writer, writerPrepare.Ballot, newer);
        RouteAccept(writer, writerAccept, a);
        AssertNotCompleted(writer);
        RouteAccept(writer, writerAccept, b);
        AssertCompleted(writer, newer);
        Assert.Equal(cached, reader.CachedValue);
        Assert.Equal(reusableBallot, reader.PreparedBallot);

        var readInputs = new List<TestValue>();
        var read = new Operation<int, TestValue>
        {
            Input = 0,
            Name = "Read current value",
            Apply = (current, _) =>
            {
                readInputs.Add(current);
                return (OperationStatus.NotApplicable, current);
            },
        };
        reader.ResetRecording();
        reader.StartProposal(read);
        Assert.Null(reader.LastSendPrepare);
        var staleAccept = AssertAccept(reader, reusableBallot, cached, new Ballot(3, 1));
        Assert.Equal(new[] { cached }, readInputs);
        AssertNotCompleted(reader);

        // Both acceptors reject the obsolete reusable ballot after choosing the newer value.
        reader.ResetRecording();
        RouteAccept(reader, staleAccept, a);
        Assert.Equal(writerAccept.Ballot, a.LastAcceptRejected!.Value.ConflictingBallot);
        Assert.Null(reader.LastSendPrepare);
        AssertNotCompleted(reader);
        RouteAccept(reader, staleAccept, b);
        Assert.Equal(writerAccept.Ballot, b.LastAcceptRejected!.Value.ConflictingBallot);
        var retryPrepare = AssertPrepare(reader, new Ballot(3, 1));
        AssertNotCompleted(reader);
        Assert.Equal(newer, a.AcceptedValue);
        Assert.Equal(newer, b.AcceptedValue);

        reader.ResetRecording();
        RoutePrepare(reader, retryPrepare, a);
        Assert.Null(reader.LastSendAccept);
        AssertNotCompleted(reader);
        RoutePrepare(reader, retryPrepare, b);
        var freshAccept = AssertAccept(reader, retryPrepare.Ballot, newer, new Ballot(4, 1));
        Assert.Equal(new[] { cached, newer }, readInputs);
        AssertNotCompleted(reader);
        RouteAccept(reader, freshAccept, a);
        AssertNotCompleted(reader);
        RouteAccept(reader, freshAccept, b);
        AssertCompleted(reader, newer);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ProposerEngineNotApplicableIgnoresOutputAndPreservesRecoveredValue(bool returnDefault)
    {
        var recovered = new TestValue(7, "whole-history");
        var ignored = returnDefault ? default : new TestValue(99, "must-not-be-accepted");
        var ballot = new Ballot(1, 1);
        var a = new RecordingAcceptorEngine<TestValue, string>("A", ballot, ballot, recovered);
        var b = new RecordingAcceptorEngine<TestValue, string>("B", ballot, ballot, recovered);
        var engine = CreateThreeNodeProposer(2);
        var operation = new Operation<TestValue, TestValue>
        {
            Input = ignored,
            Name = "Not applicable with ignored output",
            Apply = static (_, output) => (OperationStatus.NotApplicable, output),
        };

        engine.StartProposal(operation);
        var prepare = AssertPrepare(engine, new Ballot(1, 2));
        RoutePrepare(engine, prepare, a);
        RoutePrepare(engine, prepare, b);
        var identityAccept = AssertAccept(engine, prepare.Ballot, recovered);
        AssertNotCompleted(engine);
        RouteAccept(engine, identityAccept, a);
        AssertNotCompleted(engine);
        RouteAccept(engine, identityAccept, b);

        AssertCompleted(engine, recovered);
        Assert.Equal(recovered, a.AcceptedValue);
        Assert.Equal(recovered, b.AcceptedValue);
    }

    [Fact]
    public void ProposerEngineFailedCasPreservesValueUntilIdentityAcceptQuorum()
    {
        var recovered = new TestValue(7, "unchanged");
        var ballot = new Ballot(1, 1);
        var a = new RecordingAcceptorEngine<TestValue, string>("A", ballot, ballot, recovered);
        var b = new RecordingAcceptorEngine<TestValue, string>("B", ballot, ballot, recovered);
        var engine = CreateThreeNodeProposer(2);

        // Version 6 cannot replace the recovered version 7: AppendOperation returns NotApplicable.
        engine.StartProposal(AppendOperation(6, "must-not-append"));
        var prepare = AssertPrepare(engine, new Ballot(1, 2));
        RoutePrepare(engine, prepare, a);
        RoutePrepare(engine, prepare, b);
        var identityAccept = AssertAccept(engine, prepare.Ballot, recovered);
        AssertNotCompleted(engine);
        RouteAccept(engine, identityAccept, a);
        AssertNotCompleted(engine);
        RouteAccept(engine, identityAccept, b);

        AssertCompleted(engine, recovered);
        Assert.Equal(recovered, a.AcceptedValue);
        Assert.Equal(recovered, b.AcceptedValue);
    }

    private static RecordingProposerEngine<TestValue, string> CreateThreeNodeProposer(
        int proposerId, ProposerModePolicy? modePolicy = null) =>
        new($"p{proposerId}", proposerId, default, ["A", "B", "C"], modePolicy ?? ProposerModePolicy.StaticClassic);

    private static Operation<int, TestValue> ReadOperation() =>
        new Operation<int, TestValue>
        {
            Input = 0,
            Name = "Read current value",
            Apply = static (current, _) => (OperationStatus.NotApplicable, current),
        };

    private static void RoutePrepare(
        RecordingProposerEngine<TestValue, string> proposer,
        PrepareRequest request,
        RecordingAcceptorEngine<TestValue, string> acceptor)
    {
        acceptor.ResetRecording();
        acceptor.Prepare(request);
        if (acceptor.LastPreparePromise is { } promise)
        {
            proposer.HandlePreparePromised(promise);
        }
        else
        {
            proposer.HandlePrepareRejected(acceptor.LastPrepareRejection!.Value);
        }
    }

    private static void RouteAccept(
        RecordingProposerEngine<TestValue, string> proposer,
        AcceptRequest<TestValue> request,
        RecordingAcceptorEngine<TestValue, string> acceptor)
    {
        acceptor.ResetRecording();
        acceptor.Accept(request);
        if (acceptor.LastAcceptAccepted is { } accepted)
        {
            proposer.HandleAcceptAccepted(accepted);
        }
        else
        {
            proposer.HandleAcceptRejected(acceptor.LastAcceptRejected!.Value);
        }
    }

    private static void AssertNotCompleted(RecordingProposerEngine<TestValue, string> engine)
    {
        Assert.Null(engine.LastProposalCompleted);
        Assert.Null(engine.LastValueCommitted);
    }

    private static void AssertCompleted(RecordingProposerEngine<TestValue, string> engine, TestValue expected)
    {
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Equal(expected, engine.LastProposalCompleted!.Value.CommittedValue);
        Assert.NotNull(engine.LastValueCommitted);
        Assert.Equal(expected, engine.LastValueCommitted!.Value.Value);
        Assert.False(engine.HasCurrentOperation);
    }

    private static RecordingProposerEngine<TestValue, string> CreateClassicProposer() =>
        new("p1", proposerId: 1, initialValue: default, acceptors: ["a1", "a2", "a3", "a4", "a5"], ProposerModePolicy.StaticClassic);

    private static RecordingProposerEngine<TestValue, string> CreateFastProposer() =>
        new("p1", proposerId: 1, initialValue: default, acceptors: ["a1", "a2", "a3", "a4", "a5"], ProposerModePolicy.StaticFast);

    private static RecordingProposerEngine<TestValue, string> CreateAdaptiveProposer(AdaptiveProposerPolicy? adaptivePolicy = null) =>
        new(
            "p1",
            proposerId: 1,
            initialValue: default,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            ProposerModePolicy.Adaptive(adaptivePolicy));

    private static Operation<TestValue, TestValue> AppendOperation(int expectedVersion, string suffix) =>
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
        Ballot nextBallotToPrepare = default)
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
