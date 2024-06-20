using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class CoreProtocolEdgeCaseTests
{
    [Fact]
    public void AcceptorEngine_Prepare_RejectsBallotBelowAcceptedBallot_ReturnsAcceptedValue()
    {
        var promisedBallot = new Ballot(2, 3);
        var acceptedBallot = new Ballot(1, 2);
        var acceptedValue = new TestValue(1, "B");
        var engine = new RecordingAcceptorEngine<TestValue, string>(
            "a1",
            new AcceptorState<TestValue>
            {
                PromisedBallot = promisedBallot,
                AcceptedBallot = acceptedBallot,
                AcceptedValue = acceptedValue,
            });

        engine.ResetRecording();
        engine.Prepare(new PrepareRequest(acceptedBallot));

        Assert.NotNull(engine.LastPrepareRejection);
        Assert.Equal(promisedBallot, engine.LastPrepareRejection!.Value.ConflictBallot);
        Assert.Equal(acceptedBallot, engine.LastPrepareRejection!.Value.AcceptedBallot);
        Assert.Equal(acceptedValue, engine.LastPrepareRejection!.Value.AcceptedValue);
    }

    [Fact]
    public void AcceptorEngine_Accept_RejectsBelowPromisedBallot_ReportsPromisedConflictBallot()
    {
        var promisedBallot = new Ballot(3, 2);
        var acceptedBallot = new Ballot(2, 1);
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
        engine.Accept(new AcceptRequest<TestValue>(
            new Ballot(2, 2), new TestValue(2, "AB"), null));

        Assert.NotNull(engine.LastAcceptRejected);
        Assert.Equal(promisedBallot, engine.LastAcceptRejected!.Value.ConflictingBallot);
        Assert.Equal(acceptedBallot, engine.AcceptedBallot);
        Assert.Equal(acceptedValue, engine.AcceptedValue);
    }

    [Fact]
    public void ProposerEngine_PrepareRecovery_AdoptsPluralityFastValueAndDirectlyCommitsNextValue()
    {
        var engine = CreateFastProposer();
        var firstProposal = AppendOperation(1, "A");
        var initialFastBallot = Ballot.InitialFast();
        var secondFastBallot = initialFastBallot.NextRound(proposer: 0);
        var thirdFastBallot = secondFastBallot.NextRound(proposer: 0);
        var fourthFastBallot = thirdFastBallot.NextRound(proposer: 0);

        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        Assert.Null(engine.LastSendPrepare);
        var firstAccept = AssertAccept(engine, initialFastBallot, new TestValue(1, "A"), nextBallotToPrepare: secondFastBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a1", preparedNextBallot: secondFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a2", preparedNextBallot: secondFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a3", preparedNextBallot: secondFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a4", preparedNextBallot: secondFastBallot));
        Assert.NotNull(engine.LastProposalCompleted);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);
        var secondAccept = AssertAccept(engine, secondFastBallot, new TestValue(2, "AB"), nextBallotToPrepare: thirdFastBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(secondAccept.Ballot.Round, "a1", preparedNextBallot: thirdFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(secondAccept.Ballot.Round, "a2", preparedNextBallot: thirdFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(secondAccept.Ballot.Round, "a3", preparedNextBallot: thirdFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(secondAccept.Ballot.Round, "a4"));
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Null(engine.PreparedBallot);

        var thirdProposal = AppendOperation(3, "C");
        engine.ResetRecording();
        engine.StartProposal(thirdProposal);
        var thirdPrepare = AssertPrepare(engine, thirdFastBallot);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a1", ballot: secondFastBallot, value: new TestValue(2, "AX")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a2", ballot: secondFastBallot, value: new TestValue(2, "AX")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a3", ballot: secondFastBallot, value: new TestValue(2, "AX")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(thirdPrepare.Ballot.Round, "a4", ballot: secondFastBallot, value: new TestValue(2, "AB")));
        var recoveryAccept = AssertAccept(engine, thirdFastBallot, new TestValue(3, "AXC"), nextBallotToPrepare: fourthFastBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a1", preparedNextBallot: fourthFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a2", preparedNextBallot: fourthFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a3", preparedNextBallot: fourthFastBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(recoveryAccept.Ballot.Round, "a4", preparedNextBallot: fourthFastBallot));
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.NotNull(engine.LastValueCommitted);
        Assert.Equal(new TestValue(3, "AXC"), engine.LastValueCommitted!.Value.Value);
        Assert.Equal(new TestValue(3, "AXC"), engine.LastProposalCompleted!.Value.CommittedValue);
    }

    [Fact]
    public void ProposerEngine_InitialFastRead_FallsBackToPrepare()
    {
        var engine = CreateFastProposer();
        var proposal = ReadOperation();

        engine.ResetRecording();
        engine.StartProposal(proposal);

        _ = AssertPrepare(engine, Ballot.InitialFast());
        Assert.Null(engine.LastSendAccept);
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_IgnoresStaleResponsesFromEarlierRequest()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(1, "A");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var initialPrepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePrepareRejected(RejectedPrepare(initialPrepare.Ballot.Round, "a1", new Ballot(1, 2)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(RejectedPrepare(initialPrepare.Ballot.Round, "a2", new Ballot(2, 4)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(RejectedPrepare(initialPrepare.Ballot.Round, "a3", new Ballot(2, 3)));
        var retryPrepare = AssertPrepare(engine, new Ballot(3, 1));

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(initialPrepare.Ballot.Round, "a4", ballot: new Ballot(9, 0), value: new TestValue(9, "STALE")));
        Assert.False(engine.HasWork);
        Assert.Equal(new Ballot(3, 1), engine.Ballot);
        Assert.Equal(0, engine.CachedValue.Version);
        Assert.Null(engine.CachedValue.Value);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a3"));
        _ = AssertAccept(engine, new Ballot(3, 1), new TestValue(1, "A"));
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_UsesHighestConflictBallotButCachesHighestAcceptedValue()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(6, "F");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var highestConflictBallot = new Ballot(4, 3);
        var highestAccepted = new TestValue(5, "ABCDE");

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(
                prepare.Ballot.Round,
                "a1",
                ballot: new Ballot(5, 1),
                value: highestAccepted));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(
                prepare.Ballot.Round,
                "a2",
                ballot: new Ballot(4, 2),
                value: new TestValue(4, "ABCD")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a3",
                conflictBallot: new Ballot(3, 4),
                acceptedBallot: new Ballot(2, 2),
                value: new TestValue(2, "AB")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a4",
                conflictBallot: highestConflictBallot,
                acceptedBallot: new Ballot(3, 1),
                value: new TestValue(3, "ABC")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a5",
                conflictBallot: new Ballot(2, 3),
                acceptedBallot: new Ballot(1, 2),
                value: new TestValue(1, "A")));
        var retryPrepare = AssertPrepare(engine, highestConflictBallot.NextRound(proposer: 1));

        Assert.Equal(highestConflictBallot.NextRound(proposer: 1), retryPrepare.Ballot);
        Assert.Equal(highestAccepted, engine.CachedValue);
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_RefreshesCachedValueBeforePrepareQuorum()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(3, "C");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var promisedValue = new TestValue(1, "A");
        var rejectedValue = new TestValue(2, "AX");

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a1", ballot: Ballot.InitialClassic(1), value: promisedValue));
        Assert.False(engine.HasWork);
        Assert.Equal(promisedValue, engine.CachedValue);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a2",
                conflictBallot: new Ballot(3, 2),
                acceptedBallot: new Ballot(2, 1),
                value: rejectedValue));
        Assert.False(engine.HasWork);
        Assert.Equal(rejectedValue, engine.CachedValue);
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_RecoversFromHighestAcceptedValueAcrossPrepareResponses()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(2, "B");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var promisedValue = new TestValue(1, "A");
        var rejectedValue = new TestValue(2, "AX");

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a1", ballot: Ballot.InitialClassic(1), value: promisedValue));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a2", ballot: Ballot.InitialClassic(1), value: promisedValue));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a3",
                conflictBallot: new Ballot(3, 2),
                acceptedBallot: new Ballot(2, 1),
                value: rejectedValue));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a4", ballot: Ballot.InitialClassic(1), value: promisedValue));
        Assert.NotNull(engine.LastProposalCompleted);

        Assert.Equal(rejectedValue, engine.LastProposalCompleted!.Value.CommittedValue);
        Assert.Equal(rejectedValue, engine.CachedValue);
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_RecoversWhenNoFastValueCanStillReachQuorum()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(3, "C");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var fastBallot = Ballot.InitialFast();
        var ax = new TestValue(2, "AX");
        var ab = new TestValue(2, "AB");

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1", ballot: fastBallot, value: ax));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2", ballot: fastBallot, value: ax));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePrepareRejected(
            RejectedPrepare(
                prepare.Ballot.Round,
                "a3",
                conflictBallot: new Ballot(3, 2),
                acceptedBallot: fastBallot,
                value: ab));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a4", ballot: fastBallot, value: ab));
        _ = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(3, "AXC"));
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_RecoversFromThreeWayFastSplitWhenNoFastValueCanStillReachQuorum()
    {
        var engine = CreateClassicProposer();
        var proposal = AppendOperation(2, "Z");

        engine.ResetRecording();
        engine.StartProposal(proposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var fastBallot = Ballot.InitialFast();

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a1", ballot: fastBallot, value: new TestValue(1, "A")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a2", ballot: fastBallot, value: new TestValue(1, "B")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(prepare.Ballot.Round, "a3", ballot: fastBallot, value: new TestValue(1, "C")));
        _ = AssertAccept(engine, Ballot.InitialClassic(1), new TestValue(2, "AZ"));
    }

    [Fact]
    public void ProposerEngine_HandleAcceptResponse_IgnoresStaleResponsesFromEarlierRequest()
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
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a1", new Ballot(1, 2)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a2", new Ballot(2, 4)));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptRejected(RejectedAccept(accept.Ballot.Round, "a3", new Ballot(2, 3)));
        var retryPrepare = AssertPrepare(engine, new Ballot(3, 1));

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a4"));
        Assert.False(engine.HasWork);
        Assert.Equal(new Ballot(3, 1), engine.Ballot);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(retryPrepare.Ballot.Round, "a3"));
        _ = AssertAccept(engine, new Ballot(3, 1), new TestValue(1, "A"));
    }

    [Fact]
    public void ProposerEngine_HandlePrepareResponse_IgnoresLateResponsesAfterProposalCompletion()
    {
        var engine = CreateLeaderProposer();
        var recovered = new TestValue(24, "BASE");
        var recoveredBallot = Ballot.InitialFast();
        var firstProposal = BlindAppendOperation("U");

        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        var prepare = AssertPrepare(engine, Ballot.InitialClassic(1));
        var preparedBallot = prepare.Ballot.NextRound(engine.ProposerId);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a1", ballot: recoveredBallot, value: recovered));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a2", ballot: recoveredBallot, value: recovered));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a3", ballot: recoveredBallot, value: recovered));
        var firstAccept = AssertAccept(
            engine,
            Ballot.InitialClassic(1),
            new TestValue(25, "BASEU"),
            nextBallotToPrepare: preparedBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a1", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a2", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(firstAccept.Ballot.Round, "a3", preparedNextBallot: preparedBallot));
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Equal(new TestValue(25, "BASEU"), engine.CachedValue);
        Assert.Equal(preparedBallot, engine.PreparedBallot);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(prepare.Ballot.Round, "a4", ballot: recoveredBallot, value: recovered));
        Assert.False(engine.HasWork);
        Assert.Equal(new TestValue(25, "BASEU"), engine.CachedValue);
        Assert.Equal(preparedBallot, engine.PreparedBallot);

        engine.ResetRecording();
        engine.StartProposal(BlindAppendOperation("Y"));
        Assert.Null(engine.LastSendPrepare);
        _ = AssertAccept(
            engine,
            preparedBallot,
            new TestValue(26, "BASEUY"),
            nextBallotToPrepare: preparedBallot.NextRound(engine.ProposerId));
    }

    [Fact]
    public void ProposerEngine_PrepareRecovery_ReadReturnsRecoveredHistoryWithoutStandaloneRepairCommit()
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
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(initialAccept.Ballot.Round, "a3"));

        var repairProposal = ReadOperation();
        engine.ResetRecording();
        engine.StartProposal(repairProposal);
        var repairPrepare = AssertPrepare(engine, new Ballot(2, 1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(repairPrepare.Ballot.Round, "a1", ballot: new Ballot(2, 1), value: new TestValue(1, "B")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(repairPrepare.Ballot.Round, "a2", ballot: new Ballot(2, 1), value: new TestValue(1, "B")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(repairPrepare.Ballot.Round, "a3", ballot: new Ballot(2, 1), value: new TestValue(1, "B")));
        Assert.NotNull(engine.LastProposalCompleted);

        Assert.Equal(new TestValue(1, "B"), engine.LastProposalCompleted!.Value.CommittedValue);
        Assert.Equal(new TestValue(1, "B"), engine.CachedValue);
    }

    private static RecordingProposerEngine<TestValue, string> CreateClassicProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: false, enableFastCommit: false);

    private static RecordingProposerEngine<TestValue, string> CreateLeaderProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: true, enableFastCommit: false);

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

    private static IOperation<TestValue> BlindAppendOperation(string suffix) =>
        new Operation<string, TestValue>
        {
            Input = suffix,
            Name = $"Append '{suffix}'",
            Apply = static (current, input) =>
            {
                var currentValue = current.Value ?? string.Empty;
                return (OperationStatus.Success, new TestValue(current.Version + 1, currentValue + input));
            },
        };

    private static Operation<TestValue, TestValue> ReadOperation() =>
        new Operation<TestValue, TestValue>
        {
            Input = default,
            Name = "Read current value",
            Apply = static (current, _) => (OperationStatus.NotApplicable, current),
        };

    private static PreparePromise<TestValue, string> SuccessPrepare(int round, string acceptor, Ballot ballot = default, TestValue value = default) =>
        new(round, acceptor, ballot, value);

    private static PrepareRejection<TestValue, string> RejectedPrepare(int round, string acceptor, Ballot conflictBallot, Ballot acceptedBallot = default, TestValue value = default) =>
        new(round, acceptor, acceptedBallot, value, conflictBallot);

    private static AcceptAccepted<string> SuccessAccept(int round, string acceptor, Ballot? preparedNextBallot = null) =>
        new(round, acceptor, preparedNextBallot ?? Ballot.Zero);

    private static AcceptRejected<string> RejectedAccept(int round, string acceptor, Ballot ballot) =>
        new(round, acceptor, ballot);

    private static PrepareRequest AssertPrepare(RecordingProposerEngine<TestValue, string> engine, Ballot ballot)
    {
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
        var accept = engine.LastSendAccept!.Value;
        Assert.Equal(ballot, accept.Ballot);
        Assert.Equal(value, accept.Value);
        Assert.Equal(nextBallotToPrepare, accept.NextBallotToPrepare);
        return accept;
    }

    private readonly record struct TestValue(int Version, string Value)
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
