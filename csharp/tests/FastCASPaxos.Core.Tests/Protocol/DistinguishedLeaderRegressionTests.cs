using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class DistinguishedLeaderRegressionTests
{
    [Fact]
    public void ClassicLeaderCommit_PiggybacksAndReusesPreparedNextBallot()
    {
        var engine = CreateLeaderProposer();
        var firstProposal = AppendOperation(1, "A");
        var preparedBallot = Ballot.InitialClassic(1).NextRound(1);

        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        var firstPrepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a3"));
        var accept = AssertAccept(
            engine,
            Ballot.InitialClassic(1),
            new TestValue(1, "A"),
            nextBallotToPrepare: preparedBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a1", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a2", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a3", preparedNextBallot: preparedBallot));
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.NotNull(engine.LastValueCommitted);
        Assert.Equal(preparedBallot, engine.PreparedBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);

        _ = AssertAccept(
            engine,
            preparedBallot,
            new TestValue(2, "AB"),
            nextBallotToPrepare: new Ballot(3, 1));
        Assert.Null(engine.PreparedBallot);
    }

    [Fact]
    public void ClassicLeaderCommit_DoesNotCachePreparedNextBallotWithoutQuorum()
    {
        var engine = CreateLeaderProposer();
        var firstProposal = AppendOperation(1, "A");
        var preparedBallot = Ballot.InitialClassic(1).NextRound(1);

        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        var firstPrepare = AssertPrepare(engine, Ballot.InitialClassic(1));

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a1"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a2"));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(SuccessPrepare(firstPrepare.Ballot.Round, "a3"));
        var accept = AssertAccept(
            engine,
            Ballot.InitialClassic(1),
            new TestValue(1, "A"),
            nextBallotToPrepare: preparedBallot);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a1", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a2", preparedNextBallot: preparedBallot));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandleAcceptAccepted(SuccessAccept(accept.Ballot.Round, "a3"));
        Assert.NotNull(engine.LastProposalCompleted);
        Assert.Null(engine.PreparedBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);

        _ = AssertPrepare(engine, preparedBallot);
    }

    [Fact]
    public void FastAndLeaderConflict_RecoversWithoutExtraClassicRetryWhenNoFastValueCanStillWin()
    {
        var engine = CreateFastLeaderProposer();
        var initialFastBallot = Ballot.InitialFast();
        var secondFastBallot = initialFastBallot.NextRound(proposer: 0);
        var thirdFastBallot = secondFastBallot.NextRound(proposer: 0);

        var firstProposal = AppendOperation(1, "A");
        engine.ResetRecording();
        engine.StartProposal(firstProposal);
        Assert.Null(engine.LastSendPrepare);
        var firstAccept = AssertAccept(engine, initialFastBallot, new TestValue(1, "A"), nextBallotToPrepare: secondFastBallot);
        DriveFastAcceptQuorum(engine, firstAccept.Ballot.Round, preparedNextBallot: secondFastBallot);

        var secondProposal = AppendOperation(2, "B");
        engine.ResetRecording();
        engine.StartProposal(secondProposal);
        var secondAccept = AssertAccept(
            engine,
            secondFastBallot,
            new TestValue(2, "AB"),
            nextBallotToPrepare: thirdFastBallot);
        DriveFastAcceptQuorum(
            engine,
            secondAccept.Ballot.Round,
            preparedNextBallot: thirdFastBallot,
            matchingPreparedResponses: 3);

        var thirdProposal = AppendOperation(3, "C");
        engine.ResetRecording();
        engine.StartProposal(thirdProposal);
        var thirdPrepare = AssertPrepare(engine, thirdFastBallot);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(thirdPrepare.Ballot.Round, "a1", ballot: thirdFastBallot, value: new TestValue(2, "AB")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(thirdPrepare.Ballot.Round, "a2", ballot: thirdFastBallot, value: new TestValue(2, "AB")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(thirdPrepare.Ballot.Round, "a3", ballot: thirdFastBallot, value: new TestValue(2, "AX")));
        Assert.False(engine.HasWork);

        engine.ResetRecording();
        engine.HandlePreparePromised(
            SuccessPrepare(thirdPrepare.Ballot.Round, "a4", ballot: thirdFastBallot, value: new TestValue(2, "AX")));
        _ = AssertAccept(engine, thirdFastBallot, new TestValue(3, "ABC"), nextBallotToPrepare: new Ballot(4, 0));
    }

    private static RecordingProposerEngine<TestValue, string> CreateLeaderProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: true, enableFastCommit: false);

    private static RecordingProposerEngine<TestValue, string> CreateFastLeaderProposer() =>
        new("p1", proposerId: 1, acceptors: ["a1", "a2", "a3", "a4", "a5"], enableDistinguishedLeader: true, enableFastCommit: true);

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

    private static AcceptAccepted<string> SuccessAccept(int round, string acceptor, Ballot? preparedNextBallot = null) =>
        new(round, acceptor, preparedNextBallot ?? Ballot.Zero);

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
        Ballot? nextBallotToPrepare)
    {
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
        return engine.LastSendAccept!.Value;
    }

    private static void DriveFastAcceptQuorum(
        RecordingProposerEngine<TestValue, string> engine,
        int round,
        Ballot preparedNextBallot,
        int matchingPreparedResponses = 4)
    {
        for (var index = 1; index <= 4; index++)
        {
            Ballot? responsePreparedBallot = index <= matchingPreparedResponses ? preparedNextBallot : null;
            engine.ResetRecording();
            engine.HandleAcceptAccepted(SuccessAccept(round, $"a{index}", responsePreparedBallot));
            if (index < 4)
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
