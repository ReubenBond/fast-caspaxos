using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class FastHandoffTests
{
    [Theory]
    [InlineData(8, 0, 9, 0)]
    [InlineData(12, 2, 9, 0)]
    [InlineData(0, 0, 15, 3)]
    public void RawLocalBallotsAdvancePrepareWithoutCertifyingValue(
        int acceptedRound, int acceptedProposer, int promisedRound, int promisedProposer)
    {
        var proposer = Create();
        var accepted = new Ballot(acceptedRound, acceptedProposer);
        var promised = new Ballot(promisedRound, promisedProposer);

        proposer.StartProposal(Increment(), accepted, promised);

        Assert.NotNull(proposer.LastSendPrepare);
        Assert.True(proposer.LastSendPrepare.Value.Ballot > Ballot.Max(accepted, promised));
        Assert.Null(proposer.LastSendAccept);
        Assert.Null(proposer.LastValueCommitted);
        Assert.Null(proposer.LastProposalCompleted);
        Assert.Equal(0, proposer.CachedValue);
    }

    [Theory]
    [InlineData(0, 0, 1, 0)]
    [InlineData(8, 0, 8, 0)]
    [InlineData(8, 0, 9, 0)]
    public void CertifiedFastHandoffKeepsPreparedBallotUntilAcceptQuorum(
        int acceptedRound, int acceptedProposer, int promisedRound, int promisedProposer)
    {
        var proposer = Create();
        var prepared = new Ballot(9, 0);
        proposer.LearnCommittedValue(new(8, 0), 42, prepared);

        proposer.StartProposal(Increment(),
            new(acceptedRound, acceptedProposer), new(promisedRound, promisedProposer));

        Assert.Null(proposer.LastSendPrepare);
        Assert.NotNull(proposer.LastSendAccept);
        Assert.Equal(prepared, proposer.LastSendAccept.Value.Ballot);
        Assert.Equal(43, proposer.LastSendAccept.Value.Value);
        Assert.Null(proposer.LastValueCommitted);
        Assert.Null(proposer.LastProposalCompleted);
        proposer.HandleAcceptAccepted(new(9, "a", new(10, 0)));
        proposer.HandleAcceptAccepted(new(9, "b", new(10, 0)));
        Assert.Null(proposer.LastProposalCompleted); // A three-member fast quorum requires all three.
        proposer.HandleAcceptAccepted(new(9, "c", new(10, 0)));
        Assert.Equal(43, proposer.LastProposalCompleted!.Value.CommittedValue);
        Assert.Equal(new Ballot(10, 0), proposer.PreparedBallot);
    }

    [Theory]
    [InlineData(9, 0, 9, 0)]
    [InlineData(8, 0, 9, 1)]
    [InlineData(8, 0, 10, 0)]
    public void LocalFloorAtOrBeyondCertificateForcesPrepare(
        int acceptedRound, int acceptedProposer, int promisedRound, int promisedProposer)
    {
        var proposer = Create();
        proposer.LearnCommittedValue(new(8, 0), 42, new(9, 0));
        var accepted = new Ballot(acceptedRound, acceptedProposer);
        var promised = new Ballot(promisedRound, promisedProposer);

        proposer.StartProposal(Increment(), accepted, promised);

        Assert.Null(proposer.LastSendAccept);
        Assert.NotNull(proposer.LastSendPrepare);
        Assert.True(proposer.LastSendPrepare.Value.Ballot > Ballot.Max(accepted, promised));
        Assert.Null(proposer.LastProposalCompleted);
    }

    [Theory]
    [InlineData("leader", 1, true)]
    [InlineData("leader", 2, false)]
    [InlineData("classic", 1, false)]
    [InlineData("classic", 2, false)]
    [InlineData("fast", 1, false)]
    [InlineData("fast", 2, false)]
    [InlineData("adaptive", 1, true)]
    [InlineData("adaptive", 2, false)]
    public void LearnedOwnedPreparedBallotCannotBeUsedByAnotherProposer(
        string mode, int proposerId, bool canReuse)
    {
        var proposer = Create(mode, proposerId);
        var prepared = new Ballot(9, 1);
        proposer.LearnCommittedValue(new(8, 1), 42, prepared);

        proposer.StartProposal(Increment(), new(8, 1), prepared);

        if (canReuse)
        {
            Assert.Null(proposer.LastSendPrepare);
            Assert.Equal(prepared, proposer.LastSendAccept!.Value.Ballot);
        }
        else
        {
            Assert.Null(proposer.LastSendAccept);
            Assert.NotNull(proposer.LastSendPrepare);
            Assert.Equal(mode == "fast" ? 0 : proposerId, proposer.LastSendPrepare.Value.Ballot.Proposer);
            Assert.True(proposer.LastSendPrepare.Value.Ballot > new Ballot(8, 1));
        }
    }

    [Theory]
    [InlineData("classic", false)]
    [InlineData("leader", false)]
    [InlineData("fast", true)]
    [InlineData("adaptive", true)]
    public void LearnedSharedPreparedBallotRespectsSelectedMode(string mode, bool canReuse)
    {
        var proposer = Create(mode);
        var prepared = new Ballot(9, 0);
        proposer.LearnCommittedValue(new(8, 0), 42, prepared);

        proposer.StartProposal(Increment(), new(8, 0), prepared);

        if (canReuse)
        {
            Assert.Null(proposer.LastSendPrepare);
            Assert.Equal(prepared, proposer.LastSendAccept!.Value.Ballot);
            Assert.Equal(43, proposer.LastSendAccept.Value.Value);
        }
        else
        {
            Assert.Null(proposer.LastSendAccept);
            Assert.Equal(3, proposer.LastSendPrepare!.Value.Ballot.Proposer);
            Assert.True(proposer.LastSendPrepare.Value.Ballot > prepared);
        }
    }

    private static RecordingProposerEngine<int, string> Create(string mode = "fast", int proposerId = 3) =>
        new("proposer", proposerId, 0, ["a", "b", "c"], ProposerModePolicy.Parse(mode));

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void ClassicProposalConsumesUnusablePromiseAsFloorWithoutRepublishingIt(int preparedProposer)
    {
        var proposer = Create("classic");
        var accepted = new Ballot(8, 0);
        var prepared = new Ballot(9, preparedProposer);
        proposer.LearnCommittedValue(accepted, 42, prepared);

        proposer.StartProposal(Increment());

        var ballot = proposer.LastSendPrepare!.Value.Ballot;
        Assert.True(ballot > prepared);
        Assert.Null(proposer.PreparedBallot);
        proposer.HandlePreparePromised(new(ballot.Round, "a", accepted, 42));
        proposer.HandlePreparePromised(new(ballot.Round, "b", accepted, 42));
        Assert.Equal(Ballot.Zero, proposer.LastSendAccept!.Value.NextBallotToPrepare);
        proposer.HandleAcceptAccepted(new(ballot.Round, "a", Ballot.Zero));
        proposer.HandleAcceptAccepted(new(ballot.Round, "b", Ballot.Zero));
        Assert.Equal(43, proposer.LastProposalCompleted!.Value.CommittedValue);
        Assert.Null(proposer.PreparedBallot);
    }

    private static Operation<int, int> Increment() => new()
    {
        Input = 1,
        Apply = static (current, increment) => (OperationStatus.Success, current + increment)
    };
}
