using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

// Executable counterexamples, not an optimization contract. These tests deliberately
// inject the unsafe proposer action that the supported handoff path must never emit.
public sealed class RawLocalSpeculationCounterexampleTests
{
    [Fact]
    public void UnconditionalRawLocalSuccessorCanReplacePreviouslyChosenCreate()
    {
        var acceptors = SplitInitialCreateAndIdentity();
        var replacement = new RegisterState(1, "replacement", 5);
        var successor = new AcceptRequest<RegisterState>(new(2, 0), replacement, new(3, 0));

        // E locally observes identity-accepted absence at b1, then applies create.
        Assert.Equal(Absent, acceptors[4].AcceptedValue);
        foreach (var acceptor in acceptors)
        {
            acceptor.Accept(successor);
            Assert.True(acceptor.LastAcceptResult!.Value.IsAccepted);
        }

        // Both revision-one creates reached a fast quorum in successive ballots.
        // No rejection occurs, so a prepare-on-rejection fallback cannot save this path.
        Assert.All(acceptors, acceptor =>
        {
            Assert.Equal(new Ballot(2, 0), acceptor.AcceptedBallot);
            Assert.Equal(replacement, acceptor.AcceptedValue);
        });
        Assert.NotEqual(Winner, replacement);
        Assert.Equal(Winner.Revision, replacement.Revision);
    }

    [Fact]
    public void MinorityRawLocalSuccessorCanReplacePreviouslyChosenCreateDuringRecovery()
    {
        var acceptors = SplitInitialCreateAndIdentity();
        var replacement = new RegisterState(1, "replacement", 5);
        var successor = new AcceptRequest<RegisterState>(new(2, 0), replacement, new(3, 0));

        // Model the strongest effect an exact-predecessor guard could have here:
        // only E accepts the successor, while A-D retain the already chosen winner.
        // No conditional-accept API is needed to demonstrate this reachable storage cut.
        acceptors[4].Accept(successor);
        Assert.True(acceptors[4].LastAcceptResult!.Value.IsAccepted);
        Assert.All(acceptors[..4], acceptor => Assert.Equal(Winner, acceptor.AcceptedValue));

        var recovery = new RecordingProposerEngine<RegisterState, string>(
            "recovery", 9, Absent, ["a", "b", "c", "d", "e"], ProposerModePolicy.StaticClassic);
        recovery.StartProposal(new Operation<int, RegisterState>
        {
            Input = 0,
            Apply = static (current, _) => (OperationStatus.NotApplicable, current)
        });
        var initial = recovery.LastSendPrepare!.Value;
        Assert.Equal(new Ballot(1, 9), initial.Ballot);
        foreach (var acceptor in new[] { acceptors[0], acceptors[1], acceptors[4] })
        {
            acceptor.Prepare(initial);
            Assert.False(acceptor.LastPrepareResult!.Value.IsPromised);
            recovery.HandlePrepareRejected(acceptor.LastPrepareRejection!.Value);
        }

        var prepared = recovery.LastSendPrepare!.Value;
        Assert.Equal(new Ballot(4, 9), prepared.Ballot);
        foreach (var acceptor in new[] { acceptors[0], acceptors[1], acceptors[4] })
        {
            acceptor.Prepare(prepared);
            Assert.True(acceptor.LastPrepareResult!.Value.IsPromised);
            recovery.HandlePreparePromised(acceptor.LastPreparePromise!.Value);
        }
        var recovered = recovery.LastSendAccept!.Value;
        Assert.Equal(prepared.Ballot, recovered.Ballot);
        Assert.Equal(replacement, recovered.Value);
        Assert.Null(recovery.LastProposalCompleted);
        foreach (var acceptor in new[] { acceptors[0], acceptors[1], acceptors[4] })
        {
            acceptor.Accept(recovered);
            Assert.True(acceptor.LastAcceptResult!.Value.IsAccepted);
            recovery.HandleAcceptAccepted(acceptor.LastAcceptAccepted!.Value);
        }

        // Classic recovery sees b2 only on E, treats it as highest accepted, then
        // actually chooses that value at b4. A fallback did not preserve the b1 winner.
        Assert.Equal(replacement, recovery.LastProposalCompleted!.Value.CommittedValue);
        Assert.All(new[] { acceptors[0], acceptors[1], acceptors[4] },
            acceptor => Assert.Equal(replacement, acceptor.AcceptedValue));
        Assert.Equal(Winner.Revision, replacement.Revision);
        Assert.NotEqual(Winner, replacement);
    }

    private static readonly RegisterState Absent = new(0, null, 0);
    private static readonly RegisterState Winner = new(1, "winner", 1);
    private static readonly string[] AcceptorIds = ["a", "b", "c", "d", "e"];

    private static RecordingAcceptorEngine<RegisterState, string>[] SplitInitialCreateAndIdentity()
    {
        var acceptors = AcceptorIds
            .Select(id => new RecordingAcceptorEngine<RegisterState, string>(id, Absent)).ToArray();
        for (var index = 0; index < acceptors.Length; index++)
        {
            acceptors[index].Accept(new(new(1, 0), index == 4 ? Absent : Winner, new(2, 0)));
            Assert.True(acceptors[index].LastAcceptResult!.Value.IsAccepted);
            Assert.Equal(new Ballot(2, 0), acceptors[index].PromisedBallot);
        }
        Assert.Equal(4, acceptors.Count(acceptor => acceptor.AcceptedValue == Winner));
        Assert.Equal(Absent, acceptors[4].AcceptedValue);
        return acceptors;
    }

    private readonly record struct RegisterState(int Revision, string? Payload, int Writer);
}
