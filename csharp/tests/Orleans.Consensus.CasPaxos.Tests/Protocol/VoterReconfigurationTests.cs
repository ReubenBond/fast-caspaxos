using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Tests.Protocol;

public sealed class VoterReconfigurationTests
{
    [Fact]
    public void BallotOrdersLexicographicallyByConfigurationRoundAndProposer()
    {
        var ordered = new[]
        {
            Ballot.Zero,
            Ballot.Floor(1),
            Ballot.InitialFast(),
            Ballot.InitialClassic(1, 1),
            Ballot.InitialClassic(1, 2),
            new Ballot(1, 2, 0),
            Ballot.Floor(2),
            new Ballot(2, 1, 0),
            Ballot.InitialClassic(2, 1),
            new Ballot(2, 2, 0),
        };

        for (var i = 0; i < ordered.Length - 1; i++)
        {
            Assert.True(ordered[i] < ordered[i + 1], $"Expected {ordered[i]} < {ordered[i + 1]}");
            Assert.True(ordered[i + 1] > ordered[i]);
        }

        Assert.True(Ballot.Zero.IsZero);
        Assert.False(Ballot.Zero.IsFloorBallot);
        Assert.False(Ballot.Zero.IsFastRoundBallot);
        Assert.False(Ballot.Zero.IsClassicRoundBallot);

        var floor2 = Ballot.Floor(2);
        Assert.True(floor2.IsFloorBallot);
        Assert.False(floor2.IsFastRoundBallot);
        Assert.False(floor2.IsClassicRoundBallot);
        Assert.Equal(new Ballot(2, 2, 1), Ballot.InitialClassic(2, 1).NextRound(1));
    }

    [Fact]
    public void AcceptorSetFloorIsMonotonicFencesUnseenAndHeldKeysAndPreservesDuplicateReplay()
    {
        var state = AcceptorState.CreateInitial<string?>(null, configuration: 1);
        var f1 = Ballot.InitialFast();
        var accept1 = AcceptorEngine.Accept(ref state, new AcceptRequest<string?>(f1, "v1", new Ballot(1, 2, 0)));
        Assert.True(accept1.IsAccepted);
        Assert.Equal(new Ballot(1, 2, 0), accept1.PromisedBallot);

        Assert.Equal(Ballot.Floor(2), AcceptorEngine.SetFloor(ref state, Ballot.Floor(2)));
        // Monotonic: lower floor never regresses.
        Assert.Equal(Ballot.Floor(2), AcceptorEngine.SetFloor(ref state, Ballot.Floor(1)));

        // Config 1 prepare and new accept are rejected with floor (2, 0, 0).
        var rejectedPrepare = AcceptorEngine.Prepare(ref state, new PrepareRequest(new Ballot(1, 5, 1)));
        Assert.False(rejectedPrepare.IsPromised);
        Assert.Equal(Ballot.Floor(2), rejectedPrepare.ConflictingBallot);
        Assert.Equal("v1", rejectedPrepare.AcceptedValue);

        var rejectedAccept = AcceptorEngine.Accept(ref state, new AcceptRequest<string?>(new Ballot(1, 2, 0), "v2", Ballot.Zero));
        Assert.False(rejectedAccept.IsAccepted);
        Assert.Equal(Ballot.Floor(2), rejectedAccept.ConflictingBallot);

        // Exact-duplicate replay remains read-only and echoes effPromised >= floor.
        var dup = AcceptorEngine.Accept(ref state, new AcceptRequest<string?>(f1, "v1", new Ballot(1, 2, 0)));
        Assert.True(dup.IsAccepted);
        Assert.Equal(Ballot.Floor(2), dup.PromisedBallot);
        Assert.Equal(f1, state.AcceptedBallot);
        Assert.Equal("v1", state.AcceptedValue);

        // New voter initialized at c_join = 2 rejects config 1 ballots before accepting anything.
        var fresh = AcceptorState.CreateInitial<string?>(null, configuration: 2);
        var preJoin = AcceptorEngine.Prepare(ref fresh, new PrepareRequest(Ballot.InitialClassic(1, 1)));
        Assert.False(preJoin.IsPromised);
        Assert.Equal(Ballot.Floor(2), preJoin.ConflictingBallot);
    }

    [Fact]
    public void ProposerInConfigAboveOneDoesNotUseImplicitInitialFastBallot()
    {
        // With 4 voters, classic quorum (3) equals fast quorum (3), so the first classic round in c = 2
        // commits and certifies piggybacked (2, 2, 0) in one shot, enabling 1-RTT fast writes thereafter.
        ImmutableArray<string> voters = ["a1", "a2", "a3", "a4"];
        var proposer = new ConfigProposerEngine(1, voters, ProposerModePolicy.Adaptive(), configuration: 2);

        proposer.StartProposal(new SetOp("hello"));

        // §4.2: First ballot in c > 1 must be classic (2, 1, 1), never implicit fast (2, 1, 0).
        Assert.NotNull(proposer.LastPrepare);
        Assert.Null(proposer.LastAccept);
        Assert.Equal(Ballot.InitialClassic(2, 1), proposer.LastPrepare!.Value.Ballot);

        // Complete classic round; piggyback (2, 2, 0) enables 1-RTT fast write on next op.
        proposer.HandlePreparePromised(new(1, "a1", Ballot.Zero, null));
        proposer.HandlePreparePromised(new(1, "a2", Ballot.Zero, null));
        proposer.HandlePreparePromised(new(1, "a4", Ballot.Zero, null));
        Assert.NotNull(proposer.LastAccept);
        Assert.Equal(Ballot.InitialClassic(2, 1), proposer.LastAccept!.Value.Ballot);
        Assert.Equal(new Ballot(2, 2, 0), proposer.LastAccept!.Value.NextBallotToPrepare);

        proposer.HandleAcceptAccepted(new(1, "a1", new Ballot(2, 2, 0)));
        proposer.HandleAcceptAccepted(new(1, "a2", new Ballot(2, 2, 0)));
        proposer.HandleAcceptAccepted(new(1, "a4", new Ballot(2, 2, 0)));
        Assert.Equal("hello", proposer.CompletedValue);
        Assert.Equal(new Ballot(2, 2, 0), proposer.PreparedBallot);

        proposer.ResetObserved();
        proposer.StartProposal(new SetOp("fast-in-c2"));
        Assert.Null(proposer.LastPrepare);
        Assert.NotNull(proposer.LastAccept);
        Assert.Equal(new Ballot(2, 2, 0), proposer.LastAccept!.Value.Ballot);
    }

    [Fact]
    public void VerticalPaxosHandOffRequiresClassicQuorumsOfBothConfigsAndScopesFastTally()
    {
        ImmutableArray<string> prev = ["a1", "a2", "a3"];
        ImmutableArray<string> next = ["a1", "a2", "a4"];
        var proposer = new ConfigProposerEngine(1, next, ProposerModePolicy.StaticClassic, configuration: 2, previousAcceptors: prev);

        Assert.True(proposer.IsHandOff);
        Assert.Equal<string>(["a1", "a2", "a3", "a4"], proposer.PrepareAcceptors);
        Assert.Equal<string>(["a1", "a2", "a4"], proposer.Acceptors);

        proposer.StartProposal(new IdentityOp());
        Assert.Equal(Ballot.InitialClassic(2, 1), proposer.LastPrepare!.Value.Ballot);

        // Classic quorum of V(1) only ({a2, a3}) must NOT advance to accept without classic quorum of V(2).
        proposer.HandlePreparePromised(new(1, "a2", Ballot.InitialFast(), "v1"));
        proposer.HandlePreparePromised(new(1, "a3", Ballot.InitialFast(), "v1"));
        Assert.Null(proposer.LastAccept);

        // Adding a4 completes classic quorum of V(2) ({a2, a4}) as well -> advances to Accept with next = Zero.
        proposer.HandlePreparePromised(new(1, "a4", Ballot.Zero, null));
        Assert.NotNull(proposer.LastAccept);
        Assert.Equal(Ballot.InitialClassic(2, 1), proposer.LastAccept!.Value.Ballot);
        Assert.Equal("v1", proposer.LastAccept!.Value.Value);
        Assert.Equal(Ballot.Zero, proposer.LastAccept!.Value.NextBallotToPrepare);

        // Accept from retired a3 (outside V(2)) is ignored; needs classic quorum of V(2).
        proposer.HandleAcceptAccepted(new(1, "a3", Ballot.Floor(2)));
        proposer.HandleAcceptAccepted(new(1, "a4", Ballot.Floor(2)));
        Assert.Null(proposer.CompletedValue);

        proposer.HandleAcceptAccepted(new(1, "a1", Ballot.Floor(2)));
        Assert.Equal("v1", proposer.CompletedValue);
    }

    [Fact]
    public void RecoveryIgnoresRetiredNonMemberFastVotesAndStaleUnchosenRemnants()
    {
        // Active config 2 has 4 voters {a1, a2, a3, a4}. Stale unchosen fast remnant from config 1
        // (< minRelevantCfg = 2) must not trigger conflict retries (zero viable fast winners).
        ImmutableArray<string> v2 = ["a1", "a2", "a3", "a4"];
        var proposer = new ConfigProposerEngine(1, v2, ProposerModePolicy.StaticClassic, configuration: 2);
        proposer.StartProposal(new IdentityOp());

        proposer.HandlePreparePromised(new(1, "a1", Ballot.InitialFast(), "stale-A"));
        proposer.HandlePreparePromised(new(1, "a2", Ballot.InitialFast(), "stale-B"));
        proposer.HandlePreparePromised(new(1, "a3", Ballot.Zero, null));

        // Proceeds directly to Accept without retrying Prepare despite colliding config-1 fast remnants.
        Assert.NotNull(proposer.LastAccept);
        Assert.Equal(Ballot.InitialClassic(2, 1), proposer.LastAccept!.Value.Ballot);
    }

    [Fact]
    public void HigherConfigurationConflictAbortsStaleProposerWithoutIssuingCrossConfigBallot()
    {
        ImmutableArray<string> voters = ["a1", "a2", "a3"];
        var proposer = new ConfigProposerEngine(1, voters, ProposerModePolicy.StaticClassic, configuration: 1);

        proposer.StartProposal(new SetOp("x"));
        proposer.ResetObserved();

        proposer.HandlePrepareRejected(new(1, "a1", Ballot.Zero, null, Ballot.Floor(2)));

        Assert.Equal(Ballot.Floor(2), proposer.ObservedHigherConfigurationBallot);
        Assert.Equal(Ballot.Floor(2), proposer.HigherConfigCallback);
        Assert.False(proposer.HasPendingProposal);
        Assert.Null(proposer.LastPrepare);
    }

    private sealed class SetOp(string value) : IOperation<string?>
    {
        public (OperationStatus Status, string? Result) Apply(string? current) => (OperationStatus.Success, value);
    }

    private sealed class IdentityOp : IOperation<string?>
    {
        public (OperationStatus Status, string? Result) Apply(string? current) => (OperationStatus.NotApplicable, current);
    }

    private sealed class ConfigProposerEngine(
        int proposerId,
        ImmutableArray<string> acceptors,
        ProposerModePolicy modePolicy,
        long configuration,
        ImmutableArray<string> previousAcceptors = default)
        : ProposerEngine<string?, string>(new ProposerRuntime(), null, acceptors, modePolicy, configuration, previousAcceptors)
    {
        public override int ProposerId => proposerId;
        public PrepareRequest? LastPrepare { get; private set; }
        public AcceptRequest<string?>? LastAccept { get; private set; }
        public string? CompletedValue { get; private set; }
        public Ballot? HigherConfigCallback { get; private set; }

        public void ResetObserved()
        {
            LastPrepare = null;
            LastAccept = null;
            CompletedValue = null;
        }

        protected override void OnSendPrepare(PrepareRequest request) => LastPrepare = request;
        protected override void OnSendAccept(AcceptRequest<string?> request) => LastAccept = request;
        protected override void OnProposalCompleted(ProposeResponse<string?> response) => CompletedValue = response.CommittedValue;
        protected override void OnValueCommitted(CommittedValue<string?> committedValue) { }
        protected override void OnHigherConfigurationObserved(Ballot observedBallot) => HigherConfigCallback = observedBallot;
    }
}
