using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public enum ProposerModeStrategy
{
    StaticClassic,
    StaticFast,
    StaticLeader,
    StaticFastLeader,
    Adaptive,
}

public sealed class AdaptiveProposerPolicy
{
    public static AdaptiveProposerPolicy Default { get; } = new();

    public bool StartInFastMode { get; init; } = true;

    public bool EnableForwarding { get; init; } = true;

    public int ConflictDebtIncrement { get; init; } = 4;

    public int ConflictDebtDecay { get; init; } = 1;

    public int FastReopenThreshold { get; init; }

    public int LeaderMissThreshold { get; init; } = 1;

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ConflictDebtIncrement);
        ArgumentOutOfRangeException.ThrowIfNegative(ConflictDebtDecay);
        ArgumentOutOfRangeException.ThrowIfNegative(FastReopenThreshold);
        ArgumentOutOfRangeException.ThrowIfLessThan(LeaderMissThreshold, 1);
    }
}

public sealed class ProposerModePolicy
{
    private ProposerModePolicy(ProposerModeStrategy strategy, AdaptiveProposerPolicy? adaptive = null)
    {
        Strategy = strategy;
        AdaptivePolicy = adaptive ?? AdaptiveProposerPolicy.Default;
        ResetRuntimeState();
    }

    public ProposerModeStrategy Strategy { get; }

    public AdaptiveProposerPolicy AdaptivePolicy { get; }

    public int ConflictDebt { get; private set; }

    public int LeaderMisses { get; private set; }

    public int? LeaderHintProposerId { get; private set; }

    public bool IsAdaptive => Strategy == ProposerModeStrategy.Adaptive;

    public bool StaticFastCommitEnabled =>
        Strategy is ProposerModeStrategy.StaticFast or ProposerModeStrategy.StaticFastLeader;

    public bool StaticLeaderEnabled =>
        Strategy is ProposerModeStrategy.StaticLeader or ProposerModeStrategy.StaticFastLeader;

    public static ProposerModePolicy StaticClassic { get; } = new(ProposerModeStrategy.StaticClassic);

    public static ProposerModePolicy StaticFast { get; } = new(ProposerModeStrategy.StaticFast);

    public static ProposerModePolicy StaticLeader { get; } = new(ProposerModeStrategy.StaticLeader);

    public static ProposerModePolicy StaticFastLeader { get; } = new(ProposerModeStrategy.StaticFastLeader);

    public static ProposerModePolicy Adaptive(AdaptiveProposerPolicy? adaptive = null) =>
        new(ProposerModeStrategy.Adaptive, adaptive);

    public static ProposerModePolicy Parse(string mode) =>
        (mode ?? throw new ArgumentNullException(nameof(mode))).Trim().ToLowerInvariant() switch
        {
            "classic" => StaticClassic,
            "fast" => StaticFast,
            "leader" => StaticLeader,
            "fast+leader" => StaticFastLeader,
            "adaptive" => Adaptive(),
            _ => throw new ArgumentException(
                $"Unknown proposer mode '{mode}'. Expected one of: classic, fast, leader, fast+leader, adaptive.",
                nameof(mode)),
        };

    public string Name =>
        Strategy switch
        {
            ProposerModeStrategy.StaticClassic => "classic",
            ProposerModeStrategy.StaticFast => "fast",
            ProposerModeStrategy.StaticLeader => "leader",
            ProposerModeStrategy.StaticFastLeader => "fast+leader",
            ProposerModeStrategy.Adaptive => "adaptive",
            _ => throw new InvalidOperationException($"Unknown proposer mode strategy '{Strategy}'."),
        };

    public void Validate()
    {
        if (IsAdaptive)
        {
            AdaptivePolicy.Validate();
        }
    }

    internal ProposerModePolicy CreateRuntimeCopy() => new(Strategy, AdaptivePolicy);

    internal bool ShouldForwardToLeader(int proposerId) =>
        IsAdaptive
        && AdaptivePolicy.EnableForwarding
        && LeaderHintProposerId is { } leaderHint
        && leaderHint != proposerId
        && LeaderMisses < AdaptivePolicy.LeaderMissThreshold
        && !PreferFastPath(proposerId);

    internal bool ShouldAttemptLeaderTakeover(int proposerId) =>
        IsAdaptive
        && LeaderHintProposerId is { } leaderHint
        && leaderHint != proposerId
        && LeaderMisses >= AdaptivePolicy.LeaderMissThreshold;

    internal bool ShouldSkipInitialPrepare(int proposerId) =>
        IsAdaptive ? PreferFastPath(proposerId) : StaticFastCommitEnabled;

    internal bool CanReusePreparedBallot(Ballot ballot, int proposerId) =>
        ballot.IsFastRoundBallot
            ? ShouldSkipInitialPrepare(proposerId)
            : ballot.Proposer == proposerId && (StaticLeaderEnabled || IsAdaptive);

    internal Ballot CreateProposalBallot(int proposerId, Ballot currentBallot)
    {
        if (currentBallot.IsZero)
        {
            return PreferFastPath(proposerId) ? Ballot.InitialFast() : Ballot.InitialClassic(proposerId);
        }

        return PreferFastPath(proposerId) ? currentBallot.NextRound(proposer: 0) : currentBallot.NextRound(proposerId);
    }

    internal Ballot ChooseNextPrepareBallot(Ballot currentBallot, int proposerId)
    {
        if (IsAdaptive)
        {
            return PreferFastPath(proposerId)
                ? currentBallot.NextRound(proposer: 0)
                : currentBallot.NextRound(proposerId);
        }

        if (StaticFastCommitEnabled)
        {
            return currentBallot.NextRound(proposer: 0);
        }

        return StaticLeaderEnabled ? currentBallot.NextRound(proposerId) : Ballot.Zero;
    }

    internal void RecordLeaderForwardFailure()
    {
        if (!IsAdaptive)
        {
            return;
        }

        LeaderMisses = Math.Min(int.MaxValue, LeaderMisses + 1);
    }

    internal void RecordLeaderForwardSuccess()
    {
        if (!IsAdaptive)
        {
            return;
        }

        LeaderMisses = 0;
    }

    internal void RecordConflictObserved(Ballot currentBallot, Ballot maxConflictBallot, int proposerId)
    {
        if (!IsAdaptive)
        {
            return;
        }

        if (maxConflictBallot.IsFastRoundBallot || currentBallot.IsFastRoundBallot)
        {
            ConflictDebt = Math.Min(int.MaxValue, ConflictDebt + AdaptivePolicy.ConflictDebtIncrement);
        }

        LeaderHintProposerId = proposerId;
        LeaderMisses = 0;
    }

    internal void RecordAcceptQuorumSucceeded(Ballot ballot, int proposerId)
    {
        if (!IsAdaptive)
        {
            return;
        }

        LeaderMisses = 0;
        if (ballot.IsFastRoundBallot)
        {
            if (ConflictDebt > AdaptivePolicy.FastReopenThreshold)
            {
                LeaderHintProposerId = proposerId;
            }

            return;
        }

        LeaderHintProposerId = proposerId;
        ConflictDebt = Math.Max(0, ConflictDebt - AdaptivePolicy.ConflictDebtDecay);
    }

    internal void LearnCommittedValue(Ballot acceptedBallot, Ballot preparedBallot)
    {
        if (!IsAdaptive)
        {
            return;
        }

        var leaderBallot = preparedBallot.IsZero ? acceptedBallot : preparedBallot;
        if (leaderBallot.IsZero)
        {
            return;
        }

        LeaderMisses = 0;
        if (leaderBallot.IsFastRoundBallot)
        {
            // A fast ballot in the broadcast means the leader verified the fast path is stable.
            // All proposers that learn it clear their hint and debt so they can rejoin fast mode.
            LeaderHintProposerId = null;
            ConflictDebt = 0;
        }
        else
        {
            LeaderHintProposerId = leaderBallot.Proposer;
        }
    }

    public override string ToString() => Name;

    private bool PreferFastPath(int proposerId)
    {
        if (!IsAdaptive)
        {
            return StaticFastCommitEnabled;
        }

        if (LeaderHintProposerId is { } leaderHint && leaderHint != proposerId)
        {
            return false;
        }

        return ConflictDebt <= AdaptivePolicy.FastReopenThreshold;
    }

    private void ResetRuntimeState()
    {
        ConflictDebt = 0;
        LeaderMisses = 0;
        LeaderHintProposerId = null;

        if (IsAdaptive && !AdaptivePolicy.StartInFastMode)
        {
            ConflictDebt = AdaptivePolicy.FastReopenThreshold == int.MaxValue
                ? int.MaxValue
                : AdaptivePolicy.FastReopenThreshold + 1;
        }
    }
}
