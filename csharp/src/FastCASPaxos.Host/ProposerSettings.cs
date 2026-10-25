using FastCASPaxos.Protocol;

namespace FastCASPaxos.Host;

public sealed record ProposerSettings
{
    public string Mode { get; init; } = "adaptive";
    public bool StartInFastMode { get; init; } = true;
    public bool EnableForwarding { get; init; } = true;
    public int ConflictDebtIncrement { get; init; } = 4;
    public int ConflictDebtDecay { get; init; } = 1;
    public int FastReopenThreshold { get; init; }
    public int LeaderMissThreshold { get; init; } = 1;

    public ProposerModePolicy CreatePolicy()
    {
        var policy = new AdaptiveProposerPolicy
        {
            StartInFastMode = StartInFastMode,
            EnableForwarding = EnableForwarding,
            ConflictDebtIncrement = ConflictDebtIncrement,
            ConflictDebtDecay = ConflictDebtDecay,
            FastReopenThreshold = FastReopenThreshold,
            LeaderMissThreshold = LeaderMissThreshold,
        };
        policy.Validate();
        // Starting leadered requires a representable debt above the reopening threshold.
        if (!StartInFastMode && FastReopenThreshold == int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(FastReopenThreshold), "Leadered startup requires a threshold below Int32.MaxValue.");
        var mode = ProposerModePolicy.Parse(Mode);
        return mode.IsAdaptive ? ProposerModePolicy.Adaptive(policy) : mode;
    }
}
