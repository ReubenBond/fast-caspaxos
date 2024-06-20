using FastCASPaxos.Model;

namespace FastCASPaxos.Diagnostics;

/// <summary>
/// Event payload records emitted through <see cref="System.Diagnostics.DiagnosticSource"/> by <see cref="ProposerDiagnostics"/>.
/// Each event carries the <see cref="ProposerEngine{TValue,TRoute}"/> instance that produced it
/// so that subscribers (including tests) can inspect engine state at the moment the event fired.
/// </summary>
public static class ProposerEventNames
{
    public const string ProposalStarted = "FastCASPaxos.ProposerEngine.ProposalStarted";
    public const string ValueCommitted = "FastCASPaxos.ProposerEngine.ValueCommitted";
    public const string ProposalCompleted = "FastCASPaxos.ProposerEngine.ProposalCompleted";
    public const string ConflictDetected = "FastCASPaxos.ProposerEngine.ConflictDetected";
    public const string PrepareRetried = "FastCASPaxos.ProposerEngine.PrepareRetried";
    public const string ValueAdopted = "FastCASPaxos.ProposerEngine.ValueAdopted";
}

public sealed record ProposalStartedEvent(object Engine);

public sealed record ValueCommittedEvent(object Engine, object CommittedValue, Ballot Ballot);

public sealed record ProposalCompletedEvent(object Engine, object CommittedValue);

public sealed record ConflictDetectedEvent(object Engine, Ballot ConflictBallot);

public sealed record PrepareRetriedEvent(object Engine, Ballot OldBallot, Ballot NewBallot, Ballot ConflictBallot);

public sealed record ValueAdoptedEvent(object Engine, object AdoptedValue);
