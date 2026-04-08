namespace FastCASPaxos.Simulation;

/// <summary>
/// Defines how simulated node restarts treat protocol state.
/// </summary>
public enum FastCasCrashRestartStateModel
{
    /// <summary>
    /// Restart clears volatile proposer/node state and pending in-memory work, but preserves
    /// the acceptor's promised ballot, accepted ballot, and accepted value as durable state.
    /// </summary>
    PreserveDurableAcceptorState,
}
