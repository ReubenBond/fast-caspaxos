using FastCASPaxos.Diagnostics;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationOptions
{
    public bool EnableDistinguishedLeader { get; init; }

    public bool EnableFastCommit { get; init; }

    public ILoggerFactory? LoggerFactory { get; init; }

    public Func<FastCasNodeId, ProposerDiagnostics>? CreateProposerDiagnostics { get; init; }

    public IFastCasSimulationTransportObserver? TransportObserver { get; init; }

    public IFastCasSimulationHistoryObserver? HistoryObserver { get; init; }

    public IFastCasSimulationAcceptorStateObserver? AcceptorStateObserver { get; init; }

    public FastCasSimulationProposerRetryBackoffOptions ProposerRetryBackoff { get; init; } = new();

    /// <summary>
    /// Maximum simulated time the proposer waits for prepare/accept responses before
    /// forcing a retry with a higher ballot. Prevents indefinite stalls when acceptors
    /// are unreachable. Should be a multiple of the maximum expected link latency.
    /// Set to <see cref="TimeSpan.Zero"/> to disable.
    /// Default is 1 second of simulated time.
    /// </summary>
    public TimeSpan ProposerResponseTimeout { get; init; } = TimeSpan.FromSeconds(1);

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; init; } =
        FastCasCrashRestartStateModel.PreserveDurableAcceptorState;
}
