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

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; init; } =
        FastCasCrashRestartStateModel.PreserveDurableAcceptorState;
}
