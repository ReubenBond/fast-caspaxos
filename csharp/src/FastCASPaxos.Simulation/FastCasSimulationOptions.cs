using FastCASPaxos.Diagnostics;
using FastCASPaxos.Protocol;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationOptions
{
    private static readonly FastCasSimulationProposerRetryBackoffOptions DefaultProposerRetryBackoff = new();
    private static readonly TimeSpan DefaultProposerResponseTimeout = TimeSpan.FromSeconds(1);
    private TimeSpan _expectedOneWayLatency;
    private TimeSpan _expectedRoundTripLatency;

    public ProposerModePolicy ProposerModePolicy
    {
        get;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            value.Validate();
            field = value;
        }
    } = ProposerModePolicy.StaticClassic;

    public ILoggerFactory? LoggerFactory { get; init; }

    public Func<FastCasNodeId, ProposerDiagnostics>? CreateProposerDiagnostics { get; init; }

    public IFastCasSimulationTransportObserver? TransportObserver { get; init; }

    public IFastCasSimulationHistoryObserver? HistoryObserver { get; init; }

    public IFastCasSimulationAcceptorStateObserver? AcceptorStateObserver { get; init; }

    public FastCasSimulationProposerRetryBackoffOptions ProposerRetryBackoff
    {
        get;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            value.Validate();
            field = value.Clone();
        }
    } = DefaultProposerRetryBackoff.Clone();

    /// <summary>
    /// Maximum simulated time the proposer waits for prepare/accept responses before
    /// forcing a retry with a higher ballot. Prevents indefinite stalls when acceptors
    /// are unreachable. Set to <see cref="TimeSpan.Zero"/> to disable.
    /// Default is 1 second of simulated time.
    /// </summary>
    public TimeSpan ProposerResponseTimeout
    {
        get;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            field = value;
        }
    } = DefaultProposerResponseTimeout;

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; init; } =
        FastCasCrashRestartStateModel.PreserveDurableAcceptorState;

    /// <summary>
    /// Maximum simulated time the proposer waits for a forwarded proposal response from the leader.
    /// When zero (the default), the timeout is computed as <see cref="ProposerResponseTimeout"/> * 4.
    /// Set to <see cref="TimeSpan.Zero"/> explicitly to inherit; set to a negative value to disable.
    /// </summary>
    public TimeSpan ForwardingResponseTimeout { get; init; } = TimeSpan.Zero;

    internal TimeSpan GetEffectiveForwardingResponseTimeout() =>
        ForwardingResponseTimeout > TimeSpan.Zero
            ? ForwardingResponseTimeout
            : ProposerResponseTimeout == TimeSpan.Zero
                ? TimeSpan.Zero
                : TimeSpan.FromTicks(ProposerResponseTimeout.Ticks * 4);

    /// <summary>
    /// Updates the observed one-way and round-trip latency estimates.
    /// These are informational only — backoff and response-timeout values
    /// are fixed and not derived from latency.
    /// </summary>
    internal void UpdateExpectedLatency(TimeSpan expectedOneWayLatency)
    {
        _expectedOneWayLatency = expectedOneWayLatency > TimeSpan.Zero
            ? expectedOneWayLatency
            : TimeSpan.Zero;
        _expectedRoundTripLatency = _expectedOneWayLatency == TimeSpan.Zero
            ? TimeSpan.Zero
            : TimeSpan.FromTicks(_expectedOneWayLatency.Ticks * 2);
    }

    public FastCasSimulationProposerTimingSnapshot GetProposerTimingSnapshot() =>
        new(
            PolicyName: FastCasSimulationProposerRetryBackoffOptions.PolicyName,
            Source: "fixed-defaults",
            ExpectedOneWayLatency: _expectedOneWayLatency,
            ExpectedRoundTripLatency: _expectedRoundTripLatency,
            RetryBackoffEnabled: ProposerRetryBackoff.Enabled,
            RetryStartAfterFailures: ProposerRetryBackoff.StartAfterFailures,
            RetryBaseDelay: ProposerRetryBackoff.BaseDelay,
            RetryMaxDelay: ProposerRetryBackoff.MaxDelay,
            RetryJitterRatio: ProposerRetryBackoff.JitterRatio,
            ResponseTimeout: ProposerResponseTimeout);
}
