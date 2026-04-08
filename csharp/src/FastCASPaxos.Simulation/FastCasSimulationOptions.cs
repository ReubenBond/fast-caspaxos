using FastCASPaxos.Diagnostics;
using FastCASPaxos.Protocol;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationOptions
{
    private static readonly FastCasSimulationProposerRetryBackoffOptions DefaultProposerRetryBackoff = new();
    private static readonly TimeSpan DefaultProposerResponseTimeout = TimeSpan.FromSeconds(1);

    private FastCasSimulationProposerRetryBackoffOptions _proposerRetryBackoff = DefaultProposerRetryBackoff.Clone();
    private TimeSpan _proposerResponseTimeout = DefaultProposerResponseTimeout;
    private TimeSpan _expectedOneWayLatency;
    private TimeSpan _expectedRoundTripLatency;
    private ProposerModePolicy _proposerModePolicy = ProposerModePolicy.StaticClassic;

    public ProposerModePolicy ProposerModePolicy
    {
        get => _proposerModePolicy;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            value?.Validate();
            _proposerModePolicy = value;
        }
    }

    public ILoggerFactory? LoggerFactory { get; init; }

    public Func<FastCasNodeId, ProposerDiagnostics>? CreateProposerDiagnostics { get; init; }

    public IFastCasSimulationTransportObserver? TransportObserver { get; init; }

    public IFastCasSimulationHistoryObserver? HistoryObserver { get; init; }

    public IFastCasSimulationAcceptorStateObserver? AcceptorStateObserver { get; init; }

    public FastCasSimulationProposerRetryBackoffOptions ProposerRetryBackoff
    {
        get => _proposerRetryBackoff;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            value.Validate();
            _proposerRetryBackoff = value.Clone();
        }
    }

    /// <summary>
    /// Maximum simulated time the proposer waits for prepare/accept responses before
    /// forcing a retry with a higher ballot. Prevents indefinite stalls when acceptors
    /// are unreachable. Set to <see cref="TimeSpan.Zero"/> to disable.
    /// Default is 1 second of simulated time.
    /// </summary>
    public TimeSpan ProposerResponseTimeout
    {
        get => _proposerResponseTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _proposerResponseTimeout = value;
        }
    }

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
            : _proposerResponseTimeout == TimeSpan.Zero
                ? TimeSpan.Zero
                : TimeSpan.FromTicks(_proposerResponseTimeout.Ticks * 4);

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
            RetryBackoffEnabled: _proposerRetryBackoff.Enabled,
            RetryStartAfterFailures: _proposerRetryBackoff.StartAfterFailures,
            RetryBaseDelay: _proposerRetryBackoff.BaseDelay,
            RetryMaxDelay: _proposerRetryBackoff.MaxDelay,
            RetryJitterRatio: _proposerRetryBackoff.JitterRatio,
            ResponseTimeout: _proposerResponseTimeout);
}
