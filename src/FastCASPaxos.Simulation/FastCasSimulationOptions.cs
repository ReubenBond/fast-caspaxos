using FastCASPaxos.Diagnostics;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationOptions
{
    private static readonly FastCasSimulationProposerRetryBackoffOptions DefaultProposerRetryBackoff = new();
    private static readonly TimeSpan DefaultProposerResponseTimeout = TimeSpan.FromSeconds(1);

    private FastCasSimulationProposerRetryBackoffOptions _proposerRetryBackoff = DefaultProposerRetryBackoff.Clone();
    private bool _hasExplicitProposerRetryBackoff;
    private TimeSpan _proposerResponseTimeout = DefaultProposerResponseTimeout;
    private bool _hasExplicitProposerResponseTimeout;
    private TimeSpan _expectedOneWayLatency;
    private TimeSpan _expectedRoundTripLatency;

    public bool EnableDistinguishedLeader { get; init; }

    public bool EnableFastCommit { get; init; }

    public ILoggerFactory? LoggerFactory { get; init; }

    public Func<FastCasNodeId, ProposerDiagnostics>? CreateProposerDiagnostics { get; init; }

    public IFastCasSimulationTransportObserver? TransportObserver { get; init; }

    public IFastCasSimulationHistoryObserver? HistoryObserver { get; init; }

    public IFastCasSimulationAcceptorStateObserver? AcceptorStateObserver { get; init; }

    public FastCasSimulationLatencyAwareProposerTimingOptions LatencyAwareProposerTiming { get; init; } = new();

    public FastCasSimulationProposerRetryBackoffOptions ProposerRetryBackoff
    {
        get => _proposerRetryBackoff;
        set => SetProposerRetryBackoff(value, explicitOverride: true);
    }

    /// <summary>
    /// Maximum simulated time the proposer waits for prepare/accept responses before
    /// forcing a retry with a higher ballot. Prevents indefinite stalls when acceptors
    /// are unreachable. Should be a multiple of the maximum expected link latency.
    /// Set to <see cref="TimeSpan.Zero"/> to disable.
    /// Default is 1 second of simulated time.
    /// </summary>
    public TimeSpan ProposerResponseTimeout
    {
        get => _proposerResponseTimeout;
        set => SetProposerResponseTimeout(value, explicitOverride: true);
    }

    public FastCasCrashRestartStateModel CrashRestartStateModel { get; init; } =
        FastCasCrashRestartStateModel.PreserveDurableAcceptorState;

    internal void ApplyLatencyAwareProposerTiming(TimeSpan expectedOneWayLatency)
    {
        var effectiveExpectedOneWayLatency = expectedOneWayLatency > TimeSpan.Zero
            ? expectedOneWayLatency
            : TimeSpan.Zero;

        if (LatencyAwareProposerTiming.Enabled && effectiveExpectedOneWayLatency > TimeSpan.Zero)
        {
            var derived = LatencyAwareProposerTiming.Derive(effectiveExpectedOneWayLatency);
            _expectedOneWayLatency = derived.ExpectedOneWayLatency;
            _expectedRoundTripLatency = derived.ExpectedRoundTripLatency;

            if (!_hasExplicitProposerRetryBackoff)
            {
                SetProposerRetryBackoff(derived.CreateRetryBackoffOptions(), explicitOverride: false);
            }

            if (!_hasExplicitProposerResponseTimeout)
            {
                SetProposerResponseTimeout(derived.ResponseTimeout, explicitOverride: false);
            }

            return;
        }

        _expectedOneWayLatency = effectiveExpectedOneWayLatency;
        _expectedRoundTripLatency = effectiveExpectedOneWayLatency == TimeSpan.Zero
            ? TimeSpan.Zero
            : TimeSpan.FromTicks(effectiveExpectedOneWayLatency.Ticks * 2);
        if (!_hasExplicitProposerRetryBackoff)
        {
            SetProposerRetryBackoff(DefaultProposerRetryBackoff, explicitOverride: false);
        }

        if (!_hasExplicitProposerResponseTimeout)
        {
            SetProposerResponseTimeout(DefaultProposerResponseTimeout, explicitOverride: false);
        }
    }

    public FastCasSimulationProposerTimingSnapshot GetProposerTimingSnapshot() =>
        new(
            PolicyName: FastCasSimulationProposerRetryBackoffOptions.PolicyName,
            Source: GetProposerTimingSource(),
            ExpectedOneWayLatency: _expectedOneWayLatency,
            ExpectedRoundTripLatency: _expectedRoundTripLatency,
            RetryBackoffEnabled: _proposerRetryBackoff.Enabled,
            RetryStartAfterFailures: _proposerRetryBackoff.StartAfterFailures,
            RetryBaseDelay: _proposerRetryBackoff.BaseDelay,
            RetryMaxDelay: _proposerRetryBackoff.MaxDelay,
            RetryJitterRatio: _proposerRetryBackoff.JitterRatio,
            ResponseTimeout: _proposerResponseTimeout);

    private string GetProposerTimingSource()
    {
        if (!LatencyAwareProposerTiming.Enabled || _expectedOneWayLatency == TimeSpan.Zero)
        {
            return _hasExplicitProposerRetryBackoff || _hasExplicitProposerResponseTimeout
                ? "explicit-overrides"
                : "fixed-defaults";
        }

        return (_hasExplicitProposerRetryBackoff, _hasExplicitProposerResponseTimeout) switch
        {
            (false, false) => "latency-aware-derived",
            (true, true) => "explicit-overrides",
            _ => "partial-explicit-overrides",
        };
    }

    private void SetProposerRetryBackoff(
        FastCasSimulationProposerRetryBackoffOptions value,
        bool explicitOverride)
    {
        ArgumentNullException.ThrowIfNull(value);
        value.Validate();
        _proposerRetryBackoff = value.Clone();
        if (explicitOverride)
        {
            _hasExplicitProposerRetryBackoff = true;
        }
    }

    private void SetProposerResponseTimeout(TimeSpan value, bool explicitOverride)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
        _proposerResponseTimeout = value;
        if (explicitOverride)
        {
            _hasExplicitProposerResponseTimeout = true;
        }
    }
}
