namespace FastCASPaxos.Simulation;

public readonly record struct FastCasSimulationProposerTimingSnapshot(
    string PolicyName,
    string Source,
    TimeSpan ExpectedOneWayLatency,
    TimeSpan ExpectedRoundTripLatency,
    bool RetryBackoffEnabled,
    int RetryStartAfterFailures,
    TimeSpan RetryBaseDelay,
    TimeSpan RetryMaxDelay,
    double RetryJitterRatio,
    TimeSpan ResponseTimeout)
{
    public FastCasSimulationProposerRetryBackoffOptions CreateRetryBackoffOptions() =>
        new()
        {
            Enabled = RetryBackoffEnabled,
            StartAfterFailures = RetryStartAfterFailures,
            BaseDelay = RetryBaseDelay,
            MaxDelay = RetryMaxDelay,
            JitterRatio = RetryJitterRatio,
        };
}

public sealed class FastCasSimulationLatencyAwareProposerTimingOptions
{
    public bool Enabled { get; init; } = true;

    public int RetryStartAfterFailures { get; init; } = 0;

    public double InitialRetryBackoffRttMultiplier { get; init; } = 3;

    public double MaxRetryBackoffRttMultiplier { get; init; } = 20;

    public double RetryJitterRatio { get; init; } = 0.5;

    public double ResponseTimeoutRttMultiplier { get; init; } = 4;

    public TimeSpan MinimumExpectedOneWayLatency { get; init; } = TimeSpan.FromMilliseconds(1);

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(RetryStartAfterFailures, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(InitialRetryBackoffRttMultiplier, 0d);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxRetryBackoffRttMultiplier, 0d);
        ArgumentOutOfRangeException.ThrowIfLessThan(ResponseTimeoutRttMultiplier, 0d);
        ArgumentOutOfRangeException.ThrowIfLessThan(RetryJitterRatio, 0d);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(RetryJitterRatio, 1d);
        ArgumentOutOfRangeException.ThrowIfLessThan(MinimumExpectedOneWayLatency, TimeSpan.Zero);
        if (InitialRetryBackoffRttMultiplier > MaxRetryBackoffRttMultiplier)
        {
            throw new ArgumentOutOfRangeException(
                nameof(InitialRetryBackoffRttMultiplier),
                InitialRetryBackoffRttMultiplier,
                $"Initial retry multiplier '{InitialRetryBackoffRttMultiplier}' cannot exceed max retry multiplier '{MaxRetryBackoffRttMultiplier}'.");
        }
    }

    public FastCasSimulationProposerTimingSnapshot Derive(TimeSpan expectedOneWayLatency)
    {
        Validate();

        var effectiveOneWayLatency = expectedOneWayLatency > MinimumExpectedOneWayLatency
            ? expectedOneWayLatency
            : MinimumExpectedOneWayLatency;
        var expectedRoundTripLatency = Multiply(effectiveOneWayLatency, 2);

        return new(
            PolicyName: FastCasSimulationProposerRetryBackoffOptions.PolicyName,
            Source: "latency-aware-derived",
            ExpectedOneWayLatency: effectiveOneWayLatency,
            ExpectedRoundTripLatency: expectedRoundTripLatency,
            RetryBackoffEnabled: true,
            RetryStartAfterFailures: RetryStartAfterFailures,
            RetryBaseDelay: Multiply(expectedRoundTripLatency, InitialRetryBackoffRttMultiplier),
            RetryMaxDelay: Multiply(expectedRoundTripLatency, MaxRetryBackoffRttMultiplier),
            RetryJitterRatio: RetryJitterRatio,
            ResponseTimeout: Multiply(expectedRoundTripLatency, ResponseTimeoutRttMultiplier));
    }

    private static TimeSpan Multiply(TimeSpan value, double factor)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(factor, 0d);
        if (value == TimeSpan.Zero || factor == 0d)
        {
            return TimeSpan.Zero;
        }

        var ticks = value.Ticks * factor;
        if (ticks >= TimeSpan.MaxValue.Ticks)
        {
            return TimeSpan.MaxValue;
        }

        return TimeSpan.FromTicks((long)Math.Round(ticks, MidpointRounding.AwayFromZero));
    }
}
