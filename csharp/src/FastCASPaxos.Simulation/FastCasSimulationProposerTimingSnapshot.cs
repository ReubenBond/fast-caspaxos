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
