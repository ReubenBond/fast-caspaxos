using Clockwork;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationProposerRetryBackoffOptions
{
    public const string PolicyName = "exponential-proportional-jitter";

    public bool Enabled { get; init; } = true;

    public int StartAfterFailures { get; init; } = 2;

    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(4);

    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMilliseconds(64);

    public double JitterRatio { get; init; } = 0.5;

    public FastCasSimulationProposerRetryBackoffOptions Clone() =>
        new()
        {
            Enabled = Enabled,
            StartAfterFailures = StartAfterFailures,
            BaseDelay = BaseDelay,
            MaxDelay = MaxDelay,
            JitterRatio = JitterRatio,
        };

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(StartAfterFailures, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(BaseDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(JitterRatio, 0d);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(JitterRatio, 1d);
        if (BaseDelay > MaxDelay)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BaseDelay),
                BaseDelay,
                $"Base delay '{BaseDelay}' cannot exceed max delay '{MaxDelay}'.");
        }
    }

    public TimeSpan GetRetryDelay(int consecutiveFailedRetries, SimulationRandom random)
    {
        ArgumentNullException.ThrowIfNull(random);
        ArgumentOutOfRangeException.ThrowIfLessThan(consecutiveFailedRetries, 0);
        Validate();

        if (!Enabled
            || BaseDelay == TimeSpan.Zero
            || consecutiveFailedRetries <= StartAfterFailures)
        {
            return TimeSpan.Zero;
        }

        var delayTicks = BaseDelay.Ticks;
        var maxTicks = MaxDelay.Ticks;
        var exponent = consecutiveFailedRetries - StartAfterFailures - 1;
        for (var index = 0; index < exponent && delayTicks < maxTicks; index++)
        {
            delayTicks = delayTicks > maxTicks / 2
                ? maxTicks
                : delayTicks * 2;
        }

        if (JitterRatio > 0d && delayTicks > 0)
        {
            var minDelayTicks = Math.Max(
                0L,
                (long)Math.Round(delayTicks * (1d - JitterRatio), MidpointRounding.AwayFromZero));
            if (minDelayTicks < delayTicks)
            {
                delayTicks = minDelayTicks + (long)Math.Round(
                    random.NextDouble() * (delayTicks - minDelayTicks),
                    MidpointRounding.AwayFromZero);
            }
        }

        return TimeSpan.FromTicks(delayTicks);
    }
}
