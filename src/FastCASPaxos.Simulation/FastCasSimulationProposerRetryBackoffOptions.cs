using Clockwork;

namespace FastCASPaxos.Simulation;

public sealed class FastCasSimulationProposerRetryBackoffOptions
{
    public bool Enabled { get; init; } = true;

    public int StartAfterFailures { get; init; } = 2;

    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(4);

    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMilliseconds(64);

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(StartAfterFailures, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(BaseDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxDelay, TimeSpan.Zero);
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

        var jitterWindow = Math.Min(delayTicks, maxTicks - delayTicks);
        if (jitterWindow > 0)
        {
            delayTicks += (long)(random.NextDouble() * jitterWindow);
        }

        return TimeSpan.FromTicks(delayTicks);
    }
}
