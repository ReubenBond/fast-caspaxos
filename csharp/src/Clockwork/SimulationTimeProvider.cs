using System.Globalization;

namespace Clockwork;

/// <summary>
/// <para>
/// A time provider for simulation testing that integrates with <see cref="SimulationTaskQueue"/>
/// for deterministic timer execution.
/// </para>
/// <para>
/// Timer callbacks are scheduled through the queue instead of being executed immediately.
/// This enables fully deterministic simulation testing where task execution order is controlled.
/// </para>
/// <para>
/// Time is tracked centrally by the <see cref="SimulationClock"/> - this provider
/// delegates all time queries to the clock.
/// </para>
/// </summary>
public sealed class SimulationTimeProvider : TimeProvider
{
    private readonly SimulationTaskQueue _taskQueue;
    private readonly SimulationClock _clock;

    /// <summary>
    /// Initializes a new instance of the <see cref="SimulationTimeProvider"/> class with a task queue and clock.
    /// </summary>
    /// <param name="taskQueue">The task queue for scheduling timer callbacks.</param>
    /// <param name="clock">The simulation clock for time queries.</param>
    public SimulationTimeProvider(SimulationTaskQueue taskQueue, SimulationClock clock)
    {
        ArgumentNullException.ThrowIfNull(taskQueue);
        ArgumentNullException.ThrowIfNull(clock);

        _taskQueue = taskQueue;
        _clock = clock;
    }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => _clock.UtcNow;

    /// <inheritdoc />
    public override long GetTimestamp() => _clock.UtcNow.Ticks;

    /// <inheritdoc />
    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    /// <inheritdoc />
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override string ToString() => GetUtcNow().ToString("yyyy-MM-ddTHH:mm:ss.fff", CultureInfo.InvariantCulture);

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);

        var timer = new SimulationTimer(_taskQueue, callback, state);
        _ = timer.Change(dueTime, period);
        return timer;
    }
}
