namespace Clockwork;

/// <summary>
/// A timer implementation for the simulation time provider.
/// This implements the timer abstractions using SimulationTaskQueue for scheduling.
/// </summary>
public sealed class SimulationTimer(SimulationTaskQueue taskQueue, TimerCallback callback, object? state) : ITimer
{
    private const uint MaxSupportedTimeout = 0xfffffffe;

    private readonly TimerCallback? _callback = callback;
    private readonly object? _state = state;
    private SimulationTaskQueue? _taskQueue = taskQueue;
    private ScheduledTimerItem? _scheduledTimer;

    /// <summary>
    /// Gets the current period for this timer.
    /// </summary>
    public TimeSpan Period { get; private set; }

    /// <inheritdoc />
    public bool Change(TimeSpan dueTime, TimeSpan period)
    {
        var dueTimeMs = (long)dueTime.TotalMilliseconds;
        var periodMs = (long)period.TotalMilliseconds;

        // -1 means infinite (valid), otherwise must be non-negative and within MaxSupportedTimeout
        if (dueTimeMs < -1)
            throw new ArgumentOutOfRangeException(nameof(dueTime));
        if (dueTimeMs != -1 && (ulong)dueTimeMs > MaxSupportedTimeout)
            throw new ArgumentOutOfRangeException(nameof(dueTime));
        if (periodMs < -1)
            throw new ArgumentOutOfRangeException(nameof(period));
        if (periodMs != -1 && (ulong)periodMs > MaxSupportedTimeout)
            throw new ArgumentOutOfRangeException(nameof(period));

        var queue = _taskQueue;
        if (queue is null)
        {
            // timer has been disposed
            return false;
        }

        // Cancel any existing timer
        _scheduledTimer?.Dispose();
        _scheduledTimer = null;

        if (dueTimeMs < 0)
        {
            // Infinite due time means the timer is disabled
            Period = TimeSpan.Zero;
            return true;
        }

        if (periodMs < 0 || periodMs == Timeout.Infinite)
        {
            // Normalize period
            period = TimeSpan.Zero;
        }

        Period = period;

        // Schedule the new timer
        ScheduleNextFiring(queue, dueTime);

        return true;
    }

    private void ScheduleNextFiring(SimulationTaskQueue queue, TimeSpan delay)
    {
        _scheduledTimer = queue.EnqueueAfter(
            new ScheduledTimerItem(this),
            delay);
    }

    private void TimerFired()
    {
        // Invoke the user callback
        _callback!(_state);

        // Reschedule if this is a periodic timer
        var queue = _taskQueue;
        if (queue is not null && Period > TimeSpan.Zero)
        {
            ScheduleNextFiring(queue, Period);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _scheduledTimer?.Dispose();
        _scheduledTimer = null;
        _taskQueue = null;
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Gets information about all pending timers from the task queue.
    /// </summary>
    /// <param name="taskQueue">The task queue to query.</param>
    /// <returns>A list of timer info for all pending timers.</returns>
    public static IReadOnlyList<(DateTimeOffset DueTime, TimeSpan Period)> GetTimers(SimulationTaskQueue taskQueue)
    {
        ArgumentNullException.ThrowIfNull(taskQueue);
        return taskQueue.GetItemsOfType<ScheduledTimerItem, (DateTimeOffset, TimeSpan)>(timer => (timer.DueTime, timer.Timer.Period));
    }

    /// <summary>
    /// Gets the count of pending timers from the task queue.
    /// </summary>
    /// <param name="taskQueue">The task queue to query.</param>
    /// <returns>The count of pending timers.</returns>
    public static int GetPendingTimerCount(SimulationTaskQueue taskQueue)
    {
        ArgumentNullException.ThrowIfNull(taskQueue);
        return taskQueue.GetWaitingCount<ScheduledTimerItem>();
    }

    private sealed class ScheduledTimerItem(SimulationTimer timer) : ScheduledItem
    {
        public SimulationTimer Timer => timer;

        protected internal override void Invoke() => timer.TimerFired();
    }
}
