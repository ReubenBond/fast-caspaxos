using System.Diagnostics;
using System.Globalization;

namespace Clockwork;

/// <summary>
/// <para>
/// A time-aware task queue that serves as the common core for both
/// <see cref="TaskScheduler"/> and <see cref="SimulationTimeProvider"/>.
/// </para>
/// <para>
/// Items are stored in a single queue ordered by due time, then sequence number.
/// Items with DueTime &lt;= UtcNow are considered "ready" for execution.
/// This enables deterministic simulation testing by providing unified control
/// over task execution order and time advancement.
/// </para>
/// <para>
/// The queue delegates time to a shared <see cref="SimulationClock"/>,
/// enabling multiple queues to share a unified view of time.
/// </para>
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
[DebuggerTypeProxy(typeof(SimulationTaskQueueDebugView))]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "TaskQueue accurately describes this component's purpose")]
public sealed class SimulationTaskQueue
{
    // Single queue ordered by due time, then sequence number
    private readonly SortedSet<ScheduledItem> _queue = new(ScheduledItem.Comparer);
    private readonly SimulationClock _clock;
    private readonly SingleThreadedGuard _guard;
    private long _sequenceNumber;

    /// <summary>
    /// Gets the scheduled items in the queue, ordered by due time then sequence number.
    /// This is a read-only view that cannot be modified.
    /// </summary>
    public IReadOnlySet<ScheduledItem> ScheduledItems { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SimulationTaskQueue"/> class.
    /// Creates a new simulation task queue that uses the specified clock for time.
    /// Multiple queues can share the same clock for unified time coordination.
    /// </summary>
    /// <param name="clock">The clock to use for time.</param>
    /// <param name="guard">The single-threaded guard used to detect concurrent access on simulation-thread-only operations.</param>
    public SimulationTaskQueue(SimulationClock clock, SingleThreadedGuard guard)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(guard);
        _clock = clock;
        _guard = guard;
        SynchronizationContext = new SimulationSynchronizationContext(this);
        ScheduledItems = _queue.AsReadOnly();
    }

    /// <summary>
    /// Gets the current simulated date/time.
    /// </summary>
    public DateTimeOffset UtcNow => _clock.UtcNow;

    /// <summary>
    /// Gets the synchronization context used to execute callbacks.
    /// </summary>
    public SimulationSynchronizationContext SynchronizationContext { get; }

    /// <summary>
    /// Gets a value indicating whether gets whether there are any items in the queue.
    /// This is called from the simulation thread only.
    /// </summary>
    public bool HasItems
    {
        get
        {
            using var _ = _guard.Enter();
            return _queue.Count > 0;
        }
    }

    /// <summary>
    /// Gets the due time of the next waiting (not yet ready) task, or null if no waiting tasks exist.
    /// This is called from the simulation thread only.
    /// </summary>
    public DateTimeOffset? NextWaitingDueTime
    {
        get
        {
            using var _ = _guard.Enter();
            foreach (var item in _queue)
            {
                if (item.DueTime > UtcNow)
                    return item.DueTime;
            }

            return null;
        }
    }

    /// <summary>
    /// Enqueues a scheduled item to be executed immediately (at current time).
    /// The item's DueTime, SequenceNumber, and queue reference are set by this method.
    /// This method must be called from the simulation thread - the guard will throw
    /// if called from another thread, indicating async work has escaped the simulation.
    /// </summary>
    /// <param name="item">The scheduled item to enqueue.</param>
    public void Enqueue(ScheduledItem item)
    {
        ArgumentNullException.ThrowIfNull(item);
        using var _ = _guard.Enter();
        ScheduleCore(item, UtcNow);
    }

    /// <summary>
    /// Enqueues an action to be executed after a delay from the current time.
    /// Convenience method that creates a <see cref="ScheduledActionItem"/>.
    /// This is called from the simulation thread only.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="delay">The delay from the current time.</param>
    public void EnqueueAfter(Action action, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        using var _ = _guard.Enter();
        ScheduleCore(new ScheduledActionItem(action), UtcNow + delay);
    }

    /// <summary>
    /// Enqueues an item to be executed after a delay from the current time.
    /// This is called from the simulation thread only.
    /// </summary>
    /// <param name="item">The item to execute.</param>
    /// <param name="delay">The delay from the current time.</param>
    public TItem EnqueueAfter<TItem>(TItem item, TimeSpan delay)
        where TItem : ScheduledItem
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        using var _ = _guard.Enter();
        ScheduleCore(item, UtcNow + delay);
        return item;
    }

    /// <summary>
    /// Schedules an item to be executed at a specific absolute time.
    /// The item's DueTime, SequenceNumber, and queue reference are set by this method.
    /// Returns the scheduled item which can be disposed to cancel it.
    /// CALLER MUST HOLD _guard.
    /// </summary>
    /// <param name="item">The scheduled item to schedule.</param>
    /// <param name="dueTime">The absolute time when the item should be executed.</param>
    private void ScheduleCore(ScheduledItem item, DateTimeOffset dueTime)
    {
        ArgumentNullException.ThrowIfNull(item);
        item.OnScheduled(this, dueTime, _sequenceNumber++);
        _queue.Add(item);
    }

    /// <summary>
    /// Removes an item from the queue. Called by ScheduledItem.Dispose().
    /// This method must be called from the simulation thread.
    /// </summary>
    /// <param name="item">The item to remove.</param>
    internal void RemoveItem(ScheduledItem item)
    {
        using var _ = _guard.Enter();
        _queue.Remove(item);
    }

    /// <summary>
    /// Tries to dequeue and execute the next ready item.
    /// This is called from the simulation thread only.
    /// </summary>
    /// <returns>True if an item was dequeued and executed, false if no items are ready.</returns>
    public bool RunOnce()
    {
        using var _ = _guard.Enter();
        if (_queue.Count == 0)
            return false;

        var item = _queue.Min!;
        if (item.DueTime > UtcNow)
            return false; // No ready items

        _queue.Remove(item);
        using (SynchronizationContext.Install())
        {
            item.Invoke();
        }

        return true;
    }

    /// <summary>
    /// Executes all ready items in the queue.
    /// </summary>
    /// <returns>The number of items executed.</returns>
    public int RunUntilIdle()
    {
        var count = 0;
        while (RunOnce())
        {
            count++;
        }

        return count;
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"Count={_queue.Count} UtcNow={UtcNow:HH:mm:ss.fff}");
}

/// <summary>
/// Debug view for SimulationTaskQueue that shows scheduled items in a more readable format.
/// </summary>
internal sealed class SimulationTaskQueueDebugView(SimulationTaskQueue queue)
{
    private readonly SimulationTaskQueue _queue = queue;

    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public ScheduledItem[] Items => [.. _queue.ScheduledItems];
}
