using System.Diagnostics;
using System.Globalization;

namespace Clockwork;

/// <summary>
/// Base class for all scheduled items in the queue.
/// Implements IDisposable for cancellation support.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public abstract class ScheduledItem : IDisposable
{
    private SimulationTaskQueue? _queue;
    private bool _disposed;

    /// <summary>
    /// Gets the comparer for ordering scheduled items.
    /// </summary>
    public static IComparer<ScheduledItem> Comparer { get; } = ScheduledItemComparer.Instance;

    /// <summary>
    /// Gets the absolute time when this item is due.
    /// Set internally by <see cref="SimulationTaskQueue"/> when the item is scheduled.
    /// </summary>
    public DateTimeOffset DueTime { get; private set; }

    /// <summary>
    /// Gets the sequence number for ordering items with the same due time.
    /// Set internally by <see cref="SimulationTaskQueue"/> when the item is scheduled.
    /// </summary>
    public long SequenceNumber { get; private set; }

    /// <summary>
    /// Called by <see cref="SimulationTaskQueue"/> when the item is added to the queue.
    /// Sets the queue reference, due time, and sequence number.
    /// </summary>
    /// <param name="queue">The queue this item belongs to.</param>
    /// <param name="dueTime">The absolute time when this item is due.</param>
    /// <param name="sequenceNumber">The sequence number for ordering.</param>
    internal void OnScheduled(SimulationTaskQueue queue, DateTimeOffset dueTime, long sequenceNumber)
    {
        if (_queue is not null)
        {
            throw new InvalidOperationException("Item has already been scheduled.");
        }

        _queue = queue;
        DueTime = dueTime;
        SequenceNumber = sequenceNumber;
    }

    /// <summary>
    /// Executes the scheduled item's action.
    /// </summary>
    protected internal abstract void Invoke();

    /// <summary>
    /// Cancels the item by removing it from the queue.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes the scheduled item.
    /// </summary>
    /// <param name="disposing">True if called from Dispose(), false if from finalizer.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;
        if (disposing)
        {
            _queue?.RemoveItem(this);
        }
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"Due={DueTime:HH:mm:ss.fff} Seq={SequenceNumber}");

    /// <summary>
    /// Comparer for ordering <see cref="ScheduledItem"/> instances by due time, then by sequence number.
    /// </summary>
    private sealed class ScheduledItemComparer : IComparer<ScheduledItem>
    {
        /// <summary>
        /// Gets the singleton instance of the comparer.
        /// </summary>
        public static ScheduledItemComparer Instance { get; } = new();

        private ScheduledItemComparer() { }

        /// <inheritdoc />
        public int Compare(ScheduledItem? x, ScheduledItem? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            var dueTimeComparison = x.DueTime.CompareTo(y.DueTime);
            return dueTimeComparison != 0 ? dueTimeComparison : x.SequenceNumber.CompareTo(y.SequenceNumber);
        }
    }
}
