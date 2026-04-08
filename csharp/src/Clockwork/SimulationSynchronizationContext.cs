namespace Clockwork;

/// <summary>
/// A synchronization context that routes all continuations through a <see cref="SimulationTaskQueue"/>.
/// </summary>
public sealed class SimulationSynchronizationContext(SimulationTaskQueue taskQueue) : SynchronizationContext
{
    /// <inheritdoc />
    public override void Post(SendOrPostCallback d, object? state)
    {
        ArgumentNullException.ThrowIfNull(d);
        taskQueue.Enqueue(new ScheduledSyncContextItem(d, state));
    }

    /// <inheritdoc />
    public override void Send(SendOrPostCallback d, object? state)
    {
        ArgumentNullException.ThrowIfNull(d);
#pragma warning disable EPC20 // Avoid using default ToString implementation
#pragma warning disable MA0150 // Do not call the default object.ToString explicitly
        throw new InvalidOperationException($"Cannot synchronously execute callback {d} with state {state}. Current ctx is {Current}");
#pragma warning restore MA0150 // Do not call the default object.ToString explicitly
#pragma warning restore EPC20 // Avoid using default ToString implementation
    }

    /// <inheritdoc />
    public override SynchronizationContext CreateCopy() => new SimulationSynchronizationContext(taskQueue);

    /// <summary>
    /// Gets the underlying task queue for this synchronization context.
    /// </summary>
    public object UnderlyingScheduler => taskQueue;

    /// <summary>
    /// Checks if this synchronization context shares the same scheduler as another context.
    /// </summary>
    /// <param name="syncCtx">The context to compare with.</param>
    /// <returns>True if they share the same underlying scheduler.</returns>
    public bool IsSameScheduler(SynchronizationContext syncCtx) => ReferenceEquals(syncCtx, this) || syncCtx is SimulationSynchronizationContext simSyncCtx && simSyncCtx.UnderlyingScheduler.Equals(UnderlyingScheduler);

    /// <summary>
    /// Installs this synchronization context on the current thread and returns a scope
    /// that restores the previous context when disposed.
    /// If the current context is already this instance, or if the current TaskScheduler
    /// is a SimulationTaskScheduler sharing the same underlying queue, returns an empty
    /// scope (no-op) to avoid redundant context switching.
    /// </summary>
    /// <returns>A disposable scope that restores the previous synchronization context when disposed.</returns>
    public SynchronizationContextScope Install()
    {
        // Check if already installed (same instance)
        if (Current is not null && IsSameScheduler(Current))
        {
            return SynchronizationContextScope.Empty;
        }

        // Check if current TaskScheduler shares the same underlying queue
        if (TaskScheduler.Current is SimulationTaskScheduler simScheduler && simScheduler.IsSameScheduler(this))
        {
            return SynchronizationContextScope.Empty;
        }

        var previous = Current;
        SetSynchronizationContext(this);
        return new SynchronizationContextScope(previous);
    }

    private sealed class ScheduledSyncContextItem(SendOrPostCallback callback, object? state) : ScheduledItem
    {
        protected internal override void Invoke() => callback(state);
    }
}
