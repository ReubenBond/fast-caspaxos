namespace Clockwork;

/// <summary>
/// A deterministic task scheduler that queues tasks through a <see cref="SimulationTaskQueue"/>
/// and executes them only when explicitly stepped.
/// </summary>
public sealed class SimulationTaskScheduler(SimulationTaskQueue taskQueue) : TaskScheduler
{
    /// <inheritdoc />
    protected override IEnumerable<Task>? GetScheduledTasks() => taskQueue.GetItemsOfType<ScheduledTaskItem, Task>(item => item.Task);

    /// <summary>
    /// Gets the scheduled tasks in the queue.
    /// </summary>
    public IReadOnlyList<Task> Tasks => taskQueue.GetItemsOfType<ScheduledTaskItem, Task>(item => item.Task);

    /// <inheritdoc />
    protected override void QueueTask(Task task) => taskQueue.Enqueue(new ScheduledTaskItem(task, this));

    /// <inheritdoc />
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

    /// <summary>
    /// Gets the underlying task queue for this scheduler.
    /// </summary>
    public object UnderlyingScheduler => taskQueue;

    /// <summary>
    /// Checks if this scheduler shares the same underlying queue as a synchronization context.
    /// </summary>
    /// <param name="syncCtx">The synchronization context to compare with.</param>
    /// <returns>True if they share the same underlying queue.</returns>
    public bool IsSameScheduler(SynchronizationContext syncCtx) => syncCtx is SimulationSynchronizationContext simSyncCtx && simSyncCtx.UnderlyingScheduler.Equals(UnderlyingScheduler);

    private sealed class ScheduledTaskItem(Task task, SimulationTaskScheduler scheduler) : ScheduledItem
    {
        public Task Task => task;

        protected internal override void Invoke() => scheduler.TryExecuteTask(task);
    }
}
