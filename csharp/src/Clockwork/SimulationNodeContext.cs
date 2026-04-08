using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Clockwork;

/// <summary>
/// Represents the execution state of a simulated node.
/// </summary>
public enum SimulationNodeState
{
    /// <summary>
    /// The node is running and will execute tasks during simulation stepping.
    /// </summary>
    Running,

    /// <summary>
    /// The node is suspended and will not execute tasks.
    /// Messages sent to the node will be queued but not processed until resumed.
    /// Timers will accumulate and fire when the node is resumed if their due time has passed.
    /// </summary>
    Suspended,
}

/// <summary>
/// <para>
/// Encapsulates all per-node simulation state, including the node's task queue,
/// task scheduler, synchronization context, time provider, and random number generator.
/// </para>
/// <para>
/// Each simulated node has its own context, allowing fine-grained control
/// over individual node execution (pause, resume, step) while sharing a unified
/// <see cref="SimulationClock"/> for time synchronization.
/// </para>
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class SimulationNodeContext
{
    private readonly SimulationTaskQueue? _externalTaskQueue;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SimulationNodeContext"/> class.
    /// Creates a new simulation node context using the specified shared clock and random generator.
    /// </summary>
    /// <param name="clock">The shared simulation clock for time coordination.</param>
    /// <param name="guard">The shared single-threaded guard for detecting concurrent access.</param>
    /// <param name="random">The deterministic random number generator for this node.</param>
    /// <param name="externalTaskQueue">Optional external task queue for scheduling operations that must run
    /// even when this node is suspended (e.g., auto-resume from SuspendFor). If not provided,
    /// SuspendFor will throw InvalidOperationException.</param>
    /// <param name="logger">Optional logger for suspend/resume operations.</param>
    public SimulationNodeContext(
        SimulationClock clock,
        SingleThreadedGuard guard,
        SimulationRandom random,
        SimulationTaskQueue? externalTaskQueue = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(guard);
        ArgumentNullException.ThrowIfNull(random);

        Clock = clock;
        Random = random;
        _externalTaskQueue = externalTaskQueue;
        _logger = logger ?? NullLogger.Instance;
        TaskQueue = new SimulationTaskQueue(clock, guard);
        TaskScheduler = new SimulationTaskScheduler(TaskQueue);
        TimeProvider = new SimulationTimeProvider(TaskQueue, clock);
    }

    /// <summary>
    /// Gets the shared simulation clock.
    /// </summary>
    public SimulationClock Clock { get; }

    /// <summary>
    /// Gets the deterministic random number generator for this node.
    /// </summary>
    public SimulationRandom Random { get; }

    /// <summary>
    /// Gets the task queue for this node.
    /// Tasks scheduled on this queue are only executed when this node is stepped.
    /// </summary>
    public SimulationTaskQueue TaskQueue { get; }

    /// <summary>
    /// Gets the task scheduler for this node.
    /// Used for scheduling TPL tasks on this node's queue.
    /// </summary>
    public SimulationTaskScheduler TaskScheduler { get; }

    /// <summary>
    /// Gets the synchronization context for this node.
    /// Used for async/await continuations on this node's queue.
    /// </summary>
    public SimulationSynchronizationContext SynchronizationContext => TaskQueue.SynchronizationContext;

    /// <summary>
    /// Gets the time provider for this node.
    /// Timers created through this provider are scheduled on this node's queue.
    /// </summary>
    public SimulationTimeProvider TimeProvider { get; }

    /// <summary>
    /// Gets the current execution state of this node.
    /// </summary>
    public SimulationNodeState State { get; private set; } = SimulationNodeState.Running;

    /// <summary>
    /// Gets a value indicating whether gets whether this node is currently suspended.
    /// </summary>
    public bool IsSuspended => State == SimulationNodeState.Suspended;

    /// <summary>
    /// Gets a value indicating whether gets whether this node has any tasks ready to execute at the current time.
    /// </summary>
    public bool HasReadyTasks
    {
        get
        {
            if (State == SimulationNodeState.Suspended)
                return false;

            // Check if the queue has any items due at or before the current time
            var items = TaskQueue.ScheduledItems;
            if (items.Count == 0)
                return false;

            // The first item in the sorted set has the earliest due time
            // Use FirstOrDefault since IReadOnlySet doesn't have Min
            var firstItem = items.FirstOrDefault();
            return firstItem != null && firstItem.DueTime <= Clock.UtcNow;
        }
    }

    /// <summary>
    /// Gets the due time of the next waiting (not yet ready) task on this node's queue,
    /// or null if no tasks are waiting.
    /// </summary>
    public DateTimeOffset? NextWaitingDueTime => TaskQueue.NextWaitingDueTime;

    /// <summary>
    /// Executes one ready task from this node's queue.
    /// </summary>
    /// <returns>True if a task was executed; false if no tasks are ready or the node is suspended.</returns>
    public bool Step()
    {
        if (State == SimulationNodeState.Suspended)
            return false;

        return TaskQueue.RunOnce();
    }

    /// <summary>
    /// Executes all ready tasks from this node's queue.
    /// </summary>
    /// <returns>The number of tasks executed. Returns 0 if the node is suspended.</returns>
    public int RunUntilIdle()
    {
        if (State == SimulationNodeState.Suspended)
            return 0;

        return TaskQueue.RunUntilIdle();
    }

    /// <summary>
    /// Suspends this node, preventing it from executing tasks.
    /// Messages sent to the node will be queued but not processed until resumed.
    /// </summary>
    public void Suspend()
    {
        State = SimulationNodeState.Suspended;
        Log.NodeSuspended(_logger);
    }

    /// <summary>
    /// Resumes this node, allowing it to execute tasks again.
    /// Any tasks that became ready while suspended will be executed on subsequent steps.
    /// </summary>
    public void Resume()
    {
        State = SimulationNodeState.Running;
        Log.NodeResumed(_logger);
    }

    /// <summary>
    /// Suspends this node for the specified duration, then automatically resumes it.
    /// The resume occurs when simulated time advances past the duration.
    /// Requires an external task queue to be provided at construction time.
    /// </summary>
    /// <param name="duration">How long to suspend the node (in simulated time).</param>
    /// <exception cref="InvalidOperationException">Thrown if no external task queue was provided.</exception>
    public void SuspendFor(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);

        if (_externalTaskQueue is null)
        {
            throw new InvalidOperationException(
                "SuspendFor requires an external task queue to be provided at construction time.");
        }

        State = SimulationNodeState.Suspended;
        Log.NodeSuspendedFor(_logger, duration);

        // Schedule auto-resume on the external queue (not this node's queue,
        // since it won't run while suspended)
        _externalTaskQueue.EnqueueAfter(Resume, duration);
    }

    private string DebuggerDisplay => $"SimulationNodeContext({State}, Tasks={TaskQueue.ScheduledItems.Count})";

    private static partial class Log
    {
        [LoggerMessage(Level = LogLevel.Debug, Message = "Node suspended")]
        public static partial void NodeSuspended(ILogger logger);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Node resumed")]
        public static partial void NodeResumed(ILogger logger);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Node suspended for {Duration}")]
        public static partial void NodeSuspendedFor(ILogger logger, TimeSpan duration);
    }
}
