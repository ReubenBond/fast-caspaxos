using System.Diagnostics.CodeAnalysis;

namespace Clockwork;

/// <summary>
/// Abstract base class for simulated nodes in a distributed system simulation.
/// Provides common functionality for node lifecycle management
/// independent of any specific application domain.
/// </summary>
[SuppressMessage("Naming", "CA1716:Identifiers should not match keywords",
    Justification = "Resume is the natural name for this operation and this is an internal simulation library")]
public abstract class SimulationNode
{
    /// <summary>
    /// Gets the simulation context for this node, containing the task queue,
    /// scheduler, time provider, and random number generator.
    /// </summary>
    public abstract SimulationNodeContext Context { get; }

    /// <summary>
    /// Gets the network address of this node as a string.
    /// Used by the simulation network for routing and partition management.
    /// </summary>
    public abstract string NetworkAddress { get; }

    /// <summary>
    /// Gets a value indicating whether gets whether this node has been initialized and is ready for operation.
    /// </summary>
    public abstract bool IsInitialized { get; }

    /// <summary>
    /// Gets a value indicating whether gets whether this node is currently suspended.
    /// Suspended nodes do not execute tasks from their queue.
    /// </summary>
    public bool IsSuspended => Context.IsSuspended;

    /// <summary>
    /// Suspends this node, preventing it from executing tasks.
    /// Messages sent to the node will be queued but not processed until resumed.
    /// </summary>
    public void Suspend() => Context.Suspend();

    /// <summary>
    /// Resumes this node, allowing it to execute tasks again.
    /// </summary>
    public void Resume() => Context.Resume();

    /// <summary>
    /// Suspends this node for the specified duration, then automatically resumes it.
    /// The resume occurs when simulated time advances past the duration.
    /// </summary>
    /// <param name="duration">How long to suspend the node (in simulated time).</param>
    public void SuspendFor(TimeSpan duration) => Context.SuspendFor(duration);

    /// <summary>
    /// Executes one ready task from this node's queue.
    /// </summary>
    /// <returns>True if a task was executed; false if no tasks are ready or the node is suspended.</returns>
    public bool Step() => Context.Step();
}
