# Clockwork

A deterministic simulation testing framework for distributed systems in .NET.

Clockwork provides controlled time, task scheduling, network simulation, and chaos injection for fully reproducible testing of distributed systems. By controlling all sources of non-determinism, Clockwork enables you to write tests that are:

- **Deterministic**: Same seed produces identical execution every time
- **Fast**: No real-time delays - simulated time advances instantly
- **Comprehensive**: Test edge cases like network partitions, message loss, and node failures
- **Debuggable**: Reproduce any failure with just a seed value

## How It Works

Clockwork achieves determinism by replacing all non-deterministic components with simulation-controlled alternatives:

| Real World | Clockwork Replacement |
|------------|----------------------|
| `TimeProvider.System` | `SimulationTimeProvider` - controlled clock |
| `Task.Delay()` | Timer scheduled on simulation queue |
| `TaskScheduler.Default` | `SimulationTaskScheduler` - explicit stepping |
| `Random` | `SimulationRandom` - seeded PRNG |
| Network I/O | `SimulationNetwork` - in-memory with fault injection |

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   SimulationHarness                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ Clock       │  │ Random      │  │ Network         │  │
│  │ (shared)    │  │ (seeded)    │  │ (fault inject)  │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │              SimulationNode (per node)          │    │
│  │  ┌───────────┐  ┌───────────┐  ┌─────────────┐  │    │
│  │  │ TaskQueue │  │ Scheduler │  │TimeProvider │  │    │
│  │  └───────────┘  └───────────┘  └─────────────┘  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

The harness owns a shared `SimulationClock` and coordinates execution across all nodes. Each node has its own task queue, enabling fine-grained control (suspend, resume, step individual nodes).

### Execution Model

Instead of running tasks concurrently, Clockwork uses **cooperative scheduling**:

1. Tasks are queued with a due time (based on simulated clock)
2. The harness executes tasks one at a time in deterministic order
3. When no tasks are ready, time advances to the next scheduled task
4. This continues until a condition is met or the simulation is idle

```csharp
// Drive simulation until all nodes agree on membership
harness.RunUntil(() => nodes.All(n => n.MembershipSize == 5));

// Or run for a specific simulated duration
harness.RunForDuration(TimeSpan.FromMinutes(5));
```

## Requirements for the System Under Test

**Clockwork can only provide determinism if your system under test follows these rules.** Violating these rules will cause non-deterministic behavior or test failures.

### 1. Use Injected TimeProvider

Never use `DateTime.Now`, `DateTime.UtcNow`, or `DateTimeOffset.Now`. Instead, inject a `TimeProvider` and use it for all time operations:

```csharp
// BAD - bypasses simulation
var now = DateTime.UtcNow;
await Task.Delay(TimeSpan.FromSeconds(1));

// GOOD - uses simulation time
var now = _timeProvider.GetUtcNow();
using var timer = _timeProvider.CreateTimer(callback, null, dueTime, period);
```

### 2. Never Use ConfigureAwait(false)

`ConfigureAwait(false)` causes continuations to run on thread pool threads, escaping the simulation's `SynchronizationContext`. This breaks determinism.

```csharp
// BAD - escapes simulation context
var result = await SomeAsyncOperation().ConfigureAwait(true);

// GOOD - stays on simulation context
var result = await SomeAsyncOperation().ConfigureAwait(true);
// or simply:
var result = await SomeAsyncOperation();
```

**Note**: If you need `ConfigureAwait(false)` for production performance, consider using a compile-time flag or source generator to strip it during testing.

### 3. Respect TaskScheduler and SynchronizationContext

All async work must flow through the simulation's task scheduler. Code that explicitly uses `Task.Run()` or `ThreadPool.QueueUserWorkItem()` will escape the simulation.

```csharp
// BAD - escapes to thread pool
await Task.Run(() => DoWork());
ThreadPool.QueueUserWorkItem(_ => DoWork());

// GOOD - use the simulation's scheduler
var task = new Task(DoWork);
task.Start(_taskScheduler);  // Injected SimulationTaskScheduler
await task;

// Or use Task.Factory with the scheduler
await Task.Factory.StartNew(
    DoWork, 
    CancellationToken.None, 
    TaskCreationOptions.None, 
    _taskScheduler);
```

### 4. Never Use CancellationTokenSource.CancelAsync()

`CancelAsync()` posts the cancellation callback to thread pool threads, which escapes the simulation context.

```csharp
// BAD - callbacks escape to thread pool
await cts.CancelAsync();

// GOOD - synchronous cancellation stays in simulation
cts.Cancel();
```

### 5. Use Seeded Random

Never use `Random.Shared` or `new Random()`. Instead, use the injected `SimulationRandom` (or derive from the harness's random):

```csharp
// BAD - non-deterministic
var delay = Random.Shared.Next(100, 500);

// GOOD - deterministic
var delay = _random.Next(100, 500);  // Injected SimulationRandom
```

### 6. No Real I/O

All network communication must go through the simulated network. File I/O, database access, and other external dependencies must be mocked or abstracted.

### 7. Forward CancellationTokens

Always propagate cancellation tokens through async call chains. This allows the simulation to cleanly cancel in-flight operations during teardown.

```csharp
// GOOD - cancellation propagates correctly
public async Task DoWorkAsync(CancellationToken cancellationToken)
{
    await Step1Async(cancellationToken);
    await Step2Async(cancellationToken);
}
```

## Quick Start

```csharp
// Create a harness with a seed for reproducibility
await using var harness = new SimulationHarness(seed: 12345);

// Create nodes (your system under test)
var node1 = harness.CreateNode(...);
var node2 = harness.CreateNode(...);

// Run simulation until a condition is met
harness.RunUntil(() => node1.IsConnectedTo(node2));

// Inject faults
harness.Network.CreateBidirectionalPartition(node1.Address, node2.Address);
harness.RunForDuration(TimeSpan.FromSeconds(30));
harness.Network.HealBidirectionalPartition(node1.Address, node2.Address);

// Verify recovery
harness.RunUntil(() => node1.IsConnectedTo(node2));
```

## Core Components

### SimulationClock

Shared time source for all simulation components. Time only advances when the harness explicitly advances it - there are no real-time delays.

```csharp
var clock = harness.Clock;
var now = clock.UtcNow;
clock.Advance(TimeSpan.FromMinutes(5));
```

### SimulationTaskQueue

Time-aware priority queue that orders tasks by due time, then sequence number. Tasks with `DueTime <= Clock.UtcNow` are "ready" for execution.

```csharp
// Schedule work for later
taskQueue.EnqueueAfter(() => DoSomething(), TimeSpan.FromSeconds(10));

// Execute one ready task
taskQueue.RunOnce();

// Execute all ready tasks
taskQueue.RunUntilIdle();
```

### SimulationTaskScheduler

A `TaskScheduler` implementation that queues all tasks through the `SimulationTaskQueue`. Importantly, it **never executes tasks inline** - all tasks go through the queue for deterministic ordering.

### SimulationTimeProvider

A `TimeProvider` implementation for .NET 8+ that integrates with the simulation:

- `GetUtcNow()` returns the simulated time
- `CreateTimer()` returns timers that fire based on simulated time

### SimulationNetwork

Simulates network communication with support for:

- Message delays (configurable base delay + jitter)
- Random message drops
- Network partitions (unidirectional or bidirectional)
- Node isolation

```csharp
harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(5);
harness.Network.MaxJitter = TimeSpan.FromMilliseconds(10);
harness.Network.MessageDropRate = 0.01; // 1% drop rate

// Create partition
harness.Network.CreateBidirectionalPartition("node1", "node2");

// Isolate a node from all others
harness.Network.IsolateNode("node3");
```

### ChaosInjector

Automated fault injection for stress testing:

```csharp
var chaos = new ChaosInjector(harness);
chaos.NodeCrashRate = 0.001;      // 0.1% chance per step
chaos.PartitionRate = 0.005;      // 0.5% chance per step
chaos.MinimumAliveNodes = 3;      // Keep at least 3 nodes alive

// Run chaos for 1000 steps
var faultsInjected = chaos.RunChaos(steps: 1000, stepInterval: TimeSpan.FromMilliseconds(100));
```

### SingleThreadedGuard

Debug helper that detects accidental concurrent access. If async work escapes the simulation (e.g., due to `ConfigureAwait(false)`), the guard will throw with a helpful error message including the stack trace of the original owner.

## Debugging Tips

### Reproducing Failures

When a test fails, log the seed:

```csharp
harness.LogSeedForReproduction();
// Output: [SEED FOR REPRODUCTION] 12345
```

Re-run with the same seed to get identical behavior.

### Detecting Escaped Async Work

If you see `InvalidOperationException` from `SingleThreadedGuard`, it means async work escaped the simulation context. Common causes:

1. `ConfigureAwait(false)` somewhere in the call chain
2. `Task.Run()` or `ThreadPool.QueueUserWorkItem()`
3. `CancellationTokenSource.CancelAsync()`
4. Third-party libraries that don't respect `SynchronizationContext`

### Simulation Stuck?

If `RunUntil` returns `false` or throws `TimeoutException`:

1. Check if any nodes are suspended
2. Check for network partitions preventing progress
3. Increase `MaxSimulatedTimeAdvance` if the operation legitimately takes long
4. Check logs for deadlocks or infinite loops in your system

## Best Practices

1. **Use small seeds for development** - easier to remember and type
2. **Log seeds in CI** - always be able to reproduce failures
3. **Test with various seeds** - run the same test with multiple seeds in CI
4. **Start with simple scenarios** - get basic tests working before adding chaos
5. **Use assertions liberally** - the deterministic nature makes debugging easier
6. **Keep simulation time reasonable** - hours of simulated time is fine, but days might indicate a bug

## License

MIT License - see LICENSE file for details.
