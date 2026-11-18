# Shared deterministic register fixture

`Orleans.Consensus.CasPaxos.Testing` is a framework-neutral `net11.0` library for both upstream
tests and downstream `IRegisterService` consumers (including Clog adapters).
Its library closure contains **Simulation** and **Server**, transitively
**Core** and Clockwork's simulation kernel, `Microsoft.Orleans.Testing.Simulation`
(see [Clockwork source build](#clockwork-source-build)). Consumers supply their own
assertion framework.

```csharp
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Testing;

await using var cluster = new RegisterSimulationCluster(seed: 42);
cluster.Run(async simulation =>
{
    IRegisterService client = simulation.CreateClient();
    var created = await client.CompareExchangeAsync("key", null, new ByteString([1]));
    simulation.CrashNode(4);
    simulation.CrashNode(5);
    var updated = await client.CompareExchangeAsync("key", created.Current!.Version, new ByteString([2]));
    // A Clog adapter can consume `client` here. Its domain oracles stay downstream.
});
```

## Execution and faults

- `RegisterSimulationCluster(int seed, int nodeCount = 5)` starts at the Unix
  epoch in **Fast** mode. Five members require four fast acknowledgements, but
  classic recovery still progresses with three members.
- `Run(Func<RegisterSimulationCluster, Task>, int maxIterations = 100000)` installs
  the cluster's Clockwork synchronization context and drives its scheduler to scenario
  completion. The calling thread joins the simulation's logical thread for the run;
  simulated work runs one operation at a time on it. Async requests overlap through
  queued protocol operations. Assertion/operation exceptions propagate to the caller.
  Stalled/escaped work fails boundedly with seed and runtime diagnostics.
- `CreateClient(int? node = null)` returns `IRegisterService`, pinned to the
  **1-based** member (`null` means member 1). A shared sticky client uses the
  actual proposer's existing request queue: 32 healthy concurrent creates or
  same-version/same-byte updates yield one Applied and 31 Mismatch.
  Explicit clients on different members exercise real speculative contention.
  Recovery of the winner at the proposed revision proves the other calls
  mismatched; a higher revision or a lost result can preserve uncertainty.
- `DelayAsync(TimeSpan)` and `TimeProvider` use the **same** Clockwork virtual clock and
  scheduler as the protocol. `OperationTimeout { get; init; }` defaults to two
  virtual seconds. Configure it before `Run`.
- `CrashNode(int)` and `RestartNode(int)` affect both proposer and acceptor
  roles. Restart preserves the existing durable acceptor store and discards
  volatile proposer state. Per-key revisions live in the durable application
  map and increment on successful conditional transitions. Distinct request IDs
  identify competing writes and remain unique across member restarts.
- `Partition(int, int)` blocks both directions between both roles of physical
  members. `SetLinkDelay(int, int, TimeSpan)` configures their bidirectional
  protocol links without jitter. Other links use seeded 1–3 ms delays.
- `SetReplyLoss(int node, bool enabled)` drops **client replies** while protocol
  acknowledgements and incoming requests continue. `SetReplyDelay(int node,
  TimeSpan)` sets a fixed client reply delay. Both apply to future clients too.
  Already sent messages retain the delivery decision/delay made at send time.
- `Heal()` removes all partitions, including reply loss, while retaining member
  liveness and latency overrides.
- `Seed` and read-only `Trace` expose the reproducible seed, virtual timestamps,
  request IDs, transport message kinds/statuses, committed ballot modes, and
  fault controls. Retain the scenario together with its seed for replay.
- `IsSimulationThread` reports whether the calling thread is the simulation's
  logical thread: an operation of this cluster's scheduler, or the thread inside `Run`.
- Dispose outside `Run` using `await using`, after awaiting every scenario task.
  Disposal stops every member and discards its pending work, including messages
  still in flight to it, without advancing virtual time. Nothing after the
  scenario reaches the protocol or the trace, and an operation or delay the
  scenario abandoned stays incomplete.

### Production adapters and `ConfigureAwait(false)`

**Production wrappers may keep `ConfigureAwait(false)` on `IRegisterService`
operations**, including layered async `ValueTask`/Task-backed wrappers:

```csharp
async ValueTask<RegisterValue> ReadForApplicationAsync(IRegisterService service, string key)
    => await service.ReadAsync(key).ConfigureAwait(false);

cluster.Run(async simulation =>
{
    // Scenario awaits retain Clockwork's context.
    var value = await ReadForApplicationAsync(simulation.CreateClient(), "key");
    await simulation.DelayAsync(TimeSpan.FromMilliseconds(10));
});
```

The shared fixture returns a private `IValueTaskSource` bridge. Internal protocol
awaits always capture Clockwork. A context-free consumer continuation runs inline
on the simulation's logical thread with `SynchronizationContext.Current` temporarily
null. This also allows subsequent Task-backed wrapper completions to inline their
context-free continuations instead of dispatching to the thread pool. Register
calls made by those continuations re-enter Clockwork internally; ordinary scenario
awaits are posted back to its context. ExecutionContext/AsyncLocal values and
cancellation flow normally. There is no wall-clock wait or thread-pool pump.

Keep the **test scenario's** awaits context-capturing, including virtual delays.
The bridge covers register-operation completions, not arbitrary asynchronous
dependencies. `Task.Run`, wall-clock delays, external I/O, or context-free awaits
on unrelated Task-based primitives are not made deterministic by supplying a
TimeProvider. Use deterministic substitutes for those separate dependencies.
`Run` still reports stalled or escaped work with reproduction diagnostics.

`ContextFreeConsumerWrappersCompleteOnSimulationThreadAndReplayFaultTrace`
exercises direct and layered false-await wrappers, eight overlapping read/CAS
pairs, delayed replies, two crashed/restarted members, lost replies, timeout and
healing. Six runs assert identical protocol and consumer-continuation traces,
execution on the simulation's logical thread, null consumer contexts, and restored
scenario context.
`ContextFreeConsumerWrappersPreserveCancellationAndExecutionContext` additionally
checks cancellation tokens, independent AsyncLocal values and inventory reads.

## Evidence boundaries

There is exactly one existing `FastCasSimulationCluster` with actual
`FastCasConsensusRegister`, proposer/acceptor hosts, protocol engines, durable
in-memory stores, transport, Clockwork scheduler/virtual time/network, and runtime
safety monitors. The adapter's `RegisterState` is an immutable ordinal-key application map;
conditional operations run through the protocol and identity reads establish
their result through an accept quorum.

The map **serializes keys in one consensus register**. It establishes ordinal
key isolation, opaque fresh versions, conditional writes, and ordered paging.
Paging observes one snapshot, which is stronger than the production per-key
scan guarantee. Production per-key throughput/caches, quorum inventory enumeration
during concurrent insertion, SQLite durability, gRPC transport, server admission,
and multi-endpoint behavior have separate integration coverage.

A missing reply after a dispatched candidate is **Unknown**, including when a
different write supersedes it. A lost mismatch reply from an operation that
proposed only an identity update is an unavailable operation. A received successful reply
retains its confirmed value/version even if newer writes occurred in transit.
Pre-cancellation fails before dispatch. Queued requests check expiry before
proposing a write after healing; already sent accepts may still commit.

Upstream behavioral evidence lives in
`tests/Orleans.Consensus.CasPaxos.Simulation.Tests/RegisterSimulationClusterTests.cs`.
Downstream tests establish their application's invariants using this fixture.

## Clockwork source build

The simulation projects reference Clockwork's `Microsoft.Orleans.Testing.Simulation`
packages by package ID: the kernel, and in the scenario library the framework-neutral
`Microsoft.Orleans.Testing.Simulation.Testing` for in-memory log capture. No feed
publishes them yet, so `csharp/eng/OrleansSimulation.targets`
builds every package in Clockwork's source-build map
(`eng/Microsoft.Orleans.Testing.Simulation.SourceBuild.props`) from the Clockwork checkout
which `OrleansSimulationSourceRoot` names, and restores none. By default that is this
repository's `vendor/Clockwork` submodule; initialize it with
`git submodule update --init vendor/Clockwork`. Clockwork runs this fixture in
cooperative mode: the simulation routes time, scheduling, randomness, and the network
through Clockwork's APIs, and nothing is rewritten.

A repository which builds this one from source and has its own Clockwork checkout,
such as Clog, names that checkout so the build has one simulation kernel: it places
`Orleans.Consensus.CasPaxos.Embedding.props` in the directory which contains this
repository's checkout, setting `OrleansSimulationSourceRoot` to an absolute path:

```xml
<Project>
  <PropertyGroup>
    <OrleansSimulationSourceRoot>$(MSBuildThisFileDirectory)Clockwork/</OrleansSimulationSourceRoot>
  </PropertyGroup>
</Project>
```

That file is the only one this repository's build imports from outside its own
boundary. `csharp/scripts/Test-SourceBoundary.ps1` builds this fixture inside a
hostile embedding repository with its own Clockwork, and checks that the fixture's
closure resolves exactly that one simulation kernel.

## Fatal storage failures

`RecordingFatalErrorHandler` implements `IFatalErrorHandler` for tests which open
SQLite storage directly. It records each reported local storage failure where a
host's handler stops the process; the reporting component fails its operation.
