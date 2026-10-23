# Shared deterministic register fixture

`FastCASPaxos.Testing` is a framework-neutral `net10.0` library for both upstream
tests and downstream `IRegisterService` consumers (including Clog adapters).
Its library closure contains **Simulation** and **Server**, transitively
**Clockwork** and **Core**. Consumers supply their own assertion framework.

```csharp
using FastCASPaxos.Server;
using FastCASPaxos.Testing;

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
  the existing Clockwork synchronization context and drives its queue to scenario
  completion. Async requests overlap through queued protocol operations.
  Assertion/operation exceptions propagate to the caller.
  Stalled/escaped work fails boundedly with seed and runtime diagnostics.
- `CreateClient(int? node = null)` returns `IRegisterService`, pinned to the
  **1-based** member (`null` means member 1). A shared sticky client uses the
  actual proposer's existing request queue: 32 healthy concurrent creates or
  same-version/same-byte updates yield one Applied and 31 Mismatch.
  Explicit clients on different members retain real speculative contention:
  losing proposers can correctly return Unknown rather than inventing a
  definitive mismatch.
- `DelayAsync(TimeSpan)` and `TimeProvider` use the **same** Clockwork clock and
  scheduler as the protocol. `OperationTimeout { get; init; }` defaults to two
  virtual seconds. Configure it before `Run`.
- `CrashNode(int)` and `RestartNode(int)` affect both proposer and acceptor
  roles. Restart preserves the existing durable acceptor store and discards
  volatile proposer state; client version allocation never resets.
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
- Dispose outside `Run` using `await using`, after awaiting every scenario task.

Consumer awaits must capture the simulation context. Use fixture operations
and virtual-time delays throughout the simulated path. `Task.Run`, wall-clock
delays, external I/O, and `ConfigureAwait(false)` escape this scheduling boundary;
`Run` reports stalled or escaped work with reproduction diagnostics.

## Evidence boundaries

There is exactly one existing `FastCasSimulationCluster` with actual
`FastCasConsensusRegister`, proposer/acceptor hosts, protocol engines, durable
in-memory stores, transport, Clockwork clock/network/scheduler, and runtime
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
`tests/FastCASPaxos.Simulation.Tests/RegisterSimulationClusterTests.cs`.
Downstream tests establish their application's invariants using this fixture.
