# Fast CASPaxos — Rust Implementation

A Rust port of the [Fast CASPaxos](../README.md) distributed consensus protocol.

## Crate Structure

- **`fast-caspaxos-core`** — Pure protocol implementation (no I/O, no async)
  - `Ballot` — Epoch identifier (round, proposer)
  - `AcceptorEngine` — Acceptor state machine
  - `ProposerEngine` — Proposer state machine (core algorithm)
  - `OperationDriver` — Client-side work queue

- **`fast-caspaxos-simulation`** — Deterministic simulation harness
  - Synchronous `SimulationCluster` with step-based execution
  - Async `turmoil_sim` with turmoil-based deterministic networking (feature-gated)
  - Network simulation with partitions, delays, drops
  - Host adapters bridging engines to simulated transport
  - Safety monitoring and invariant checking
  - Chaos testing harness for automated fault injection
  - Porcupine history export for linearizability verification
  - Structured tracing/diagnostics

- **`fast-caspaxos-scenarios`** — Test scenarios and batch execution

## Design

### Handler Trait Pattern (Zero-Cost Callbacks)

Instead of allocating vectors of effects, protocol engines call methods on a
generic handler trait. This is the Rust equivalent of C#'s abstract class pattern:

```rust
// The host implements the handler trait
impl ProposerHandler<V, A> for MyHost {
    fn send_prepare(&mut self, request: PrepareRequest) { /* route to network */ }
    fn send_accept(&mut self, request: AcceptRequest<V>) { /* route to network */ }
    // ...
}

// Engine calls handler directly — zero allocations, inlined
engine.handle_prepare_promise(&mut my_host, message);
```

### Determinism

- `BTreeMap`/`BTreeSet` everywhere (no `HashMap`)
- Seeded RNG for all randomness
- Synchronous core engines (no async in protocol logic)

## Building & Testing

```bash
# Build
cargo build --workspace

# Run all tests (unit + property + simulation + chaos)
cargo test --workspace

# Run turmoil DST tests (async deterministic simulation)
cargo test -p fast-caspaxos-simulation --features turmoil

# Run benchmarks
cargo bench -p fast-caspaxos-core

# Clippy (zero warnings)
cargo clippy --workspace
```

## Test Coverage

- **Unit tests** — Edge cases for all protocol engines
- **Property tests** (proptest) — Ballot ordering, quorum intersection, state machine invariants
- **Simulation tests** — Full cluster with fault injection (partitions, crashes, drops)
- **Concurrent scenarios** — Multiple proposals in-flight with conflict resolution
- **Chaos tests** — Randomized fault injection across 50+ seeds per mode
- **Turmoil DST** — Async deterministic simulation with simulated TCP networking
- **Linearizability tests** — Safety monitor verifying commit consistency
- **Porcupine history** — Export for external linearizability checking
- **Mode matrix** — All protocol configurations tested (classic, fast, leader, fast+leader)
