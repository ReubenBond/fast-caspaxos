# Fast CASPaxos — Rust Implementation

A Rust port of the [Fast CASPaxos](../README.md) distributed consensus protocol.

## Crate Structure

- **`fast-caspaxos-core`** — Pure protocol implementation (no I/O, no async)
  - `Ballot` — Epoch identifier (round, proposer)
  - `AcceptorEngine` — Acceptor state machine
  - `ProposerEngine` — Proposer state machine (core algorithm)
  - `OperationDriver` — Client-side work queue

- **`fast-caspaxos-simulation`** — Deterministic simulation harness
  - Network simulation with partitions, delays, drops
  - Host adapters bridging engines to simulated transport
  - `SimulationCluster` orchestrator
  - Safety monitoring and invariant checking

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

- `BTreeMap`/`BTreeSet` everywhere (no HashMap)
- Seeded RNG for all randomness
- Synchronous core engines (no async in protocol logic)

## Building & Testing

```bash
# Build
cargo build --workspace

# Run all tests (unit + property + simulation)
cargo test --workspace

# Run benchmarks
cargo bench -p fast-caspaxos-core

# Clippy
cargo clippy --workspace
```

## Test Coverage

- **Unit tests** — Edge cases for all protocol engines
- **Property tests** (proptest) — Ballot ordering, quorum intersection, state machine invariants
- **Simulation tests** — Full cluster with fault injection (partitions, crashes, drops)
- **Linearizability tests** — Safety monitor verifying commit consistency
- **Mode matrix** — All protocol configurations tested (classic, fast, leader, fast+leader)
