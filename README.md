# Fast CASPaxos

> **TL;DR:** Fast CASPaxos applies the Fast Paxos optimization to CASPaxos so that uncontended writes can commit in 1 round-trip (RTT) without logs, while still allowing a proposer to switch back to leadered recovery when conflicts show up. It still needs only two pieces of metadata per replicated value (16 to 32 bytes).

## Introduction

I've written about [CASPaxos](https://reubenbond.github.io/posts/caspaxos) before, so go there for an intro. In short, it's a very simple distributed consensus algorithm that requires much less code and infrastructure than most while still being optimal. Learning CASPaxos is a great way to understand the fundamentals of distributed consensus. Like most distributed consensus protocols, CASPaxos is _leadered_ under the definition I'm using here, and there are scenarios where _leaderless_ protocols are superior. This repository explores my experiment to bring leaderless consensus to CASPaxos to achieve the best of both worlds. I'm calling the resulting algorithm **Fast CASPaxos**. It's a blend of **Fast Paxos** and **CASPaxos**. I also want to give readers an intuition for the difference between _leadered_ and _leaderless_ protocols, and where each might be better.

## Leader-based vs leaderless consensus

Classic Paxos, Multi-Paxos, Raft, and ordinary CASPaxos are all examples of leader-based consensus. In leader-based consensus, before a proposer can have a value committed, it must first obtain mutually exclusive (but revocable) rights to have a value committed. Leader election grants this right. In Paxos, the `prepare` phase is where a proposer tries to acquire that right and the `accept` phase is where it tries to commit a value. If leadership is uncontended then each phase typically takes 1 round-trip (one message back and forth between the proposer and all acceptors). Multi-Paxos, Raft, and CASPaxos let a proposer amortize the cost of becoming a leader by allowing a leader to continue committing values as long as it is uncontended. The key point is that leadership is about mutual exclusion: only one proposer at a time has the right to commit values. For CASPaxos, the gist of how this works is that every time you commit a value you piggyback leader election for the _next_ value in the same message: each `accept` request has the next `prepare` piggy-backed on.

Leadered and leaderless consensus both have a 1 RTT fast case, but they optimize for different failure and contention patterns. The interesting part is choosing when each fast path is worth the trade-off.

Leadered consensus lets the leader commit in 1 RTT. If another proposer wants to commit, it must first become the leader (1 RTT) and then commit. That's 2 RTT in total if you were to have a different proposer for each commit. If proposers are trying to commit values concurrently, they can end up contending, possibly indefinitely, trying to squeeze those 2 RTT in before the other proposer deposes them as leader. This is the _dueling proposers_ problem.

Leaderless consensus algorithms allow any proposer to commit a value without first obtaining exclusive rights. This is the crux of the Fast Paxos optimization: acceptors are prepared ahead of time for a shared fast round, so proposers can send candidate values directly to acceptors instead of first electing a leader. If enough acceptors receive the same value, it commits in 1 RTT. If concurrent proposers propose conflicting values, the protocol falls back to a leadered recovery round to reconcile them. So Fast Paxos is leaderless in fast rounds, but leadered in recovery. The catch is that this leaderless fast path needs larger quorums than ordinary leadered consensus.

An intuition for larger quorums in fast rounds is that the real challenge is not just getting a value accepted quickly. It is making sure that any later classic recovery round can still tell which fast-round value might have been committed. In a classic round there is only one proposer, so the ballot uniquely identifies a single candidate value. In a fast ballot, many proposers can race in the same ballot, so different acceptors may hold different values for that one ballot. That is why recovery has to tally up fast-round votes by value: when a proposer runs `prepare` and sees accepted entries from a fast ballot, it must group responses by the value reported for that ballot and tally how many acceptors reported each one. The fast quorum has to be large enough that any later classic quorum is forced to see the fast winner as the dominant value in its responses.

Aside: why fast rounds need a larger quorum

```text
5 acceptors
classic quorum = 3
fast quorum    = 4

If fast quorum were only 3, this could happen:

acceptor:         A1  A2  A3  A4  A5
accepted value:    X   X   X   Y   Y

Later, a classic recovery quorum might read A3, A4, A5:

responses seen:            X   Y   Y

Recovery sees 2 votes for Y and only 1 for X,
so it could recover Y even though X reached more acceptors overall.

Requiring 4-of-5 for the fast-round winner prevents that:

acceptor:         A1  A2  A3  A4  A5
accepted value:    X   X   X   X   .

Any 3-of-5 classic quorum must see at least 2 copies of X,
leaving at most 1 slot for a competing value.
```

A fast quorum is a supermajority so that any later classic recovery quorum is forced to observe the fast-round winner often enough to preserve safety.

That is why fast rounds need a supermajority: not because the fast proposer needs extra votes for its own sake, but because any later classic recovery quorum must be _unable_ to reinterpret the round as supporting a different value. Leaderless consensus is attractive in many scenarios, but those larger quorums and the risk of conflicts mean it generally performs worse than leadered consensus under contention. Since the trade-offs vary by scenario, and can vary over time in the same system, the ideal solution would adapt between leaderless and leadered operation depending on the current situation.

## When each mode fits best

### Leadered

- **High write rate.** If one proposer is likely to drive a long sequence of updates, leadered mode amortizes prepare costs across many operations instead of paying them repeatedly.
- **High conflict rate.** When independent proposers frequently want different next values, a leader reduces repeated proposer-versus-proposer collisions and gives more stable progress than optimistic fast rounds. To take advantage of this, you need to route requests to a stable leader.

### Leaderless

- **Geo-distributed, low-conflict deployments.** When network latency is high, leaderless 1 RTT commits are particularly attractive, but the penalty of contention is more pronounced.
- **Infrequent writes.** When writes are infrequent and proposers have a chance to learn the latest committed value without going through consensus, fast rounds allow 1 RTT commits at any proposer.
- **Everywhere agreement.** When every client and proposer is trying to advance the register to the same next value, shared fast rounds let them proceed without dueling. The [Rapid cluster membership algorithm](https://www.usenix.org/conference/atc18/presentation/suresh) uses Fast Paxos for exactly this property: Rapid's cut detector produces proposals based on observer alerts and delays action until churn stabilizes into the same multi-process cut, resulting in very low conflict rates among proposers, allowing them to commit in 1 RTT most of the time without going through a leader.
- **Register initialization.** The initial fast ballot is implicitly prepared, so the first write can skip `prepare` entirely and go straight to `accept`. That makes 1 RTT initialization especially attractive when a system creates many lightweight registers which may only be written once.

## When it's not clear-cut

I've seen arguments saying that the smaller quorum requirements of leadered protocols confer advantages like:

- Reduced chance that a straggler replica will slow down consensus
- Improved failure masking (more replicas can fail before progress stops)

Those are not unreasonable ideas, but in a leadered protocol, the leader itself can be the straggler and slow down all requests (a gray failure). If the leader crashes you need to first detect that, which is inherently slow, and then elect a new leader. That handoff naturally creates a stutter where progress pauses while the system re-establishes mutual exclusion. The system becomes temporarily unavailable during this period. Leaderless algorithms don't have these problems. Given that, these attributes are not clear-cut.

## Fast CASPaxos

Fast CASPaxos mixes the Fast Paxos fast-round optimization with CASPaxos to implement a replicated, rewritable, linearizable register which supports leaderless 1 RTT commits.

The CASPaxos paper gives the baseline algorithm and its piggybacked-`prepare` 1 RTT optimization. Fast CASPaxos keeps that structure, but lets the piggybacked ballot be either the next shared fast ballot or the next proposer-owned classic ballot. Ballots are represented as tuples of the form `(round, proposerId)`, and `proposerId = 0` is reserved to denote a shared round. Therefore, the piggybacked ballot is either `(r + 1, proposerId)` for a classic round or `(r + 1, 0)` for a fast round. In a fast round, _any_ proposer may have its value accepted by an acceptor, whereas in a classic round only the proposer that owns the ballot may do so.

In addition to reserving `proposerId = 0`, the Fast Paxos vote tallying rules described above are adopted.

That's all that is needed to turn CASPaxos into Fast CASPaxos.

I've also added two further optimizations described below, but they are not fundamental.

### Concurrently committing identical values

To allow multiple proposers to concurrently commit identical values without conflicting, we modify acceptors to reply with an acknowledgement when they receive a request matching their current accepted ballot and value exactly. Of course, this only works in fast rounds, where multiple proposers use the same ballot. In non-fast rounds, this same functionality serves to make `accept` retries idempotent, so it has an extra added benefit.

### Register initialization

To allow proposers to initialize a register in 1 RTT, they start with the `accept` phase for the initial fast round, `(1, 0)`. This way, either they can have their initial value committed if there is no contention, or they will learn of the conflicting ballot and can retry at the `prepare` phase. Acceptors are initialized with a promised ballot of `(1, 0)`.

## The protocol

This version is intentionally simplified to show the core protocol only. It omits implementation details like duplicate-message handling, cached local views, and early-stop bookkeeping.

Notation:

- ballots are `(round, proposerId)`, where `proposerId = 0` denotes the shared fast ballot for that round
- `classicQuorum = floor(N / 2) + 1`
- `fastQuorum = ceil(3N / 4)`
- `quorumFor(ballot) = fastQuorum` for fast ballots and `classicQuorum` for classic ballots
- every `prepare` response returns `(acceptedBallot, acceptedValue, maxBallot)`
- `prepare(b)` succeeds at an acceptor exactly when the response reports `maxBallot = b`
- acceptors start with `promised = (1, 0)`, so the first fast round is implicitly prepared

### Proposer

```text
function propose(update):
    b := initial ballot to try

    loop:
        if b is not already prepared:
            responses := collect Prepare(b) responses
            if responses with maxBallot == b do not reach quorumFor(b):
                b := next classic ballot above the highest maxBallot returned
                continue

            base := recoverValue(responses)
            if base is conflict(highestFastBallot):
                b := next classic ballot above highestFastBallot
                continue
        else:
            base := previously recovered value

        newValue := update(base)
        next := chooseNextBallotToPrepare(b)

        responses := collect Accept(b, newValue, next) responses
        if accepts in responses reach quorumFor(b):
            return newValue

        b := next classic ballot above the highest conflicting ballot returned

function recoverValue(responses):
    highest := maximum acceptedBallot reported in the responses

    if highest == ⊥:
        return ⊥

    if highest is a classic ballot:
        return the value paired with highest

    // highest is a fast ballot, so several values may appear at that ballot
    votes := count values among responses whose acceptedBallot == highest
    if some value has a unique largest count:
        return that value

    return conflict(highest)

function chooseNextBallotToPrepare(b):
    if b is a fast ballot:
        return (b.round + 1, 0)          // keep the next round leaderless

    if distinguishedLeader mode is enabled:
        return (b.round + 1, self)       // keep the next round leader-owned

    return none
```

Notes:

- The initial fast ballot `(1, 0)` is implicitly prepared at acceptors, so the first fast round can skip a standalone `prepare`.
- In practice, a proposer only skips `prepare` when it already has a prepared ballot and enough local knowledge to compute `newValue`.
- Reads and inapplicable updates establish the recovered value through an identity `accept` quorum before replying. This preserves the value and its logical version while ensuring that later operations recover it. Reusing a piggybacked prepared ballot still requires an `accept` quorum; a superseding ballot forces fresh prepare/recovery.

### Acceptor

```text
state:
    promisedBallot := (1, 0)       // initial fast ballot is implicitly prepared
    acceptedBallot := ⊥
    acceptedValue := ⊥

function onPrepare(b):
    maxBallot := max(promisedBallot, acceptedBallot)
    if b >= maxBallot:
        promisedBallot := b
        maxBallot := b
    reply prepareResponse(acceptedBallot, acceptedValue, maxBallot)

function onAccept(b, v, next):
    // Optimization: idempotency from possibly multiple proposers
    if b == acceptedBallot and v == acceptedValue:
        reply accept(promisedBallot)
        return

    maxBallot := max(promisedBallot, acceptedBallot)
    if b < maxBallot:
        reply reject(maxBallot)
        return

    if b == acceptedBallot and v != acceptedValue:
        reply reject(maxBallot)
        return

    acceptedBallot := b
    acceptedValue := v

    if next != none:
        promisedBallot := max(promisedBallot, next)

    reply accept(promisedBallot)
```

## Conclusion

Fast CASPaxos is a tidy modification to CASPaxos which is a tidy modification to single-decree Paxos. Piggybacking `prepare` onto `accept` already gives us distinguished-proposer mode. Using the same trick to prepare a shared fast ballot (and adopting Fast Paxos vote tallying and value selection rules) gives us leaderless mode too. All in all, this lets the same implementation tune for different scenarios on a whim, and there are many cases where leaderless consensus is superior to leadered consensus, as discussed above.

This repository includes TLA+ and [Quint models](quint/README.md), plus a deterministic simulation suite with fault injection and a linearizability checker. The Quint model checks fast/classic agreement, recovery, piggybacked promises, and committed-value ancestry, with reproducible scenarios and finite-state safety verification in GitHub Actions. The repo-specific details are below.

## C# durable keyed acceptors

`FastCASPaxos.Host` serves both the protocol and replicated register APIs over
gRPC, backed by SQLite and a configured fixed peer set. The gRPC package includes
remote adapters for both interfaces. See the [gRPC server guide](csharp/src/FastCASPaxos.Grpc/README.md)
for executable setup, a three-process example, client usage, and operational
configuration.

`ReplicatedRegisterService` provides quorum-backed `Read`, `CompareExchange`,
and paginated logical key listing over those acceptors. It uses stable opaque
versions, durable proposer ballot allocation, and explicit uncertain write
outcomes. See the [replicated register guide](csharp/src/FastCASPaxos.Server/README.md)
for API usage and deployment guarantees.

The C# implementation includes a transport-independent keyed protocol API and a
SQLite-backed server for `Prepare`, `Accept`, and paginated local key inventory.
The server persists promises and accepted values in WAL transactions and releases
responses after durable commit. See the [SQLite acceptor guide](csharp/src/FastCASPaxos.Sqlite/README.md)
for the public contracts, lifecycle, and an example.

## Testing, etc

This repository centers on a deterministic simulation suite built on a DST harness that I've called `Clockwork`. The simulation outputs traces that can be checked by Porcupine for linearizability violations. The repo also includes TLA+ models under the 'tla' directory that I've generated with copious amounts of assistance from coding agents.

Use the helper scripts to run the sim suite:

```powershell
.\run_simulation.cmd --scenario all --seed 7001 --rounds 2
.\run_simulation_suite.ps1 -Scenario string-corpus,seeded-transient-fault-mix-fast -Seed 7001 -Rounds 3
```

Named wrapper scripts are under `scripts\`:

| Script | Scenario |
|---|---|
| `scripts\run_string.cmd` | `string-corpus` (1000 rounds) |
| `scripts\run_string_once.cmd` | `string-corpus` (1 round) |
| `scripts\run_string_long.cmd` | `string-corpus` (5000 rounds) |
| `scripts\run_set.cmd` | `set-corpus` (1000 rounds) |
| `scripts\run_randomstring.cmd` | `random-string-corpus` (10000 rounds) |
| `scripts\run_forkingstring.cmd` | `forking-string-corpus` (10000 rounds) |
| `scripts\run_parallel_forkingstring.ps1` | `forking-string-corpus` in parallel jobs |
| `scripts\build_debug.cmd` | Debug build of the runner project |

All `scripts\` wrappers can be run from any working directory.

The runner also supports scenario-specific parameters via repeated `--param key=value`
arguments, plus convenience flags for the parameterized append-sequence scenario:
`--proposer-count`, `--acceptor-count`, `--value-count`, `--conflict-rate`,
`--conflict-fanout`, and `--mode`. Use `--wait-for-debugger` to have
the runner print its PID and pause until a debugger attaches before scenario execution
starts. Independent round/scenario combinations run in parallel by default, using
the built-in `Parallel` scheduler limit (roughly one worker per core), so stress
sweeps scale across available CPUs without extra flags.

The runner writes artifacts under `artifacts\simulation-runner\` by default:

- `summary.txt` - success/failure summary and reproduction hint
- `trace.log` - deterministic scenario-level trace
- `logs.txt` - buffered runtime/network/proposer logs
- `stats.txt` - collected counters and run metadata
- `porcupine-history.json` - structured call/return history for Porcupine linearizability checks
- `batch-summary.txt` - aggregate counters and derived rates across all executed runs

To validate the emitted histories with Porcupine,
run the dedicated script, which executes the simulator and then runs the Go checker from source:

```powershell
.\scripts\validate_porcupine.ps1
```

## Benchmarking

The `benchmark-workload` scenario runs a sustained workload and emits per-proposal latency data as CSV, suitable for graphing.

By default, the workload issues one synthetic request stream per proposer. Set `client-ordinal` to drive the workload from a single explicit benchmark client at that topology ordinal; non-leader modes target the nearest healthy proposer while leader mode still targets the current distinguished primary. Proposers that commit broadcast the accepted ballot, accepted value, and prepared ballot to peer proposers so later requests can start from shared state.

When `agreement-mode=true`, the workload models one benchmark client per proposer geography. Fast mode fans a logical request to all proposers, classic mode targets the nearest healthy proposer, and leader mode targets the current leader and updates that choice after failover.

### Quick start

Run a single benchmark:

```powershell
.\run_simulation_suite.ps1 -Scenario benchmark-workload -Seed 42 -Rounds 1 `
    -ScenarioParam 'proposal-count=200','mode=fast','link-latency-profile=lan'
```

### Benchmark sweep

`run_benchmark_suite.ps1` sweeps across protocol modes × topologies × faults × proposer counts:

```powershell
# Full sweep (default: 4 modes × 3 topologies × 2 faults × 2 proposer counts = 48 runs)
.\run_benchmark_suite.ps1 -ProposalCount 1000

# Smaller sweep
.\run_benchmark_suite.ps1 -Mode classic,fast -Topology lan,geo5region -ProposerCount 1 -ProposalCount 200

# Dry run to see what would execute
.\run_benchmark_suite.ps1 -DryRun
```

### Generating charts

After running the benchmark suite, generate charts with the Python script:

```bash
uv run scripts/plot_benchmarks.py --input artifacts/benchmark
```

This produces:
- **Latency time-series** — commit latency vs proposal index, one line per protocol mode
- **Throughput** — proposals/second over simulated time (sliding window)
- **Latency CDF** — cumulative distribution comparison across modes
- **Box plot comparison** — latency distribution across topologies

Charts are written to `artifacts/benchmark/charts/` as PNG (matplotlib) and interactive HTML (plotly).

### Benchmark parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `proposer-count` | 3 | Number of proposers |
| `acceptor-count` | 5 | Number of acceptors (minimum 5 for `benchmark-workload`) |
| `proposal-count` | 1000 | Total proposals to run |
| `mode` | classic | Protocol mode: `classic`, `fast`, `leader`, `fast+leader`, or `adaptive` |
| `agreement-mode` | false | Simulate everywhere agreement on the next value across geo-aware benchmark clients |
| `client-ordinal` | auto | Use one explicit benchmark client at this topology ordinal instead of the default synthetic per-proposer workload |
| `base-delay-ms` | 1 | Global base message delay |
| `max-jitter-ms` | 1 | Global max random jitter |
| `link-latency-profile` | lan | Topology: `lan`, `us3region`, `geo5region` |
| `fault-schedule` | none | Fault injection DSL (see below) |

### Topology profiles

- **`lan`** — Same datacenter, 1ms base + 1ms jitter
- **`us3region`** — `East US 2`, `West US`, `West US 3`, with 20-66ms RTT between regions
- **`geo5region`** — `East US 2`, `France Central`, `Southeast Asia`, `Brazil South`, `Australia Southeast`, with 85-332ms RTT between regions
- Legacy aliases `uscrossregion` and `geo3region` still parse for compatibility

### Fault schedule DSL

Inject faults at specific proposal indices:

```text
suspend:acceptor:3:at:333:until:666
partition:proposer:1:acceptor:2:at:100:until:200
crash:acceptor:1:at:50:restart:80
```

Multiple faults can be comma-separated.
