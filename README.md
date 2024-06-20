# Fast CASPaxos

TL;DR: we can apply the Fast Paxos optimization to CASPaxos to accomplish 1 RTT leaderless consensus without the need for state machine replication via logs: just 2 extra pieces of metadata per replicated value (16 to 32 bytes). We can switch between leadered & leaderless mode at runtime.

## Introduction

Cloud services build reliable systems from unreliable components. Hosts crash, hard disks stop working, networks faulter, but reliable cloud services keep running, hopefully without you noticing. Distributed consensus is at the core of making that work.

Distributed consensus is the problem of getting a collection of hosts to agree on the same result even when messages can be delayed, dropped, or reordered and some hosts may fail. Consensus algorithms must satisfy two core goals:

- **Safety:** different participants do not decide different outcomes.
- **Liveness:** when enough of the system is healthy, some outcome eventually gets chosen.

Distributed consensus is extremely common, almost universally used, in distributed database systems. It's used for replicating state across hosts so that the system keeps serving requests even if some number of hosts fail.

[Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) solves this for a single write-once register. A **proposer** tries to get a value chosen by a quorum of **acceptors**, and **learners** observe the chosen result. In classic Paxos, a proposer first runs `prepare` to claim a ballot and discover any earlier accepted value, then runs `accept` to try to have one value chosen for that ballot. Once a quorum of acceptors accepts a value, that Paxos instance has chosen its one immutable result. I depicted the happy path below. [Wikipedia](https://en.wikipedia.org/wiki/Paxos_(computer_science)) depicts other scenarios. I omitted the learners, since in practice the proposer does double-duty as a learner.

```text
  Proposer     Acceptor 1  Acceptor 2  Acceptor 3
         │              │           │           │
phase 1: ├─prepare(n)──>│           │           │
         ├─prepare(n)───┼──────────>│           │
         ├─prepare(n)───┼───────────┼──────────>│
         │<─────promise─┤           │           │
         │<─────────────┼───promise─┤           │
         │<─────────────┼───────────┼───promise─┤
phase 2: ├─accept(n,v)─>│           │           │
         ├─accept(n,v)──┼──────────>│           │
         ├─accept(n,v)──┼───────────┼──────────>│
         │<────accepted─┤           │           │
         │<─────────────┼──accepted─┤           │
         │<─────────────┼───────────┼──accepted─┤
```

**Raft & Multi-Paxos** apply the same idea to state machine replication: conceptually, using many Paxos instances chained together, one for each slot in a replicated log. A stable leader amortizes the cost of `prepare` across many log entries, so the system can keep appending commands without paying for a full leadership handoff on every slot.

**CASPaxos** takes a different approach. Instead of agreeing on an append-only log, it agrees on the next value of a single rewritable register. `prepare` recovers the latest accepted value, the proposer applies its update function locally, and `accept` tries to install the resulting new value. That gives you linearizable read-modify-write style updates without needing a replicated log. CASPaxos needs just 2 persistent fields per register: a `promised` ballot and an `accepted` ballot. Learning CASPaxos is also the best way, in my opinion, to learn the fundamentals of distributed consensus.

## Leadered vs leaderless consensus

These are examples of leadered consensus. What is leadered consensus? Well, before a proposer can have a value committed, it needs to do some work to get exclusive (but revokable) rights to have a value committed. This is called leader election. The `prepare` phase of Paxos elects a leader and the `accept` phase commits a value. If leadership is uncontended then this typically takes 1 RTT (1 round-trip-time - one message back and forth between the proposer and all acceptors). Multi-Paxos, Raft, and CASPaxos let a proposer avoid repeatedly getting this right for every single value / log entry they want to have committed. This _distinguished proposer_ is the leader. For CASPaxos, the gist of how this works is that every time you commit a value you piggyback leader election for the _next_ value in the same message: you merge `accept` and `prepare` messages into one.

Leadered consensus lets the leader commit in 1 RTT. If another proposer wants to commit, they must first become the leader (1 RTT) and then commit. That's 2 RTT in total if you were to have a different proposer for each commit. If proposers are trying to commit values concurrently, they contend possibly indefinitely trying to squeeze 2 RTT in before the other proposer deposes them as leader. This is known as the _dueling proposers_ problem.

Leaderless consensus algorithms allow any participant to commit a value without first obtaining exclusive rights. This is the core Fast Paxos optimization: acceptors are prepared ahead of time for a shared fast round, so proposers can send candidate values directly to acceptors instead of first electing a leader. If enough acceptors receive the same value, it commits in 1 RTT with no leader hop. If concurrent proposers propose conflicting values, the protocol falls back to a leadered recovery round to reconcile them. The catch is that this leaderless fast path needs larger quorums than ordinary leadered consensus.

An intuition for larger quorums in fast rounds is that the real challenge is not just getting a value accepted quickly; it is making sure that any later classic recovery round can still tell which fast-round value might have been committed. In a classic round there is only one proposer, so the ballot effectively points at one candidate value. In a fast ballot, many proposers can race in the same ballot, so different acceptors may hold different values for that one ballot. That is why recovery has to count fast-round votes by value: when a proposer runs `prepare` and sees accepted entries from a fast ballot, it must group responses by the value reported for that ballot and tally how many acceptors reported each one. The fast quorum has to be large enough that any later classic quorum is forced to see the fast winner as the dominant value in its responses.

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

That is why fast rounds need a supermajority: not because the fast proposer needs extra votes for their own sake, but because any later classic recovery quorum must be _unable_ to reinterpret the round as supporting a different value. Leaderless consensus is attractive in many scenarios, but those larger quorums and the risk of conflicts mean it generally performs worse than leadered consensus algorithms under contention. Given the pros and cons vary by scenario, and the scenario can vary over time, an ideal solution would be to adapt between leaderless and leadered depending on the current situation.

## Fast CASPaxos

Fast CASPaxos mixes that Fast Paxos-style fast-round optimization with CASPaxos to implement a replicated, rewritable, linearizable register which supports leaderless 1 RTT commits.

Fast CASPaxos uses tuple ballots of the form `(round, proposerId)`, where `proposerId = 0` denotes a shared fast round. In a fast round, _any_ proposer may have its value accepted by an acceptor, whereas in a classic round only the proposer that owns the ballot may do so.

Fast CASPaxos piggybacks the next fast round `prepare` onto each `accept` message. By doing so, we can have many fast rounds in sequence to repeatedly update the committed value from any proposer in 1 RTT. Conflicts are resolved as in regular Fast Paxos: by falling back to a classic round.

Both the Fast Paxos optimization and the Distinguished Proposer optimization use the same mechanism: piggybacking a `prepare` request onto each `accept` request. The only difference is the choice of `proposerId` in the `prepare` ballot. This makes dynamically switching between leadered and leaderless consensus a runtime decision at the proposer, and we can achieve 1 RTT commits in either leadered or leaderless modes, making Fast CASPaxos useful for a wide variety of scenarios.

I find this fascinating. Fast CASPaxos is a straightforward modification to a very simple algorithm (CASPaxos). It requires only two fields at each acceptor: a `promised` ballot and an `accepted` ballot, both tuples of `(round, proposerId)`. It doesn't require logs, so you can easily use an off-the-shelf embedded database engine (eg RocksDB, LMDB, SQLite, etc) to implement it for many independent replicated key-value pairs. It requires 16 to 32 bytes of memory / disk space depending on whether you use 32 or 64-bit values for round & proposerId. Using variable-width integers you can reduce that even further.

## Fast CASPaxos algorithm (pseudocode)

The CASPaxos paper gives the baseline two-phase register algorithm and its piggybacked-`prepare` 1 RTT optimization. Fast CASPaxos keeps that structure, but lets the piggybacked ballot be either the next shared fast ballot `(r + 1, 0)` or the next proposer-owned classic ballot `(r + 1, proposerId)`.

This version is intentionally simplified to show the core protocol only. It omits implementation details like duplicate-message handling, cached local views, and early-stop bookkeeping.

Notation:

- ballots are `(round, proposerId)`, where `proposerId = 0` denotes the shared fast ballot for that round
- `classicQuorum = floor(N / 2) + 1`
- `fastQuorum = ceil(3N / 4)`
- `quorumFor(ballot) = fastQuorum` for fast ballots and `classicQuorum` for classic ballots
- every `prepare` response, including rejections, reports the acceptor's current `(acceptedBallot, acceptedValue)`
- acceptors start with `promised = (1, 0)`, so the first fast round is implicitly prepared

### Proposer

```text
function propose(update):
    b := initial ballot to try

    loop:
        if b is not already prepared:
            responses := collect Prepare(b) responses
            if promises in responses do not reach quorumFor(b):
                b := next classic ballot above the highest conflicting ballot in responses
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

### Acceptor

```text
state:
    promised := (1, 0)       // initial fast ballot is implicitly prepared
    acceptedBallot := ⊥
    acceptedValue := ⊥

function onPrepare(b):
    if b < promised:
        reply reject(acceptedBallot, acceptedValue, promised)
        return

    promised := b
    reply promise(acceptedBallot, acceptedValue)

function onAccept(b, v, next):
    maxBallot := max(promised, acceptedBallot)
    if b < maxBallot:
        reply reject(maxBallot)
        return

    // Idempotency for fast rounds & retries: if we receive the same ballot + value,
    // we 
    if b == acceptedBallot and v != acceptedValue:
        reply reject(maxBallot)
        return

    acceptedBallot := b
    acceptedValue := v

    if next != none:
        promised := max(promised, next)

    reply accept(promised)
```

In short: fast rounds try to commit directly in the shared ballot `(r, 0)` with a `3/4` quorum; if that round splits between competing values, a proposer falls back to a proposer-owned classic ballot `(r', proposerId)`, runs `prepare`, recovers the fast-round winner by counting votes, and then commits with a majority quorum. Successful accepts can piggyback the next fast ballot or the next same-proposer classic ballot so the following operation can skip a standalone `prepare`.

## Intuition from Paxos to Fast CASPaxos

### Paxos

A proposer normally does `prepare` first to claim a ballot, then `accept` to try to commit a value. That extra `prepare` RTT is the latency we want to avoid when possible.

### Multi-Paxos and Raft

Amortize leader establishment across many log entries: once a leader is in place, later appends usually avoid a fresh `prepare` or election and commit in 1 RTT through that leader. Log entries are used to implement state machine replication. Replaying the log and applying the state machine logic lets you reconstruct the state.

### CASPaxos

Instead of choosing a log entry, CASPaxos chooses the next value of a single register. `prepare` recovers the latest accepted value, the proposer applies its update function locally, and `accept` tries to commit the new register value.

### Distinguished proposer (aka leader)

If one proposer is likely to drive many updates, a successful `accept` can also ask acceptors to promise that proposer's next classic ballot. The next operation from the same proposer can then skip a standalone `prepare`. This is the usual sticky-leader optimization.

### Fast Paxos

Rather than giving the next ballot to one proposer, a coordinator can open a shared fast ballot so proposers send values directly to acceptors. If the fast round does not split between incompatible values, the value commits in 1 RTT. If it does split, recovery falls back to a classic round.

### Fast CASPaxos

Fast CASPaxos reuses the same piggybacked-prepare machinery as distinguished-proposer mode, but it can target the next shared fast ballot instead of the next classic one. That is what lets the Fast Paxos optimization apply to CASPaxos too: after one round succeeds, acceptors can already be prepared either for the next leader-owned classic round or for the next leaderless fast round.

The first fast round is bootstrapped by treating the initial fast ballot as implicitly prepared. After that, successful accepts piggyback the next ballot to prepare.

## When each mode fits best 

- **Leadered**
  - **Hot registers with sustained writes.** If one proposer is likely to drive a long sequence of updates, leadered mode amortizes prepare costs across many operations instead of paying them repeatedly.
  - **Higher-conflict workloads.** When independent proposers frequently want different next values, leaders reduces repeated proposer-versus-proposer collisions and gives more stable progress than optimistic fast rounds. To take advantage of this, you need to route requests to a stable leader.
- **Leaderless**
  - **Geo-distributed, low-conflict deployments.** When network latency is high, leaderless 1 RTT commits are particularly attractive, but the penalty of contention is more pronounced.
  - **Infrequent writes.** When writes are infrequent and proposers have a chance to learn the latest committed value without going through consensus, fast rounds allow 1 RTT commits at any proposer.
  - **Almost-everywhere agreement.** When concurrent proposers are proposing the same value anyway, shared fast rounds let them proceed without dueling. The [Rapid cluster membership algorithm](https://www.usenix.org/conference/atc18/presentation/suresh) uses Fast Paxos for exactly this property: Rapid's cut detector produces proposals based on observer alerts and delays action until churn stabilizes into the same multi-process cut, resulting in very low conflict rates among proposers, allowing them to commit in 1 RTT most of the time without going through a leader.
  - **Register initialization.** The initial fast ballot is implicitly prepared, so the first write can skip prepare entirely and go straight to accept. That makes 1 RTT initialization especially attractive when a system creates many lightweight registers that may only be written once.

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
`--conflict-fanout`, `--fast`, and `--leader`. Use `--wait-for-debugger` to have
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

