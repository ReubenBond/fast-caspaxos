# Fast CASPaxos Voter Reconfiguration Specification

Status: **Proposed (awaiting Quint model validation in `ReubenBond/roux`)**

This document is the normative, language- and transport-neutral specification for operator-driven reconfiguration of the Fast CASPaxos voter set (replacing a lost voter, adding a voter, removing a voter, or migrating to a new voter set). It is written to be shared verbatim across `ReubenBond/fast-caspaxos`, `ReubenBond/roux`, and `ReubenBond/orleans`; repository-specific bindings appear in [§13](#13-repository-binding-notes).

## 1. Overview and Design Summary

Fast CASPaxos reconfigures its voter set by storing the voter configuration in an ordinary Fast CASPaxos register (the **configuration register**) and adding three mechanisms to the single-register protocol:

1. **Configuration-qualified ballots.** Every ballot is a lexicographically ordered triple `(configuration, round, owner)`. Quorums for a ballot are counted strictly among the voters of `ballot.configuration`.
2. **Acceptor floor.** Each acceptor durably stores a single acceptor-wide floor ballot `(c, 0, 0)`. The effective promise for every key—including keys the acceptor has never seen—is `max(promised[key], floor)`. Raising the floor to `(c + 1, 0, 0)` fences every ballot of configuration `c` and below atomically across all keys.
3. **Vertical Paxos I hand-off.** While configuration `c + 1` is incomplete (`pending`), every ballot of `c + 1` is a classic hand-off ballot `(c + 1, r ≥ 1, owner > 0)` whose phase 1 (`Prepare`) collects a classic quorum of the latest complete configuration `c` **and** a classic quorum of `c + 1`, and whose phase 2 (`Accept`) writes to a classic quorum of `c + 1`. Once every key has completed in `c + 1` (by explicit transfer or vacuous absence across a fenced classic quorum of `c`), configuration `c + 1` activates and subsequent ballots use only `c + 1`'s voters.

The operator executes reconfiguration through four idempotent, resumable steps built from existing register and acceptor operations: **Choose → Fence `c` → Install → Activate**.

### Rejected alternative: CASPaxos stepwise quorum widening

The CASPaxos paper describes an online reconfiguration procedure that adds an acceptor by widening every proposer's accept quorum, rewriting every key through an identity transition, and then widening the prepare quorum (with replacement performed as add followed by remove). That procedure avoids a metadata pause, but requires every proposer to adopt each intermediate quorum step before the next step begins and requires per-step intersection proofs for Fast Paxos supermajority quorums. Because Fast CASPaxos serves the off-path metadata/control plane rather than the hot data path, the configuration-register + floor + Vertical Paxos hand-off design is chosen for its single linearizable choice point, single uniform fencing rule, and minimal change to the existing protocol and formal model.

---

## 2. State and Data Model

### 2.1 Configuration-qualified ballots

A **ballot** is a triple `b = (configuration, round, owner)` ordered lexicographically:

```text
(c1, r1, o1) < (c2, r2, o2)  iff  c1 < c2
                              or (c1 == c2 and r1 < r2)
                              or (c1 == c2 and r1 == r2 and o1 < o2)
```

| Ballot kind | Representation | Meaning |
| --- | --- | --- |
| Zero ballot | `(0, 0, 0)` | Unaccepted state or absent piggybacked `next_ballot`. Strictly less than every non-zero ballot. |
| Floor ballot | `(c, 0, 0)`, `c ≥ 1` | Acceptor-wide fence for all configurations `< c`. Never carries an accepted value (`accepted` is never a floor ballot). |
| Fast ballot | `(c, r, 0)`, `c ≥ 1, r ≥ 1` | Shared fast-round ballot in configuration `c`. |
| Classic ballot | `(c, r, o)`, `c ≥ 1, r ≥ 1, o ≥ 1` | Exclusive ballot owned by proposer `o` in configuration `c`. |

Configuration numbers are 1-based unsigned integers (`c ≥ 1`), with `c = 1` denoting the bootstrap configuration and `0` reserved exclusively for the zero ballot `(0, 0, 0)`.

### 2.2 Voter sets and quorums

Each configuration `c ≥ 1` defines a non-empty finite set of distinct voter identities `V(c)` of size `N(c) = |V(c)| ≥ 1`, with quorum thresholds:

- `classicQuorum(c) = floor(N(c) / 2) + 1`
- `fastQuorum(c) = ceil(3 * N(c) / 4)` (satisfying `2 * fastQuorum(c) > N(c)` and `2 * fastQuorum(c) + classicQuorum(c) > 2 * N(c)`)
- `quorum(b) = fastQuorum(b.configuration)` if `b.owner == 0`, else `classicQuorum(b.configuration)`

Votes for ballot `b` count **only** from acceptors in `V(b.configuration)`.

### 2.3 Configuration register

The voter set is stored in a dedicated Fast CASPaxos register under a reserved key `K_cfg` (bound per repository in [§13](#13-repository-binding-notes)). Its value is a triple:

```text
ConfigurationRecord = (number: u64, voters: Set<VoterDescriptor>, state: pending | active)
```

- `number` starts at `1` (bootstrap) and increments by exactly `1` on each reconfiguration (`c → c + 1`). Because Fast CASPaxos register versions are opaque, `number` provides the total order over configurations.
- `voters` is the canonical set of voter descriptors for `number`, each naming a unique durable `VoterId` and its transport addressing metadata.
- `state` is `pending` from **Choose** through **Install**, and transitions to `active` upon **Activate**.

At genesis (`c = 1`), `K_cfg` may be initially absent in `V(1)`; an absent `K_cfg` in `c = 1` with no voter reporting `floor > (1, 0, 0)` represents `(1, V(1), active)` by bootstrap agreement ([§8](#8-bootstrap-validation)).

### 2.4 Acceptor durable state

Each acceptor durably stores:

1. **Acceptor floor:** `floor = (c_floor, 0, 0)` with `c_floor ≥ 1`. One singleton row per acceptor database. Initialized to `(1, 0, 0)` for bootstrap voters, and to `(c_join, 0, 0)` for a voter first introduced in configuration `c_join > 1`.
2. **Per-key state:** for each key `k` that has been prepared or accepted, `(promised[k], accepted[k], value[k])`. For any absent key `k`, the implicit state is `promised[k] = (0, 0, 0)`, `accepted[k] = (0, 0, 0)`, `value[k] = None`.
3. **Effective promise:** for every key `k` (present or absent):

```text
effPromised(k) = max(promised[k], floor)
```

---

## 3. Acceptor Rules

Acceptors track neither configuration membership nor configuration completion; they enforce only monotonic ballots and the acceptor floor.

### 3.1 `SetFloor(target_floor)`

- **Validation:** `target_floor` MUST be a valid floor ballot `(c, 0, 0)` with `c ≥ 1`.
- **Transition:** Atomically persist `floor = max(floor, target_floor)` and return the resulting durable `floor`.
- **Properties:** Monotonic, idempotent, and uniform across all keys (present and absent). Once durable, every subsequent `Prepare` and non-duplicate `Accept` for any ballot `< target_floor` is rejected on every key.

### 3.2 `Prepare(key, ballot)`

- **Validation:** `ballot` MUST be a classic ballot `(c, r ≥ 1, o ≥ 1)`.
- **Effective gate:** Let `eff = effPromised(key)` and `gate = max(eff, accepted[key])`.
- **Rejection:** If `ballot < gate`, return `Rejected(conflicting_ballot = gate, accepted = accepted[key], value = value[key])`. When `floor` is the cause of rejection, `gate` carries `floor` (whose `configuration ≥ c_floor`), signaling stale proposers to refresh their configuration.
- **Promise:** Otherwise persist `promised[key] = ballot` atomically and return `Promised(accepted = accepted[key], value = value[key])`.

### 3.3 `Accept(key, ballot, value, next_ballot)`

- **Validation:** `ballot` MUST be a fast or classic ballot (`c ≥ 1, r ≥ 1`); `next_ballot` MUST be `(0, 0, 0)` or a valid ballot `(c, r', o')` in the same configuration `c` with `r' = r + 1` (and `o' = 0` when `o = 0`, or `o' ∈ {0, o}` when `o > 0`).
- **Exact-duplicate replay (read-only):** If `ballot == accepted[key]` and `value == value[key]`, return `Accepted(promised = effPromised(key))` without mutating durable state. Because this adds no new vote and echoes `effPromised(key)` (which is `≥ floor`), it preserves idempotence and never certifies a piggybacked promise below `floor`.
- **Effective gate:** Let `eff = effPromised(key)` and `gate = max(eff, accepted[key])`.
- **Rejection:** If `ballot < gate` or `ballot == accepted[key]` (first-write-wins for conflicting values on the same ballot), return `Rejected(conflicting_ballot = gate)`.
- **Acceptance:** Otherwise atomically persist `accepted[key] = ballot`, `value[key] = value`, and `promised[key] = max(promised[key], next_ballot)`, then return `Accepted(promised = max(promised[key], floor))`.

### 3.4 `Read(key)` and `ListKeys(prefix, after_key, limit)`

- Both operations are **ballot-free** and **MUST remain available when `floor` is raised**.
- `Read(key)` returns a consistent committed local snapshot of `(effPromised(key), accepted[key], value[key])` (or absence).
- `ListKeys` enumerates all keys with `accepted[key] != (0, 0, 0)` in deterministic ordinal key order, each with its committed `(effPromised(key), accepted[key], value[key])`.
- Keeping `Read` and `ListKeys` available at fenced acceptors enables both **Install** key enumeration and stale-proposer discovery of `V(c + 1)` from fenced `V(c)` voters.

### 3.5 New-voter initialization

A voter first introduced in configuration `c_join > 1` MUST durably persist `floor = (c_join, 0, 0)` (and its voter identity binding and admission, [§7](#7-identity-binding-retirement-and-admission)) **before answering any protocol RPC** (`Read`, `Prepare`, `Accept`, `ListKeys`, `SetFloor`).

---

## 4. Proposer Rules

### 4.1 Active configuration authority

Each proposer tracks its known active configuration `(c, V(c))`. A proposer issues ordinary register operations (`Read`, `CompareExchange`) **only** when `K_cfg` for `c` is `active` (or at bootstrap `c = 1`). Proposers MUST NOT issue ordinary operations in a `pending` configuration.

### 4.2 Initial fast ballot rule

- In bootstrap configuration `c = 1`, the initial fast ballot `(1, 1, 0)` is implicitly prepared for an absent base (`None`), preserving 1-RTT register creation at genesis.
- In every configuration `c > 1`, `(c, 1, 0)` is **not** implicitly prepared. A key's first ballot in `c > 1` is always a classic ballot `(c, r ≥ 1, owner > 0)`—either the hand-off ballot during **Install** or a standard classic `Prepare` + `Accept` for a key first created after **Activate**. That classic accept may piggyback `(c, r + 1, 0)` to establish 1-RTT fast writes for subsequent operations.

*Rationale:* In `c = 1`, `(1, 1, 0)` is the lowest protocol ballot in the system, so no prior chosen ballot can lie below it. For `c > 1`, `(c, 1, 0)` outranks all ballots of `< c`. Allowing an implicitly prepared `(c, 1, 0)` would let an unrecovered absent-based accept overwrite a value chosen in `c - 1` before completion, or be accepted by a `V(c)` voter that missed the hand-off quorum after completion (violating `acceptedExtendsChosen`). Disallowing implicit `(c, 1, 0)` for `c > 1` preserves every existing single-register invariant unchanged at the cost of a single classic prepare on the rare first creation of a new key after a reconfiguration.

### 4.3 Quorum selection, accept targeting, and recovery counting

- **Voter-scoped accept targeting (`onlyVotersAccept`):** Proposers MUST send `Accept(b, ...)` **only** to acceptors in `V(b.configuration)`. Every phase-2 `Accept` for ballot `b` counts acknowledgments **only** from acceptors in `V(b.configuration)` against `quorum(b)`.
- Every piggybacked promise certificate for `next_ballot` requires `quorum(next_ballot)` matching echoes (`promised == next_ballot`) from acceptors in `V(next_ballot.configuration)` alongside a committed source ballot in the same configuration.
- **Configuration-scoped recovery (`recoveredBases`):** Let `minRelevantCfg = b.configuration` when `b.configuration` is `active` (complete), and `minRelevantCfg = c` (`= b.configuration - 1`) during hand-off `c → c + 1`. Given a phase-1 promise quorum `q` for classic ballot `b`:
  1. Let `highest = max(r.accepted for r in replies(b, q))`.
  2. If `highest == (0, 0, 0)`, the recovered base is `None`.
  3. **Stale unchosen remnants (`hCfg < minRelevantCfg`):** Because `minRelevantCfg` is already complete for every key and `q` contains a classic quorum of `V(minRelevantCfg)`, any value chosen in any configuration `< minRelevantCfg` was already transferred to a quorum of `V(minRelevantCfg)` intersecting `q` (making `highest.configuration ≥ minRelevantCfg`). Contrapositively, if `highest.configuration < minRelevantCfg`, no value was ever chosen in `< minRelevantCfg`: there are zero viable fast winners (`HasConflict = false`), no historical `V(hCfg)` lookup is needed, and the proposer deterministically selects any value in `top = { r in replies(b, q) : r.accepted == highest }` (or treats `r.accepted.configuration < minRelevantCfg` as unaccepted).
  4. Otherwise (`hCfg = highest.configuration ≥ minRelevantCfg`), filter `top = { r in replies(b, q) : r.accepted == highest and r.acceptor in V(hCfg) }`.
  5. If `highest.owner > 0` (classic), all replies in `top` carry the same value; recover that value.
  6. If `highest.owner == 0` (fast), tally frequencies over `top` (members of `V(hCfg)` only) and compute fast-ballot viability strictly against `V(hCfg)`:
     - `seenInHCfg = |{ r in replies(b, q) : r.acceptor in V(hCfg) }|` (all promise responses from `V(hCfg)`, not `|q|` and not `|top|`).
     - `remainingInHCfg = |V(hCfg)| - seenInHCfg`.
     - Candidate `v` is viable iff `count(v) + remainingInHCfg >= fastQuorum(hCfg)`.
     Select a value of maximum frequency in `top`.

*Rationale:* Targeting `Accept(b)` strictly to `V(b.configuration)` prevents a retired non-voter from overwriting its witness of an earlier configuration's chosen ballot with a ballot outside its membership. Scoping fast-ballot vote counts and viability to `V(hCfg)` prevents a non-member's vote from tying a fast winner, and the `hCfg < minRelevantCfg` rule eliminates both livelock on colliding unchosen remnants from retired configurations and any need to retain historical `V(1)..V(c - 1)` sets.

### 4.4 Hand-off phase 1 and phase 2 (Vertical Paxos I)

Reconfigurations advance sequentially: configuration `c + 1` is chosen only after configuration `c` is `active` (complete for every key). While configuration `c + 1` is `pending` (incomplete), the operator's hand-off recovery for any key `k` uses a classic ballot `b = (c + 1, r ≥ 1, owner > 0)`:

- **Phase 1 (`Prepare(b)`):** Broadcast `Prepare(k, b)` to `V(c) ∪ V(c + 1)` and wait until the promise set `q ⊆ V(c) ∪ V(c + 1)` satisfies **both**:
  1. `|q ∩ V(c)| ≥ classicQuorum(c)`, **and**
  2. `|q ∩ V(c + 1)| ≥ classicQuorum(c + 1)`.
  (Acceptors in `V(c) ∩ V(c + 1)` answer a single `Prepare(k, b)` RPC that counts toward both quorums.)
- **Recovery:** Compute `base = recoveredBases(b, q)` per [§4.3](#43-quorum-selection-accept-targeting-and-recovery-counting).
- **Phase 2 (`Accept(b, base, next = zero)`):** Broadcast the identity transition (`value = base`) to `V(c + 1)` only, and wait for `classicQuorum(c + 1)` acknowledgments from `V(c + 1)`.

*Design decision (Vertical Paxos I vs single-voter restriction):* Reading phase 1 only from `V(c)` is unsafe even for a single-voter **replacement** (`V(c) = {1, 2, 3} → V(c + 1) = {1, 2, 4}`), because a classic quorum `{2, 3}` of `V(c)` is disjoint from a classic quorum `{1, 4}` of `V(c + 1)`: two racing hand-off installs can recover different values from a tied fast collision in `V(c)` and both commit in `V(c + 1)`. Requiring phase 1 to intersect both `V(c)` and `V(c + 1)` (Vertical Paxos I) adds zero round-trips (all prepares run in parallel), supports 1-step replacement of a lost voter directly, and generalizes identically to add-one, remove-one, and arbitrary multi-voter migrations.

### 4.5 Per-key and configuration completion

Once configuration `c` is complete, a key `k` is **complete** in configuration `c + 1` when either:

1. **Transferred:** a ballot `b` with `b.configuration == c + 1` has been chosen for `k` on a quorum of `V(c + 1)`, **or**
2. **Vacuously complete:** a classic quorum `F ⊆ V(c)` all have durable `floor ≥ (c + 1, 0, 0)` and every `a ∈ F` has `accepted[k] == (0, 0, 0)` (so `c` never chose a value for `k` and can never choose one).

Configuration `c + 1` is **complete** once every key is complete in `c + 1` (established by transferring every key listed by any member of `F`, with all other keys vacuously complete).

### 4.6 Stale-configuration recovery

All mutations to `K_cfg` (**Choose** and **Activate**) use classic ballots (`Prepare` + `Accept`, never fast ballots). When a proposer operating in configuration `c` receives a `Prepare` or `Accept` refusal whose `conflicting_ballot.configuration = c' > c` (or a duplicate `Accept` reply whose echoed `promised.configuration > c`):

1. **Discard stale caches:** Clear cached prepared ballots and in-flight phase state for configuration `c`.
2. **Discover candidate successors from fenced `V(c)`:** Perform a ballot-free `Read(K_cfg)` across all reachable voters of `V(c)` (waiting for at least `classicQuorum(c)` responses). Because `Read` is unaffected by the acceptor floor and **Choose** committed `(c + 1, V(c + 1), pending)` on a classic quorum of `V(c)` before **Fence `c`**, at least one voter in that quorum holds a `ConfigurationRecord` with `number ≥ c + 1`. Treat the returned `(number > c, voters)` records as **discovery hints** (ordered from highest `number` downward, falling back to `c + 1` if a higher candidate is unreachable or unconfirmed).
3. **Verify and follow the chain:** Query `Read(K_cfg)` from the candidate voter set `V(c_cand)`. If `V(c_cand)` reports a further `floor` or higher `ConfigurationRecord`, follow the chain until reaching the highest reachable configuration `c_cur`. (If all voters of `V(c)` have already been decommissioned, fall back to operator-updated discovery endpoints.)
4. **Confirm `active` on a quorum of `V(c_cur)`:**
   - If `K_cfg` in `V(c_cur)` is `pending`, reconfiguration is still in progress: do not issue ordinary operations in `c_cur`; wait/retry or return a transient unavailability error to the caller.
   - Adopt `(c_cur, V(c_cur))` **only** after confirming `(c_cur, V(c_cur), active)` via a quorum of `V(c_cur)` (a linearizable read or matching committed `active` state across `classicQuorum(c_cur)`). Then retry or fail-and-reconcile the caller's operation according to normal `Unknown`/retry semantics.

---

## 5. Operator Reconfiguration Procedure

All four steps are **idempotent**, **resumable after any crash** (operator, voter, or network), and **safe under concurrent operators**.

### Step 1: Choose

- **Precondition:** Current configuration `c` is `active` with voter set `V(c)`; replacement voters have fresh, never-previously-used `VoterId`s; target `V(c + 1)` is non-empty.
- **Action:** Run classic `CompareExchange(K_cfg, expected_version = v_c, new_value = (c + 1, V(c + 1), pending))` in configuration `c` (if `c = 1` and `K_cfg` is absent, `expected_version = None`).
- **Effect:** Linearizability of `K_cfg` chooses exactly one next configuration `V(c + 1)` for number `c + 1`.
- **Concurrent operators:** Two racing **Choose** commands targeting different `V(c + 1)` contend on `v_c`; at most one succeeds. The loser receives `Mismatch` and observes the winning `(c + 1, V_win, pending)`.
- **Crash/resume:** If the operator crashes with an `Unknown` outcome, on restart it reads `K_cfg` in `c`:
  - If `(c, V(c), active)`, retry Step 1.
  - If `(c + 1, V(c + 1), pending)`, proceed to Step 2 using the chosen `V(c + 1)`.
  - If the read receives a floor refusal (`configuration ≥ c + 1`, because a concurrent operator already ran Step 2), discover `(c + 1, V(c + 1), ...)` via [§4.6](#46-stale-configuration-recovery) and resume from Step 2 (or terminate if `c + 1` is already `active`).

### Step 2: Fence `c` (and prepare new voters)

- **Precondition:** `(c + 1, V(c + 1), pending)` is committed in `K_cfg` in `V(c)`.
- **Action:**
  1. Ensure every new voter `a ∈ V(c + 1) \ V(c)` has its durable floor initialized to `(c + 1, 0, 0)` and its identity binding/admission completed ([§3.5](#35-new-voter-initialization), [§7](#7-identity-binding-retirement-and-admission)) before it serves.
  2. Send `SetFloor((c + 1, 0, 0))` to all reachable voters in `V(c)` until a classic quorum `F ⊆ V(c)` (`|F| ≥ classicQuorum(c)`) durably acknowledges `floor ≥ (c + 1, 0, 0)`.
- **Effect:** Every classic and fast quorum of `V(c)` intersects `F`. Consequently, configuration `c` can never choose any value for any key absent across `F`, can never prepare or piggyback-certify any new ballot of `c`, and any in-flight ballot of `c` that can still complete a quorum via unfenced voters in `V(c) \ F` is already durably present on at least one member of `F` (so it appears in `ListKeys(F)` and is recovered or fenced by Install's Phase 1). Any stale proposer of `c` contacting `F` receives a refusal carrying `configuration ≥ c + 1`.
- **Idempotence / crash / concurrency:** `SetFloor` is monotonic and idempotent. Re-running Step 2 on resume or concurrently from two operators is always safe; if a member of `F` fails before Step 3 completes and another surviving voter of `V(c)` exists, Step 2 fences that survivor to form a new classic quorum `F'`.

### Step 3: Install

- **Precondition:** A classic quorum `F ⊆ V(c)` holds durable `floor ≥ (c + 1, 0, 0)`, and a classic quorum of `V(c + 1)` is reachable with new voters initialized at `floor = (c + 1, 0, 0)`.
- **Action:**
  1. Run `ListKeys` to exhaustion on every voter `a ∈ F`, forming `Keys = ⋃_{a ∈ F} ListKeys(a)`. Because `K_cfg` was committed on a quorum of `V(c)` in Step 1 and `F` intersects that quorum, `K_cfg ∈ Keys`.
  2. For every key `k ∈ Keys` (including `K_cfg`, transferring `K_cfg` last as operational hygiene), run Vertical Paxos I hand-off recovery ([§4.4](#44-hand-off-phase-1-and-phase-2-vertical-paxos-i)) at a classic ballot `b = (c + 1, r ≥ 1, owner > 0)` using the identity transition (`value = base`), preserving the key's logical register version.
     - *Note on `K_cfg`:* Its identity transfer writes `(c + 1, V(c + 1), pending)` into a classic quorum of `V(c + 1)` (or `(c + 1, V(c + 1), active)` if a concurrent operator already completed Step 4). Because `K_cfg` remains `pending` after transfer, ordinary proposers still do not use `c + 1` until Step 4.
- **Effect:** Every key in `Keys` is transferred into `c + 1`; every key not in `Keys` is vacuously absent across the fenced quorum `F` and is vacuously complete in `c + 1`. Configuration `c + 1` is now complete.
- **Idempotence / crash / concurrency:** Re-running Install for an already-transferred key recovers its `c + 1` value via `q ∩ V(c + 1)` and re-commits the same identity value. Two concurrent operators installing the same key at different hand-off ballots `(c + 1, r1, o1)` and `(c + 1, r2, o2)` serialize through `q ∩ V(c + 1)` without divergence.

### Step 4: Activate

- **Precondition:** Step 3 has succeeded for every key `k ∈ Keys` (including `K_cfg`).
- **Action:** In configuration `c + 1` (using `V(c + 1)` quorums only), run classic `CompareExchange(K_cfg, expected_version = v_pending, new_value = (c + 1, V(c + 1), active))`.
- **Effect:** `K_cfg` in `V(c + 1)` transitions from `pending` to `active`. Stale proposers discovering `c + 1` now see `active` and resume normal 1-RTT / classic operations in `c + 1`. Retired voters (`V(c) \ V(c + 1)`) may now be safely decommissioned.
- **Idempotence / crash / concurrency:** If a concurrent operator already activated `c + 1`, `CompareExchange` returns `Mismatch` showing `(c + 1, V(c + 1), active)` (or a later configuration) and the operator finishes cleanly. On **any** crash/resume where `K_cfg` is still `pending` (whether observed in `V(c)` or `V(c + 1)`), the operator MUST resume from **Step 2 (Fence `c`) → Step 3 (Install all `Keys`) → Step 4 (Activate)** so a crash mid-Step 3 after transferring `K_cfg` never activates `c + 1` with untransferred user keys. Only when `K_cfg` in `V(c + 1)` is already `active` (or a higher configuration) may the operator finish without re-running Steps 2–3.

---

## 6. Persistence and Durability Requirements

Every protocol answer MUST be durable on stable local storage before leaving the process:

| State | Durability point | Failure behavior |
| --- | --- | --- |
| Acceptor `floor` | Committed and synced (e.g., SQLite `WAL` + `synchronous=FULL`) before `SetFloor` returns, and before a new voter serves any RPC. | Report to fatal-error handler and stop the process immediately. |
| Acceptor `(promised, accepted, value)` | Committed and synced before `Promised` or `Accepted` leaves the acceptor. | Report to fatal-error handler and stop the process immediately. |
| Proposer ballot reservation `(configuration, round)` | Committed and synced in `proposer.db` monotonically (lexicographically non-decreasing) before sending `Prepare` or `Accept` for any classic ballot `(configuration, round, proposer_id)` (including piggybacked classic accepts). | Report to fatal-error handler and stop the process immediately. |
| Voter identity binding & admission | Committed and synced in the acceptor's own database before answering any protocol RPC. | Fail startup or stop the process immediately. |
| Database directories | Created and parent + database directories flushed on every startup before serving. | Fail startup before serving. |

A process experiencing a local storage failure MUST terminate immediately (fail-stop) without running graceful drain or departure barriers against the failed store.

---

## 7. Identity, Binding, Retirement, and Admission

1. **Permanent retirement:** A lost or removed voter's `VoterId` is permanently retired by its absence from `V(c + 1)` and all subsequent configurations. A retired `VoterId` MUST NEVER be reused in any future configuration.
2. **Fresh replacement identity:** Replacing a lost voter allocates a brand-new `VoterId` with a fresh acceptor database bound to `(new_voter_id, fresh_incarnation)`.
3. **Composition with majority admission:**
   - Surviving voters in `V(c) ∩ V(c + 1)` keep their existing `VoterId`, `incarnation`, `admitted = 1` flag, and `voter_admissions` records.
   - Because surviving voters still record the lost voter's old `(lost_voter_id, old_incarnation)`, any accidental restart of the lost voter on an empty disk under `lost_voter_id` is still refused by admission (in addition to being excluded from `V(c + 1)`).
   - The replacement voter's `new_voter_id` has no prior admission record, so a majority of the other voters in `V(c + 1)` durably admit `(new_voter_id, fresh_incarnation)` before the replacement answers protocol RPCs.
   - New-voter startup order: bind `(new_voter_id, fresh_incarnation)` → persist `floor = (c + 1, 0, 0)` → serve admission RPCs (only protocol RPCs wait for `admitted = 1`, so disjoint/multi-voter additions never deadlock waiting on mutually unadmitted peers) → obtain majority admission from `V(c + 1) \ {new_voter_id}` (a single-voter cluster `N = 1` self-admits) → mark `admitted = 1` → begin answering protocol RPCs.

---

## 8. Bootstrap Validation

- Static seed/silo options define **only** the bootstrap configuration `(1, V(1))`. After the first reconfiguration (`c ≥ 2`), `K_cfg` is the sole authority for the active voter set.
- **Bootstrap agreement:** Configuration-register safety is proved by induction from an agreed bootstrap `V(1)`. At startup (before a voter serves in `c = 1`), voters MUST validate agreement on `(1, V(1))` by exchanging a canonical digest of the sorted bootstrap voter set during admission/handshake; a mismatch fails startup rather than allowing split-brain bootstrap quorums.
- **Restart after reconfiguration:** A node restarting with older seed/bootstrap options reads `K_cfg` (or receives a floor refusal) and follows `1 → 2 → ... → c` ([§4.6](#46-stale-configuration-recovery)). If all bootstrap voters have been retired, operators supply current `V(c)` endpoints as discovery seeds; the node still verifies `(c, V(c), active)` from `K_cfg` on those voters.

---

## 9. Availability and Fault Tolerance During Reconfiguration

### 9.1 Availability during the pause

- Register mutations and linearizable reads are **unavailable** between **Fence `c`** (Step 2) and **Activate** (Step 4).
- Data-plane operations that do not consult the MetaStore (ledger appends, hot reads, in-flight turn execution, already-established leases) continue uninterrupted during the pause.
- The pause duration is bounded by `ListKeys` + one classic round-trip per MetaStore key (typically a handful of keys).

### 9.2 Fence-quorum retention and fault tolerance (for every `N`)

Let `f_lost` be the number of voters in `V(c)` already lost when reconfiguration begins, and `f_max(c) = N(c) - classicQuorum(c) = floor((N(c) - 1) / 2)`.

- **Floor-quorum retention:** A classic quorum `F ⊆ V(c)` with durable `floor ≥ (c + 1, 0, 0)` MUST remain available and retain its durable state until **Activate** commits in `c + 1`. Do not wipe or decommission any voter of `V(c)` until Step 4 commits.
- **Additional failures tolerated during reconfiguration:**
  - In `V(c)`: at most `f_max(c) - f_lost` additional voter losses before **Activate**.
    - Example (`N = 3`, `f_lost = 1`): `f_max = 1`, tolerates `0` additional losses in `V(c)`.
    - Example (`N = 5`, `f_lost = 1`): `f_max = 2`, tolerates `1` additional loss in `V(c)`. If a member of the active floor quorum `F` fails during **Install**, resuming from **Fence `c`** raises the floor on the remaining surviving voter of `V(c)` to form a new classic quorum `F'` and re-runs **Install** over `F'` (every value chosen in `c` intersects `F'`).
    - Example (`N = 5`, `f_lost = 2`): `f_max = 2`, tolerates `0` additional losses in `V(c)`.
  - In `V(c + 1)`: at least `classicQuorum(c + 1)` voters of `V(c + 1)` must be available to accept **Install** and **Activate**.

---

## 10. Normative Invariants

| ID | Invariant | Scope / Classification |
| --- | --- | --- |
| `VR-I1` | **Per-configuration quorum intersection:** For every `c`, `2 * Q_c(c) > N(c)`, `2 * Q_f(c) > N(c)`, and `2 * Q_f(c) + Q_c(c) > 2 * N(c)`. | Safety (`quorumIntersection`) |
| `VR-I2` | **First-write-wins & classic ownership:** An acceptor records at most one value per ballot; each classic ballot `(c, r, o > 0)` carries at most one request. | Safety (`firstWriteWins`, `classicOwnership`) |
| `VR-I3` | **Single decision per ballot:** Each ballot `b` chooses at most one value, counted strictly over `V(b.configuration)`. | Safety (`oneDecisionPerBallot`) |
| `VR-I4` | **Cross-configuration chosen chain:** For all chosen `(b1, v1)` and `(b2, v2)` across any configurations, `b1 ≤ b2 ⇒ ancestor(v1, v2)`. | Safety (`chosenChain`) |
| `VR-I5` | **Accepted extends chosen:** For every chosen `(b, v)` and every acceptance `a` with `b < a.ballot`, `ancestor(v, a.value)`. | Safety (`acceptedExtendsChosen`) |
| `VR-I6` | **Durable history & provenance:** Every acceptor's durable `(floor, promised, accepted, value)` preserves all prior promises and new acceptances; every ack corresponds to an issued request. | Safety (`durableHistory`, `acknowledgmentProvenance`) |
| `VR-I7` | **Uniform monotonic floor:** An acceptor's `floor` is monotonically non-decreasing; once `floor ≥ (c + 1, 0, 0)`, the acceptor grants no new promise and records no new acceptance for any ballot `< (c + 1, 0, 0)` on any key (present or absent). Exact-duplicate accept replays remain read-only and echo `effPromised ≥ floor`. | Safety (`floorMonotoneAndEffective`) |
| `VR-I8` | **Single configuration choice:** By induction from agreed bootstrap `(1, V(1))` and `VR-I4` applied to `K_cfg`, every configuration number `c` maps to at most one voter set `V(c)`. | Safety (induction) |
| `VR-I9` | **Sequential Vertical Paxos I hand-off:** Configuration `c + 1` begins only after `c` is complete (`complete.contains(c)`). Before `c + 1` is complete for a key, every ballot of `c + 1` is classic (`r ≥ 1, o > 0`), reads a classic quorum of `V(c)` and a classic quorum of `V(c + 1)` in phase 1, sends `Accept` only to `V(c + 1)`, and scopes fast-ballot tallies/viability to `V(highest.configuration)` (treating `highest.configuration < minRelevantCfg` as unchosen). | Safety (protocol rule) |
| `VR-I10` | **Dual per-key completion:** A key completes in `c + 1` only via a chosen ballot in `c + 1` or vacuous absence across a classic quorum of `V(c)` fenced at `≥ (c + 1, 0, 0)`. | Safety (protocol rule) |
| `VR-I11` | **Pre-join refusal & hygiene:** A voter newly introduced in `c_join > 1` initializes `floor = (c_join, 0, 0)` before answering and never promises or accepts any ballot with `configuration < c_join`. | Refusal / liveness hygiene (`noPreJoinAcceptance`) |
| `VR-I12` | **Identity non-reuse:** A retired `VoterId` never appears in any later configuration and is refused if restarted on a fresh disk. | Operational / admission |

---

## 11. Formal Verification Plan (Quint)

### 11.1 Verification minimization & refinement obligation

Because keys are independent and the acceptor floor applies uniformly to every key, **a single key remains the unit of verification**. We extend `FastCASPaxos.qnt` in place rather than introducing a multi-key model.

Two proof obligations bridge the multi-key system to the single-key model:

1. **Per-key projection refinement (acceptor floor):** In any multi-key execution, projecting to a single key `k` maps `SetFloor(a, (c, 0, 0))` to `raiseFloor(a, c)` on key `k`'s acceptor state, which raises `effPromised(a) = max(promised, floor)` to at least `(c, 0, 0)` identically whether `k` has a row (`accepted != zero`) or is unseen (`accepted == zero`). Thus every per-key projection of a multi-key floor-raising execution is a legal trace of `FastCASPaxos.qnt`.
2. **Configuration-map induction:** `Configs: int -> Set[int]` is modeled as a constant map justified by induction on `c`: bootstrap `V(1)` is agreed ([§8](#8-bootstrap-validation)), and assuming all proposers agree on `V(1)..V(c)`, applying single-key safety (`VR-I4`) to `K_cfg` in `c` ensures at most one `V(c + 1)` is ever chosen.

### 11.2 Exact edits to `FastCASPaxos.qnt`

1. **Constants:** Replace `AcceptorCount: int` and `FastQuorum: int` with:
   - `Configs: int -> Set[int]` (mapping `1..MaxConfig` to voter sets)
   - `FastQuorums: int -> int` (mapping `1..MaxConfig` to fast quorum sizes)
   - Keep `ProposerCount`, `MaxRound`, `ValueDepth`.
   - Derive `configIds = Configs.keys()`, `acceptors = configIds.map(c => Configs.get(c)).flatten()`, `votersOf(c)`, `classicQuorumOf(c)`, `fastQuorumOf(c)`, `firstConfig(a)`.
2. **Ballot, Acceptor, and message types:**
   - `type Ballot = { config: int, round: int, owner: int }`
   - `zero = { config: 0, round: 0, owner: 0 }`, `floorBallot(c) = { config: c, round: 0, owner: 0 }`, `initialFast = { config: 1, round: 1, owner: 0 }`.
   - `ballots = tuples(configIds, 1.to(MaxRound), 0.to(ProposerCount)).map(((c, r, p)) => { config: c, round: r, owner: p })`; `floorBallots = configIds.map(c => floorBallot(c))`.
   - `lt(a, b)` compares `config`, then `round`, then `owner`.
   - Update `nextBallots(b)` to stay within `b.config` and allow classic ballots to piggyback fast ballots (enabling 1-RTT fast writes in `c > 1`):
     ```quint
     pure def nextBallots(b: Ballot): Set[Ballot] =
       if (b.round == MaxRound) Set(zero)
       else if (b.owner == 0) Set(zero, { config: b.config, round: b.round + 1, owner: 0 })
       else Set(zero, { config: b.config, round: b.round + 1, owner: 0 }, { config: b.config, round: b.round + 1, owner: b.owner })
     ```
   - `type Acceptor = { floor: Ballot, promised: Ballot, accepted: Ballot, value: int }`; `effPromised(acc) = maxBallot(acc.promised, acc.floor)`.
   - Record `floor: Ballot` on `PrepareReply` and `(floor: Ballot, isDuplicate: bool)` on `Ack` (capturing `old.floor` at handling time) so `floorMonotoneAndEffective` is a pure state invariant.
   - `init` sets `floor: floorBallot(firstConfig(a))`, `promised: if (firstConfig(a) == 1) initialFast else floorBallot(firstConfig(a))`, `accepted: zero`, `value: 0`, and `complete' = Set(1)`.
3. **Scoped quorums and recovery:**
   - `voters(b, v)` filters `acks` by `votersOf(b.config).contains(a.acceptor)`.
   - `preparedBases(b)` returns `Set(0)` only for `b == initialFast` (`(1, 1, 0)`), and for all other `b` requires `r.ballot.config == b.config` and `quorum(b)` echoes from `acks` filtered by `votersOf(b.config).contains(a.acceptor)`.
   - `recoveredBases(b, q)` filters `top` by `votersOf(highest.config).contains(r.acceptor)` when `highest.config >= minRelevantCfg` (where `minRelevantCfg = if (complete.contains(b.config)) b.config else b.config - 1`), and when `highest.config < minRelevantCfg` returns `top.map(r => r.value)` directly (zero viable fast winners).
4. **Actions (with sequentiality gate `c == 1 or complete.contains(c - 1)`):**
   - Add `raiseFloor(a, c)`: requires `configIds.contains(c)`, `c > 1`, `complete.contains(c - 1)`, `acceptors.contains(a)`, `lt(nodes.get(a).floor, floorBallot(c))`; sets `nodes.get(a).floor = floorBallot(c)`.
   - Update `prepare(a, b)`: requires `b.config == 1 or complete.contains(b.config - 1)`, leaves `a ∈ acceptors` ungated on `votersOf` (so misrouted prepares test the new voter's floor), gates on `le(maxBallot(effPromised(old), old.accepted), b)`, and records `floor: old.floor` on `PrepareReply`.
   - Update `accept(a, r)`: gates on `votersOf(r.ballot.config).contains(a)` (`onlyVotersAccept`) and `maxBallot(effPromised(old), old.accepted)`; echoes `effPromised(updated)` and records `floor: old.floor, isDuplicate: duplicate(a, r)` on `Ack` (read-only duplicates leave `nodes` unchanged).
   - Gate `issuePrepared` on `complete.contains(b.config)`.
   - Split `issueRecovered` into:
     - **Normal (`complete.contains(b.config)`):** `q ⊆ responders(b).intersect(votersOf(b.config))`, `q.size() == classicQuorumOf(b.config)`.
     - **Hand-off (`not(complete.contains(b.config))` and `complete.contains(b.config - 1)`):** `prev = b.config - 1`, `q ⊆ responders(b).intersect(votersOf(prev).union(votersOf(b.config)))`, `q.intersect(votersOf(prev)).size() >= classicQuorumOf(prev)`, and `q.intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config)`.
   - Add `completeTransferred(c)`: requires `configIds.contains(c)`, `not(complete.contains(c))`, `c == 1 or complete.contains(c - 1)`, and `ballots.exists(b => b.config == c and chosen(b) != Set())`.
   - Add `completeVacuous(c)`: requires `configIds.contains(c)`, `not(complete.contains(c))`, `c > 1`, `complete.contains(c - 1)`, and a classic quorum `f ⊆ votersOf(c - 1)` where every `a ∈ f` has `le(floorBallot(c), nodes.get(a).floor)` and `nodes.get(a).accepted == zero`.
5. **Invariants:**
   - Generalize `quorumIntersection` over `configIds.forall(c => ...)`.
   - Update `stateWellFormed` to check `floorBallots.contains(nodes.get(a).floor)` and `ballots.union(floorBallots).contains(nodes.get(a).promised)`.
   - Update `durableHistory` to compare against `effPromised(nodes.get(a))`.
   - Keep `firstWriteWins`, `classicOwnership`, `oneDecisionPerBallot`, `chosenChain`, `acceptedExtendsChosen`, and `acknowledgmentProvenance` unchanged.
   - Add `floorMonotoneAndEffective`:
     ```quint
     val floorMonotoneAndEffective = and {
       prepares.forall(p => le(p.floor, p.ballot) and le(p.floor, nodes.get(p.acceptor).floor)),
       acks.forall(r => and {
         le(r.floor, nodes.get(r.acceptor).floor),
         le(r.floor, r.promised),
         not(r.isDuplicate) implies le(r.floor, r.ballot),
       }),
     }
     ```
   - Add `noPreJoinAcceptance` (for `VR-I11`):
     ```quint
     val noPreJoinAcceptance = and {
       prepares.forall(p => firstConfig(p.acceptor) <= p.ballot.config),
       acks.forall(a => firstConfig(a.acceptor) <= a.ballot.config),
     }
     ```

### 11.3 Required mutants (for `check-mutants.sh`)

Every mutant MUST violate its target invariant under Quint simulation:

| Mutant action | Bug injected | Must break |
| --- | --- | --- |
| `mutantFloorOnlyHeldKeys` | `raiseFloor` still advances `floor` (so `completeVacuous` can fire), while `prepare`/`accept` apply `floor` only when `old.accepted != zero` (leaving unseen keys unfenced) | `chosenChain` |
| `mutantPhase1InNewConfigBeforeComplete` | Hand-off `issueRecovered` reads phase 1 only from `votersOf(b.config)`, skipping `votersOf(c - 1)` | `chosenChain` |
| `mutantHandoffSkipsNewConfigQuorum` | Hand-off `issueRecovered` reads phase 1 only from `votersOf(c - 1)`, skipping `votersOf(b.config)` (racing-install divergence on replace-1 / multi-voter change) | `chosenChain` |
| `mutantImplicitFastInNewConfig` | `preparedBases` treats `(c, 1, 0)` as implicitly prepared for `c > 1` | `acceptedExtendsChosen` |
| `mutantNewVoterBeforeFloor` | New voter (`firstConfig(a) > 1`) initializes with `floor = (1, 0, 0)` instead of `(firstConfig(a), 0, 0)` (reclassified to refusal/pre-join hygiene; caught via ungated `prepare` hitting the new voter) | `noPreJoinAcceptance` |
| `mutantUnscopedFastTally` | Disables both the `accept` `votersOf` gate and the `recoveredBases` `V(hCfg)` filter (run against `FastCASPaxosReconfig4Voter` with `MaxRound = 3` so a retired non-member's fast vote ties a 3-of-4 fast winner) | `chosenChain` |
| `mutantSkipSingleChoice` | Allows hand-off / quorum counting at config `2` against two conflicting voter sets `V_A = {1, 2, 4}` and `V_B = {1, 3, 4}` (skipping `Choose`'s single choice) so `{2, 4}` and `{1, 3}` commit diverging values at config `2` | `chosenChain` |

### 11.4 MC configurations and bounds

Keep all five existing single-configuration modules (updated to `Configs = Map(1 -> ...)`), and add reconfiguration modules:

| Module | `Configs` | `FastQuorums` | Owners / Rounds / Depth | Check |
| --- | --- | --- | --- | --- |
| `FastCASPaxosReconfigReplace` | `1 -> {1,2,3}, 2 -> {1,2,4}` | `1 -> 3, 2 -> 3` | 2 / 2 / 2 | Simulation + core mutants |
| `FastCASPaxosReconfig4Voter` | `1 -> {1,2,3,5}, 2 -> {1,2,3,4}` | `1 -> 3, 2 -> 3` | 2 / 3 / 2 | Simulation + `mutantUnscopedFastTally` |
| `FastCASPaxosReconfigDoubleReplace` | `1 -> {1,2,3}, 2 -> {1,2,4}, 3 -> {1,4,5}` | `1 -> 3, 2 -> 3, 3 -> 3` | 2 / 2 / 2 | Simulation (3-config chain exposing skip bugs) |
| `FastCASPaxosReconfigAddRemove` | `1 -> {1,2,3}, 2 -> {1,2,3,4}, 3 -> {2,3,4}` | `1 -> 3, 2 -> 3, 3 -> 3` | 2 / 2 / 2 | Simulation (add then remove) |
| `FastCASPaxosReconfigDisjoint` | `1 -> {1,2,3}, 2 -> {4,5,6}` | `1 -> 3, 2 -> 3` | 2 / 2 / 2 | Simulation (full migration) |
| `FastCASPaxosReconfigExhaustive` | `1 -> {1,2}, 2 -> {1,3}` | `1 -> 2, 2 -> 2` | 1 / 2 / 1 (or 1 / 1 / 1 if TLC budget requires) | Exhaustive TLC |

Add directed tests in `FastCASPaxosTests.qnt` covering: fast-round winner recovery across hand-off, tied fast-collision recovery across hand-off, vacuous completion for unseen keys, stale proposer holding prepared fast ballot of `c` fenced by floor, exact-duplicate replay below floor, and chained `1 → 2 → 3` reconfiguration.

### 11.5 Trace-validation mapping (Phase 2)

Implementation simulation runs (C# `Orleans.Consensus.CasPaxos.Simulation` and Rust `stateright`/`turmoil`) emit per-key event traces mapping directly to `FastCASPaxos.qnt` actions:

- `SetFloor(acceptor, c)` → `raiseFloor(acceptor, c)`
- `PreparePromised(acceptor, ballot, accepted, value)` → `prepare(acceptor, ballot)`
- `AcceptDispatched(ballot, value, next, mode, quorum)` → `issuePrepared` / `issueRecovered`
- `AcceptAcknowledged(acceptor, ballot, value, promised)` → `accept(acceptor, request)`
- `KeyTransferred(c)` / `KeyVacuouslyComplete(c)` → `completeTransferred(c)` / `completeVacuous(c)`

Traces replay against `FastCASPaxos.qnt` (via Quint trace validation / ITF check) to prove every simulated execution refines the model.

---

## 12. Cross-Repository Sharing Plan

To maximize sharing and prevent specification or model drift across `ReubenBond/roux`, `ReubenBond/fast-caspaxos`, and `ReubenBond/orleans`:

1. **Byte-identical Quint files:** `FastCASPaxos.qnt`, `FastCASPaxosMC.qnt`, `FastCASPaxosTests.qnt`, and `tlc.json` MUST remain **byte-identical** between `ReubenBond/roux` (`specs/fast-caspaxos/`) and `ReubenBond/fast-caspaxos` (`quint/`).
2. **Single spec body:** The normative sections (§1–§11) of this document are the single shared specification text. Per-repository transport, storage, and hosting bindings live only in repository-local binding sections ([§13](#13-repository-binding-notes) / `docs/design/fast-caspaxos.md` / `metastore.md §8.1`).
3. **Authoring sequence:**
   - **Phase 1 (Roux):** `ReubenBond/roux` implements the Quint model, mutants, MC configs, tests, CI wiring (`specs-storage.yml`), and design-doc updates first, iterating until all directed tests, simulations, mutants, and TLC checks pass in CI. Any model-discovered spec adjustments update the normative text.
   - **Phase 2 (`fast-caspaxos`):** `ReubenBond/fast-caspaxos` copies the four Quint files byte-identically from `roux`, wires `check-mutants.sh` into `.github/workflows/quint.yml`, updates this spec to **Decided**, implements the protocol in `Orleans.Consensus.CasPaxos*` with trace validation, and merges to `main`. From this point onward, `ReubenBond/fast-caspaxos` is the long-term canonical upstream for the protocol and Quint model, synced byte-identically to `roux`.
   - **Phase 3 (Orleans):** `ReubenBond/orleans` imports the updated `Orleans.Consensus.CasPaxos*` implementation from `fast-caspaxos` and binds the operator reconfiguration API to Orleans hosting without forking the Quint model.
4. **Drift detection:** Each copy of `FastCASPaxos.qnt` / `README.md` records the upstream commit SHA and SHA-256 of the four shared Quint files; CI or a sync-check script fails on any unintended divergence.

---

## 13. Repository Binding Notes

### 13.1 `ReubenBond/roux` (`clog-caspaxos`)

- **Transport & wire contract:** L1 Messaging Target `96` (`MetaStoreCasPaxos`, `Lane::Normal`), `roux.storage.v1` protobufs (`prost` + `protox`). Extend `Ballot` with `uint64 configuration = 1;` (renumbering or adding field cleanly—unshipped). Add `SetFloorRequest { Ballot floor }` → `SetFloorResponse { Ballot floor }` and `ListKeysRequest / ListKeysResponse` on Target `96`. `PrepareResponse` and `AcceptResponse` already carry `promised` / `conflicting_ballot` for floor refusals.
- **Configuration register key:** `{cluster:N}/caspaxos-config` (alongside `{cluster:N}/ledger-map`, `{cluster:N}/node-registry`, `{cluster:N}/membership`).
- **SQLite (`caspaxos/acceptor.db`, `caspaxos/proposer.db`):** Add singleton `acceptor_floor(singleton INTEGER PRIMARY KEY CHECK (singleton = 1), config INTEGER NOT NULL)` in `acceptor.db`, and store `(config, round)` in `proposer.db`. Preserve directory fsync and fail-stop termination.
- **Interplay with `Departure` and periodic repair:** `Departure` drain remains for graceful planned seed retirement; voter reconfiguration replaces the deferred dynamic seed reconfiguration note in `docs/design/fast-caspaxos.md`, and background repair skips keys in configurations below `floor` and targets only `V(c)`.
- **Interplay with Rapid / L3 Membership:** Seed nodes (`clog-caspaxos` voters) are distinct from Rapid cluster members. Rapid commits views into the `{cluster:N}/membership` register over `clog-caspaxos`. During the brief Fence→Activate pause, Rapid view commits wait/retry until Activate, while already-published views and L2 ledger-replica leases (`ST8`) continue unaffected.
- **Verification:** `specs/fast-caspaxos/` (Quint + `check-mutants.sh` + TLC in `specs-storage.yml`), followed later by Rust sans-I/O `stateright` BFS + `turmoil` fault injection + trace replay.

### 13.2 `ReubenBond/fast-caspaxos` (C#)

- **Core (`Orleans.Consensus.CasPaxos`):** Extend `Ballot` to `(long Configuration, int Round, int Proposer)` (lexicographic `CompareTo`, `Zero = default = (0, 0, 0)`). Add `Floor` to `AcceptorState<TValue>` and `AcceptorEngine.SetFloor` / floor-aware `Prepare` and `Accept`. Update `ProposerEngine` for configuration-scoped quorums, Vertical Paxos I hand-off phase 1, and `V(hCfg)`-scoped fast-ballot tallying/viability.
- **Server (`Orleans.Consensus.CasPaxos.Server`):** Add `SetFloorAsync` to `IKeyedAcceptor`; reserve `K_cfg` (e.g., `__caspaxos/config`); implement configuration refresh on floor refusal in `ReplicatedRegisterService` and the 4-step operator driver.
- **SQLite (`Orleans.Consensus.CasPaxos.Sqlite`):** Persist `acceptor_floor` in `SqliteAcceptorServer` (same immediate WAL/`FULL` transactions) and `(configuration, round)` in `SqliteProposerBallotSource`. Extend `SqliteVoterRegistry` to record bootstrap configuration digest and initialize `floor = (c_join, 0, 0)` atomically with new-voter binding.
- **gRPC Host (`Orleans.Consensus.CasPaxos.Grpc`, `.Host`):** Extend `fastcaspaxos.v1.Ballot` with `configuration` and add `SetFloor` RPC. The host already terminates immediately with exit code `74` (`TerminateProcess`/`_exit`) on fatal storage reports so drain and departure barriers never run against failed storage.

### 13.3 `ReubenBond/orleans` (`Orleans.Durable.Storage.CasPaxos`)

- **Hosting & transport:** Expose `SetFloorAsync` on `ICasPaxosVoter` system target and update `VoterSurrogates` for configuration-qualified `Ballot`. `CasPaxosMetaStoreOptions.Voters` becomes the bootstrap configuration `(1, V(1))`; `K_cfg` is authoritative after the first reconfiguration.
- **Admission (`VoterAdmission.cs`):** Extend admission to exchange and verify the bootstrap configuration digest for `c = 1`, and for a replacement voter in `c + 1`, initialize `floor = (c + 1, 0, 0)` before obtaining majority admission from `V(c + 1) \ {self}` and serving.
- **Spec update:** Replace the deferral in `test/Durable.Storage/docs/spec/components/metastore.md §8.1` with a reference to this specification and the Orleans operator tooling.
