# Proposal for Roux Agent: Specify and Formally Model Fast CASPaxos Voter Reconfiguration

> **Model requirement:** Run this task—and any subagents you spawn—on **Gemini Neon (`gemini-4-neon`) with `high` reasoning effort and `long_context`**.

## Context and Objective

We have codified the **Fast CASPaxos voter reconfiguration procedure** (replacing a lost voter, adding/removing voters, or migrating voter sets) so that `ReubenBond/roux`, `ReubenBond/fast-caspaxos`, and `ReubenBond/orleans` share one normative specification and one byte-identical Quint model.

Your task in **`ReubenBond/roux`** (Phase 1) is to:

1. **Adopt the codified specification** into `docs/design/fast-caspaxos.md` (replacing the `"Dynamic seed membership reconfiguration is deferred"` deferral) and make the related Roux design-doc updates.
2. **Implement and verify the Quint formal model changes** in `specs/fast-caspaxos/`, including all new invariants, directed tests, MC configurations, all 7 guard-disabling mutants wired into `specs/check-mutants.sh`, exhaustive TLC checks, `specs/fast-caspaxos/README.md`, and `.github/workflows/specs-storage.yml`.

After your PR passes CI in `ReubenBond/roux`, Phase 2 in `ReubenBond/fast-caspaxos` will copy the validated Quint files byte-identically into `quint/` and implement the C# protocol + trace validation.

**Canonical codified specification (read in full before editing):**
- Branch in `ReubenBond/fast-caspaxos`: `rb-docs-codify-caspaxos-reconfiguration-88d`, file `docs/voter-reconfiguration-spec.md` (local path in fast-caspaxos worktree or fetch from `origin/rb-docs-codify-caspaxos-reconfiguration-88d:docs/voter-reconfiguration-spec.md`).

---

## Core Protocol Rules to Implement (Summary of `docs/voter-reconfiguration-spec.md`)

Do not reopen settled design choices; implement the exact rules below (all vetted by adversarial safety review):

1. **Configuration-qualified ballots:** `Ballot = { config: int, round: int, owner: int }` ordered lexicographically (`config`, `round`, `owner`), with 1-based configuration numbers (`c = 1` is bootstrap; `zero = (0, 0, 0)`). Floor ballots are `(c, 0, 0)` (`c ≥ 1`). Quorums for ballot `b` are counted strictly among `V(b.config)` (`classicQuorum(c) = floor(N(c)/2) + 1`, `fastQuorum(c) = ceil(3*N(c)/4)`).
2. **Acceptor floor:** One durable singleton `floor = (c_floor, 0, 0)` per acceptor. Effective promise for every key (present or unseen) is `effPromised(k) = max(promised[k], floor)`. `SetFloor(target)` is monotonic and idempotent. `Read` and `ListKeys` are ballot-free and remain available when fenced. Exact-duplicate `Accept` replays remain read-only and echo `effPromised(k)` (adding no new vote). New voters introduced in `c_join > 1` persist `floor = (c_join, 0, 0)` (and identity binding/admission) before answering protocol RPCs.
3. **Initial fast ballot:** `(1, 1, 0)` is implicitly prepared **only** at bootstrap `c = 1`. For `c > 1`, `(c, 1, 0)` is never implicitly prepared; a key's first ballot in `c > 1` is a classic ballot `(c, r ≥ 1, owner > 0)` which may piggyback `(c, r + 1, 0)` for 1-RTT steady-state fast writes.
4. **Sequential Vertical Paxos I hand-off:** Configuration `c + 1` begins only after `c` is complete (`complete.contains(c)`). While `c + 1` is `pending` (incomplete), every ballot of `c + 1` is classic `(c + 1, r ≥ 1, owner > 0)`:
   - Phase 1 (`Prepare`) collects promises from `q ⊆ V(c) ∪ V(c + 1)` satisfying **both** `|q ∩ V(c)| ≥ classicQuorum(c)` and `|q ∩ V(c + 1)| ≥ classicQuorum(c + 1)` (safe for replace-1, add-1, remove-1, and arbitrary multi-voter changes).
   - Phase 2 (`Accept`) is sent **only** to `V(c + 1)` (`onlyVotersAccept`) and waits for `classicQuorum(c + 1)` acks from `V(c + 1)`.
   - Recovery (`recoveredBases`) defines `minRelevantCfg = b.config` (when `b.config` is complete) or `b.config - 1` (during hand-off). If `highest.config < minRelevantCfg`, there are zero viable fast winners (`HasConflict = false`) and no historical `V(hCfg)` lookup is needed. Otherwise, fast-ballot tallies and viability (`seenInHCfg`, `remainingInHCfg = |V(hCfg)| - seenInHCfg`) are scoped strictly to `V(hCfg)`.
5. **Dual per-key completion:** Key `k` completes in `c + 1` via either (a) a chosen ballot in `c + 1` (`completeTransferred`), or (b) vacuous absence across a classic quorum `F ⊆ V(c)` with `floor ≥ (c + 1, 0, 0)` and `accepted[k] == zero` (`completeVacuous`).
6. **Four-step idempotent operator procedure:**
   - **Step 1 (Choose):** Classic `CompareExchange` on `K_cfg` (`{cluster:N}/caspaxos-config`) in `c`: `(c, V(c), active)` → `(c + 1, V(c + 1), pending)`.
   - **Step 2 (Fence `c`):** Initialize new voters at `floor = (c + 1, 0, 0)`; `SetFloor((c + 1, 0, 0))` on a classic quorum `F ⊆ V(c)`.
   - **Step 3 (Install):** `ListKeys` on `F`; run Vertical Paxos I identity hand-off for every listed key (including `K_cfg`, transferring `K_cfg` last).
   - **Step 4 (Activate):** Classic `CompareExchange` on `K_cfg` in `c + 1`: `pending` → `active`. On any crash/resume where `K_cfg` is still `pending`, resume from **Step 2 → Step 3 → Step 4**.
7. **Stale-proposer recovery:** On refusal carrying `conflicting_ballot.config > c` (or duplicate ack with `promised.config > c`), clear `c` caches, run ballot-free `Read(K_cfg)` across `V(c)` for candidate hints `(c' > c, V')`, follow the chain, and adopt `(c_cur, V(c_cur))` only after confirming `active` on a quorum of `V(c_cur)`.

---

## Required Deliverables in `ReubenBond/roux`

### 1. Quint Model (`specs/fast-caspaxos/`)

Update `FastCASPaxos.qnt`, `FastCASPaxosMC.qnt`, `FastCASPaxosTests.qnt`, `tlc.json`, and `README.md` per `docs/voter-reconfiguration-spec.md §11`:

- **Extend `FastCASPaxos.qnt`:**
  - Replace `AcceptorCount`/`FastQuorum` with `Configs: int -> Set[int]` and `FastQuorums: int -> int`.
  - Add `config` to `Ballot`, `floor` to `Acceptor`, `effPromised`, `floorBallot(c)`, `complete: Set[int]`, and record `floor` on `PrepareReply` and `(floor, isDuplicate)` on `Ack`.
  - Update `nextBallots(b)` so classic ballots can piggyback fast ballots `(b.config, b.round + 1, 0)` (enabling 1-RTT fast writes in `c > 1`).
  - Update `stateWellFormed` to allow `floorBallots` in `promised`, `durableHistory` to use `effPromised`, and `quorumIntersection` to quantify over `configIds`.
  - Gate `raiseFloor(a, c)`, `prepare(a, b)`, hand-off `issueRecovered`, `completeTransferred(c)`, and `completeVacuous(c)` on `c == 1 or complete.contains(c - 1)`.
  - Gate `accept(a, r)` on `votersOf(r.ballot.config).contains(a)` (`onlyVotersAccept`); leave `prepare(a, b)` ungated on `a ∈ acceptors` so misrouted prepares test `noPreJoinAcceptance`.
  - Add invariants `floorMonotoneAndEffective` and `noPreJoinAcceptance` to `safety` (keeping all 9 existing invariants intact).
- **Add all 7 required mutants (compatible with `specs/check-mutants.sh` using `// breaks: <invariant>`):**
  1. `mutantFloorOnlyHeldKeys` (`// breaks: chosenChain`): `raiseFloor` advances `floor` (so `completeVacuous` fires), while `prepare`/`accept` ignore `floor` when `old.accepted == zero`.
  2. `mutantPhase1InNewConfigBeforeComplete` (`// breaks: chosenChain`): hand-off `issueRecovered` reads only `votersOf(b.config)`.
  3. `mutantHandoffSkipsNewConfigQuorum` (`// breaks: chosenChain`): hand-off `issueRecovered` reads only `votersOf(c - 1)`.
  4. `mutantImplicitFastInNewConfig` (`// breaks: acceptedExtendsChosen`): treats `(c, 1, 0)` as implicitly prepared for `c > 1`.
  5. `mutantNewVoterBeforeFloor` (`// breaks: noPreJoinAcceptance`): new voter (`firstConfig(a) > 1`) initializes with `floor = (1, 0, 0)`.
  6. `mutantUnscopedFastTally` (`// breaks: chosenChain`): disables both the `accept` `votersOf` gate and the `recoveredBases` `V(hCfg)` filter (run on `FastCASPaxosReconfig4Voter` with `MaxRound = 3`).
  7. `mutantSkipSingleChoice` (`// breaks: chosenChain`): allows config `2` hand-off / quorum counting against two conflicting voter sets `V_A = {1, 2, 4}` and `V_B = {1, 3, 4}` simultaneously.
- **Update `FastCASPaxosMC.qnt`:**
  - Keep all 5 existing single-configuration modules (updated to `Configs = Map(1 -> ...)`).
  - Add `FastCASPaxosReconfigReplace` (`1->{1,2,3}, 2->{1,2,4}`), `FastCASPaxosReconfig4Voter` (`1->{1,2,3,5}, 2->{1,2,3,4}`, `MaxRound=3`), `FastCASPaxosReconfigDoubleReplace` (`1->{1,2,3}, 2->{1,2,4}, 3->{1,4,5}`), `FastCASPaxosReconfigAddRemove` (`1->{1,2,3}, 2->{1,2,3,4}, 3->{2,3,4}`), `FastCASPaxosReconfigDisjoint` (`1->{1,2,3}, 2->{4,5,6}`), and `FastCASPaxosReconfigExhaustive` (`1->{1,2}, 2->{1,3}`).
- **Update `FastCASPaxosTests.qnt`:**
  - Update existing tests to the `Configs`/`FastQuorums` constants and add directed reconfiguration tests: committed fast winner across hand-off, tied fast collision across hand-off, vacuous completion for unseen keys, stale proposer holding prepared fast ballot of `c` fenced by floor, exact-duplicate replay below floor, and chained `1 → 2 → 3` double-replace.
- **Wire CI in `.github/workflows/specs-storage.yml` and update `specs/fast-caspaxos/README.md` + `specs/README.md`:**
  - Run typecheck, all directed/negative/boundary tests, all single-config and reconfig simulations, `../check-mutants.sh` against every mutant (verifying each fails its exact invariant), and TLC (`FastCASPaxosExhaustive`, `FastCASPaxosPipeline`, `FastCASPaxosReconfigExhaustive`).

### 2. Design Doc Changes (`docs/design/`)

- **`docs/design/fast-caspaxos.md`:**
  - Replace the `"Dynamic seed membership reconfiguration is deferred"` deferral with the codified voter reconfiguration specification (adopting the normative rules from `docs/voter-reconfiguration-spec.md` §1–§10 verbatim or as a cleanly structured section).
  - **Wire contract (L1 Target `96`, `roux.storage.v1`):** Add `configuration: u64` to `Ballot`; add `SetFloorRequest / SetFloorResponse` and `ListKeysRequest / ListKeysResponse`; document that `PrepareResponse` and `AcceptResponse` refusals carry `conflicting_ballot` (which carries `floor` when fenced).
  - **SQLite (`acceptor.db` & `proposer.db`):** Add singleton `acceptor_floor` table in `acceptor.db` and `(configuration, round)` monotonic reservation in `proposer.db` (persisted before `Prepare` or `Accept` for any classic ballot, including piggybacked classic accepts).
  - **Seed operations:** Document bootstrap validation (digest agreement at startup), permanent retirement of lost `AcceptorId`s, new-voter `floor = (c + 1, 0, 0)` initialization before serving, the 4-step operator procedure, fence-quorum retention and fault tolerance for every `N`, and interplay with `Departure` drain (planned graceful retirement) and periodic repair (skips keys below `floor`, targets `V(c)`).
- **`docs/design/membership.md` & `docs/design/rapid.md`:**
  - Add brief, affirmative cross-references clarifying that `clog-caspaxos` seed voters are distinct from Rapid cluster members, and that during a seed-voter reconfiguration's brief Fence→Activate pause, Rapid `{cluster:N}/membership` register commits wait/retry until Activate while already-published views and L2 ledger-replica leases (`ST8`) continue uninterrupted.

---

## Cross-Repository Sharing Rule

- The four Quint files **`FastCASPaxos.qnt`**, **`FastCASPaxosMC.qnt`**, **`FastCASPaxosTests.qnt`**, and **`tlc.json`** MUST remain **byte-identical** between `ReubenBond/roux` (`specs/fast-caspaxos/`) and `ReubenBond/fast-caspaxos` (`quint/`). Do not introduce Roux-only imports, paths, or conventions inside those four files.
- Keep repository-specific bindings (Target `96`, `roux.storage.v1`, SQLite paths, Rapid interplay) in `docs/design/fast-caspaxos.md` and `specs/fast-caspaxos/README.md`, separate from the shared Quint files.
- If model checking uncovers any refinement to the normative rules, record it clearly in the PR summary so Phase 2 syncs both the Quint files and `docs/voter-reconfiguration-spec.md` together.

## Out of Scope

- No Rust implementation code (`ReubenBond/roux` is docs and formal specifications only).
- Do not modify `ReubenBond/fast-caspaxos` or `ReubenBond/orleans` in this task.

## Acceptance Criteria

1. `quint typecheck` passes on `FastCASPaxosMC.qnt` and `FastCASPaxosTests.qnt`.
2. All existing and new directed, negative, and boundary tests in `FastCASPaxosTests.qnt` pass.
3. All single-config and reconfiguration MC simulations pass `safety` (including `floorMonotoneAndEffective` and `noPreJoinAcceptance`) with zero violations and hit expected witnesses (including fast commits in `c > 1`).
4. All 7 mutants in `specs/fast-caspaxos/` are caught by `specs/check-mutants.sh` violating their exact target invariants.
5. Exhaustive TLC passes on `FastCASPaxosExhaustive`, `FastCASPaxosPipeline`, and `FastCASPaxosReconfigExhaustive`.
6. `.github/workflows/specs-storage.yml` runs all of the above and passes in CI.
7. `docs/design/fast-caspaxos.md`, `docs/design/membership.md`, `docs/design/rapid.md`, `specs/fast-caspaxos/README.md`, and `specs/README.md` are updated and internally consistent, with the dynamic seed reconfiguration deferral removed.
