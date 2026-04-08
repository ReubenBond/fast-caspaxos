Fast CASPaxos TLA+ workspace

Layout
- FastCASPaxos.tla
  Canonical bounded-slot Fast CASPaxos model. It keeps only fast/classic
  ballots, prepare recovery, piggybacked next-ballot preparation (next shared
  fast from fast ballots, same-proposer classic from classic ballots), and
  compact ancestry-based safety/liveness properties.
- MCFastCASPaxosSafety.tla / MCFastCASPaxosSafety.cfg
  Larger 5-acceptor safety sweep for `FastCASPaxos`.
- MCFastCASPaxosLiveness.tla / MCFastCASPaxosLiveness.cfg
  Smaller FairSpec/Liveness smoke for `FastCASPaxos`.
- Run-FastCASPaxos.ps1
  Helper script that runs SANY plus the safety and liveness TLC wrappers.
  Defaults to 32 workers.
- references\
  Vendored upstream examples plus link-only manifests for sources whose
  redistribution terms are still unclear.

Validation
Run the tools from the tla\ directory so local module resolution works.

  C:\tools\tlaplus\sany.cmd .\MCFastCASPaxosSafety.tla
  C:\tools\tlaplus\sany.cmd .\MCFastCASPaxosLiveness.tla
  .\Run-FastCASPaxos.ps1

Individual TLC commands:

  C:\tools\tlaplus\tlc.cmd -workers 32 -checkpoint 0 -config .\MCFastCASPaxosSafety.cfg .\MCFastCASPaxosSafety.tla
  C:\tools\tlaplus\tlc.cmd -workers 32 -checkpoint 0 -config .\MCFastCASPaxosLiveness.cfg .\MCFastCASPaxosLiveness.tla

Notes
- Message channels use slot-based response functions instead of growing sets,
  eliminating the powerset state-space explosion. Each proposer-acceptor pair
  has a single prepare-response slot and accept-response slot, reset on round
  changes. Requests are implicit from proposer state
  (phase, currentBallot, acceptValue, requestedNextBallot).
- A lightweight `messageCount` variable tracks cumulative protocol messages and
  bounds the state space.
- `FastCASPaxos` uses bounded prepare/accept response slots plus a single
  classic accept-request slot per ballot, while the fast path models request
  delivery directly as guarded acceptor responses so TLC does not explore a
  growing message powerset.
- `FastCASPaxos` keeps proposer choices abstract and precomputes bounded value
  reachability so the ancestry invariants do not re-enumerate paths on every
  check.
- Piggyback preparation matches the implementation: fast ballots piggyback only
  the next fast ballot; classic ballots piggyback only the same-proposer
  classic ballot at the next round. This keeps the state space tight without
  over-modelling behaviors the implementation never exercises.
- Safety runs keep state invariants in `Safety` and check transition-local slot
  monotonicity, request/response linkage, newly-chosen value
  reachability/comparability, and single-valued classic/committed outcomes via
  the temporal property `TransitionSafety`.
- `MCFastCASPaxosSafety` uses a safety-only `VIEW` that hides prepare
  response history after a ballot has moved on to an accept request, collapsing
  states that differ only by closed recovery history.
- `MCFastCASPaxosLiveness` uses `FairSpec`, keeps deadlock checking disabled,
  uses a modest step budget plus a smaller constant set, and terminates into a
  wrapper-level quiescent `done` state after the first non-zero commit so
  liveness runs do not keep exploring irrelevant post-success histories.
- `Run-FastCASPaxos.ps1` defaults to 32 workers, disables checkpointing for
  speed, and sets `JAVA_TOOL_OPTIONS=-XX:+UseParallelGC` for TLC runs unless
  the caller already supplied that option, matching TLC's own throughput
  recommendation.
