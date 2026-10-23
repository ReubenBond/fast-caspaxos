# Fast CASPaxos in Quint

`FastCASPaxos.qnt` is an executable, finite-state model of the Fast CASPaxos
register protocol. It checks fast and classic agreement, recovery, prepared-ballot
reuse, and preservation of committed update histories under asynchronous delivery.

## Protocol and abstraction

Acceptors retain `(promised, accepted, value)` durably. Ballots are ordered
lexicographically by `(round, owner)`; owner zero denotes a shared fast ballot.
The initial fast ballot is implicitly prepared. Classic ballots have one immutable
request, representing their owner's exclusive right to choose a value.

`prepare` captures one successful response per acceptor and classic ballot.
`issueRecovered` selects a classic quorum of those snapshots, takes the highest
accepted ballot, and recovers its value. For a fast ballot it counts values at that
ballot and chooses a maximum-frequency value. Ties allow either maximum, matching
the implementation's recovery of split, uncommitted fast rounds. Recovery derives
its base entirely from these responses.

`issuePrepared` uses either the initial value or a certificate containing a
committed source value and a target quorum of matching piggybacked promises.
Fast ballots prepare the next shared ballot; classic ballots optionally prepare
the next ballot belonging to the same owner. Certificates remain available to
model delayed learning and stale proposer knowledge. Acceptor fencing decides
whether subsequent requests can still succeed.

`accept` delivers a request to one acceptor. Successful acknowledgments enter
history, preserving evidence of a choice after acceptors advance to newer ballots.
Quorum counts use distinct acceptor identities. Exact duplicate accepts acknowledge
the current durable value and echo the current promise, including a higher promise.
New accepts compare against **both** promised and accepted ballots; an acceptor may
receive an accept after missing prepare.

Values form a bounded binary tree: `0 -> {1, 2}`, `1 -> {3, 4}`, and so on.
Each proposal either takes one child or preserves its base. These identity updates
model reads and inapplicable CAS operations established through an accept quorum.
Tree ancestry distinguishes diverging histories even when they have the same
length.

Requests and snapshots persist, so the scheduler can reorder delivery, delay a
response arbitrarily, and replay requests. A step combines acceptor processing
and publication of its reply; later proposer actions choose when to use that
reply. Successful prepare snapshots and quorum certificates abstract proposer
knowledge. Rejections, message loss, and crashes that retain durable state are
stuttering steps. The model assumes fixed membership, non-Byzantine participants,
durable acceptor state, unique classic-ballot ownership, and correctly learned
commit certificates.

This is an algorithm-level safety model. Its guarantees cover the modeled state
transitions and finite configurations below. Implementation refinement, client
invocation/response histories, membership changes, and fairness-based liveness
remain separate verification obligations.

## Checked properties

`safety` combines:

| Property | Guarantee |
| --- | --- |
| `quorumIntersection` | Classic quorums intersect; fast quorums intersect each other and satisfy `2 * fast + classic > 2 * N`. |
| `stateWellFormed` | Ballots, values, and participants stay in their finite domains; prepare snapshots precede their recovery ballot. |
| `firstWriteWins` | An acceptor acknowledges one value per ballot. |
| `classicOwnership` | Each classic ballot carries one immutable request. |
| `oneDecisionPerBallot` | Each ballot chooses at most one value. |
| `chosenChain` | Values chosen at increasing ballots extend the earlier chosen histories. |
| `acceptedExtendsChosen` | Every higher-ballot acceptance extends every lower-ballot chosen value. |
| `durableHistory` | Current accepted ballots and promises preserve all prior acknowledgment and prepare evidence. |
| `acknowledgmentProvenance` | Each acknowledgment corresponds to an issued request. |

The ancestry checks use historical votes, so they also cover a lower ballot
reaching quorum after work has started at a higher ballot.

## Reproduce the checks

Use Node.js 22, Quint **0.33.0**, and Java 21 for Apalache verification.
Install the CLI if needed:

```powershell
npm install --global @informalsystems/quint@0.33.0
```

From this directory:

```powershell
quint typecheck .\FastCASPaxosMC.qnt
quint typecheck .\FastCASPaxosTests.qnt
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosTests --max-samples 1 --seed 1
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosNegativeTests --max-samples 1 --seed 1
quint run .\FastCASPaxosMC.qnt --main FastCASPaxosMC --invariant safety --max-samples 2500 --max-steps 60 --n-threads 2 --seed 17
quint verify .\FastCASPaxosMC.qnt --main FastCASPaxosSmall --invariant safety --max-steps 12
```

The simulator defaults to the Rust backend. On Windows, `--backend typescript`
provides a portable alternative for `test` and small `run` checks. GitHub Actions
runs the larger checks on Linux and retains simulation traces and failure
diagnostics as artifacts. Repository-level recipes are `just quint-test`,
`just quint-simulate`, and `just quint-verify`.

| Module | Acceptors | Owners | Rounds | Update depth | Classic / fast quorum | CI check |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `FastCASPaxosSmall` | 3 | 1 | 2 | 1 | 2 / 3 | Simulation and bounded verification through 12 transitions |
| `FastCASPaxosEven` | 4 | 2 | 3 | 2 | 3 / 3 | Simulation |
| `FastCASPaxosMC` | 5 | 2 | 4 | 3 | 3 / 4 | Simulation and scenarios |

Each simulation configuration runs 2,500 traces of up to 60 transitions for each
of seeds `17` and `20261023`. Witness counters report fast commits, classic
commits, successive distinct committed values, and fast collisions. Deterministic
scenarios establish reachability of these paths independently of random sampling.
Apalache explores all executions through its stated transition bound; simulation
samples schedules. Increasing the constants or bounds expands the checked scope.

`FastCASPaxosTests.qnt` includes fast and classic pipelines, committed-fast recovery,
tied collision recovery, identity reads, read-only duplicate handling, stale
prepare snapshots, accept-without-prepare fencing, and same-round owner ordering.
`undersizedFastQuorumBreaksSafetyTest` is a negative control: with an intentionally
unsafe 3-of-5 fast quorum, it constructs two divergent decisions and asserts that
the ancestry invariants detect them.

The model follows the protocol in the repository README and the acceptor/recovery
rules in the C# and Rust implementations. The existing `tla` directory provides
the complementary TLA+ models. Quint language and tool documentation:
<https://quint-lang.org/docs>.
