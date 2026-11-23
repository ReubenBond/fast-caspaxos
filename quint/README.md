# Fast CASPaxos in Quint

`FastCASPaxos.qnt` is an executable, finite-state model of the Fast CASPaxos
register protocol with voter reconfiguration. It checks fast and classic
agreement, recovery, prepared-ballot reuse, acceptor floor fencing, Vertical
Paxos I hand-off across configurations, and preservation of committed update
histories under asynchronous delivery.

## Protocol and abstraction

Acceptors retain `(floor, promised, accepted, value)` durably. Ballots are
ordered lexicographically by `(config, round, owner)`: `(0, 0, 0)` is the zero
ballot, `(c, 0, 0)` is an acceptor floor ballot for configuration `c ≥ 1`,
`(c, r ≥ 1, 0)` is a shared fast ballot, and `(c, r ≥ 1, o ≥ 1)` is a classic
ballot owned by proposer `o`. Only `(1, 1, 0)` is implicitly prepared; keys in
configurations `c > 1` always start with a classic ballot (which may piggyback
`(c, r + 1, 0)` to enable 1-RTT fast writes).

Raising an acceptor's floor to `(c + 1, 0, 0)` via `raiseFloor` raises its
effective promise `max(promised, floor)` across all keys (present and absent).
While configuration `c + 1` is incomplete (`not(complete.contains(c + 1))`),
hand-off `issueRecovered` requires phase-1 promises from both a classic quorum
of `V(c)` and a classic quorum of `V(c + 1)`, while `accept` writes only to
`V(c + 1)`. A key completes in `c + 1` either via a chosen ballot in `c + 1`
(`completeTransferred`) or vacuously when a fenced classic quorum of `V(c)`
holds `accepted == zero` (`completeVacuous`).

`prepare` captures one successful response per acceptor and classic ballot.
`issueRecovered` selects a classic quorum (or Vertical Paxos I hand-off quorum),
takes the highest accepted ballot, and recovers its value. For a fast ballot at
`highest.config >= minRelevantCfg`, it counts votes strictly among voters of
`V(highest.config)` and chooses a maximum-frequency value. Ties allow either
maximum, matching the implementation's recovery of split, uncommitted fast
rounds. Stale unchosen remnants from configurations below `minRelevantCfg` have
zero viable fast winners.

`issuePrepared` uses either the initial value at `(1, 1, 0)` or a certificate
containing a committed source value and a target quorum of matching piggybacked
promises from voters of the same configuration. Fast ballots optionally prepare
the next shared ballot; classic ballots optionally prepare the next shared fast
ballot or the next ballot belonging to the same owner. Certificates remain
available to model delayed learning and stale proposer knowledge. Acceptor
fencing decides whether subsequent requests can still succeed.

`accept` delivers a request to one voter of `V(r.ballot.config)`. Successful
acknowledgments enter history, preserving evidence of a choice after acceptors
advance to newer ballots. Quorum counts use distinct voter identities of
`V(b.config)`. Exact duplicate accepts acknowledge the current durable value
and echo `effPromised`, including a higher promise or floor, without mutating
durable state. New accepts compare against **both** `effPromised` and
`accepted`; an acceptor may receive an accept after missing prepare.

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
| `floorMonotoneAndEffective` | Acceptor floors are monotonic and fence every subsequent prepare and non-duplicate accept; duplicate replays echo `effPromised ≥ floor`. |
| `noPreJoinAcceptance` | Newly introduced voters (`firstConfig(a) > 1`) initialize `floor = (firstConfig(a), 0, 0)` and never promise or accept ballots of earlier configurations. |

The ancestry checks use historical votes, so they also cover a lower ballot
reaching quorum after work has started at a higher ballot.

## Reproduce the checks

Use Node.js 22, Quint **0.33.0**, and Java 21 for model checking.
Install the CLI if needed:

```powershell
npm install --global @informalsystems/quint@0.33.0
```

From this directory:

```powershell
quint typecheck .\FastCASPaxosMC.qnt
quint typecheck .\FastCASPaxosReconfigMC.qnt
quint typecheck .\FastCASPaxosTests.qnt
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosTests --max-samples 1 --seed 1
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosReconfigTests --max-samples 1 --seed 1
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosNegativeTests --max-samples 1 --seed 1
quint test .\FastCASPaxosTests.qnt --main FastCASPaxosBoundaryTests --max-samples 1 --seed 1
quint run .\FastCASPaxosMC.qnt --main FastCASPaxosMC --invariant safety --max-samples 2500 --max-steps 60 --n-threads 2 --seed 17
quint run .\FastCASPaxosReconfigMC.qnt --main FastCASPaxosReconfigReplace --invariant safety --max-samples 2500 --max-steps 60 --n-threads 2 --seed 17
quint verify .\FastCASPaxosMC.qnt --main FastCASPaxosExhaustive --backend tlc --tlc-config .\tlc.json --invariant safety
quint verify .\FastCASPaxosMC.qnt --main FastCASPaxosPipeline --backend tlc --tlc-config .\tlc.json --invariant safety
quint verify .\FastCASPaxosMC.qnt --main FastCASPaxosReconfigExhaustive --backend tlc --tlc-config .\tlc.json --invariant safety
```

## Cross-repository sync (`ReubenBond/roux`)

The five Quint files below are kept byte-identical with `ReubenBond/roux`
(`specs/fast-caspaxos/`, synced at `7c65a97d52b3d10deaa1a9f3f559f17eff9a863f`):

| File | SHA-256 |
| --- | --- |
| `FastCASPaxos.qnt` | `7b1894d0a96724a2237b7bce9343496d7bcb7be1daaef97e1c1fb760d69c760e` |
| `FastCASPaxosMC.qnt` | `aae2ec62e5a3b597020577383f8b57c5692c83112dd39bf92972b1f6a7bd3f40` |
| `FastCASPaxosReconfigMC.qnt` | `c4caa55dea20e9ed8b24d71e464f0cce766a0729c304ae5cfede18258f959a05` |
| `FastCASPaxosTests.qnt` | `a5e4391f560bc6ae4a9b839da9089bb32475a3dbca59f21fb93e28fd2683fe23` |
| `tlc.json` | `e6fed10edb7ff52a5338266ec15193de1d24e07813b69c51de83d5ec49a8891c` |

The simulator defaults to the Rust backend. On Windows, `--backend typescript`
provides a portable alternative for `test` and small `run` checks. GitHub Actions
runs the larger checks on Linux and retains simulation traces and failure
diagnostics as artifacts. Repository-level recipes are `just quint-test`,
`just quint-simulate`, and `just quint-verify`.

Automatic Quint/TLC checks run for changes under `quint/**` or to
`.github/workflows/quint.yml`, on pushes to `main` and on pull requests.
Feature branches use their pull-request diff for formal-check selection, so
ordinary code changes and feature-branch rebases retain their code-only checks.
Use the commands above for local formal checking; `workflow_dispatch` provides
deliberate manual CI runs.

| Module | Acceptors | Owners | Rounds | Update depth | Classic / fast quorum | CI check |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `FastCASPaxosSmall` | 3 | 1 | 2 | 1 | 2 / 3 | Simulation |
| `FastCASPaxosEven` | 4 | 2 | 3 | 2 | 3 / 3 | Simulation |
| `FastCASPaxosMC` | 5 | 2 | 4 | 3 | 3 / 4 | Simulation and scenarios |
| `FastCASPaxosExhaustive` | 3 | 1 | 1 | 1 | 2 / 3 | Complete state-space enumeration: shared ballot and same-round classic recovery |
| `FastCASPaxosPipeline` | 2 | 1 | 2 | 1 | 2 / 2 | Complete state-space enumeration: fast/classic pipelining across two rounds |

Each simulation configuration runs 2,500 traces of up to 60 transitions for each
of seeds `17` and `20261023`. Witness counters report fast commits, classic
commits, successive distinct committed values, and fast collisions. Deterministic
scenarios establish reachability of these paths independently of random sampling.
TLC explores every reachable state of the two finite verification configurations,
including arbitrarily many stuttering and replay steps. Simulation samples
schedules in larger configurations. Increasing the constants expands the checked
scope. The TLC configuration limits the checker to two workers and a 4 GiB heap.
These compact instances make full enumeration practical for per-change CI.

`FastCASPaxosTests.qnt` includes fast and classic pipelines, committed-fast recovery,
tied collision recovery, identity reads, read-only duplicate handling, stale
prepare snapshots, accept-without-prepare fencing, same-round owner ordering, and
same-ballot conflicts at the final round where piggybacking ends.
`undersizedFastQuorumBreaksSafetyTest` is a negative control: with an intentionally
unsafe 3-of-5 fast quorum, it constructs two divergent decisions and asserts that
the ancestry invariants detect them.

The model follows the protocol in the repository README and the acceptor/recovery
rules in the C# and Rust implementations. The existing `tla` directory provides
the complementary TLA+ models. Quint language and tool documentation:
<https://quint-lang.org/docs>.
