// Auto-generated from Clockwork deterministic reconfiguration simulation traces (spec §11.5).
// Replays concrete per-key C# protocol traces directly against the unmodified FastCASPaxos.qnt model.

module TraceReplaceLostVoter {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run replaceLostVoterTraceTest = init
    .then(issuePrepared({ config: 1, round: 1, owner: 0 }, 0, 1, { config: 1, round: 2, owner: 0 }))
    .then(accept(2, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(3, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(1, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(1, 2))
    .then(prepare(2, { config: 2, round: 1, owner: 1 }))
    .then(prepare(4, { config: 2, round: 1, owner: 1 }))
    .then(prepare(1, { config: 2, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 1, owner: 1 }, Set(1, 2, 4), 1, 1, zero))
    .then(accept(1, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(accept(4, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(completeTransferred(2))
    .then(accept(2, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(issuePrepared({ config: 1, round: 2, owner: 0 }, 1, 3, { config: 1, round: 3, owner: 0 }))
    .then(prepare(2, { config: 2, round: 2, owner: 2 }))
    .then(prepare(1, { config: 2, round: 2, owner: 2 }))
    .then(prepare(4, { config: 2, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 2, round: 2, owner: 2 }, Set(1, 2), 1, 3, { config: 2, round: 3, owner: 0 }))
    .then(accept(2, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(4, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .expect(safety)
}

module TracePostReconfigFastCommit {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4), 2 -> Set(1, 2, 3, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 1, MaxRound = 4, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run postReconfigFastCommitTraceTest = init
    .then(raiseFloor(4, 2))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(3, 2))
    .then(raiseFloor(2, 2))
    .then(completeVacuous(2))
    .then(prepare(3, { config: 2, round: 2, owner: 1 }))
    .then(prepare(5, { config: 2, round: 2, owner: 1 }))
    .then(prepare(1, { config: 2, round: 2, owner: 1 }))
    .then(prepare(2, { config: 2, round: 2, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 2, owner: 1 }, Set(1, 3, 5), 0, 1, { config: 2, round: 3, owner: 0 }))
    .then(accept(5, { ballot: { config: 2, round: 2, owner: 1 }, value: 1, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(2, { ballot: { config: 2, round: 2, owner: 1 }, value: 1, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(3, { ballot: { config: 2, round: 2, owner: 1 }, value: 1, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 2, owner: 1 }, value: 1, next: { config: 2, round: 3, owner: 0 } }))
    .then(issuePrepared({ config: 2, round: 3, owner: 0 }, 1, 3, { config: 2, round: 4, owner: 0 }))
    .then(accept(2, { ballot: { config: 2, round: 3, owner: 0 }, value: 3, next: { config: 2, round: 4, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 3, owner: 0 }, value: 3, next: { config: 2, round: 4, owner: 0 } }))
    .then(accept(5, { ballot: { config: 2, round: 3, owner: 0 }, value: 3, next: { config: 2, round: 4, owner: 0 } }))
    .then(accept(3, { ballot: { config: 2, round: 3, owner: 0 }, value: 3, next: { config: 2, round: 4, owner: 0 } }))
    .expect(safety)
}

module TraceChainedDoubleReplace {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4), 3 -> Set(1, 4, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 3
  ).* from "./FastCASPaxos"

  run chainedDoubleReplaceTraceTest = init
    .then(issuePrepared({ config: 1, round: 1, owner: 0 }, 0, 1, { config: 1, round: 2, owner: 0 }))
    .then(accept(1, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(2, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(3, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(prepare(4, { config: 2, round: 1, owner: 1 }))
    .then(prepare(1, { config: 2, round: 1, owner: 1 }))
    .then(prepare(2, { config: 2, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 1, owner: 1 }, Set(1, 2), 1, 1, zero))
    .then(accept(2, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(accept(1, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(completeTransferred(2))
    .then(accept(4, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(issuePrepared({ config: 1, round: 2, owner: 0 }, 1, 3, { config: 1, round: 3, owner: 0 }))
    .then(prepare(1, { config: 2, round: 2, owner: 2 }))
    .then(prepare(2, { config: 2, round: 2, owner: 2 }))
    .then(prepare(4, { config: 2, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 2, round: 2, owner: 2 }, Set(1, 2), 1, 3, { config: 2, round: 3, owner: 0 }))
    .then(accept(2, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(4, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(raiseFloor(1, 3))
    .then(raiseFloor(4, 3))
    .then(prepare(5, { config: 3, round: 1, owner: 1 }))
    .then(prepare(1, { config: 3, round: 1, owner: 1 }))
    .then(prepare(4, { config: 3, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 3, round: 1, owner: 1 }, Set(1, 4, 5), 3, 3, zero))
    .then(accept(1, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(accept(4, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(completeTransferred(3))
    .then(accept(5, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(prepare(4, { config: 3, round: 2, owner: 2 }))
    .then(prepare(1, { config: 3, round: 2, owner: 2 }))
    .then(prepare(5, { config: 3, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 3, round: 2, owner: 2 }, Set(1, 4), 3, 7, { config: 3, round: 3, owner: 0 }))
    .then(accept(1, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .then(accept(5, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .then(accept(4, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .expect(safety)
}

module TraceAddRemove {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 3, 4), 3 -> Set(2, 3, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 4, ValueDepth = 3
  ).* from "./FastCASPaxos"

  run addRemoveTraceTest = init
    .then(issuePrepared({ config: 1, round: 1, owner: 0 }, 0, 1, { config: 1, round: 2, owner: 0 }))
    .then(accept(1, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(2, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(3, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    .then(raiseFloor(1, 2))
    .then(prepare(3, { config: 2, round: 1, owner: 1 }))
    .then(prepare(1, { config: 2, round: 1, owner: 1 }))
    .then(prepare(4, { config: 2, round: 1, owner: 1 }))
    .then(prepare(2, { config: 2, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 1, owner: 1 }, Set(1, 3, 4), 1, 1, zero))
    .then(accept(1, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(accept(2, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(accept(3, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(completeTransferred(2))
    .then(accept(4, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(issuePrepared({ config: 1, round: 2, owner: 0 }, 1, 3, { config: 1, round: 3, owner: 0 }))
    .then(prepare(2, { config: 2, round: 2, owner: 2 }))
    .then(prepare(1, { config: 2, round: 2, owner: 2 }))
    .then(prepare(3, { config: 2, round: 2, owner: 2 }))
    .then(prepare(4, { config: 2, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 2, round: 2, owner: 2 }, Set(1, 2, 3), 1, 3, { config: 2, round: 3, owner: 0 }))
    .then(accept(3, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(4, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(2, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(raiseFloor(1, 3))
    .then(raiseFloor(3, 3))
    .then(raiseFloor(4, 3))
    .then(raiseFloor(2, 3))
    .then(prepare(4, { config: 3, round: 1, owner: 1 }))
    .then(prepare(3, { config: 3, round: 1, owner: 1 }))
    .then(prepare(2, { config: 3, round: 1, owner: 1 }))
    .then(prepare(1, { config: 3, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 3, round: 1, owner: 1 }, Set(2, 3, 4), 3, 3, zero))
    .then(accept(2, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(accept(4, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(completeTransferred(3))
    .then(accept(3, { ballot: { config: 3, round: 1, owner: 1 }, value: 3, next: zero }))
    .then(issuePrepared({ config: 2, round: 3, owner: 0 }, 3, 7, { config: 2, round: 4, owner: 0 }))
    .then(prepare(2, { config: 3, round: 2, owner: 2 }))
    .then(prepare(3, { config: 3, round: 2, owner: 2 }))
    .then(prepare(4, { config: 3, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 3, round: 2, owner: 2 }, Set(2, 3), 3, 7, { config: 3, round: 3, owner: 0 }))
    .then(accept(4, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .then(accept(2, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .then(accept(3, { ballot: { config: 3, round: 2, owner: 2 }, value: 7, next: { config: 3, round: 3, owner: 0 } }))
    .expect(safety)
}

module TraceStaleFastFenced {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run staleFastFencedTraceTest = init
    .then(issuePrepared({ config: 1, round: 1, owner: 0 }, 0, 1, { config: 1, round: 2, owner: 0 }))
    .then(accept(2, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(3, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(1, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    .then(prepare(4, { config: 2, round: 1, owner: 1 }))
    .then(prepare(2, { config: 2, round: 1, owner: 1 }))
    .then(prepare(3, { config: 2, round: 1, owner: 1 }))
    .then(prepare(1, { config: 2, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 1, owner: 1 }, Set(2, 3, 4), 1, 1, zero))
    .then(accept(1, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(accept(2, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(completeTransferred(2))
    .then(accept(4, { ballot: { config: 2, round: 1, owner: 1 }, value: 1, next: zero }))
    .then(issuePrepared({ config: 1, round: 2, owner: 0 }, 1, 3, { config: 1, round: 3, owner: 0 }))
    .then(prepare(2, { config: 2, round: 2, owner: 2 }))
    .then(prepare(1, { config: 2, round: 2, owner: 2 }))
    .then(prepare(4, { config: 2, round: 2, owner: 2 }))
    .then(issueRecovered({ config: 2, round: 2, owner: 2 }, Set(1, 4), 1, 3, { config: 2, round: 3, owner: 0 }))
    .then(accept(4, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(2, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .then(accept(1, { ballot: { config: 2, round: 2, owner: 2 }, value: 3, next: { config: 2, round: 3, owner: 0 } }))
    .expect(safety)
}

// Negative control: Omitting raiseFloor(_, 2) makes completeVacuous(2) illegal in FastCASPaxos.qnt.
module TraceNegativeSkipFloorVacuous {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4), 2 -> Set(1, 2, 3, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 1, MaxRound = 4, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run skipFloorBreaksVacuousCompletionTraceTest = init
    .then(completeVacuous(2).fail())
}

// Negative control: Hand-off issueRecovered without classic quorum intersection in V(1) is rejected by FastCASPaxos.qnt.
module TraceNegativeIllegalHandoffQuorum {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run handoffMissingOldQuorumFailsTraceTest = init
    .then(issuePrepared({ config: 1, round: 1, owner: 0 }, 0, 1, { config: 1, round: 2, owner: 0 }))
    .then(accept(2, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(3, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(accept(1, { ballot: { config: 1, round: 1, owner: 0 }, value: 1, next: { config: 1, round: 2, owner: 0 } }))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(1, 2))
    .then(prepare(2, { config: 2, round: 1, owner: 1 }))
    .then(prepare(4, { config: 2, round: 1, owner: 1 }))
    .then(prepare(1, { config: 2, round: 1, owner: 1 }))
    .then(issueRecovered({ config: 2, round: 1, owner: 1 }, Set(4), 1, 1, zero).fail())
}

// Negative control: New voter 4 (firstConfig = 2) has initial floor (2,0,0) and rejects prepare in c = 1.
module TraceNegativePreJoinPrepare {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run newVoterRefusesPreJoinPrepareTraceTest = init
    .then(prepare(4, { config: 1, round: 1, owner: 1 }).fail())
}

// Negative control: Implicit fast ballot (2, 1, 0) is not prepared in c > 1; issuePrepared must fail.
module TraceNegativeImplicitFastInNewConfig {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4), 2 -> Set(1, 2, 3, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 1, MaxRound = 4, ValueDepth = 2
  ).* from "./FastCASPaxos"

  run implicitFastInConfigTwoFailsTraceTest = init
    .then(raiseFloor(4, 2))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(3, 2))
    .then(raiseFloor(2, 2))
    .then(completeVacuous(2))
    .then(issuePrepared({ config: 2, round: 1, owner: 0 }, 0, 1, { config: 2, round: 2, owner: 0 }).fail())
}

