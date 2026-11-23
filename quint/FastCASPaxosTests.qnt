module FastCASPaxosTests {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4, 5)),
    FastQuorums = Map(1 -> 4),
    ProposerCount = 2, MaxRound = 4, ValueDepth = 3
  ).* from "./FastCASPaxos"

  pure val f1 = { config: 1, round: 1, owner: 0 }
  pure val f2 = { config: 1, round: 2, owner: 0 }
  pure val f3 = { config: 1, round: 3, owner: 0 }
  pure val c1 = { config: 1, round: 1, owner: 1 }
  pure val c2 = { config: 1, round: 2, owner: 1 }
  pure val c3 = { config: 1, round: 3, owner: 2 }
  pure val fastX = { ballot: f1, value: 1, next: f2 }
  pure val fastY = { ballot: f1, value: 2, next: f2 }

  run fastPipelineTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(3.reps(i => accept(i + 1, fastX)))
    .expect(chosen(f1) == Set() and preparedBases(f2) == Set())
    .then(accept(4, fastX))
    .expect(chosen(f1) == Set(1) and preparedBases(f2) == Set(1))
    .then(issuePrepared(f2, 1, 3, f3))
    .then(4.reps(i => accept(i + 2, { ballot: f2, value: 3, next: f3 })))
    .expect(chosen(f2) == Set(3) and safety)

  run fastWinnerRecoveryTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(issuePrepared(f1, 0, 2, f2))
    .then(4.reps(i => accept(i + 1, fastX)))
    .then(accept(5, fastY))
    .then(3.reps(i => prepare(i + 3, c2)))
    .expect(recoveredBases(c2, Set(3, 4, 5)) == Set(1))
    .then(issueRecovered(c2, Set(3, 4, 5), 1, 3, zero))
    .then(3.reps(i => accept(i + 3, { ballot: c2, value: 3, next: zero })))
    .expect(chosen(f1) == Set(1) and chosen(c2) == Set(3) and safety)

  run tiedCollisionRecoveryTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(issuePrepared(f1, 0, 2, f2))
    .then(accept(1, fastX))
    .then(accept(2, fastY))
    .then(3.reps(i => prepare(i + 1, c2)))
    .expect(chosen(f1) == Set() and recoveredBases(c2, Set(1, 2, 3)) == Set(1, 2))
    .then(issueRecovered(c2, Set(1, 2, 3), 2, 5, zero))
    .then(3.reps(i => accept(i + 1, { ballot: c2, value: 5, next: zero })))
    .expect(chosen(c2) == Set(5) and safety)

  run identityReadPreservesRecoveredValueTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(accept(1, fastX))
    .then(3.reps(i => prepare(i + 1, c2)))
    .then(issueRecovered(c2, Set(1, 2, 3), 1, 1, zero))
    .then(3.reps(i => accept(i + 1, { ballot: c2, value: 1, next: zero })))
    .expect(chosen(c2) == Set(1))
    .then(3.reps(i => prepare(i + 3, c3)))
    .expect(recoveredBases(c3, Set(3, 4, 5)) == Set(1))
    .then(issueRecovered(c3, Set(3, 4, 5), 1, 3, zero))
    .then(3.reps(i => accept(i + 3, { ballot: c3, value: 3, next: zero })))
    .expect(chosen(c3) == Set(3) and safety)

  run duplicateAfterHigherPromiseTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(issuePrepared(f1, 0, 2, f2))
    .then(accept(1, fastX))
    .then(prepare(1, c3))
    .then(accept(1, fastX))
    .then(accept(1, fastX))
    .expect(and {
      nodes.get(1) == { floor: floorBallot(1), promised: c3, accepted: f1, value: 1 },
      acks.contains({ ballot: f1, acceptor: 1, value: 1, promised: c3, floor: floorBallot(1), isDuplicate: true }),
      voters(f1, 1) == Set(1),
      preparedBases(f2) == Set(),
      not(canAccept(1, fastY)),
      safety,
    })

  run classicPipelineTest = init
    .then(3.reps(i => prepare(i + 1, c1)))
    .then(issueRecovered(c1, Set(1, 2, 3), 0, 1, c2))
    .then(3.reps(i => accept(i + 1, { ballot: c1, value: 1, next: c2 })))
    .expect(chosen(c1) == Set(1) and preparedBases(c2) == Set(1) and not(canIssue(c1)))
    .then(issuePrepared(c2, 1, 3, zero))
    .then(3.reps(i => accept(i + 3, { ballot: c2, value: 3, next: zero })))
    .expect(chosen(c2) == Set(3) and safety)

  run stalePrepareSnapshotsAreFencedTest = init
    .then(3.reps(i => prepare(i + 1, c2)))
    .then(3.reps(i => prepare(i + 2, c3)))
    .then(issueRecovered(c3, Set(2, 3, 4), 0, 2, zero))
    .then(3.reps(i => accept(i + 2, { ballot: c3, value: 2, next: zero })))
    .then(issueRecovered(c2, Set(1, 2, 3), 0, 1, zero))
    .then(accept(1, { ballot: c2, value: 1, next: zero }))
    .then(accept(5, { ballot: c2, value: 1, next: zero }))
    .expect(and {
      not(canAccept(2, { ballot: c2, value: 1, next: zero })),
      not(canAccept(3, { ballot: c2, value: 1, next: zero })),
      not(canAccept(4, { ballot: c2, value: 1, next: zero })),
      chosen(c2) == Set(), chosen(c3) == Set(2), safety,
    })

  run acceptWithoutPrepareFencesOlderBallotsTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(3.reps(i => prepare(i + 1, c2)))
    .then(issueRecovered(c2, Set(1, 2, 3), 0, 2, zero))
    .then(accept(5, { ballot: c2, value: 2, next: zero }))
    .expect(and {
      nodes.get(5) == { floor: floorBallot(1), promised: f1, accepted: c2, value: 2 },
      not(canAccept(5, fastX)), safety,
    })
    .then(prepare(5, c1).fail())

  run sameRoundOwnersAreOrderedTest = init
    .then(3.reps(i => prepare(i + 1, c1)))
    .then(issueRecovered(c1, Set(1, 2, 3), 0, 1, zero))
    .then(3.reps(i => accept(i + 1, { ballot: c1, value: 1, next: zero })))
    .then(3.reps(i => prepare(i + 3, { config: 1, round: 1, owner: 2 })))
    .expect(recoveredBases({ config: 1, round: 1, owner: 2 }, Set(3, 4, 5)) == Set(1) and safety)
}

module FastCASPaxosNegativeTests {
  // Deliberately violate the fast/classic triple-intersection requirement.
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4, 5)),
    FastQuorums = Map(1 -> 3),
    ProposerCount = 1, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"

  pure val f1 = { config: 1, round: 1, owner: 0 }
  pure val f2 = { config: 1, round: 2, owner: 0 }
  pure val c2 = { config: 1, round: 2, owner: 1 }

  run undersizedFastQuorumBreaksSafetyTest = init
    .then(issuePrepared(f1, 0, 1, f2))
    .then(issuePrepared(f1, 0, 2, f2))
    .then(3.reps(i => accept(i + 1, { ballot: f1, value: 1, next: f2 })))
    .then(2.reps(i => accept(i + 4, { ballot: f1, value: 2, next: f2 })))
    .then(3.reps(i => prepare(i + 3, c2)))
    .expect(chosen(f1) == Set(1) and recoveredBases(c2, Set(3, 4, 5)) == Set(2))
    .then(issueRecovered(c2, Set(3, 4, 5), 2, 5, zero))
    .then(3.reps(i => accept(i + 3, { ballot: c2, value: 5, next: zero })))
    .expect(chosen(c2) == Set(5) and not(chosenChain) and not(acceptedExtendsChosen))
}

module FastCASPaxosBoundaryTests {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3)),
    FastQuorums = Map(1 -> 3),
    ProposerCount = 1, MaxRound = 1, ValueDepth = 1
  ).* from "./FastCASPaxos"

  run sameBallotConflictWithoutPiggybackTest = init
    .then(issuePrepared(initialFast, 0, 1, zero))
    .then(issuePrepared(initialFast, 0, 2, zero))
    .then(accept(1, { ballot: initialFast, value: 1, next: zero }))
    .then(accept(1, { ballot: initialFast, value: 1, next: zero }))
    .expect(and {
      nodes.get(1) == { floor: floorBallot(1), promised: initialFast, accepted: initialFast, value: 1 },
      voters(initialFast, 1) == Set(1),
      not(canAccept(1, { ballot: initialFast, value: 2, next: zero })),
      safety,
    })
}

module FastCASPaxosReconfigTests {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4), 3 -> Set(1, 4, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 3
  ).* from "./FastCASPaxos"

  pure val c1f1 = { config: 1, round: 1, owner: 0 }
  pure val c1f2 = { config: 1, round: 2, owner: 0 }
  pure val c1c1 = { config: 1, round: 1, owner: 1 }
  pure val c2c1 = { config: 2, round: 1, owner: 1 }
  pure val c2f2 = { config: 2, round: 2, owner: 0 }
  pure val c3c1 = { config: 3, round: 1, owner: 1 }
  pure val fast1X = { ballot: c1f1, value: 1, next: c1f2 }
  pure val fast1Y = { ballot: c1f1, value: 2, next: c1f2 }

  run fastWinnerHandoffTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, fast1X))
    .then(accept(2, fast1X))
    .then(accept(3, fast1X))
    .expect(chosen(c1f1) == Set(1))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(prepare(1, c2c1))
    .then(prepare(2, c2c1))
    .then(prepare(4, c2c1))
    .expect(recoveredBases(c2c1, Set(1, 2, 4)) == Set(1))
    .then(issueRecovered(c2c1, Set(1, 2, 4), 1, 1, c2f2))
    .then(accept(1, { ballot: c2c1, value: 1, next: c2f2 }))
    .then(accept(2, { ballot: c2c1, value: 1, next: c2f2 }))
    .then(accept(4, { ballot: c2c1, value: 1, next: c2f2 }))
    .then(completeTransferred(2))
    .expect(complete == Set(1, 2) and chosen(c2c1) == Set(1) and preparedBases(c2f2) == Set(1) and safety)
    .then(issuePrepared(c2f2, 1, 3, zero))
    .then(accept(1, { ballot: c2f2, value: 3, next: zero }))
    .then(accept(2, { ballot: c2f2, value: 3, next: zero }))
    .then(accept(4, { ballot: c2f2, value: 3, next: zero }))
    .expect(chosen(c2f2) == Set(3) and safety)

  run tiedFastCollisionHandoffTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(issuePrepared(c1f1, 0, 2, c1f2))
    .then(accept(1, fast1X))
    .then(accept(2, fast1Y))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(prepare(1, c2c1))
    .then(prepare(2, c2c1))
    .then(prepare(4, c2c1))
    .expect(chosen(c1f1) == Set() and recoveredBases(c2c1, Set(1, 2, 4)) == Set(1, 2))
    .then(issueRecovered(c2c1, Set(1, 2, 4), 2, 2, zero))
    .then(accept(1, { ballot: c2c1, value: 2, next: zero }))
    .then(accept(4, { ballot: c2c1, value: 2, next: zero }))
    .then(completeTransferred(2))
    .expect(chosen(c2c1) == Set(2) and safety)

  run vacuousCompletionForUnseenKeyTest = init
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(completeVacuous(2))
    .expect(complete == Set(1, 2) and not(canAccept(1, fast1X)) and not(canAccept(2, fast1X)))
    .then(prepare(1, c2c1))
    .then(prepare(4, c2c1))
    .then(issueRecovered(c2c1, Set(1, 4), 0, 1, c2f2))
    .then(accept(1, { ballot: c2c1, value: 1, next: c2f2 }))
    .then(accept(4, { ballot: c2c1, value: 1, next: c2f2 }))
    .expect(chosen(c2c1) == Set(1) and safety)

  run stalePreparedFastBallotFencedByFloorTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, fast1X))
    .then(accept(2, fast1X))
    .then(accept(3, fast1X))
    .expect(preparedBases(c1f2) == Set(1))
    .then(issuePrepared(c1f2, 1, 3, zero))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .expect(and {
      not(canAccept(1, { ballot: c1f2, value: 3, next: zero })),
      not(canAccept(2, { ballot: c1f2, value: 3, next: zero })),
      canAccept(3, { ballot: c1f2, value: 3, next: zero }),
    })
    .then(accept(3, { ballot: c1f2, value: 3, next: zero }))
    .expect(chosen(c1f2) == Set() and safety)

  run duplicateReplayBelowFloorTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, fast1X))
    .then(raiseFloor(1, 2))
    .then(accept(1, fast1X))
    .expect(and {
      nodes.get(1) == { floor: floorBallot(2), promised: c1f2, accepted: c1f1, value: 1 },
      acks.contains({ ballot: c1f1, acceptor: 1, value: 1, promised: floorBallot(2), floor: floorBallot(2), isDuplicate: true }),
      not(canAccept(1, fast1Y)),
      safety,
    })

  run chainedReconfigurationTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, fast1X))
    .then(accept(2, fast1X))
    .then(accept(3, fast1X))
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(prepare(1, c2c1))
    .then(prepare(2, c2c1))
    .then(prepare(4, c2c1))
    .then(issueRecovered(c2c1, Set(1, 2, 4), 1, 3, zero))
    .then(accept(1, { ballot: c2c1, value: 3, next: zero }))
    .then(accept(4, { ballot: c2c1, value: 3, next: zero }))
    .then(completeTransferred(2))
    .then(raiseFloor(1, 3))
    .then(raiseFloor(4, 3))
    .then(prepare(1, c3c1))
    .then(prepare(4, c3c1))
    .then(prepare(5, c3c1))
    .expect(recoveredBases(c3c1, Set(1, 4, 5)) == Set(3))
    .then(issueRecovered(c3c1, Set(1, 4, 5), 3, 7, zero))
    .then(accept(1, { ballot: c3c1, value: 7, next: zero }))
    .then(accept(5, { ballot: c3c1, value: 7, next: zero }))
    .then(completeTransferred(3))
    .expect(complete == Set(1, 2, 3) and chosen(c3c1) == Set(7) and safety)

  run newVoterRejectsPreJoinPrepareConfig1Test = init
    .then(prepare(4, c1c1).fail())

  run newVoterRejectsPreJoinPrepareConfig2Test = init
    .then(prepare(5, c2c1).fail())
}

module FastCASPaxosReconfig4VoterTests {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 5), 2 -> Set(1, 2, 3, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 1, MaxRound = 3, ValueDepth = 1
  ).* from "./FastCASPaxos"

  pure val c2c1 = { config: 2, round: 1, owner: 1 }
  pure val c2f2 = { config: 2, round: 2, owner: 0 }
  pure val c2c3 = { config: 2, round: 3, owner: 1 }

  // Negative control proving that disabling votersOf scoping in accept and recoveredBases
  // allows retired voter 5's fast vote to tie a 3-of-4 fast winner in config 2.
  run unscopedFastTallyBreaksSafetyTest = init
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    .then(completeVacuous(2))
    .then(prepare(1, c2c1))
    .then(prepare(2, c2c1))
    .then(prepare(3, c2c1))
    .then(issueRecovered(c2c1, Set(1, 2, 3), 0, 0, c2f2))
    .then(accept(1, { ballot: c2c1, value: 0, next: c2f2 }))
    .then(accept(2, { ballot: c2c1, value: 0, next: c2f2 }))
    .then(accept(3, { ballot: c2c1, value: 0, next: c2f2 }))
    .expect(preparedBases(c2f2) == Set(0))
    .then(issuePrepared(c2f2, 0, 1, zero))
    .then(issuePrepared(c2f2, 0, 2, zero))
    .then(acceptUnscoped(1, { ballot: c2f2, value: 1, next: zero }))
    .then(acceptUnscoped(2, { ballot: c2f2, value: 1, next: zero }))
    .then(acceptUnscoped(3, { ballot: c2f2, value: 1, next: zero }))
    .then(acceptUnscoped(4, { ballot: c2f2, value: 2, next: zero }))
    .then(acceptUnscoped(5, { ballot: c2f2, value: 2, next: zero }))
    .expect(chosen(c2f2) == Set(1))
    .then(prepare(1, c2c3))
    .then(prepare(2, c2c3))
    .then(prepare(4, c2c3))
    .then(prepare(5, c2c3))
    // Scoped recovery ignores retired voter 5 and strictly recovers 1;
    // unscoped recovery ties 1 (voters 1,2) with 2 (voters 4,5) and breaks chosenChain.
    .expect(and {
      recoveredBases(c2c3, Set(1, 2, 4, 5)) == Set(1),
      recoveredBasesUnscoped(c2c3, Set(1, 2, 4, 5)) == Set(1, 2),
    })
    .then(issueRecoveredUnscoped(c2c3, Set(1, 2, 4, 5), 2, 2, zero))
    .then(acceptUnscoped(1, { ballot: c2c3, value: 2, next: zero }))
    .then(acceptUnscoped(2, { ballot: c2c3, value: 2, next: zero }))
    .then(acceptUnscoped(4, { ballot: c2c3, value: 2, next: zero }))
    .expect(chosen(c2f2) == Set(1) and chosen(c2c3) == Set(2) and not(chosenChain))
}
