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
      acks.contains({
        ballot: f1, acceptor: 1, value: 1, promised: c3, floor: floorBallot(1), isDuplicate: true,
      }),
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

module FastCASPaxosReconfigTests {
  import FastCASPaxos(
    Configs = Map(
      1 -> Set(1, 2, 3, 4),
      2 -> Set(1, 2, 3, 5),
      3 -> Set(1, 2, 5, 6),
    ),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 3
  ).* from "./FastCASPaxos"

  pure val c1f1 = { config: 1, round: 1, owner: 0 }
  pure val c1f2 = { config: 1, round: 2, owner: 0 }
  pure val c1f3 = { config: 1, round: 3, owner: 0 }
  pure val c1p1 = { config: 1, round: 1, owner: 1 }
  pure val c2p1 = { config: 2, round: 1, owner: 1 }
  pure val c2p2 = { config: 2, round: 1, owner: 2 }
  pure val c2f2 = { config: 2, round: 2, owner: 0 }
  pure val c2f3 = { config: 2, round: 3, owner: 0 }
  pure val c3p1 = { config: 3, round: 1, owner: 1 }
  pure val c3f2 = { config: 3, round: 2, owner: 0 }
  pure val c3f3 = { config: 3, round: 3, owner: 0 }

  // Committed fast winner in c=1 (3 of 4) is recovered across Vertical Paxos I hand-off
  // into c=2, and classic hand-off piggybacks (2, 2, 0) for 1-RTT fast writes in c=2.
  run fastWinnerAcrossHandoffTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(issuePrepared(c1f1, 0, 2, c1f2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(2, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(3, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(4, { ballot: c1f1, value: 2, next: c1f2 }))
    .expect(chosen(c1f1) == Set(1))
    // Fence classic quorum {1, 2, 3} of V(1) at floor (2, 0, 0)
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    // Hand-off phase 1 intersects V(1)={1,2,3,4} (3/4) and V(2)={1,2,3,5} (3/4)
    .then(prepare(2, c2p1))
    .then(prepare(3, c2p1))
    .then(prepare(4, c2p1))
    .then(prepare(5, c2p1))
    .expect(recoveredBases(c2p1, Set(2, 3, 4, 5)) == Set(1))
    // Hand-off identity transfer piggybacks (2, 2, 0)
    .then(issueRecovered(c2p1, Set(2, 3, 4, 5), 1, 1, c2f2))
    // Retired voter 4 cannot accept config 2 request
    .expect(not(canAccept(4, { ballot: c2p1, value: 1, next: c2f2 })))
    .then(accept(1, { ballot: c2p1, value: 1, next: c2f2 }))
    .then(accept(2, { ballot: c2p1, value: 1, next: c2f2 }))
    .then(accept(5, { ballot: c2p1, value: 1, next: c2f2 }))
    .expect(chosen(c2p1) == Set(1) and preparedBases(c2f2) == Set(1) and not(complete.contains(2)))
    .then(completeTransferred(2))
    // After completion, 1-RTT fast write succeeds in config 2
    .then(issuePrepared(c2f2, 1, 3, c2f3))
    .then(accept(1, { ballot: c2f2, value: 3, next: c2f3 }))
    .then(accept(2, { ballot: c2f2, value: 3, next: c2f3 }))
    .then(accept(5, { ballot: c2f2, value: 3, next: c2f3 }))
    .expect(chosen(c2f2) == Set(3) and reconfigFastCommit and safety)

  // New voter 5 initializes with floor (2, 0, 0) and refuses pre-join prepares in c=1.
  run newVoterRefusesPreJoinPrepareTest = init
    .expect(nodes.get(5).floor == floorBallot(2))
    .then(prepare(5, c1p1).fail())

  // Fast ballot (2, 2, 0) piggybacked on hand-off cannot be issued before c=2 completes.
  run fastBallotBlockedBeforeConfigCompleteTest = init
    .then(prepare(1, c2p1))
    .then(prepare(2, c2p1))
    .then(prepare(3, c2p1))
    .then(prepare(5, c2p1))
    .then(issueRecovered(c2p1, Set(1, 2, 3, 5), 0, 1, c2f2))
    .then(accept(1, { ballot: c2p1, value: 1, next: c2f2 }))
    .then(accept(2, { ballot: c2p1, value: 1, next: c2f2 }))
    .then(accept(5, { ballot: c2p1, value: 1, next: c2f2 }))
    .expect(preparedBases(c2f2) == Set(1) and not(complete.contains(2)))
    .then(issuePrepared(c2f2, 1, 3, c2f3).fail())

  // Sequentiality gate: config 3 floor/prepare cannot begin until config 2 completes.
  run sequentialConfigGateRefusesSkipTest = init
    .expect(complete == Set(1))
    .then(raiseFloor(1, 3).fail())

  // Tied fast collision in c=1 resolves deterministically across hand-off, and
  // a competing hand-off ballot sees the first hand-off's choice via V(2) intersection.
  run tiedFastCollisionAcrossHandoffTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(issuePrepared(c1f1, 0, 2, c1f2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(4, { ballot: c1f1, value: 2, next: c1f2 }))
    .expect(chosen(c1f1) == Set())
    .then(prepare(1, c2p1))
    .then(prepare(2, c2p1))
    .then(prepare(4, c2p1))
    .then(prepare(5, c2p1))
    .expect(recoveredBases(c2p1, Set(1, 2, 4, 5)) == Set(1, 2))
    .then(issueRecovered(c2p1, Set(1, 2, 4, 5), 2, 2, zero))
    .then(accept(1, { ballot: c2p1, value: 2, next: zero }))
    .then(accept(2, { ballot: c2p1, value: 2, next: zero }))
    .then(accept(5, { ballot: c2p1, value: 2, next: zero }))
    .expect(chosen(c2p1) == Set(2))
    // Second racing hand-off ballot must intersect V(2) and recover 2
    .then(prepare(2, c2p2))
    .then(prepare(3, c2p2))
    .then(prepare(4, c2p2))
    .then(prepare(5, c2p2))
    .expect(recoveredBases(c2p2, Set(2, 3, 4, 5)) == Set(2))
    .then(issueRecovered(c2p2, Set(2, 3, 4, 5), 2, 5, zero))
    .then(accept(2, { ballot: c2p2, value: 5, next: zero }))
    .then(accept(3, { ballot: c2p2, value: 5, next: zero }))
    .then(accept(5, { ballot: c2p2, value: 5, next: zero }))
    .expect(chosen(c2p2) == Set(5) and safety)

  // Unseen key completes vacuously once a classic quorum of V(1) holds floor >= (2,0,0)
  // with accepted == zero; stale unchosen remnant on unfenced voter is handled safely.
  run vacuousCompletionForUnseenKeyTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    // Voter 1 accepted an unchosen fast proposal; {2, 3, 4} never saw it
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    .then(raiseFloor(4, 2))
    .then(completeVacuous(2))
    .expect(complete == Set(1, 2))
    // First ballot in c=2 must be classic (implicit (2,1,0) is not prepared)
    .expect(preparedBases({ config: 2, round: 1, owner: 0 }) == Set())
    // Classic prepare in c=2 hits voter 1 (carrying unchosen c=1 remnant) plus 2 and 5
    .then(prepare(1, c2p1))
    .then(prepare(2, c2p1))
    .then(prepare(5, c2p1))
    .expect(recoveredBases(c2p1, Set(1, 2, 5)) == Set(1))
    .then(issueRecovered(c2p1, Set(1, 2, 5), 1, 3, zero))
    .then(accept(1, { ballot: c2p1, value: 3, next: zero }))
    .then(accept(2, { ballot: c2p1, value: 3, next: zero }))
    .then(accept(5, { ballot: c2p1, value: 3, next: zero }))
    .expect(chosen(c2p1) == Set(3) and safety)

  // Stale proposer holding a prepared fast ballot (1, 2, 0) in c=1 is fenced once
  // a classic quorum of V(1) raises floor to (2, 0, 0).
  run stalePreparedFastBallotFencedByFloorTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(2, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(3, { ballot: c1f1, value: 1, next: c1f2 }))
    .expect(chosen(c1f1) == Set(1) and preparedBases(c1f2) == Set(1))
    .then(issuePrepared(c1f2, 1, 3, c1f3))
    // Fence classic quorum {1, 2, 3} of V(1)
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    // Fenced voters reject the prepared (1, 2, 0) accept; only unfenced voter 4 can accept
    .expect(and {
      not(canAccept(1, { ballot: c1f2, value: 3, next: c1f3 })),
      not(canAccept(2, { ballot: c1f2, value: 3, next: c1f3 })),
      not(canAccept(3, { ballot: c1f2, value: 3, next: c1f3 })),
      canAccept(4, { ballot: c1f2, value: 3, next: c1f3 }),
    })
    .then(accept(4, { ballot: c1f2, value: 3, next: c1f3 }))
    .expect(chosen(c1f2) == Set() and safety)

  // Exact-duplicate replay below floor remains read-only and echoes effPromised >= floor,
  // never certifying a piggybacked promise below floor.
  run exactDuplicateReplayBelowFloorTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(raiseFloor(1, 2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .expect(and {
      nodes.get(1) == { floor: floorBallot(2), promised: c1f2, accepted: c1f1, value: 1 },
      acks.contains({
        ballot: c1f1, acceptor: 1, value: 1,
        promised: floorBallot(2), floor: floorBallot(2), isDuplicate: true,
      }),
      not(canAccept(1, { ballot: c1f1, value: 2, next: c1f2 })),
      safety,
    })

  // Chained 1 -> 2 -> 3 double reconfiguration preserves the chosen chain.
  run chainedDoubleReplaceTest = init
    .then(issuePrepared(c1f1, 0, 1, c1f2))
    .then(accept(1, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(2, { ballot: c1f1, value: 1, next: c1f2 }))
    .then(accept(3, { ballot: c1f1, value: 1, next: c1f2 }))
    .expect(chosen(c1f1) == Set(1))
    // Hand-off 1 -> 2
    .then(raiseFloor(1, 2))
    .then(raiseFloor(2, 2))
    .then(raiseFloor(3, 2))
    .then(prepare(1, c2p1))
    .then(prepare(2, c2p1))
    .then(prepare(3, c2p1))
    .then(prepare(5, c2p1))
    .then(issueRecovered(c2p1, Set(1, 2, 3, 5), 1, 1, zero))
    .then(accept(1, { ballot: c2p1, value: 1, next: zero }))
    .then(accept(2, { ballot: c2p1, value: 1, next: zero }))
    .then(accept(5, { ballot: c2p1, value: 1, next: zero }))
    .then(completeTransferred(2))
    // Hand-off 2 -> 3
    .then(raiseFloor(1, 3))
    .then(raiseFloor(2, 3))
    .then(raiseFloor(5, 3))
    .then(prepare(1, c3p1))
    .then(prepare(2, c3p1))
    .then(prepare(5, c3p1))
    .then(prepare(6, c3p1))
    .expect(recoveredBases(c3p1, Set(1, 2, 5, 6)) == Set(1))
    .then(issueRecovered(c3p1, Set(1, 2, 5, 6), 1, 3, c3f2))
    .then(accept(1, { ballot: c3p1, value: 3, next: c3f2 }))
    .then(accept(2, { ballot: c3p1, value: 3, next: c3f2 }))
    .then(accept(6, { ballot: c3p1, value: 3, next: c3f2 }))
    .then(completeTransferred(3))
    .then(issuePrepared(c3f2, 3, 7, c3f3))
    .then(accept(1, { ballot: c3f2, value: 7, next: c3f3 }))
    .then(accept(5, { ballot: c3f2, value: 7, next: c3f3 }))
    .then(accept(6, { ballot: c3f2, value: 7, next: c3f3 }))
    .expect(chosen(c3f2) == Set(7) and complete == Set(1, 2, 3) and safety)
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
      nodes.get(1) == {
        floor: floorBallot(1), promised: initialFast, accepted: initialFast, value: 1,
      },
      voters(initialFast, 1) == Set(1),
      not(canAccept(1, { ballot: initialFast, value: 2, next: zero })),
      safety,
    })
}
