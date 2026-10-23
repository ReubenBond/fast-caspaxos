module FastCASPaxosTests {
  import FastCASPaxos(
    AcceptorCount = 5, ProposerCount = 2, MaxRound = 4, ValueDepth = 3, FastQuorum = 4
  ).* from "./FastCASPaxos"

  pure val f1 = { round: 1, owner: 0 }
  pure val f2 = { round: 2, owner: 0 }
  pure val f3 = { round: 3, owner: 0 }
  pure val c1 = { round: 1, owner: 1 }
  pure val c2 = { round: 2, owner: 1 }
  pure val c3 = { round: 3, owner: 2 }
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
      nodes.get(1) == { promised: c3, accepted: f1, value: 1 },
      acks.contains({ ballot: f1, acceptor: 1, value: 1, promised: c3 }),
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
      nodes.get(5) == { promised: f1, accepted: c2, value: 2 },
      not(canAccept(5, fastX)), safety,
    })
    .then(prepare(5, c1).fail())

  run sameRoundOwnersAreOrderedTest = init
    .then(3.reps(i => prepare(i + 1, c1)))
    .then(issueRecovered(c1, Set(1, 2, 3), 0, 1, zero))
    .then(3.reps(i => accept(i + 1, { ballot: c1, value: 1, next: zero })))
    .then(3.reps(i => prepare(i + 3, { round: 1, owner: 2 })))
    .expect(recoveredBases({ round: 1, owner: 2 }, Set(3, 4, 5)) == Set(1) and safety)
}

module FastCASPaxosNegativeTests {
  // Deliberately violate the fast/classic triple-intersection requirement.
  import FastCASPaxos(
    AcceptorCount = 5, ProposerCount = 1, MaxRound = 2, ValueDepth = 2, FastQuorum = 3
  ).* from "./FastCASPaxos"

  pure val f1 = { round: 1, owner: 0 }
  pure val f2 = { round: 2, owner: 0 }
  pure val c2 = { round: 2, owner: 1 }

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
