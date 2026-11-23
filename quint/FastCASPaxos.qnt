module FastCASPaxos {
  const Configs: int -> Set[int]
  const FastQuorums: int -> int
  const ProposerCount: int
  const MaxRound: int
  const ValueDepth: int

  type Ballot = { config: int, round: int, owner: int }
  type Acceptor = { floor: Ballot, promised: Ballot, accepted: Ballot, value: int }
  type PrepareReply = { ballot: Ballot, acceptor: int, accepted: Ballot, value: int, floor: Ballot }
  type Request = { ballot: Ballot, value: int, next: Ballot }
  type Ack = { ballot: Ballot, acceptor: int, value: int, promised: Ballot, floor: Ballot, isDuplicate: bool }

  pure val configIds = Configs.keys()
  pure val maxConfig = configIds.fold(0, (acc, c) => if (c > acc) c else acc)
  pure val acceptors = configIds.map(c => Configs.get(c)).flatten()
  pure def votersOf(c: int): Set[int] = Configs.get(c)
  pure def classicQuorumOf(c: int): int = votersOf(c).size() / 2 + 1
  pure def fastQuorumOf(c: int): int = FastQuorums.get(c)
  pure def firstConfig(a: int): int =
    configIds.filter(c => votersOf(c).contains(a))
      .fold(maxConfig, (acc, c) => if (c < acc) c else acc)

  pure val zero = { config: 0, round: 0, owner: 0 }
  pure def floorBallot(c: int): Ballot = { config: c, round: 0, owner: 0 }
  pure val initialFast = { config: 1, round: 1, owner: 0 }
  pure val ballots = tuples(configIds, 1.to(MaxRound), 0.to(ProposerCount))
    .map(((c, r, p)) => { config: c, round: r, owner: p })
  pure val classicBallots = ballots.filter(b => b.owner > 0)
  pure val floorBallots = configIds.map(c => floorBallot(c))
  pure val values = 0.to(2 ^ (ValueDepth + 1) - 2)

  assume finiteBounds = and {
    configIds != Set(),
    configIds == 1.to(configIds.size()),
    FastQuorums.keys() == configIds,
    configIds.forall(c => and {
      votersOf(c).size() > 0,
      fastQuorumOf(c) > 0,
      fastQuorumOf(c) <= votersOf(c).size(),
    }),
    ProposerCount > 0, MaxRound > 0, ValueDepth > 0,
  }

  pure def lt(a: Ballot, b: Ballot): bool =
    a.config < b.config
      or (a.config == b.config and a.round < b.round)
      or (a.config == b.config and a.round == b.round and a.owner < b.owner)
  pure def le(a: Ballot, b: Ballot): bool = a == b or lt(a, b)
  pure def maxBallot(a: Ballot, b: Ballot): Ballot = if (lt(a, b)) b else a
  pure def quorum(b: Ballot): int = if (b.owner == 0) fastQuorumOf(b.config) else classicQuorumOf(b.config)
  pure def effPromised(acc: Acceptor): Ballot = maxBallot(acc.promised, acc.floor)

  // A binary tree represents distinguishable update histories: 0 -> {1,2},
  // 1 -> {3,4}, 2 -> {5,6}, etc. Identity updates model reads/failed CAS/hand-off transfers.
  pure def updates(base: int): Set[int] =
    Set(base, 2 * base + 1, 2 * base + 2).intersect(values)
  pure def ancestor(base: int, value: int): bool =
    0.to(ValueDepth).exists(depth => (value + 1) / (2 ^ depth) == base + 1)

  pure def nextBallots(b: Ballot): Set[Ballot] =
    if (b.round == MaxRound) Set(zero)
    else if (b.owner == 0) Set(zero, { config: b.config, round: b.round + 1, owner: 0 })
    else Set(zero, { config: b.config, round: b.round + 1, owner: 0 }, { config: b.config, round: b.round + 1, owner: b.owner })

  var nodes: int -> Acceptor
  var prepares: Set[PrepareReply]
  var requests: Set[Request]
  // Historical acknowledgments retain choices after acceptors overwrite state.
  // Counts always project to voter identities of the ballot's configuration.
  var acks: Set[Ack]
  var complete: Set[int]

  action init = all {
    nodes' = acceptors.mapBy(a => {
      floor: floorBallot(firstConfig(a)),
      promised: if (firstConfig(a) == 1) initialFast else floorBallot(firstConfig(a)),
      accepted: zero,
      value: 0,
    }),
    prepares' = Set(),
    requests' = Set(),
    acks' = Set(),
    complete' = Set(1),
  }

  def voters(b: Ballot, v: int): Set[int] =
    acks.filter(a => a.ballot == b and a.value == v and votersOf(b.config).contains(a.acceptor))
      .map(a => a.acceptor)
  def chosen(b: Ballot): Set[int] = values.filter(v => voters(b, v).size() >= quorum(b))
  val decisions = ballots.map(b => chosen(b).map(v => (b, v))).flatten()

  // A reusable promise is certified by matching echoes for the target ballot from voters of its
  // configuration, together with a committed source value in the same configuration.
  // Only (1, 1, 0) is implicitly prepared; c > 1 keys start with a classic ballot.
  def preparedBases(b: Ballot): Set[int] =
    if (b == initialFast) Set(0)
    else values.filter(v => requests.exists(r => and {
      r.ballot.config == b.config,
      r.next == b,
      r.value == v,
      voters(r.ballot, v).size() >= quorum(r.ballot),
      acks.filter(a => and {
        a.ballot == r.ballot, a.value == v, a.promised == b,
        votersOf(b.config).contains(a.acceptor),
      }).map(a => a.acceptor).size() >= quorum(b),
    }))

  def replies(b: Ballot, q: Set[int]): Set[PrepareReply] =
    prepares.filter(r => r.ballot == b and q.contains(r.acceptor))
  def responders(b: Ballot): Set[int] =
    prepares.filter(r => r.ballot == b).map(r => r.acceptor)

  def recoveredBases(b: Ballot, q: Set[int]): Set[int] = {
    val responses = replies(b, q)
    val highest = responses.fold(zero, (acc, r) => maxBallot(acc, r.accepted))
    val minRelevantCfg = if (complete.contains(b.config)) b.config else b.config - 1
    if (highest == zero) Set(0)
    else if (highest.config < minRelevantCfg) {
      val top = responses.filter(r => r.accepted == highest)
      top.map(r => r.value)
    } else {
      val top = responses.filter(r => r.accepted == highest and votersOf(highest.config).contains(r.acceptor))
      val candidates = top.map(r => r.value)
      if (highest.owner > 0) candidates
      else candidates.filter(v =>
        candidates.forall(other =>
          top.filter(r => r.value == v).size() >= top.filter(r => r.value == other).size()))
    }
  }

  def canIssue(b: Ballot): bool =
    b.owner == 0 or not(requests.exists(r => r.ballot == b))

  def validRecoveryQuorum(b: Ballot, q: Set[int]): bool =
    if (complete.contains(b.config)) and {
      q.subseteq(responders(b).intersect(votersOf(b.config))),
      q.size() >= classicQuorumOf(b.config),
    }
    else and {
      b.config > 1,
      complete.contains(b.config - 1),
      q.subseteq(responders(b).intersect(votersOf(b.config - 1).union(votersOf(b.config)))),
      q.intersect(votersOf(b.config - 1)).size() >= classicQuorumOf(b.config - 1),
      q.intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config),
    }

  action raiseFloor(a: int, c: int): bool = all {
    configIds.contains(c),
    c > 1,
    complete.contains(c - 1),
    acceptors.contains(a),
    lt(nodes.get(a).floor, floorBallot(c)),
    nodes' = nodes.set(a, nodes.get(a).with("floor", floorBallot(c))),
    prepares' = prepares,
    requests' = requests,
    acks' = acks,
    complete' = complete,
  }

  action prepare(a: int, b: Ballot): bool = {
    val old = nodes.get(a)
    all {
      classicBallots.contains(b),
      b.config == 1 or complete.contains(b.config - 1),
      acceptors.contains(a),
      not(requests.exists(r => r.ballot == b)),
      not(responders(b).contains(a)),
      le(maxBallot(effPromised(old), old.accepted), b),
      nodes' = nodes.set(a, old.with("promised", b)),
      prepares' = prepares.union(Set({
        ballot: b, acceptor: a, accepted: old.accepted, value: old.value, floor: old.floor,
      })),
      requests' = requests,
      acks' = acks,
      complete' = complete,
    }
  }

  action issuePrepared(b: Ballot, base: int, v: int, piggyback: Ballot): bool = all {
    ballots.contains(b),
    complete.contains(b.config),
    canIssue(b),
    preparedBases(b).contains(base),
    updates(base).contains(v),
    nextBallots(b).contains(piggyback),
    requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
    nodes' = nodes,
    prepares' = prepares,
    acks' = acks,
    complete' = complete,
  }

  action issueRecovered(b: Ballot, q: Set[int], base: int, v: int, piggyback: Ballot): bool = all {
    classicBallots.contains(b),
    canIssue(b),
    validRecoveryQuorum(b, q),
    recoveredBases(b, q).contains(base),
    updates(base).contains(v),
    nextBallots(b).contains(piggyback),
    requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
    nodes' = nodes,
    prepares' = prepares,
    acks' = acks,
    complete' = complete,
  }

  def duplicate(a: int, r: Request): bool =
    nodes.get(a).accepted == r.ballot and nodes.get(a).value == r.value
  def canAccept(a: int, r: Request): bool =
    votersOf(r.ballot.config).contains(a) and (
      duplicate(a, r) or and {
        le(maxBallot(effPromised(nodes.get(a)), nodes.get(a).accepted), r.ballot),
        nodes.get(a).accepted != r.ballot,
      }
    )

  action accept(a: int, r: Request): bool = {
    val old = nodes.get(a)
    val isDup = duplicate(a, r)
    // Exact replays acknowledge existing state even after a higher promise or floor.
    // The replay echoes effPromised and leaves durable state unchanged.
    val updated = if (isDup) old else {
      floor: old.floor,
      promised: maxBallot(old.promised, r.next),
      accepted: r.ballot,
      value: r.value,
    }
    all {
      requests.contains(r),
      acceptors.contains(a),
      canAccept(a, r),
      nodes' = nodes.set(a, updated),
      acks' = acks.union(Set({
        ballot: r.ballot, acceptor: a, value: r.value,
        promised: effPromised(updated), floor: old.floor, isDuplicate: isDup,
      })),
      prepares' = prepares,
      requests' = requests,
      complete' = complete,
    }
  }

  action completeTransferred(c: int): bool = all {
    configIds.contains(c),
    not(complete.contains(c)),
    c == 1 or complete.contains(c - 1),
    ballots.exists(b => b.config == c and chosen(b) != Set()),
    complete' = complete.union(Set(c)),
    nodes' = nodes,
    prepares' = prepares,
    requests' = requests,
    acks' = acks,
  }

  action completeVacuous(c: int): bool = all {
    configIds.contains(c),
    not(complete.contains(c)),
    c > 1,
    complete.contains(c - 1),
    votersOf(c - 1).filter(a =>
      le(floorBallot(c), nodes.get(a).floor) and nodes.get(a).accepted == zero
    ).size() >= classicQuorumOf(c - 1),
    complete' = complete.union(Set(c)),
    nodes' = nodes,
    prepares' = prepares,
    requests' = requests,
    acks' = acks,
  }

  // Rejection, loss, and a crash/restart preserving durable state stutter.
  action idle = all {
    nodes' = nodes, prepares' = prepares, requests' = requests, acks' = acks, complete' = complete,
  }

  action step = any {
    {
      nondet c = configIds.filter(cfg => cfg > 1 and complete.contains(cfg - 1)).oneOf()
      nondet a = acceptors.oneOf()
      raiseFloor(a, c)
    },
    {
      nondet b = classicBallots.oneOf()
      nondet a = acceptors.oneOf()
      prepare(a, b)
    },
    {
      nondet b = ballots.oneOf()
      nondet base = preparedBases(b).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issuePrepared(b, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(bal => complete.contains(bal.config)).oneOf()
      nondet q = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(bal =>
        not(complete.contains(bal.config)) and bal.config > 1 and complete.contains(bal.config - 1)
      ).oneOf()
      nondet qPrev = responders(b).intersect(votersOf(b.config - 1))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config - 1)).oneOf()
      nondet qCur = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      val q = qPrev.union(qCur)
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet r = requests.oneOf()
      nondet a = votersOf(r.ballot.config).oneOf()
      accept(a, r)
    },
    {
      nondet c = configIds.filter(cfg => not(complete.contains(cfg))).oneOf()
      completeTransferred(c)
    },
    {
      nondet c = configIds.filter(cfg => cfg > 1 and not(complete.contains(cfg))).oneOf()
      completeVacuous(c)
    },
    idle,
  }

  val quorumIntersection = configIds.forall(c => and {
    2 * classicQuorumOf(c) > votersOf(c).size(),
    2 * fastQuorumOf(c) > votersOf(c).size(),
    2 * fastQuorumOf(c) + classicQuorumOf(c) > 2 * votersOf(c).size(),
  })

  val stateWellFormed = and {
    nodes.keys() == acceptors,
    complete.subseteq(configIds),
    complete.contains(1),
    acceptors.forall(a => and {
      floorBallots.contains(nodes.get(a).floor),
      ballots.union(floorBallots).contains(nodes.get(a).promised),
      ballots.union(Set(zero)).contains(nodes.get(a).accepted),
      values.contains(nodes.get(a).value),
    }),
    prepares.forall(r => and {
      classicBallots.contains(r.ballot), acceptors.contains(r.acceptor),
      lt(r.accepted, r.ballot), values.contains(r.value),
      floorBallots.contains(r.floor),
    }),
    requests.forall(r => and {
      ballots.contains(r.ballot), values.contains(r.value), nextBallots(r.ballot).contains(r.next),
    }),
  }
  val firstWriteWins = acks.forall(a =>
    acks.forall(b => (a.acceptor == b.acceptor and a.ballot == b.ballot) implies a.value == b.value))
  val classicOwnership = classicBallots.forall(b => requests.filter(r => r.ballot == b).size() <= 1)
  val oneDecisionPerBallot = ballots.forall(b => chosen(b).size() <= 1)
  val chosenChain = decisions.forall(((b, v)) =>
    decisions.forall(((later, w)) => le(b, later) implies ancestor(v, w)))
  val acceptedExtendsChosen = decisions.forall(((b, v)) =>
    acks.forall(a => lt(b, a.ballot) implies ancestor(v, a.value)))
  val durableHistory = acceptors.forall(a => and {
    acks.filter(r => r.acceptor == a).forall(r => and {
      le(r.ballot, nodes.get(a).accepted),
      le(r.promised, effPromised(nodes.get(a))),
      (r.ballot == nodes.get(a).accepted) implies r.value == nodes.get(a).value,
    }),
    prepares.filter(r => r.acceptor == a).forall(r => le(r.ballot, effPromised(nodes.get(a)))),
  })
  val acknowledgmentProvenance = acks.forall(a =>
    requests.exists(r => r.ballot == a.ballot and r.value == a.value))
  val floorMonotoneAndEffective = and {
    prepares.forall(p => le(p.floor, p.ballot) and le(p.floor, nodes.get(p.acceptor).floor)),
    acks.forall(r => and {
      le(r.floor, nodes.get(r.acceptor).floor),
      le(r.floor, r.promised),
      not(r.isDuplicate) implies le(r.floor, r.ballot),
    }),
  }
  val noPreJoinAcceptance = and {
    prepares.forall(p => firstConfig(p.acceptor) <= p.ballot.config),
    acks.forall(a => firstConfig(a.acceptor) <= a.ballot.config),
  }
  val safety = and {
    quorumIntersection, stateWellFormed, firstWriteWins, classicOwnership,
    oneDecisionPerBallot, chosenChain, acceptedExtendsChosen, durableHistory,
    acknowledgmentProvenance, floorMonotoneAndEffective, noPreJoinAcceptance,
  }

  val fastCommit = decisions.exists(((b, _)) => b.owner == 0)
  val classicCommit = decisions.exists(((b, _)) => b.owner > 0)
  val chainedCommit = decisions.exists(((b, v)) =>
    decisions.exists(((later, w)) => lt(b, later) and v != w))
  val fastCollision = ballots.exists(b =>
    b.owner == 0 and acks.filter(a => a.ballot == b).map(a => a.value).size() > 1)
  val reconfigComplete = complete.exists(c => c > 1)

  // --- Negative-control mutants (§11.3) ---

  // Mutant 1: floor applied only to held keys (accepted != zero), leaving unseen keys unfenced.
  pure def effPromisedHeldOnly(acc: Acceptor): Ballot =
    if (acc.accepted == zero) acc.promised else maxBallot(acc.promised, acc.floor)

  action prepareFloorOnlyHeldKeys(a: int, b: Ballot): bool = {
    val old = nodes.get(a)
    all {
      classicBallots.contains(b),
      b.config == 1 or complete.contains(b.config - 1),
      acceptors.contains(a),
      not(requests.exists(r => r.ballot == b)),
      not(responders(b).contains(a)),
      le(maxBallot(effPromisedHeldOnly(old), old.accepted), b),
      nodes' = nodes.set(a, old.with("promised", b)),
      prepares' = prepares.union(Set({
        ballot: b, acceptor: a, accepted: old.accepted, value: old.value, floor: old.floor,
      })),
      requests' = requests,
      acks' = acks,
      complete' = complete,
    }
  }

  action acceptFloorOnlyHeldKeys(a: int, r: Request): bool = {
    val old = nodes.get(a)
    val isDup = duplicate(a, r)
    val updated = if (isDup) old else {
      floor: old.floor,
      promised: maxBallot(old.promised, r.next),
      accepted: r.ballot,
      value: r.value,
    }
    all {
      requests.contains(r),
      acceptors.contains(a),
      votersOf(r.ballot.config).contains(a),
      isDup or and {
        le(maxBallot(effPromisedHeldOnly(old), old.accepted), r.ballot),
        old.accepted != r.ballot,
      },
      nodes' = nodes.set(a, updated),
      acks' = acks.union(Set({
        ballot: r.ballot, acceptor: a, value: r.value,
        promised: effPromisedHeldOnly(updated), floor: old.floor, isDuplicate: isDup,
      })),
      prepares' = prepares,
      requests' = requests,
      complete' = complete,
    }
  }

  action mutantFloorOnlyHeldKeys = any {
    {
      nondet c = configIds.filter(cfg => cfg > 1 and complete.contains(cfg - 1)).oneOf()
      nondet a = acceptors.oneOf()
      raiseFloor(a, c)
    },
    {
      nondet b = classicBallots.oneOf()
      nondet a = acceptors.oneOf()
      prepareFloorOnlyHeldKeys(a, b)
    },
    {
      nondet b = ballots.oneOf()
      nondet base = preparedBases(b).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issuePrepared(b, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(bal => complete.contains(bal.config)).oneOf()
      nondet q = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(bal =>
        not(complete.contains(bal.config)) and bal.config > 1 and complete.contains(bal.config - 1)
      ).oneOf()
      nondet qPrev = responders(b).intersect(votersOf(b.config - 1))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config - 1)).oneOf()
      nondet qCur = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      val q = qPrev.union(qCur)
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet r = requests.oneOf()
      nondet a = votersOf(r.ballot.config).oneOf()
      acceptFloorOnlyHeldKeys(a, r)
    },
    {
      nondet c = configIds.filter(cfg => not(complete.contains(cfg))).oneOf()
      completeTransferred(c)
    },
    {
      nondet c = configIds.filter(cfg => cfg > 1 and not(complete.contains(cfg))).oneOf()
      completeVacuous(c)
    },
    idle,
  }

  // Mutant 2: hand-off reads phase 1 only from V(c + 1), skipping V(c).
  action mutantPhase1InNewConfigBeforeComplete = any {
    step,
    {
      nondet b = classicBallots.filter(bal =>
        not(complete.contains(bal.config)) and bal.config > 1 and complete.contains(bal.config - 1)
      ).oneOf()
      nondet q = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        canIssue(b),
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
  }

  // Mutant 3: hand-off reads phase 1 only from V(c), skipping V(c + 1).
  action mutantHandoffSkipsNewConfigQuorum = any {
    step,
    {
      nondet b = classicBallots.filter(bal =>
        not(complete.contains(bal.config)) and bal.config > 1 and complete.contains(bal.config - 1)
      ).oneOf()
      nondet q = responders(b).intersect(votersOf(b.config - 1))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config - 1)).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        canIssue(b),
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
  }

  // Mutant 4: treats (c, 1, 0) as implicitly prepared for c > 1.
  action mutantImplicitFastInNewConfig = any {
    step,
    {
      nondet c = configIds.filter(cfg => cfg > 1 and complete.contains(cfg - 1)).oneOf()
      val b = { config: c, round: 1, owner: 0 }
      nondet v = updates(0).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
  }

  // Mutant 5: new voter initializes with floor = (1, 0, 0) instead of (firstConfig(a), 0, 0).
  action initMutantNewVoterBeforeFloor = all {
    nodes' = acceptors.mapBy(a => {
      floor: floorBallot(1),
      promised: if (firstConfig(a) == 1) initialFast else floorBallot(1),
      accepted: zero,
      value: 0,
    }),
    prepares' = Set(),
    requests' = Set(),
    acks' = Set(),
    complete' = Set(1),
  }

  // Mutant 6: disables both accept votersOf gate and recoveredBases V(hCfg) filter.
  def recoveredBasesUnscoped(b: Ballot, q: Set[int]): Set[int] = {
    val responses = replies(b, q)
    val highest = responses.fold(zero, (acc, r) => maxBallot(acc, r.accepted))
    val top = responses.filter(r => r.accepted == highest)
    val candidates = top.map(r => r.value)
    if (highest == zero) Set(0)
    else if (highest.owner > 0) candidates
    else candidates.filter(v =>
      candidates.forall(other =>
        top.filter(r => r.value == v).size() >= top.filter(r => r.value == other).size()))
  }

  action acceptUnscoped(a: int, r: Request): bool = {
    val old = nodes.get(a)
    val isDup = duplicate(a, r)
    val updated = if (isDup) old else {
      floor: old.floor,
      promised: maxBallot(old.promised, r.next),
      accepted: r.ballot,
      value: r.value,
    }
    all {
      requests.contains(r),
      acceptors.contains(a),
      isDup or and {
        le(maxBallot(effPromised(old), old.accepted), r.ballot),
        old.accepted != r.ballot,
      },
      nodes' = nodes.set(a, updated),
      acks' = acks.union(Set({
        ballot: r.ballot, acceptor: a, value: r.value,
        promised: effPromised(updated), floor: old.floor, isDuplicate: isDup,
      })),
      prepares' = prepares,
      requests' = requests,
      complete' = complete,
    }
  }

  action issueRecoveredUnscoped(b: Ballot, q: Set[int], base: int, v: int, piggyback: Ballot): bool = all {
    classicBallots.contains(b),
    complete.contains(b.config),
    canIssue(b),
    q.subseteq(responders(b)),
    q.intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config),
    recoveredBasesUnscoped(b, q).contains(base),
    updates(base).contains(v),
    nextBallots(b).contains(piggyback),
    requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
    nodes' = nodes,
    prepares' = prepares,
    acks' = acks,
    complete' = complete,
  }

  // Reachable state after config 2 completes vacuously and (2, 1, 1) commits 0 with piggybacked (2, 2, 0).
  action initConfig2FastReady = {
    val c2c1 = { config: 2, round: 1, owner: 1 }
    val c2f2 = { config: 2, round: 2, owner: 0 }
    val req = { ballot: c2c1, value: 0, next: c2f2 }
    all {
      nodes' = acceptors.mapBy(a =>
        if (votersOf(2).contains(a)) {
          floor: floorBallot(2), promised: c2f2, accepted: c2c1, value: 0,
        } else {
          floor: floorBallot(firstConfig(a)), promised: initialFast, accepted: zero, value: 0,
        }),
      prepares' = votersOf(2).map(a => {
        ballot: c2c1, acceptor: a, accepted: zero, value: 0, floor: floorBallot(2),
      }),
      requests' = Set(req),
      acks' = votersOf(2).map(a => {
        ballot: c2c1, acceptor: a, value: 0, promised: c2f2, floor: floorBallot(2), isDuplicate: false,
      }),
      complete' = Set(1, 2),
    }
  }

  action mutantUnscopedFastTally = any {
    {
      nondet c = configIds.filter(cfg => cfg > 1 and complete.contains(cfg - 1)).oneOf()
      nondet a = acceptors.oneOf()
      raiseFloor(a, c)
    },
    {
      nondet b = classicBallots.oneOf()
      nondet a = acceptors.oneOf()
      prepare(a, b)
    },
    {
      nondet b = ballots.oneOf()
      nondet base = preparedBases(b).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issuePrepared(b, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(bal => complete.contains(bal.config)).oneOf()
      nondet qCore = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      val q = qCore.union(responders(b).exclude(votersOf(b.config)))
      nondet base = recoveredBasesUnscoped(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        canIssue(b),
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
    {
      nondet b = classicBallots.filter(bal =>
        not(complete.contains(bal.config)) and bal.config > 1 and complete.contains(bal.config - 1)
      ).oneOf()
      nondet qPrev = responders(b).intersect(votersOf(b.config - 1))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config - 1)).oneOf()
      nondet qCur = responders(b).intersect(votersOf(b.config))
        .powerset().filter(s => s.size() == classicQuorumOf(b.config)).oneOf()
      val q = qPrev.union(qCur)
      nondet base = recoveredBasesUnscoped(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        canIssue(b),
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
    {
      nondet r = requests.oneOf()
      nondet a = acceptors.oneOf()
      acceptUnscoped(a, r)
    },
    {
      nondet c = configIds.filter(cfg => not(complete.contains(cfg))).oneOf()
      completeTransferred(c)
    },
    {
      nondet c = configIds.filter(cfg => cfg > 1 and not(complete.contains(cfg))).oneOf()
      completeVacuous(c)
    },
    idle,
  }

  // Mutant 7: skips Choose's single choice at config 2, letting owner 1 use V_A = {1,2,4}
  // and owner 2 use V_B = {1,3,4} so {2,4} and {1,3} commit diverging values at config 2.
  pure def splitVoters(b: Ballot): Set[int] =
    if (b.config == 2 and b.owner == 2) Set(1, 3, 4) else votersOf(b.config)
  pure def splitQuorum(b: Ballot): int =
    if (b.owner == 0) fastQuorumOf(b.config) else splitVoters(b).size() / 2 + 1
  def splitVotersAcked(b: Ballot, v: int): Set[int] =
    acks.filter(a => a.ballot == b and a.value == v and splitVoters(b).contains(a.acceptor))
      .map(a => a.acceptor)
  def splitChosen(b: Ballot): Set[int] =
    values.filter(v => splitVotersAcked(b, v).size() >= splitQuorum(b))
  val splitDecisions = ballots.map(b => splitChosen(b).map(v => (b, v))).flatten()
  val splitChosenChain = splitDecisions.forall(((b, v)) =>
    splitDecisions.forall(((later, w)) => le(b, later) implies ancestor(v, w)))

  action mutantSkipSingleChoice = any {
    step,
    {
      nondet b = classicBallots.filter(bal =>
        bal.config == 2 and bal.owner == 2 and not(complete.contains(2)) and complete.contains(1)
      ).oneOf()
      nondet qPrev = responders(b).intersect(votersOf(1))
        .powerset().filter(s => s.size() == classicQuorumOf(1)).oneOf()
      nondet qCur = responders(b).intersect(Set(1, 3, 4))
        .powerset().filter(s => s.size() == 2).oneOf()
      val q = qPrev.union(qCur)
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        canIssue(b),
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes, prepares' = prepares, acks' = acks, complete' = complete,
      }
    },
    {
      nondet r = requests.filter(req => req.ballot.config == 2 and req.ballot.owner == 2).oneOf()
      nondet a = Set(1, 3, 4).oneOf()
      acceptUnscoped(a, r)
    },
  }
}
