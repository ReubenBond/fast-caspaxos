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
  pure val classicQuorums = configIds.mapBy(c => Configs.get(c).size() / 2 + 1)
  pure val firstConfigs = acceptors.mapBy(a =>
    configIds.filter(c => Configs.get(c).contains(a))
      .fold(maxConfig, (acc, c) => if (c < acc) c else acc))
  pure def votersOf(c: int): Set[int] = Configs.get(c)
  pure def classicQuorumOf(c: int): int = classicQuorums.get(c)
  pure def fastQuorumOf(c: int): int = FastQuorums.get(c)
  pure def firstConfig(a: int): int = firstConfigs.get(a)

  pure val zero = { config: 0, round: 0, owner: 0 }
  pure def floorBallot(c: int): Ballot = { config: c, round: 0, owner: 0 }
  pure val initialFast = { config: 1, round: 1, owner: 0 }
  pure val floorBallots = configIds.map(c => floorBallot(c))
  pure val ballots = tuples(configIds, 1.to(MaxRound), 0.to(ProposerCount))
    .map(((c, r, p)) => { config: c, round: r, owner: p })
  pure val classicBallots = ballots.filter(b => b.owner > 0)
  pure val values = 0.to(2 ^ (ValueDepth + 1) - 2)

  assume finiteBounds = and {
    configIds != Set(),
    configIds == 1.to(configIds.size()),
    FastQuorums.keys() == configIds,
    configIds.forall(c => and {
      votersOf(c) != Set(),
      fastQuorumOf(c) > 0,
      fastQuorumOf(c) <= votersOf(c).size(),
    }),
    // Permanent retirement (VR-I12): a retired voter never re-enters a later configuration.
    configIds.forall(c1 => configIds.forall(c2 => configIds.forall(c3 =>
      (c1 < c2 and c2 < c3) implies
        votersOf(c1).intersect(votersOf(c3)).subseteq(votersOf(c2))))),
    ProposerCount > 0,
    MaxRound > 0,
    ValueDepth > 0,
  }

  pure def lt(a: Ballot, b: Ballot): bool = or {
    a.config < b.config,
    a.config == b.config and a.round < b.round,
    a.config == b.config and a.round == b.round and a.owner < b.owner,
  }
  pure def le(a: Ballot, b: Ballot): bool = a == b or lt(a, b)
  pure def maxBallot(a: Ballot, b: Ballot): Ballot = if (lt(a, b)) b else a
  pure def quorum(b: Ballot): int =
    if (b.owner == 0) fastQuorumOf(b.config) else classicQuorumOf(b.config)
  pure def effPromised(acc: Acceptor): Ballot = maxBallot(acc.promised, acc.floor)

  // A binary tree represents distinguishable update histories: 0 -> {1,2},
  // 1 -> {3,4}, 2 -> {5,6}, etc. Identity updates model reads/failed CAS.
  pure def updates(base: int): Set[int] =
    Set(base, 2 * base + 1, 2 * base + 2).intersect(values)
  pure def ancestor(base: int, value: int): bool =
    0.to(ValueDepth).exists(depth => (value + 1) / (2 ^ depth) == base + 1)

  pure def nextBallots(b: Ballot): Set[Ballot] =
    if (b.round == MaxRound) Set(zero)
    else if (b.owner == 0) Set({ config: b.config, round: b.round + 1, owner: 0 })
    else Set(zero, { config: b.config, round: b.round + 1, owner: 0 }, { config: b.config, round: b.round + 1, owner: b.owner })

  var nodes: int -> Acceptor
  var prepares: Set[PrepareReply]
  var requests: Set[Request]
  // Historical acknowledgments retain choices after acceptors overwrite state.
  // Counts always project to acceptor identities, including duplicate replies.
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
  def chosen(b: Ballot): Set[int] =
    acks.filter(a => a.ballot == b).map(a => a.value)
      .filter(v => voters(b, v).size() >= quorum(b))
  val decisions =
    acks.map(a => (a.ballot, a.value))
      .filter(((b, v)) => voters(b, v).size() >= quorum(b))

  // A reusable promise is certified by matching echoes for the target ballot,
  // together with a committed source value in the same configuration.
  // Only bootstrap (1, 1, 0) is implicitly prepared.
  def preparedBases(b: Ballot): Set[int] =
    if (b == initialFast) Set(0)
    else requests.filter(r => and {
      r.ballot.config == b.config,
      r.next == b,
      voters(r.ballot, r.value).size() >= quorum(r.ballot),
      acks.filter(a => and {
        a.ballot == r.ballot,
        a.value == r.value,
        a.promised == b,
        votersOf(b.config).contains(a.acceptor),
      }).map(a => a.acceptor).size() >= quorum(b),
    }).map(r => r.value)

  def replies(b: Ballot, q: Set[int]): Set[PrepareReply] =
    prepares.filter(r => r.ballot == b and q.contains(r.acceptor))
  def responders(b: Ballot): Set[int] =
    prepares.filter(r => r.ballot == b).map(r => r.acceptor)

  def recoveredBases(b: Ballot, q: Set[int]): Set[int] = {
    val minRelevantCfg = if (complete.contains(b.config)) b.config else b.config - 1
    val responses = replies(b, q)
    val highest = responses.fold(zero, (acc, r) => maxBallot(acc, r.accepted))
    if (highest == zero) Set(0)
    else if (highest.config < minRelevantCfg) {
      // Any value chosen in < minRelevantCfg was already transferred to a quorum
      // of V(minRelevantCfg) intersecting q; replies below minRelevantCfg are
      // unchosen remnants with zero viable fast winners.
      responses.filter(r => r.accepted == highest).map(r => r.value)
    } else {
      val top = responses.filter(r =>
        r.accepted == highest and votersOf(highest.config).contains(r.acceptor))
      val candidates = top.map(r => r.value)
      if (highest.owner > 0) candidates
      else candidates.filter(v =>
        candidates.forall(other =>
          top.filter(r => r.value == v).size() >= top.filter(r => r.value == other).size()))
    }
  }

  def recoveryQuorums(b: Ballot): Set[Set[int]] = {
    val resp = responders(b)
    if (complete.contains(b.config))
      resp.intersect(votersOf(b.config)).powerset()
        .filter(q => q.size() == classicQuorumOf(b.config))
    else if (b.config > 1 and complete.contains(b.config - 1)) {
      val prev = b.config - 1
      resp.intersect(votersOf(prev).union(votersOf(b.config))).powerset()
        .filter(q => and {
          q.intersect(votersOf(prev)).size() >= classicQuorumOf(prev),
          q.intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config),
        })
    } else Set()
  }

  def canIssue(b: Ballot): bool =
    b.owner == 0 or not(requests.exists(r => r.ballot == b))

  action raiseFloor(a: int, c: int): bool = {
    val old = nodes.get(a)
    val target = floorBallot(c)
    all {
      configIds.contains(c),
      c > 1,
      complete.contains(c - 1),
      acceptors.contains(a),
      lt(old.floor, target),
      nodes' = nodes.set(a, old.with("floor", target)),
      prepares' = prepares,
      requests' = requests,
      acks' = acks,
      complete' = complete,
    }
  }

  def canPrepare(a: int, b: Ballot): bool = {
    val old = nodes.get(a)
    and {
      classicBallots.contains(b),
      b.config == 1 or complete.contains(b.config - 1),
      acceptors.contains(a),
      not(requests.exists(r => r.ballot == b)),
      not(responders(b).contains(a)),
      le(maxBallot(effPromised(old), old.accepted), b),
    }
  }

  // Intentionally ungated on votersOf(b.config) so misrouted prepares test
  // new-voter floor refusal (noPreJoinAcceptance).
  action prepare(a: int, b: Ballot): bool = {
    val old = nodes.get(a)
    all {
      canPrepare(a, b),
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
    recoveryQuorums(b).contains(q),
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
    // The replay echoes effPromised(old) and leaves durable state unchanged.
    val updated = if (isDup) old else {
      floor: old.floor,
      promised: maxBallot(old.promised, r.next),
      accepted: r.ballot,
      value: r.value,
    }
    val echoPromised = effPromised(updated)
    val alreadyRecorded = isDup and acks.exists(x => and {
      x.ballot == r.ballot,
      x.acceptor == a,
      x.value == r.value,
      x.promised == echoPromised,
      x.floor == old.floor,
    })
    all {
      requests.contains(r),
      acceptors.contains(a),
      canAccept(a, r),
      nodes' = nodes.set(a, updated),
      acks' = if (alreadyRecorded) acks else acks.union(Set({
        ballot: r.ballot,
        acceptor: a,
        value: r.value,
        promised: echoPromised,
        floor: old.floor,
        isDuplicate: isDup,
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
    votersOf(c - 1).filter(a => and {
      le(floorBallot(c), nodes.get(a).floor),
      nodes.get(a).accepted == zero,
    }).size() >= classicQuorumOf(c - 1),
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
      nondet c = configIds.oneOf()
      nondet a = acceptors.oneOf()
      raiseFloor(a, c)
    },
    {
      val pairs = tuples(acceptors, classicBallots).filter(((a, b)) => canPrepare(a, b))
      nondet c = pairs.map(((_, b)) => b.config).oneOf()
      val inCfg = pairs.filter(((_, b)) => b.config == c)
      val minR = inCfg.fold(MaxRound, (acc, p) => if (p._2.round < acc) p._2.round else acc)
      nondet p = inCfg.filter(((_, b)) => b.round == minR).oneOf()
      prepare(p._1, p._2)
    },
    {
      nondet b = classicBallots.oneOf()
      nondet a = acceptors.oneOf()
      prepare(a, b)
    },
    {
      nondet b = ballots.filter(b =>
        complete.contains(b.config) and canIssue(b) and preparedBases(b) != Set()).oneOf()
      nondet base = preparedBases(b).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issuePrepared(b, base, v, piggyback)
    },
    {
      nondet b = classicBallots.filter(b => canIssue(b) and recoveryQuorums(b) != Set()).oneOf()
      nondet q = recoveryQuorums(b).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet p = tuples(requests, acceptors).filter(((r, a)) => canAccept(a, r) and not(duplicate(a, r))).oneOf()
      accept(p._2, p._1)
    },
    {
      nondet r = requests.oneOf()
      nondet a = acceptors.oneOf()
      accept(a, r)
    },
    {
      nondet c = configIds.oneOf()
      completeTransferred(c)
    },
    {
      nondet c = configIds.oneOf()
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
      floorBallots.contains(r.floor),
      lt(r.accepted, r.ballot), values.contains(r.value),
    }),
    requests.forall(r => and {
      ballots.contains(r.ballot), values.contains(r.value), nextBallots(r.ballot).contains(r.next),
    }),
  }
  val firstWriteWins = acks.forall(a =>
    acks.forall(b => (a.acceptor == b.acceptor and a.ballot == b.ballot) implies a.value == b.value))
  val classicOwnership = classicBallots.forall(b => requests.filter(r => r.ballot == b).size() <= 1)
  val oneDecisionPerBallot = decisions.forall(((b1, v1)) =>
    decisions.forall(((b2, v2)) => b1 == b2 implies v1 == v2))
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
  val reconfigCommit = decisions.exists(((b, _)) => b.config > 1)
  val reconfigFastCommit = decisions.exists(((b, _)) => b.config > 1 and b.owner == 0)
}
