module FastCASPaxos {
  const AcceptorCount: int
  const ProposerCount: int
  const MaxRound: int
  const ValueDepth: int
  const FastQuorum: int

  type Ballot = { round: int, owner: int }
  type Acceptor = { promised: Ballot, accepted: Ballot, value: int }
  type PrepareReply = { ballot: Ballot, acceptor: int, accepted: Ballot, value: int }
  type Request = { ballot: Ballot, value: int, next: Ballot }
  type Ack = { ballot: Ballot, acceptor: int, value: int, promised: Ballot }

  pure val acceptors = 1.to(AcceptorCount)
  pure val zero = { round: 0, owner: 0 }
  pure val initialFast = { round: 1, owner: 0 }
  pure val ballots = tuples(1.to(MaxRound), 0.to(ProposerCount))
    .map(((r, p)) => { round: r, owner: p })
  pure val classicBallots = ballots.filter(b => b.owner > 0)
  pure val classicQuorum = AcceptorCount / 2 + 1
  pure val values = 0.to(2 ^ (ValueDepth + 1) - 2)

  assume finiteBounds = and {
    AcceptorCount > 0, ProposerCount > 0, MaxRound > 0, ValueDepth > 0,
    FastQuorum > 0, FastQuorum <= AcceptorCount,
  }

  pure def lt(a: Ballot, b: Ballot): bool =
    a.round < b.round or (a.round == b.round and a.owner < b.owner)
  pure def le(a: Ballot, b: Ballot): bool = a == b or lt(a, b)
  pure def maxBallot(a: Ballot, b: Ballot): Ballot = if (lt(a, b)) b else a
  pure def quorum(b: Ballot): int = if (b.owner == 0) FastQuorum else classicQuorum

  // A binary tree represents distinguishable update histories: 0 -> {1,2},
  // 1 -> {3,4}, 2 -> {5,6}, etc. Identity updates model reads/failed CAS.
  pure def updates(base: int): Set[int] =
    Set(base, 2 * base + 1, 2 * base + 2).intersect(values)
  pure def ancestor(base: int, value: int): bool =
    0.to(ValueDepth).exists(depth => (value + 1) / (2 ^ depth) == base + 1)

  pure def nextBallots(b: Ballot): Set[Ballot] =
    if (b.round == MaxRound) Set(zero)
    else if (b.owner == 0) Set({ round: b.round + 1, owner: 0 })
    else Set(zero, { round: b.round + 1, owner: b.owner })

  var nodes: int -> Acceptor
  var prepares: Set[PrepareReply]
  var requests: Set[Request]
  // Historical acknowledgments retain choices after acceptors overwrite state.
  // Counts always project to acceptor identities, including duplicate replies.
  var acks: Set[Ack]

  action init = all {
    nodes' = acceptors.mapBy(_ => { promised: initialFast, accepted: zero, value: 0 }),
    prepares' = Set(),
    requests' = Set(),
    acks' = Set(),
  }

  def voters(b: Ballot, v: int): Set[int] =
    acks.filter(a => a.ballot == b and a.value == v).map(a => a.acceptor)
  def chosen(b: Ballot): Set[int] = values.filter(v => voters(b, v).size() >= quorum(b))
  val decisions = ballots.map(b => chosen(b).map(v => (b, v))).flatten()

  // A reusable promise is certified by matching echoes for the target ballot,
  // together with a committed source value. Certificates can be learned late.
  def preparedBases(b: Ballot): Set[int] =
    if (b == initialFast) Set(0)
    else values.filter(v => requests.exists(r => and {
      r.next == b,
      r.value == v,
      voters(r.ballot, v).size() >= quorum(r.ballot),
      acks.filter(a => and {
        a.ballot == r.ballot, a.value == v, a.promised == b,
      }).map(a => a.acceptor).size() >= quorum(b),
    }))

  def replies(b: Ballot, q: Set[int]): Set[PrepareReply] =
    prepares.filter(r => r.ballot == b and q.contains(r.acceptor))
  def responders(b: Ballot): Set[int] =
    prepares.filter(r => r.ballot == b).map(r => r.acceptor)

  def recoveredBases(b: Ballot, q: Set[int]): Set[int] = {
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

  def canIssue(b: Ballot): bool =
    b.owner == 0 or not(requests.exists(r => r.ballot == b))

  action prepare(a: int, b: Ballot): bool = all {
    classicBallots.contains(b),
    not(requests.exists(r => r.ballot == b)),
    not(responders(b).contains(a)),
    le(maxBallot(nodes.get(a).promised, nodes.get(a).accepted), b),
    nodes' = nodes.set(a, nodes.get(a).with("promised", b)),
    prepares' = prepares.union(Set({
      ballot: b, acceptor: a, accepted: nodes.get(a).accepted, value: nodes.get(a).value,
    })),
    requests' = requests,
    acks' = acks,
  }

  action issuePrepared(b: Ballot, base: int, v: int, piggyback: Ballot): bool = all {
    ballots.contains(b),
    canIssue(b),
    preparedBases(b).contains(base),
    updates(base).contains(v),
    nextBallots(b).contains(piggyback),
    requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
    nodes' = nodes,
    prepares' = prepares,
    acks' = acks,
  }

  action issueRecovered(b: Ballot, q: Set[int], base: int, v: int, piggyback: Ballot): bool = all {
    classicBallots.contains(b),
    canIssue(b),
    q.subseteq(responders(b)),
    q.size() == classicQuorum,
    recoveredBases(b, q).contains(base),
    updates(base).contains(v),
    nextBallots(b).contains(piggyback),
    requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
    nodes' = nodes,
    prepares' = prepares,
    acks' = acks,
  }

  def duplicate(a: int, r: Request): bool =
    nodes.get(a).accepted == r.ballot and nodes.get(a).value == r.value
  def canAccept(a: int, r: Request): bool =
    duplicate(a, r) or and {
      le(maxBallot(nodes.get(a).promised, nodes.get(a).accepted), r.ballot),
      nodes.get(a).accepted != r.ballot,
    }

  action accept(a: int, r: Request): bool = {
    val old = nodes.get(a)
    // Exact replays acknowledge existing state even after a higher promise.
    // The replay echoes that promise and leaves the durable state unchanged.
    val updated = if (duplicate(a, r)) old else {
      promised: maxBallot(old.promised, r.next), accepted: r.ballot, value: r.value,
    }
    all {
      requests.contains(r),
      canAccept(a, r),
      nodes' = nodes.set(a, updated),
      acks' = acks.union(Set({
        ballot: r.ballot, acceptor: a, value: r.value, promised: updated.promised,
      })),
      prepares' = prepares,
      requests' = requests,
    }
  }

  // Rejection, loss, and a crash/restart preserving durable state stutter.
  action idle = all {
    nodes' = nodes, prepares' = prepares, requests' = requests, acks' = acks,
  }

  action step = any {
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
      nondet b = classicBallots.oneOf()
      nondet q = responders(b).powerset().filter(s => s.size() == classicQuorum).oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issueRecovered(b, q, base, v, piggyback)
    },
    {
      nondet r = requests.oneOf()
      nondet a = acceptors.oneOf()
      accept(a, r)
    },
    idle,
  }

  val quorumIntersection = and {
    2 * classicQuorum > AcceptorCount,
    2 * FastQuorum > AcceptorCount,
    2 * FastQuorum + classicQuorum > 2 * AcceptorCount,
  }

  val stateWellFormed = and {
    nodes.keys() == acceptors,
    acceptors.forall(a => and {
      ballots.contains(nodes.get(a).promised),
      ballots.union(Set(zero)).contains(nodes.get(a).accepted),
      values.contains(nodes.get(a).value),
    }),
    prepares.forall(r => and {
      classicBallots.contains(r.ballot), acceptors.contains(r.acceptor),
      lt(r.accepted, r.ballot), values.contains(r.value),
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
      le(r.promised, nodes.get(a).promised),
      (r.ballot == nodes.get(a).accepted) implies r.value == nodes.get(a).value,
    }),
    prepares.filter(r => r.acceptor == a).forall(r => le(r.ballot, nodes.get(a).promised)),
  })
  val acknowledgmentProvenance = acks.forall(a =>
    requests.exists(r => r.ballot == a.ballot and r.value == a.value))
  val safety = and {
    quorumIntersection, stateWellFormed, firstWriteWins, classicOwnership,
    oneDecisionPerBallot, chosenChain, acceptedExtendsChosen, durableHistory,
    acknowledgmentProvenance,
  }

  val fastCommit = decisions.exists(((b, _)) => b.owner == 0)
  val classicCommit = decisions.exists(((b, _)) => b.owner > 0)
  val chainedCommit = decisions.exists(((b, v)) =>
    decisions.exists(((later, w)) => lt(b, later) and v != w))
  val fastCollision = ballots.exists(b =>
    b.owner == 0 and acks.filter(a => a.ballot == b).map(a => a.value).size() > 1)
}
