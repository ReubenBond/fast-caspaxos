module FastCASPaxosReconfigReplace {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"

  // breaks: chosenChain
  action mutantFloorOnlyHeldKeys = any {
    step,
    {
      nondet b = classicBallots.oneOf()
      nondet a = acceptors.oneOf()
      val old = nodes.get(a)
      all {
        b.config == 1 or complete.contains(b.config - 1),
        old.accepted == zero,
        lt(b, old.floor),
        not(requests.exists(r => r.ballot == b)),
        not(responders(b).contains(a)),
        le(old.promised, b),
        nodes' = nodes.set(a, old.with("promised", b)),
        prepares' = prepares.union(Set({
          ballot: b, acceptor: a, accepted: old.accepted, value: old.value, floor: old.floor,
        })),
        requests' = requests,
        acks' = acks,
        complete' = complete,
      }
    },
    {
      nondet r = requests.oneOf()
      nondet a = acceptors.oneOf()
      val old = nodes.get(a)
      val updated = {
        floor: old.floor,
        promised: maxBallot(old.promised, r.next),
        accepted: r.ballot,
        value: r.value,
      }
      all {
        votersOf(r.ballot.config).contains(a),
        old.accepted == zero,
        lt(r.ballot, old.floor),
        le(old.promised, r.ballot),
        nodes' = nodes.set(a, updated),
        acks' = acks.union(Set({
          ballot: r.ballot,
          acceptor: a,
          value: r.value,
          promised: effPromised(updated),
          floor: old.floor,
          isDuplicate: false,
        })),
        prepares' = prepares,
        requests' = requests,
        complete' = complete,
      }
    },
  }

  // breaks: chosenChain
  action mutantPhase1InNewConfigBeforeComplete = any {
    step,
    {
      val readyBallots = classicBallots.filter(b => and {
        b.config > 1,
        not(complete.contains(b.config)),
        complete.contains(b.config - 1),
        canIssue(b),
        responders(b).intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config),
      })
      nondet b = readyBallots.oneOf()
      val qs = responders(b).intersect(votersOf(b.config)).powerset()
        .filter(q => q.size() == classicQuorumOf(b.config))
      nondet q = qs.oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes,
        prepares' = prepares,
        acks' = acks,
        complete' = complete,
      }
    },
  }

  // breaks: chosenChain
  action mutantHandoffSkipsNewConfigQuorum = any {
    step,
    {
      val readyBallots = classicBallots.filter(b => and {
        b.config > 1,
        not(complete.contains(b.config)),
        complete.contains(b.config - 1),
        canIssue(b),
        responders(b).intersect(votersOf(b.config - 1)).size() >= classicQuorumOf(b.config - 1),
      })
      nondet b = readyBallots.oneOf()
      val prev = b.config - 1
      val qs = responders(b).intersect(votersOf(prev)).powerset()
        .filter(q => q.size() == classicQuorumOf(prev))
      nondet q = qs.oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes,
        prepares' = prepares,
        acks' = acks,
        complete' = complete,
      }
    },
  }

  // breaks: acceptedExtendsChosen
  action mutantImplicitFastInNewConfig = any {
    step,
    {
      nondet c = configIds.filter(x => x > 1 and complete.contains(x - 1)).oneOf()
      val b = { config: c, round: 1, owner: 0 }
      nondet v = updates(0).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes,
        prepares' = prepares,
        acks' = acks,
        complete' = complete,
      }
    },
  }

  // breaks: noPreJoinAcceptance
  action mutantNewVoterBeforeFloor = any {
    step,
    {
      nondet a = acceptors.filter(x => firstConfig(x) > 1).oneOf()
      nondet b = classicBallots.filter(x => x.config < firstConfig(a)).oneOf()
      all {
        b.config == 1 or complete.contains(b.config - 1),
        not(prepares.exists(p => p.acceptor == a)),
        not(acks.exists(r => r.acceptor == a)),
        not(requests.exists(r => r.ballot == b)),
        nodes' = nodes.set(a, {
          floor: floorBallot(1), promised: b, accepted: zero, value: 0,
        }),
        prepares' = prepares.union(Set({
          ballot: b, acceptor: a, accepted: zero, value: 0, floor: floorBallot(1),
        })),
        requests' = requests,
        acks' = acks,
        complete' = complete,
      }
    },
  }

  // breaks: chosenChain
  action mutantSkipSingleChoice = any {
    step,
    {
      val readyBallots = classicBallots.filter(b => and {
        b.config > 1,
        not(complete.contains(b.config)),
        complete.contains(b.config - 1),
        canIssue(b),
        responders(b).intersect(votersOf(b.config - 1)).size() >= classicQuorumOf(b.config - 1),
      })
      nondet b = readyBallots.oneOf()
      val prev = b.config - 1
      val altSets = acceptors.powerset()
        .filter(s => s.size() == votersOf(b.config).size() and s != votersOf(b.config))
      nondet altV = altSets.oneOf()
      val qs = responders(b).intersect(votersOf(prev).union(altV)).powerset()
        .filter(q => and {
          q.intersect(votersOf(prev)).size() >= classicQuorumOf(prev),
          q.intersect(altV).size() >= classicQuorumOf(b.config),
        })
      nondet q = qs.oneOf()
      nondet base = recoveredBases(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes,
        prepares' = prepares,
        acks' = acks,
        complete' = complete,
      }
    },
  }
}

module FastCASPaxosReconfig4Voter {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 5), 2 -> Set(1, 2, 3, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"

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

  // breaks: chosenChain
  action mutantUnscopedFastTally = any {
    {
      val pairs = tuples(acceptors, Set(2)).filter(((a, c)) =>
        configIds.contains(c) and complete.contains(c - 1) and lt(nodes.get(a).floor, floorBallot(c)))
      nondet pair = pairs.oneOf()
      raiseFloor(pair._1, pair._2)
    },
    {
      val c2FastChosen = ballots.exists(b => b.config == 2 and b.owner == 0 and chosen(b) != Set())
      val targetRound = if (c2FastChosen) MaxRound else 1
      val pairs = tuples(acceptors, classicBallots).filter(((a, b)) =>
        b.config == 2 and b.round == targetRound and b.owner == 1 and canPrepare(a, b))
      nondet pair = pairs.oneOf()
      prepare(pair._1, pair._2)
    },
    {
      val readyBallots = ballots.filter(b =>
        b.config == 2 and complete.contains(b.config) and canIssue(b) and preparedBases(b) != Set())
      nondet b = readyBallots.oneOf()
      nondet base = preparedBases(b).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      issuePrepared(b, base, v, piggyback)
    },
    {
      val readyBallots = classicBallots.filter(b => and {
        b.config == 2,
        canIssue(b),
        if (complete.contains(b.config))
          responders(b).intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config)
        else
          recoveryQuorums(b) != Set(),
      })
      nondet b = readyBallots.oneOf()
      val qs =
        if (complete.contains(b.config))
          responders(b).powerset()
            .filter(q => q.intersect(votersOf(b.config)).size() >= classicQuorumOf(b.config))
        else recoveryQuorums(b)
      nondet q = qs.oneOf()
      nondet base = recoveredBasesUnscoped(b, q).oneOf()
      nondet v = updates(base).oneOf()
      nondet piggyback = nextBallots(b).oneOf()
      all {
        requests' = requests.union(Set({ ballot: b, value: v, next: piggyback })),
        nodes' = nodes,
        prepares' = prepares,
        acks' = acks,
        complete' = complete,
      }
    },
    {
      val pairs = tuples(requests, acceptors).filter(((r, a)) => {
        val old = nodes.get(a)
        and {
          r.ballot.config == 2,
          le(maxBallot(effPromised(old), old.accepted), r.ballot),
          old.accepted != r.ballot,
        }
      })
      nondet pair = pairs.oneOf()
      val r = pair._1
      val a = pair._2
      val old = nodes.get(a)
      val updated = {
        floor: old.floor,
        promised: maxBallot(old.promised, r.next),
        accepted: r.ballot,
        value: r.value,
      }
      all {
        nodes' = nodes.set(a, updated),
        acks' = acks.union(Set({
          ballot: r.ballot,
          acceptor: a,
          value: r.value,
          promised: effPromised(updated),
          floor: old.floor,
          isDuplicate: false,
        })),
        prepares' = prepares,
        requests' = requests,
        complete' = complete,
      }
    },
    completeTransferred(2),
    completeVacuous(2),
  }
}

module FastCASPaxosReconfigDoubleReplace {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4), 3 -> Set(1, 4, 5)),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"
}

module FastCASPaxosReconfigAddRemove {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 3, 4), 3 -> Set(2, 3, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3, 3 -> 3),
    ProposerCount = 2, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"
}

module FastCASPaxosReconfigDisjoint {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(4, 5, 6)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"
}
