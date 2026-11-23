module FastCASPaxosMC {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4, 5)),
    FastQuorums = Map(1 -> 4),
    ProposerCount = 2, MaxRound = 4, ValueDepth = 3
  ).* from "./FastCASPaxos"
}

module FastCASPaxosSmall {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3)),
    FastQuorums = Map(1 -> 3),
    ProposerCount = 1, MaxRound = 2, ValueDepth = 1
  ).* from "./FastCASPaxos"
}

module FastCASPaxosEven {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 4)),
    FastQuorums = Map(1 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"
}

module FastCASPaxosExhaustive {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3)),
    FastQuorums = Map(1 -> 3),
    ProposerCount = 1, MaxRound = 1, ValueDepth = 1
  ).* from "./FastCASPaxos"
}

module FastCASPaxosPipeline {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2)),
    FastQuorums = Map(1 -> 2),
    ProposerCount = 1, MaxRound = 2, ValueDepth = 1
  ).* from "./FastCASPaxos"
}

module FastCASPaxosReconfigReplace {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3), 2 -> Set(1, 2, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 2, ValueDepth = 2
  ).* from "./FastCASPaxos"
}

module FastCASPaxosReconfig4Voter {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2, 3, 5), 2 -> Set(1, 2, 3, 4)),
    FastQuorums = Map(1 -> 3, 2 -> 3),
    ProposerCount = 2, MaxRound = 3, ValueDepth = 2
  ).* from "./FastCASPaxos"
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

module FastCASPaxosReconfigExhaustive {
  import FastCASPaxos(
    Configs = Map(1 -> Set(1, 2), 2 -> Set(1, 3)),
    FastQuorums = Map(1 -> 2, 2 -> 2),
    ProposerCount = 1, MaxRound = 1, ValueDepth = 1
  ).* from "./FastCASPaxos"
}
