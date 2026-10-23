module FastCASPaxosMC {
  import FastCASPaxos(
    AcceptorCount = 5, ProposerCount = 2, MaxRound = 4, ValueDepth = 3, FastQuorum = 4
  ).* from "./FastCASPaxos"
}

module FastCASPaxosSmall {
  import FastCASPaxos(
    AcceptorCount = 3, ProposerCount = 1, MaxRound = 2, ValueDepth = 1, FastQuorum = 3
  ).* from "./FastCASPaxos"
}

module FastCASPaxosEven {
  import FastCASPaxos(
    AcceptorCount = 4, ProposerCount = 2, MaxRound = 3, ValueDepth = 2, FastQuorum = 3
  ).* from "./FastCASPaxos"
}
