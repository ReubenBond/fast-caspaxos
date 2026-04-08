using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public readonly record struct ScheduledProposal<TValue, TRoute>(TRoute Proposer, IOperation<TValue> Operation);
