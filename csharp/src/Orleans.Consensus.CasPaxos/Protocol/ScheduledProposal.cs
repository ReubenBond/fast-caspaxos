using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Protocol;

public readonly record struct ScheduledProposal<TValue, TRoute>(TRoute Proposer, IOperation<TValue> Operation);
