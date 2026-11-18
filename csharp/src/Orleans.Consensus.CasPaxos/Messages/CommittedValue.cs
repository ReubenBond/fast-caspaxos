namespace Orleans.Consensus.CasPaxos.Messages;

public readonly record struct CommittedValue<TValue>(TValue Value);
