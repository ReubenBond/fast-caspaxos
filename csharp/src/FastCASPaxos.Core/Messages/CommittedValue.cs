namespace FastCASPaxos.Messages;

public readonly record struct CommittedValue<TValue>(TValue Value);
