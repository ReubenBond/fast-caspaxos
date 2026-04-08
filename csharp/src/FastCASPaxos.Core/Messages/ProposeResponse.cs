namespace FastCASPaxos.Messages;

public readonly record struct ProposeResponse<TValue>(
    int Round,
    TValue CommittedValue)
{
    public override string ToString() => $"ProposeResponse(r{Round}, {CommittedValue})";
}
