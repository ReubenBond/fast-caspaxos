using FastCASPaxos.Model;

namespace FastCASPaxos.Messages;

public readonly record struct PrepareRequest(Ballot Ballot)
{
    public override string ToString() => $"PrepareRequest({Ballot})";
}
