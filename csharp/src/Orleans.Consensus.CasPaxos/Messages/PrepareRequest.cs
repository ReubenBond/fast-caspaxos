using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Messages;

public readonly record struct PrepareRequest(Ballot Ballot)
{
    public override string ToString() => $"PrepareRequest({Ballot})";
}
