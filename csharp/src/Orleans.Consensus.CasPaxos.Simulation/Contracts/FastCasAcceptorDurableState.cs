using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Simulation.Contracts;

public sealed class FastCasAcceptorDurableState<TValue>
{
    public Ballot PromisedBallot { get; init; }

    public Ballot AcceptedBallot { get; init; }

    public required TValue AcceptedValue { get; init; }
}
