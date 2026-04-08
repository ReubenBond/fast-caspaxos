using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Contracts;

public sealed class FastCasAcceptorDurableState<TValue>
{
    public Ballot PromisedBallot { get; init; }

    public Ballot AcceptedBallot { get; init; }

    public required TValue AcceptedValue { get; init; }
}
