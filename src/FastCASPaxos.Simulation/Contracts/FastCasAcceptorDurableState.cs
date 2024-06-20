using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Contracts;

public sealed class FastCasAcceptorDurableState<TValue>
{
    public Ballot PromisedBallot { get; init; }

    public Ballot AcceptedBallot { get; init; }

    public TValue AcceptedValue { get; init; } = default!;
}
