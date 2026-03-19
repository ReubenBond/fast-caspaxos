using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public readonly struct AcceptorState<TValue>
{
    public AcceptorState()
    {
    }

    public Ballot PromisedBallot { get; init; }

    public Ballot AcceptedBallot { get; init; }

    public TValue AcceptedValue { get; init; } = default!;
}
