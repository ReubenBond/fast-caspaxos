using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public struct AcceptorState<TValue>
{
    public AcceptorState()
    {
        AcceptedValue = default!;
    }

    public static AcceptorState<TValue> CreateInitial(TValue acceptedValue) =>
        new()
        {
            PromisedBallot = Ballot.InitialFast(),
            AcceptedBallot = Ballot.Zero,
            AcceptedValue = acceptedValue,
        };

    public bool IsCommitted { get; set; }

    public Ballot PromisedBallot { get; set; }

    public Ballot AcceptedBallot { get; set; }

    public TValue AcceptedValue { get; set; }
}
