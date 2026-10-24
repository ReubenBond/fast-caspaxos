using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public static class AcceptorState
{
    public static AcceptorState<TValue> CreateInitial<TValue>(TValue acceptedValue) =>
        new()
        {
            PromisedBallot = Ballot.InitialFast(),
            AcceptedBallot = Ballot.Zero,
            AcceptedValue = acceptedValue,
        };
}

public struct AcceptorState<TValue>
{
    public AcceptorState()
    {
        AcceptedValue = default!;
    }

    public bool IsCommitted { get; set; }

    public Ballot PromisedBallot { get; set; }

    public Ballot AcceptedBallot { get; set; }

    public TValue AcceptedValue { get; set; }
}
