using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Protocol;

public static class AcceptorState
{
    public static AcceptorState<TValue> CreateInitial<TValue>(TValue acceptedValue, long configuration = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(configuration, 0);
        var floor = Ballot.Floor(configuration);
        return new()
        {
            FloorBallot = floor,
            PromisedBallot = configuration == 1 ? Ballot.InitialFast() : floor,
            AcceptedBallot = Ballot.Zero,
            AcceptedValue = acceptedValue,
        };
    }
}

public struct AcceptorState<TValue>
{
    public AcceptorState()
    {
        AcceptedValue = default!;
    }

    public bool IsCommitted { get; set; }

    public Ballot FloorBallot { get; set; }

    public Ballot PromisedBallot { get; set; }

    public readonly Ballot EffectivePromisedBallot => Ballot.Max(PromisedBallot, FloorBallot);

    public Ballot AcceptedBallot { get; set; }

    public TValue AcceptedValue { get; set; }
}
