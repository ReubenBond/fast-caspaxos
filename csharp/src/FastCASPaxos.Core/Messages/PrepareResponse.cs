using FastCASPaxos.Model;

namespace FastCASPaxos.Messages;

public readonly record struct PreparePromise<TValue, TRoute>(int Round, TRoute Acceptor, Ballot AcceptedBallot, TValue AcceptedValue)
{
    public override string ToString() => $"PreparePromise(r{Round}, {Acceptor}, {AcceptedBallot}, {AcceptedValue})";
}

public readonly record struct PrepareRejection<TValue, TRoute>(int Round, TRoute Acceptor, Ballot AcceptedBallot, TValue AcceptedValue, Ballot ConflictBallot)
{
    public override string ToString() => $"PrepareRejection(r{Round}, {Acceptor}, {AcceptedBallot}, {AcceptedValue}, {ConflictBallot})";
}
