using FastCASPaxos.Model;

namespace FastCASPaxos.Messages;

public readonly record struct AcceptAccepted<TRoute>(int Round, TRoute Acceptor, Ballot PromisedBallot)
{
    public override string ToString() => PromisedBallot.Round > Round
        ? $"AcceptAccepted(r{Round}, {Acceptor}, Promised: {PromisedBallot})"
        : $"AcceptAccepted(r{Round}, {Acceptor})";
}

public readonly record struct AcceptRejected<TRoute>(int Round, TRoute Acceptor, Ballot ConflictingBallot)
{
    public override string ToString() => $"AcceptRejected(r{Round}, {Acceptor}, {ConflictingBallot})";
}
