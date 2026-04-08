using FastCASPaxos.Model;

namespace FastCASPaxos.Messages;

/// <summary>
/// Carries an accept phase request from a proposer to an acceptor.
/// <see cref="NextBallotToPrepare"/> piggybacks a promise for a future ballot without changing
/// the accepted value. <see cref="Ballot.Zero"/> means "do not prepare a next ballot".
/// </summary>
public readonly record struct AcceptRequest<TValue>(
    Ballot Ballot,
    TValue Value,
    Ballot NextBallotToPrepare)
{
    public override string ToString() => !NextBallotToPrepare.IsZero
        ? $"AcceptRequest({Ballot}, {Value}, next: {NextBallotToPrepare})"
        : $"AcceptRequest({Ballot}, {Value})";
}
