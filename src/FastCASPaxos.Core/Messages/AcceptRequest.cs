using FastCASPaxos.Model;

namespace FastCASPaxos.Messages;

/// <summary>
/// Carries an accept phase request from a proposer to an acceptor.
/// <see cref="NextBallotToPrepare"/> optionally piggybacks a promise for a future ballot without changing the accepted value.
/// </summary>
public readonly record struct AcceptRequest<TValue>(
    Ballot Ballot,
    TValue Value,
    Ballot? NextBallotToPrepare)
{
    public override string ToString() => NextBallotToPrepare is { } nextBallot
        ? $"AcceptRequest({Ballot}, {Value}, next: {nextBallot})"
        : $"AcceptRequest({Ballot}, {Value})";
}
