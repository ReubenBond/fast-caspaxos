using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using System.ComponentModel.Design;
using System.Diagnostics;

namespace FastCASPaxos.Protocol;

public static class AcceptorPrepareResult
{
    public static AcceptorPrepareResult<TValue> Promised<TValue>(Ballot acceptedBallot, TValue acceptedValue) =>
        new(AcceptedBallot: acceptedBallot, AcceptedValue: acceptedValue, ConflictingBallot: Ballot.Zero);

    public static AcceptorPrepareResult<TValue> Rejected<TValue>(Ballot acceptedBallot, TValue acceptedValue, Ballot conflictingBallot) =>
        new(acceptedBallot, acceptedValue, conflictingBallot);
}

public readonly record struct AcceptorPrepareResult<TValue>(
    Ballot AcceptedBallot,
    TValue AcceptedValue,
    Ballot ConflictingBallot)
{
    public bool IsPromised => ConflictingBallot.IsZero;
}

public readonly record struct AcceptorAcceptResult
{
    private readonly Ballot _ballot;

    public AcceptorAcceptResult(bool isAccepted, Ballot ballot)
    {
        IsAccepted = isAccepted;
        _ballot = ballot;
    }

    public bool IsAccepted { get; }

    public Ballot PromisedBallot => IsAccepted ? _ballot : Ballot.Zero;

    public Ballot ConflictingBallot => IsAccepted ? Ballot.Zero : _ballot;

    public Ballot GetBallot() => _ballot;

    public static AcceptorAcceptResult Accepted(Ballot promisedBallot) =>
        new(isAccepted: true, ballot: promisedBallot);

    public static AcceptorAcceptResult Rejected(Ballot conflictingBallot) =>
        new(isAccepted: false, ballot: conflictingBallot);
}

public static class AcceptorEngine
{
    public static AcceptorPrepareResult<TValue> Prepare<TValue>(
        ref AcceptorState<TValue> state,
        PrepareRequest request)
    {
        if (request.Ballot < state.PromisedBallot)
        {
            return AcceptorPrepareResult.Rejected(
                acceptedBallot: state.AcceptedBallot,
                acceptedValue: state.AcceptedValue,
                conflictingBallot: state.PromisedBallot);
        }

        state.PromisedBallot = request.Ballot;
        return AcceptorPrepareResult.Promised(
            acceptedBallot: state.AcceptedBallot,
            acceptedValue: state.AcceptedValue);
    }

    public static AcceptorAcceptResult Accept<TValue>(
        ref AcceptorState<TValue> state,
        AcceptRequest<TValue> request)
    {
        // Retries of the exact accepted ballot/value pair carry no new information, so they
        // succeed read-only and cannot perturb the state that later ballots reason about.
        if (request.Ballot == state.AcceptedBallot
            && EqualityComparer<TValue>.Default.Equals(request.Value, state.AcceptedValue))
        {
            return AcceptorAcceptResult.Accepted(state.PromisedBallot);
        }

        // Once this acceptor has moved on to a higher promise, older ballots must lose.
        if (request.Ballot < state.PromisedBallot)
        {
            return AcceptorAcceptResult.Rejected(state.PromisedBallot);
        }

        // Each ballot is allowed to carry only one value. If we reach the same ballot again after
        // the idempotent fast-path above, the proposer is trying to reuse that ballot incorrectly.
        if (request.Ballot == state.AcceptedBallot)
        {
            return AcceptorAcceptResult.Rejected(state.AcceptedBallot);
        }

        Debug.Assert(request.Ballot > state.AcceptedBallot);

        // Past the ballot-ownership guards above, this request is safe to become the acceptor's
        // latest accepted state. Any CAS/version validation lives in the caller's operation logic.
        state.AcceptedBallot = request.Ballot;
        state.AcceptedValue = request.Value;

        if (request.NextBallotToPrepare > state.PromisedBallot)
        {
            // Fold the piggybacked prepare into the promise so a successful accept can prepare
            // the next round.
            state.PromisedBallot = request.NextBallotToPrepare;
        }

        return AcceptorAcceptResult.Accepted(state.PromisedBallot);
    }

    public static AcceptorAcceptResult Learn<TValue>(
        ref AcceptorState<TValue> state,
        AcceptRequest<TValue> request)
    {
        if (request.Ballot >= state.AcceptedBallot)
        {
            state.IsCommitted = true;
            state.AcceptedBallot = request.Ballot;
            state.AcceptedValue = request.Value;
            if (request.NextBallotToPrepare > state.PromisedBallot)
            {
                state.PromisedBallot = request.NextBallotToPrepare;
            }

            return AcceptorAcceptResult.Accepted(state.PromisedBallot);
        }

        return AcceptorAcceptResult.Rejected(state.PromisedBallot);
    }
}
