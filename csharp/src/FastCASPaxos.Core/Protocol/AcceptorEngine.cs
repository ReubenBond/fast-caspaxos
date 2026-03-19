using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public abstract class AcceptorEngine<TValue, TRoute>
{
    private readonly TRoute _acceptor;

    public AcceptorEngine(TRoute acceptor, AcceptorState<TValue>? state = null)
    {
        _acceptor = acceptor;
        PromisedBallot = Ballot.InitialFast();
        if (state is null)
        {
            return;
        }

        if (state is { } validState)
        {
            PromisedBallot = validState.PromisedBallot;
            AcceptedBallot = validState.AcceptedBallot;
            AcceptedValue = validState.AcceptedValue;
        }
    }

    public Ballot PromisedBallot { get; private set; }

    public Ballot AcceptedBallot { get; private set; }

    private Ballot MaxBallot => Ballot.Max(PromisedBallot, AcceptedBallot);

    public TValue AcceptedValue { get; private set; } = default!;

    public AcceptorState<TValue> GetState() =>
        new()
        {
            PromisedBallot = PromisedBallot,
            AcceptedBallot = AcceptedBallot,
            AcceptedValue = AcceptedValue,
        };

    public void Prepare(PrepareRequest request)
    {
        // Prepare must clear both prior promises and prior accepts; otherwise a proposer could
        // begin a lower ballot round after this acceptor has already accepted a higher ballot.
        if (request.Ballot < MaxBallot)
        {
            OnPrepareRejected(new PrepareRejection<TValue, TRoute>(
                request.Ballot.Round,
                _acceptor,
                AcceptedBallot,
                AcceptedValue,
                MaxBallot));
            return;
        }

        PromisedBallot = request.Ballot;
        OnPreparePromised(new PreparePromise<TValue, TRoute>(
            request.Ballot.Round,
            _acceptor,
            AcceptedBallot,
            AcceptedValue));
    }

    public void Accept(AcceptRequest<TValue> request)
    {
        // Retries of the exact accepted ballot/value pair carry no new information, so they
        // succeed read-only and cannot perturb the state that later ballots reason about.
        if (request.Ballot == AcceptedBallot && EqualityComparer<TValue>.Default.Equals(request.Value, AcceptedValue))
        {
            Accepted();
            return;
        }

        // Once this acceptor has moved on to a higher promise or accept, older ballots must lose;
        // otherwise a stale proposer could overwrite state that a newer round is repairing.
        if (request.Ballot < MaxBallot)
        {
            Rejected();
            return;
        }

        // Each ballot is allowed to carry only one value. If we reach the same ballot again after
        // the idempotent fast-path above, the proposer is trying to reuse that ballot incorrectly.
        if (request.Ballot == AcceptedBallot)
        {
            Rejected();
            return;
        }

        // Past the ballot-ownership guards above, this request is safe to become the acceptor's
        // latest accepted state. Any CAS/version validation lives in the caller's operation logic.
        AcceptedBallot = request.Ballot;
        AcceptedValue = request.Value;

        if (request.NextBallotToPrepare is { } requestedNextBallot)
        {
            // Fold the piggybacked prepare into the promise so a successful accept can hand off
            // directly to the next round without ever lowering an existing promise.
            PromisedBallot = Ballot.Max(PromisedBallot, requestedNextBallot);
        }

        Accepted();

        void Accepted() => OnAcceptAccepted(new AcceptAccepted<TRoute>(request.Ballot.Round, _acceptor, PromisedBallot));
        void Rejected() => OnAcceptRejected(new AcceptRejected<TRoute>(request.Ballot.Round, _acceptor, MaxBallot));
    }

    protected abstract void OnPreparePromised(PreparePromise<TValue, TRoute> result);
    protected abstract void OnPrepareRejected(PrepareRejection<TValue, TRoute> result);
    protected abstract void OnAcceptAccepted(AcceptAccepted<TRoute> result);
    protected abstract void OnAcceptRejected(AcceptRejected<TRoute> result);
}
