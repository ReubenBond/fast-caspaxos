using FastCASPaxos.Events;
using FastCASPaxos.Model;
using FastCASPaxos.Utilities;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Protocol;

public class AcceptorActor<TValue, TVersionedValue> : StateMachine where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    private Ballot PromisedBallot { get; set; }
    private Ballot AcceptedBallot { get; set; }
    private TVersionedValue Value { get; set; }

    private string _name;
    public string Name => _name ??= $"A-{Id.Value}";

    [Start]
    [OnEventDoAction(typeof(PrepareRequest), nameof(OnPrepare))]
    [OnEventDoAction(typeof(AcceptRequestHolder), nameof(OnAccept))]
    public class Init : State { }

    public void OnPrepare(Event e)
    {
        if (e is not PrepareRequest request)
        {
            throw new ArgumentException($"Expected event of type {typeof(PrepareRequest)} but found {e.GetType()}");
        }

        Logger.Log($"{Name}: OnPrepare: {request}");
        if (request.Ballot < PromisedBallot)
        {
            Logger.Log($"{Name}: Rejecting Prepare({request.Ballot}) due to Promised. Promised: {PromisedBallot}, Accepted: {AcceptedBallot}, Value: {Value}");
            SendResponse(request.Proposer, new PrepareResponseHolder { Value = new PrepareResponse<TValue, TVersionedValue>(request.RequestId, Id, success: false, PromisedBallot, Value) });
            return;
        }

        if (request.Ballot < AcceptedBallot)
        {
            Logger.Log($"{Name}: Rejecting Prepare({request.Ballot}) due to Accepted. Promised: {PromisedBallot}, Accepted: {AcceptedBallot}, Value: {Value}");
            SendResponse(request.Proposer, new PrepareResponseHolder { Value = new PrepareResponse<TValue, TVersionedValue>(request.RequestId, Id, success: false, AcceptedBallot, Value) });
            return;
        }

        var response = new PrepareResponseHolder { Value = new PrepareResponse<TValue, TVersionedValue>(request.RequestId, Id, success: true, AcceptedBallot, Value) };

        // Log when we promise to accept the same fast-round ballot multiple times.
        if (request.Ballot == PromisedBallot && request.Ballot.IsFastRoundBallot)
        {
            Logger.Log($"{Name}: Promising to accept {request.Ballot} from P-{request.Proposer.Value}: {response}");
            SendResponse(request.Proposer, response);
            return;
        }

        PromisedBallot = request.Ballot;

        Logger.Log($"{Name}: Promising to accept {request.Ballot} from P-{request.Proposer.Value}: {response}");
        SendResponse(request.Proposer, response);
    }

    public void OnAccept(Event e)
    {
        var request = (AcceptRequest<TValue, TVersionedValue>)((AcceptRequestHolder)e).Value;
        Logger.Log($"{Name}: OnAccept: {request}");

        if (request.Ballot < PromisedBallot)
        {
            Logger.Log($"{Name}: Rejecting Accept({request.Ballot}, {request.Value}) due to Promised. Promised: {PromisedBallot}, Accepted: {AcceptedBallot}, Value: {Value}");
            SendResponse(request.Proposer, new AcceptResponse(request.RequestId, Id, success: false, PromisedBallot));
            return;
        }

        if (request.Ballot < AcceptedBallot)
        {
            Logger.Log($"{Name}: Rejecting Accept({request.Ballot}, {request.Value}) due to Accepted. Promised: {PromisedBallot}, Accepted: {AcceptedBallot}, Value: {Value}");
            SendResponse(request.Proposer, new AcceptResponse(request.RequestId, Id, success: false, AcceptedBallot));
            return;
        }

        // Additional optimization for fast rounds: allow multiple proposers to propose identical values.
        // This reduces needless conflicts in the anticipated scenario where multiple proposers move in lock-step.
        if (request.Ballot.IsFastRoundBallot && request.Ballot.Equals(AcceptedBallot))
        {
            if (request.Value.Equals(Value))
            {
                // Conflict.
                Logger.Log($"{Name}: Rejecting value in fast round {request.Ballot}: {request.Value}. Promised: {PromisedBallot}, Accepted: {AcceptedBallot}, Value: {Value}, IsSuccessor: {request.Value.IsValidSuccessorTo(Value)}. IsEqual: {request.Value.Equals(Value)}");
                SendResponse(request.Proposer, new AcceptResponse(request.RequestId, Id, success: false, AcceptedBallot));
                return;
            }
            else
            {
                // Ok. The proposer is having an identical value accepted.
                // This is an optimization to avoid dueling proposers from clashing when they are committing the same value.
                Logger.Log($"{Name}: Accepting duplicate value in fast round {request.Ballot}: {request.Value}");
            }
        }

        Logger.Log($"{Name}: Accepted Accept({request.Ballot}, {request.Value})");

        if (request.PrepareNextAccept)
        {
            // Dual-purpose this call as 1) an accept, 2) a prepare for the next accept.
            // We only do this if we expect that the next accept will come from the same proposer.
            // For this optimization to count, the proposer also has to follow the same logic.
            PromisedBallot = request.Ballot.Successor(request.Ballot.Proposer);
            Assert(!request.Ballot.IsFastRoundBallot);
        }
        else
        {
            PromisedBallot = request.Ballot;
        }

        AcceptedBallot = request.Ballot;
        Value = request.Value;
        SendResponse(request.Proposer, new AcceptResponse(request.RequestId, Id, success: true, request.Ballot));
    }

    private void SendResponse(ActorId target, Event response)
    {
        SendEvent(target, response, options: new SendOptions(hashedState: response.GetHashCode()));
    }
}
