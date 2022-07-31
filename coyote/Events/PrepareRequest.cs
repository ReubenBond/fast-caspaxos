using FastCASPaxos.Model;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Events;

public class PrepareRequest : Event
{
    public PrepareRequest(int requestId, ActorId proposer, Ballot ballot)
    {
        RequestId = requestId;
        Proposer = proposer;
        Ballot = ballot;
    }

    public int RequestId { get; }
    public ActorId Proposer { get; }
    public Ballot Ballot { get; }

    public override string ToString() => $"PrepareRequest({RequestId}, P-{Proposer.Value}, {Ballot})";

    public override int GetHashCode() => HashCode.Combine(RequestId, Proposer, Ballot);
}
