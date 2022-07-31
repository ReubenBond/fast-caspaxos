using FastCASPaxos.Model;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Events;

public class AcceptResponse : Event
{
    public AcceptResponse(int requestId, ActorId acceptor, bool success, Ballot ballot)
    {
        RequestId = requestId;
        Acceptor = acceptor;
        Success = success;
        Ballot = ballot;
    }

    public int RequestId { get; }
    public ActorId Acceptor { get; }
    public bool Success { get; }
    public Ballot Ballot { get; }

    public override string ToString() => $"AcceptResponse({RequestId}, A-{Acceptor.Value}, {Success}, {Ballot})";
    public override int GetHashCode() => HashCode.Combine(RequestId, Acceptor, Ballot, Success);
}
