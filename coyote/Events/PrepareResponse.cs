using FastCASPaxos.Model;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Events;

public class PrepareResponse<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public PrepareResponse(int requestId, ActorId acceptor, bool success, Ballot ballot, TVersionedValue value)
    {
        RequestId = requestId;
        Acceptor = acceptor;
        Success = success;
        Ballot = ballot;
        Value = value;
    }

    public int RequestId { get; }
    public ActorId Acceptor { get; }
    public bool Success { get; }
    public Ballot Ballot { get; }
    public TVersionedValue Value { get; }

    public override string ToString() => $"PrepareResponse({RequestId}, A-{Acceptor.Value}, {Success}, {Ballot}, {Value})";
    public override int GetHashCode() => HashCode.Combine(RequestId, Acceptor, Success, Ballot, Value);
}
