using FastCASPaxos.Model;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Events;

public class AcceptRequest<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public AcceptRequest(int requestId, ActorId proposer, Ballot ballot, TVersionedValue value, bool prepareNextRequest)
    {
        RequestId = requestId;
        Proposer = proposer;
        Ballot = ballot;
        Value = value;
        PrepareNextAccept = prepareNextRequest;
    }

    public int RequestId { get; }
    public ActorId Proposer { get; }
    public Ballot Ballot { get; }
    public TVersionedValue Value { get; }
    public bool PrepareNextAccept { get; }

    public override string ToString() => $"AcceptRequest({RequestId}, P-{Proposer.Value}, {Ballot}, {Value})";
    public override int GetHashCode() => HashCode.Combine(RequestId, Proposer, Ballot, Value, PrepareNextAccept);
}
