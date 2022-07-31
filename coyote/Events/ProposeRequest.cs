using FastCASPaxos.Model;
using Microsoft.Coyote.Actors;
using System;

namespace FastCASPaxos.Events;

public class ProposeRequest<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public ProposeRequest(int requestId, ActorId caller, IOperation<TValue, TVersionedValue> operation)
    {
        RequestId = requestId;
        Caller = caller;
        Operation = operation;
    }

    public int RequestId { get; }
    public ActorId Caller { get; }
    public IOperation<TValue, TVersionedValue> Operation { get; }

    public override string ToString() => $"ProposeRequest({RequestId}, {Operation})";
    public override int GetHashCode() => HashCode.Combine(RequestId, Caller, Operation);
}
