using FastCASPaxos.Model;
using Microsoft.Coyote.Actors;
using System;
using System.Collections.Generic;

namespace FastCASPaxos.Events;

public class ProposeResponse<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public ProposeResponse(ActorId proposer, ProposeRequest<TValue, TVersionedValue> request, List<TVersionedValue> commitHistory)
    {
        Proposer = proposer;
        Request = request;
        CommitHistory = commitHistory;
    }

    public ProposeRequest<TValue, TVersionedValue> Request { get; }
    public List<TVersionedValue> CommitHistory { get; }
    public ActorId Proposer { get; }

    public override string ToString() => $"ProposeResponse({Request}, {string.Join(", ", CommitHistory)})";

    public override int GetHashCode() => HashCode.Combine(Request, CommitHistory, Proposer);
}
