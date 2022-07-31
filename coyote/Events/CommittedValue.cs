using FastCASPaxos.Model;
using Microsoft.Coyote.Actors;
using System;
using System.Collections.Generic;

namespace FastCASPaxos.Events;

public class CommittedValue<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public List<TVersionedValue> History { get; set; }
    public ActorId Proposer { get; set; }
    public override int GetHashCode() => HashCode.Combine(History, Proposer);
}
