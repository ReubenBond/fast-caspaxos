using FastCASPaxos.Model;
using System;
using System.Collections.Generic;

namespace FastCASPaxos.Events;

public class TestConfigEvent<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    public int NumProposers { get; set; }
    public int NumAcceptors { get; set; }
    public List<IOperation<TValue, TVersionedValue>> Operations { get; set; }
    public TValue ExpectedValue { get; set; }
    public Func<TValue, TValue, bool> EqualityComparer { get; set; }
    public bool EnableFastCommitOptimization { get; set; }
    public bool EnableDistinguishedProposerOptimization { get; set; }
    public Func<TValue, string> PrintValue { get; internal set; }
}
