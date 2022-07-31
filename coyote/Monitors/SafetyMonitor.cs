using FastCASPaxos.Events;
using FastCASPaxos.Model;
using FastCASPaxos.Utilities;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using Microsoft.Coyote.Specifications;
using System.Collections.Generic;
using System.Linq;

namespace FastCASPaxos.Monitors;

public class SafetyMonitor<TValue, TVersionedValue> : Monitor where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    private readonly Dictionary<ActorId, List<TVersionedValue>> _histories = new();
    private readonly SortedDictionary<int, TVersionedValue> _allCommittedValues = new SortedDictionary<int, TVersionedValue>();

    [Start]
    [OnEventDoAction(typeof(CommittedValueHolder), nameof(OnCommittedValue))]
    public class Init : State { }

    public void OnCommittedValue(Event e)
    {
        var c = (CommittedValue<TValue, TVersionedValue>)((CommittedValueHolder)e).Value;

        // Ensure that committed values are consistent within a given proposer
        var prev = default(TVersionedValue);
        foreach (var committedValue in c.History)
        {
            if ((!committedValue.Equals(prev) && committedValue.Version == prev.Version) || committedValue.Version < prev.Version)
            {
                var message = $"Non-linearizable history on P-{c.Proposer.Value}: {committedValue} >= {prev} in history: {string.Join(", ", c.History.Select(v => $"[{v}]"))}";
                Logger.Log("SAFETY: " + message);
                Assert(committedValue.Version >= prev.Version, message);
            }

            prev = committedValue;
        }

        var historySet = new HashSet<TVersionedValue>(c.History);

        if (_histories.TryGetValue(c.Proposer, out var existingHistory))
        {
            if (existingHistory.Max(v => v.Version) < c.History.Max(v => v.Version))
            {
                foreach (var hV in existingHistory)
                {
                    Assert(historySet.Contains(hV));
                }

                _histories[c.Proposer] = c.History;
            }
            else
            {
                foreach (var cV in c.History)
                {
                    Assert(existingHistory.Contains(cV));
                }
            }
        }
        else
        {
            _histories[c.Proposer] = c.History;
        }

        // Ensure that committed values are consistent across responses
        foreach (var r in _histories)
        {
            foreach (var h in r.Value)
            {
                if (!_allCommittedValues.TryGetValue(h.Version, out var val))
                {
                    _allCommittedValues[h.Version] = h;
                }
                else
                {
                    if (!EqualityComparer<TVersionedValue>.Default.Equals(val, h))
                    {
                        var message = $"Committed value at version {h.Version} do not agree: {h} != {val}";
                        Logger.Log("SAFETY: " + message);
                        Assert(false, message);
                    }
                }
            }
        }

        // Ensure that the commit histories form a valid sequence.
        var first = true;
        prev = default;
        foreach (var (key, value) in _allCommittedValues)
        {
            if (!first)
            {
                if (!value.IsValidSuccessorTo(prev))
                {
                    var message = $"Committed value at version {value.Version} does not agree with lower-versioned value: {value} != {prev}";
                    Logger.Log("SAFETY: " + message);
                    Assert(false, message);
                }
            }

            prev = value;
            first = false;
        }
        
    }
}
