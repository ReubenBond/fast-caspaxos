using FastCASPaxos.Events;
using FastCASPaxos.Model;
using FastCASPaxos.Monitors;
using FastCASPaxos.Protocol;
using FastCASPaxos.Utilities;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FastCASPaxos;

public class Client<TValue, TVersionedValue> : StateMachine where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    private readonly Dictionary<int, ProposeResponse<TValue, TVersionedValue>> _responses = new();
    private List<ActorId> _proposers;
    private List<ActorId> _acceptors;
    private TValue _expectedValue;
    private Func<TValue, TValue, bool> _equalityComparer;
    private Func<TValue, string> _printValue;

    private int _nextRequestId = 0;

    private Dictionary<ActorId, ProposerData> _proposerRequests = new();
    private class ProposerData
    {
        public Queue<IOperation<TValue, TVersionedValue>> Unscheduled { get; } = new ();
        public Dictionary<int, ProposeRequest<TValue, TVersionedValue>> Scheduled { get; } = new();
    }

    [Start]
    [OnEntry(nameof(Initialize))]
    [OnEventDoAction(typeof(ProposeResponseHolder), nameof(OnProposeResponse))]
    public class Init : State { }

    public void Initialize(Event e)
    {
        var config = (TestConfigEvent<TValue, TVersionedValue>)((TestConfigEventHolder)e).Value;
        Monitor<LivenessMonitor<TValue, TVersionedValue>>(e);

        _acceptors = new List<ActorId>(config.NumAcceptors);
        _proposers = new List<ActorId>(config.NumProposers);
        _expectedValue = config.ExpectedValue;
        _equalityComparer = config.EqualityComparer;
        _printValue = config.PrintValue;

        for (int i = 0; i < config.NumAcceptors; ++i)
        {
            _acceptors.Add(CreateActor(typeof(AcceptorActor<TValue, TVersionedValue>)));
            Monitor<LivenessMonitor<TValue, TVersionedValue>>(new AcceptorUp());
        }

        for (int i = 0; i < config.NumProposers; ++i)
        {
            var proposer = CreateActor(typeof(ProposerActor<TValue, TVersionedValue>), new InitProposerEvent(
                id: i + 1,
                acceptors: _acceptors,
                enableDistinguishedProposer: config.EnableDistinguishedProposerOptimization,
                enableFastCommit: config.EnableFastCommitOptimization));
            _proposers.Add(proposer);
            _proposerRequests[proposer] = new ();
        }

        foreach (var op in config.Operations)
        {
            // Enqueue the op for a random proposer.
            var proposer = _proposers[RandomInteger(_proposers.Count)];
            _proposerRequests[proposer].Unscheduled.Enqueue(op);

            // Insert some reads randomly
            if (RandomBoolean(4))
            {
                _proposerRequests[proposer].Unscheduled.Enqueue(new Operation<TVersionedValue, TValue, TVersionedValue>
                {
                    Apply = (current, input) => (OperationStatus.NotApplicable, current),
                    Input = default,
                    Name = "Read current value"
                });
            }
            
            // Insert some duplicate operations randomly, on a random proposer
            if (RandomBoolean(3))
            {
                _proposerRequests[_proposers[RandomInteger(_proposers.Count)]].Unscheduled.Enqueue(op);
            }
        }

        foreach (var pair in _proposerRequests)
        {
            Logger.Log($"[Planned work for {pair.Key}]: {string.Join(", ", pair.Value.Unscheduled.Select(op => $"[{op}]"))}");
        }

        foreach (var pair in _proposerRequests)
        {
            var proposer = pair.Key;
            if (pair.Value.Unscheduled.TryDequeue(out var operation))
            {
                ScheduleOperation(proposer, operation);
            }
        }
    }

    public void OnProposeResponse(Event e)
    {
        var response = (ProposeResponse<TValue, TVersionedValue>)((ProposeResponseHolder)e).Value;
        _responses[response.Request.RequestId] = response;
        Logger.Log($"Client: Proposer response: {response}");
        //Logger.Log($"Client: History from {response.Proposer}: {string.Join(", ", response.CommitHistory.Select(v => $"[{v.Value}]"))}");

        var finalVersions = new Dictionary<ActorId, TVersionedValue>();
        var allProposerHistories = new Dictionary<ActorId, ProposeResponse<TValue, TVersionedValue>>();
        foreach (var r in _responses)
        {
            if (!allProposerHistories.TryGetValue(r.Value.Proposer, out var pr))
            {
                allProposerHistories[r.Value.Proposer] = r.Value;
            }
            else if (pr.CommitHistory.LastOrDefault() is { } prVal && r.Value.CommitHistory.LastOrDefault() is { } rVal && prVal.Version < rVal.Version)
            {
                allProposerHistories[r.Value.Proposer] = r.Value;
            }
        }

        foreach (var h in allProposerHistories)
        {
            Logger.Log($"Client: History from Proposer-{h.Key.Value}: {string.Join(", ", h.Value.CommitHistory.Select(v => $"[{v}]"))}");
            finalVersions[h.Key] = h.Value.CommitHistory.LastOrDefault();
        }

        if (_proposerRequests[response.Proposer].Unscheduled.TryDequeue(out var operation))
        {
            ScheduleOperation(response.Proposer, operation);
        }

        Assert(_proposerRequests[response.Proposer].Scheduled.Remove(response.Request.RequestId, out _), $"Unable to remove request {response.Request.RequestId} for proposer");
        var workRemains = false;
        foreach (var (actorId, ctx) in _proposerRequests)
        {
            if (ctx.Unscheduled.Count + ctx.Scheduled.Count > 0)
            {
                workRemains = true;
            }
        }

        if (!workRemains)
        {
            OnCompleted(finalVersions);
        }
    }

    private void OnCompleted(Dictionary<ActorId, TVersionedValue> values)
    {
        var highestVersionValue = default(TVersionedValue);
        var firstValue = values.Values.First();
        var finalValuesAgree = true;
        foreach (var (key, value) in values)
        {
            if (value.Version >= highestVersionValue.Version)
            {
                highestVersionValue = value;
            }

            if (!value.Equals(firstValue))
            {
                finalValuesAgree = false;
            }
        }

        if (!finalValuesAgree)
        {
            Logger.Log($"Final values differ: sending reads to all proposers");

            foreach (var (key, value) in values)
            {
                Logger.Log($"Current value for P-{key.Value}: {value}");
            }

            // Give every proposer one final read.
            foreach (var proposer in _proposers)
            {
                ScheduleOperation(proposer, new Operation<TVersionedValue, TValue, TVersionedValue>
                {
                    Apply = (current, input) => (OperationStatus.NotApplicable, current),
                    Input = default,
                    Name = "Read final value"
                });
            }

            return;
        }

        foreach (var (key, value) in values)
        {
            Logger.Log($"Final value for P-{key.Value}: {value}");
        }

        var actualValue = highestVersionValue.Value;
        if (_equalityComparer is { } comparer && !comparer(_expectedValue, actualValue))
        {
            var expected = _printValue?.Invoke(_expectedValue) ?? _expectedValue?.ToString();
            var actual = _printValue?.Invoke(actualValue) ?? actualValue?.ToString();
            var errorMessage = $"ERROR: final value \"{actual}\" does not equal expected value \"{expected}\"";
            Logger.Log(errorMessage);
            throw new InvalidOperationException(errorMessage);
        }

        this.RaiseHaltEvent();
    }

    private void ScheduleOperation(ActorId proposer, IOperation<TValue, TVersionedValue> operation)
    {
        var requestId = ++_nextRequestId;
        var request = new ProposeRequest<TValue, TVersionedValue>(requestId, Id, operation);
        _proposerRequests[proposer].Scheduled[requestId] = request;
        SendEvent(proposer, new ProposeRequestHolder { Value = request }, options: new SendOptions(hashedState: request.GetHashCode()));
    }
}
