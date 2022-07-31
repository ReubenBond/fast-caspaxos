using FastCASPaxos.Events;
using FastCASPaxos.Model;
using FastCASPaxos.Monitors;
using FastCASPaxos.Utilities;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FastCASPaxos.Protocol;

public class ProposerActor<TValue, TVersionedValue> : StateMachine where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    // For diagnostics/testing, not part of the core algorithm
    public readonly List<TVersionedValue> _commitHistory = new();

    public List<ActorId> _acceptors;
    public int _nextMessageId;
    public int _proposerId;
    private Ballot _ballot;
    private Ballot _attemptBallot;
    private bool _enableDistinguishedLeader;
    private bool _enableFastCommit;

    // Optimization: skip prepare phase if we are a distinguished leader
    private bool _prepared;
    private TVersionedValue _cachedValue;
    private bool _preferFastCommit;
    private int _waitingOnRequestId = -1;
    private ProposeRequest<TValue, TVersionedValue> _proposeRequest;
    private TVersionedValue _acceptValue;
    private OperationStatus _operationStatus;
    private readonly Dictionary<ActorId, object> _responses = new();

    private int _consecutiveSuccessfulCommits;
    private int _totalSuccessfulCommits;

    private string _name;
    public string Name => _name ??= $"P-{Id.Value}";

    public void OnInit(Event e)
    {
        if (e is not InitProposerEvent init) return;

        if (init.Id <= 0) throw new ArgumentException("id cannot be less than or equal to zero");

        _acceptors = init.Acceptors;
        _proposerId = init.Id;
        _ballot = new Ballot(0, init.Id);
        _enableDistinguishedLeader = init.EnableDistinguishedProposer;
        _enableFastCommit = init.EnableFastCommit;
    }

    [Start]
    [OnEntry(nameof(OnInit))]
    [OnEventDoAction(typeof(ProposeRequestHolder), nameof(OnProposeValue))]
    [OnEventDoAction(typeof(PrepareResponseHolder), nameof(OnPrepareResponse))]
    [OnEventDoAction(typeof(AcceptResponse), nameof(OnAcceptResponse))]
    [OnEventDoAction(typeof(StartPrepare), nameof(OnStartPrepare))]
    public class Init : State { }

    public void OnProposeValue(Event e)
    {
        var request = (ProposeRequest<TValue, TVersionedValue>)((ProposeRequestHolder)e).Value;
        Logger.Log($"{Name}: Proposing value: {request}");

        _proposeRequest = request;
        if (_prepared && _enableDistinguishedLeader && _ballot.IsClassicRoundBallot)
        {
            // Since this proposer has already prepared (via the distinguished proposer optimization), skip the prepare phase and
            // try to have a value accepted.
            _attemptBallot = _ballot = _ballot.Successor(_proposerId);
            Logger.Log($"{Name}: Attempting distinguished leader commit in round {_attemptBallot} for operation {request.Operation}");
            StartAccept();
        }
        else if (_preferFastCommit && _enableFastCommit && _ballot.IsFastRoundBallot)
        {
            if (_prepared)
            {
                // Since this proposer has prepared a fast-round, skip the prepare phase and try to have a value accepted.
                // Use a fast-round ballot: one with an id of 0.
                _attemptBallot = _ballot = _ballot.Successor(0);
                _prepared = false;

                Logger.Log($"{Name}: Attempting fast commit in round {_attemptBallot} for operation {request.Operation}");
                StartAccept();
            }
            else
            {
                // Prepare a fast round
                _attemptBallot = _ballot = _ballot.Successor(0);
                StartPrepare();
            }
        }
        else
        {
            // Prepare a classic round
            _attemptBallot = _ballot = _ballot.Successor(_proposerId);
            StartPrepare();
        }
    }

    private void StartPrepare()
    {
        var requestId = ++_nextMessageId;
        _waitingOnRequestId = requestId;
        _responses.Clear();
        var request = new PrepareRequest(requestId, Id, _attemptBallot);
        Logger.Log($"{Name}: Sending {request}. Operation: {_proposeRequest.Operation}");
        foreach (var acceptor in _acceptors)
        {
            SendEvent(acceptor, request, options: new SendOptions(hashedState: request.GetHashCode()));
        }
    }

    public void OnPrepareResponse(Event e)
    {
        // Ignore unwanted responses.
        var pr = (PrepareResponse<TValue, TVersionedValue>)((PrepareResponseHolder)e).Value;
        if (pr.RequestId != _waitingOnRequestId) return;

        Logger.Log($"{Name}: OnPrepareResponse: {pr}");
        _responses[pr.Acceptor] = pr;

        // Tally the responses which have been received so far.
        int successResponses, failureResponses;
        (TVersionedValue Value, Ballot Ballot) maxAccepted;
        Ballot maxConflict;
        (successResponses, failureResponses, maxAccepted, maxConflict) = AggregateResponses(_responses);

        // Advance the ballot if a higher ballot has been encountered
        if (maxConflict > _ballot)
        {
            _ballot = _ballot.AdvanceTo(maxConflict, _proposerId);
            Logger.Log($"{Name}: Advancing from {_attemptBallot} to {_ballot.Successor(_proposerId)} (max conflicting ballot: {maxConflict})");
        }

        // Quorum was achieved: proceed to have the value accepted
        if (HasQuorum(_attemptBallot, successResponses))
        {
            // Select a value for the accept phase.
            TVersionedValue chosenValue;
            if (maxAccepted.Ballot.IsFastRoundBallot)
            {
                // Fast rounds have different vote counting mechanics for determining the value which should be used during the accept phase.
                // If there are multiple candidates with an equal occurrence, then there is a conflict and the prepare phase will need to be
                // restarted using a classic-round ballot to resolve the conflict.
                (chosenValue, var hasConflict) = ChooseValue(maxAccepted, _responses);

                if (hasConflict)
                {
                    // Use a non-fast round to have the value committed.
                    _preferFastCommit = false;
                    RetryPrepare();
                    return;
                }
            }
            else
            {
                // For classic rounds, pick the value with the highest ballot.
                chosenValue = maxAccepted.Value;
            }

            // Skip the next prepare if the previous prepare was also by this proposer or had the same ballot as this proposer (in the case of a fast round).
            _prepared = maxAccepted.Ballot.Proposer == _attemptBallot.Proposer;
            _cachedValue = chosenValue;

            Logger.Log($"{Name}: Prepared {_attemptBallot} with {successResponses} votes for and {failureResponses} against. Current value is {chosenValue}");
            StartAccept();
        }
        else if (HasQuorum(_attemptBallot, failureResponses))
        {
            // A quorum of failures has occurred.
            _prepared = false;

            // Retry indefinitely.
            Logger.Log($"{Name}: Failed to prepare {_attemptBallot} with {successResponses} votes for and {failureResponses} against");
            RetryPrepare();
        }
        else if (!IsQuorumPossible(_attemptBallot, successResponses, failureResponses))
        {
            Logger.Log($"{Name}: Failed to prepare {_attemptBallot} since quorum is not possible. {successResponses} votes for and {failureResponses} against");
            RetryPrepare();
        }
        else
        {
            Logger.Log($"{Name} waiting for quorum of success ({successResponses}) or fail responses ({failureResponses})");
        }

        static (int SuccessResponses, int FailureResponses, (TVersionedValue Value, Ballot Ballot) MaxAccepted, Ballot MaxConflict) AggregateResponses(Dictionary<ActorId, object> responses)
        {
            var successResponses = 0;
            var failureResponses = 0;
            var maxConflict = Ballot.Zero;
            var maxAccepted = (Value: default(TVersionedValue), Ballot: default(Ballot));

            foreach (var r in responses.Values)
            {
                var response = (PrepareResponse<TValue, TVersionedValue>)r;
                if (response.Success)
                {
                    ++successResponses;
                    if (response.Ballot > maxAccepted.Ballot)
                    {
                        maxAccepted = (response.Value, response.Ballot);
                    }
                    else if (response.Ballot == maxAccepted.Ballot)
                    {
                        maxAccepted = (maxAccepted.Value, maxAccepted.Ballot);
                    }
                }
                else
                {
                    ++failureResponses;
                    if (response.Ballot > maxConflict)
                    {
                        maxConflict = response.Ballot;
                    }
                }
            }

            return (successResponses, failureResponses, maxAccepted, maxConflict);
        }
    }

    /// <summary>
    /// For fast rounds, determine the value which the proposer should try to have accepted for this round.
    /// Note that this may not be the value which this proposer is trying to commit.
    /// </summary>
    private static (TVersionedValue Value, bool HasConflict) ChooseValue((TVersionedValue Value, Ballot Ballot) maxAccepted, Dictionary<ActorId, object> responses)
    {
        // If all values are the same, choose that value.
        bool hasDistinctValues = false;
        foreach (var r in responses.Values)
        {
            var response = (PrepareResponse<TValue, TVersionedValue>)r;
            if (response.Success)
            {
                if (!response.Value.Equals(maxAccepted.Value))
                {
                    hasDistinctValues = true;
                    break;
                }
            }
        }

        if (hasDistinctValues)
        {
            List<(TVersionedValue Value, int Count)> values = FindDistinctValues(maxAccepted.Ballot, responses);

            (TVersionedValue Value, int Count) chosen = default;
            var hasConflict = false;
            foreach (var value in values)
            {
                // Find the value with the highest count.
                if (value.Count > chosen.Count)
                {
                    chosen = value;
                    hasConflict = false;
                }
                else if (value.Count == chosen.Count)
                {
                    // If there are multiple values with the same count, there is a conflict and a classic-round will
                    // need to be performed to resolve the conflict.
                    hasConflict = true;
                }
                else
                {
                    // Ignore values with fewer occurrences.
                }
            }

            return (chosen.Value, hasConflict);
        }

        return (maxAccepted.Value, HasConflict: false);

        static List<(TVersionedValue Value, int Count)> FindDistinctValues(Ballot ballot, Dictionary<ActorId, object> responses)
        {
            var values = new List<(TVersionedValue Value, int Count)>();
            foreach (var r in responses.Values)
            {
                var response = (PrepareResponse<TValue, TVersionedValue>)r;
                if (!response.Success)
                {
                    continue;
                }

                if (response.Ballot != ballot)
                {
                    continue;
                }

                var found = false;
                for (var i = 0; i < values.Count; ++i)
                {
                    var (value, count) = values[i];
                    
                    if (value.Equals(response.Value))
                    {
                        // Count the occurrences of this value.
                        values[i] = (value, count + 1);
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    values.Add((response.Value, 1));
                }
            }

            return values;
        }
    }

    private void StartAccept()
    {
        var (operationStatus, acceptValue) = _proposeRequest.Operation.Apply(_cachedValue);
        StartAcceptWithValue(operationStatus, acceptValue);
    }

    private void StartAcceptWithValue(OperationStatus operationStatus, TVersionedValue acceptValue)
    {
        (_operationStatus, _acceptValue) = (operationStatus, acceptValue);

        var requestId = ++_nextMessageId;
        var request = new AcceptRequestHolder { Value = new AcceptRequest<TValue, TVersionedValue>(requestId, Id, _attemptBallot, _acceptValue, prepareNextRequest: _prepared) };
        Logger.Log($"{Name}: Sending {request}. Cached value: {_cachedValue}. Operation: {_proposeRequest.Operation}");
        _waitingOnRequestId = requestId;
        _responses.Clear();
        foreach (var acceptor in _acceptors)
        {
            SendEvent(acceptor, request, options: new SendOptions(hashedState: request.GetHashCode()));
        }
    }

    public void OnAcceptResponse(Event e)
    {
        // Ignore unwanted responses.
        if (e is not AcceptResponse ar)
        {
            throw new ArgumentException($"Expected event of type {typeof(AcceptResponse)} but found {e.GetType()}");
        }

        if (ar.RequestId != _waitingOnRequestId) return;
        Logger.Log($"{Name}: OnAcceptResponse: {ar}");

        _responses[ar.Acceptor] = ar;

        var successResponses = 0;
        var failureResponses = 0;
        var maxConflict = Ballot.Zero;
        foreach (var r in _responses.Values)
        {
            var response = (AcceptResponse)r;
            if (response.Success)
            {
                ++successResponses;
            }
            else
            {
                ++failureResponses;
                if (response.Ballot >= maxConflict)
                {
                    maxConflict = response.Ballot;
                }
            }
        }

        var ballot = _ballot;
        if (maxConflict > _ballot)
        {
            _prepared = false;
            _ballot = _ballot.AdvanceTo(maxConflict, _proposerId);
            Logger.Log($"{Name}: Advancing from {ballot} to {_ballot.Successor(_proposerId)} (max conflicting ballot: {maxConflict})");
        }

        // Quorum was achieved: the value has been committed
        if (HasQuorum(_attemptBallot, successResponses))
        {
            _commitHistory.Add(_acceptValue);
            _cachedValue = _acceptValue;
            _preferFastCommit = true;

            if (_operationStatus is OperationStatus.Success)
            {
                Logger.Log($"{Name}: Successfully committed [{_proposeRequest.Operation}] (committed value: {_cachedValue}) Fast: {_attemptBallot.IsFastRoundBallot}. {successResponses} votes for and {failureResponses} against");
            }
            else if (_operationStatus is OperationStatus.NotApplicable)
            {
                Logger.Log($"{Name}: [{_proposeRequest.Operation}] was successfully implicitly committed (committed value: {_cachedValue}) Fast: {_attemptBallot.IsFastRoundBallot}. {successResponses} votes for and {failureResponses} against");
            }
            else if (_operationStatus is OperationStatus.Failed)
            {
                Logger.Log($"{Name}: Failed to commit [{_proposeRequest.Operation}] (version) (committed value: {_cachedValue}), trying again. Fast: {_attemptBallot.IsFastRoundBallot}. {successResponses} votes for and {failureResponses} against");
            }

            Monitor<SafetyMonitor<TValue, TVersionedValue>>(new CommittedValueHolder { Value = new CommittedValue<TValue, TVersionedValue> { History = new List<TVersionedValue>(_commitHistory), Proposer = Id } });

            if (_operationStatus != OperationStatus.Failed)
            {
                // Respond to the initial caller.
                var response = new ProposeResponse<TValue, TVersionedValue>(Id, _proposeRequest, new List<TVersionedValue>(_commitHistory));
                SendEvent(_proposeRequest.Caller, new ProposeResponseHolder { Value = response }, options: new SendOptions(hashedState: response.GetHashCode()));

                // Success, clean up request state
                _proposeRequest = default;
                _responses.Clear();
                _waitingOnRequestId = -1;
                _acceptValue = default;

                _consecutiveSuccessfulCommits++;
                _totalSuccessfulCommits++;
            }
            else
            {
                // Clean up request state
                _acceptValue = default;
                RetryPrepare();
            }
        }
        else if (HasQuorum(_attemptBallot, failureResponses))
        {
            // Retry indefinitely.
            Logger.Log($"{Name}: Failed to commit [{_proposeRequest.Operation}] (quorum conflict), trying again. Fast: {_attemptBallot.IsFastRoundBallot}. {successResponses} votes for and {failureResponses} against");
            RetryPrepare();
        }
        else if (!IsQuorumPossible(_attemptBallot, successResponses, failureResponses))
        {
            // Retry indefinitely.
            Logger.Log($"{Name}: Failed to commit [{_proposeRequest.Operation}] (quorum not possible), trying again. Fast: {_attemptBallot.IsFastRoundBallot}. {successResponses} votes for and {failureResponses} against");
            RetryPrepare();
        }
        else
        {
            Logger.Log($"{Name} waiting for quorum of success ({successResponses}) or fail responses ({failureResponses})");
        }
    }

    public bool HasQuorum(Ballot ballot, int responses)
    {
        if (ballot.IsFastRoundBallot)
        {
            // >= 3n/4
            return 4 * responses >= 3 * _acceptors.Count;
        }
        else
        {
            // >= n/2 + 1 (i.e., > n/2)
            return 2 * responses > _acceptors.Count;
        }
    }

    /// <summary>
    /// Returns true if a quorum is possible.
    /// </summary>
    /// <remarks>
    /// A quorum of either success or failure responses is not currently possible, so retry indefinitely.
    /// Typical cases where this could happen are:
    /// <list type="unordered">
    ///   <item>4 acceptors nodes with 2 success + 2 failures</item>
    ///   <item>3 acceptors and a Fast Paxos round with 2 success + 1 failure</item>
    /// </list>
    /// </remarks>
    public bool IsQuorumPossible(Ballot ballot, int successResponses, int failureResponses)
    {
        // Assuming all of the remaining votes go one direction or the other, can a quorum be achieved?
        var remaining = _acceptors.Count - (successResponses + failureResponses);
        return HasQuorum(ballot, successResponses + remaining) || HasQuorum(ballot, failureResponses + remaining);
    }

    public void RetryPrepare()
    {
        _prepared = false;
        _waitingOnRequestId = -1;
        _responses.Clear();
        _consecutiveSuccessfulCommits = 0;

        SendEvent(Id, new StartPrepare(), options: new SendOptions(hashedState: nameof(StartPrepare).GetHashCode()));
    }

    public void OnStartPrepare(Event e)
    {
        _attemptBallot = _ballot = _ballot.Successor(_proposerId);
        StartPrepare();
    }
}
