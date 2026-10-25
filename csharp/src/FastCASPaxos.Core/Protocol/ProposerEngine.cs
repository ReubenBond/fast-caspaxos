using System.Collections.Immutable;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

/// <summary>
/// Drives the proposer side of Fast/Classic CASPaxos for a single proposer.
/// It recovers the latest durable value during prepare, applies caller operations on top of that
/// recovered state, and remembers piggybacked promises that can let a later request skip prepare.
/// </summary>
public abstract class ProposerEngine<TValue, TRoute>
    where TRoute : notnull
{
    private readonly ImmutableArray<TRoute> _acceptors;
    private readonly Dictionary<TRoute, PrepareResponse> _prepareResponses = [];
    private readonly Dictionary<TRoute, AcceptResponse> _acceptResponses = [];
    private readonly ProposerRuntime _runtime;
    private readonly ProposerModePolicy _modePolicy;
    private protected readonly TValue _initialValue;
    private Ballot _preparedBallot;
    private PreparedBallotState _preparedBallotState;

    public ProposerEngine(
        ProposerRuntime runtime,
        TValue initialValue,
        ImmutableArray<TRoute> acceptors,
        ProposerModePolicy modePolicy)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        if (acceptors.IsDefault)
        {
            throw new ArgumentNullException(nameof(acceptors));
        }

        ArgumentNullException.ThrowIfNull(modePolicy);
        modePolicy.Validate();

        _runtime = runtime;
        _modePolicy = modePolicy.CreateRuntimeCopy();
        _initialValue = initialValue;
        CurrentValue = initialValue;
        _acceptors = acceptors;
    }

    protected IOperation<TValue>? CurrentOperation { get; private set; }

    public abstract int ProposerId { get; }

    public ProposerModePolicy ModePolicy => _modePolicy;

    public bool HasPendingProposal => CurrentOperation is not null;

    public Ballot Ballot { get; private set; }

    public int ConflictDebt => _modePolicy.ConflictDebt;

    public int LeaderMisses => _modePolicy.LeaderMisses;

    public int? LeaderHintProposerId => _modePolicy.LeaderHintProposerId;

    public bool ShouldForwardToLeader => _modePolicy.ShouldForwardToLeader(ProposerId);

    public Ballot? PreparedBallot
    {
        get => _preparedBallotState == PreparedBallotState.Prepared ? _preparedBallot : null;
        private set
        {
            if (value is { } ballot)
            {
                _preparedBallot = ballot;
                _preparedBallotState = PreparedBallotState.Prepared;
            }
            else if (_preparedBallotState == PreparedBallotState.Prepared)
            {
                ClearPreparedBallot();
            }
        }
    }

    /// <summary>
    /// The proposer's single value slot. Interpret it via <see cref="CurrentValueStatus"/>.
    /// When status is <see cref="ProposerValueStatus.Cached"/>, the slot holds the latest durable
    /// or recovered value. When status is <see cref="ProposerValueStatus.PendingAccept"/>, it
    /// holds the in-flight accept value for the current ballot.
    /// </summary>
    public TValue CurrentValue { get; private set; }

    public ProposerValueStatus CurrentValueStatus { get; private set; }

    public Ballot RequestedNextBallot
    {
        get => _preparedBallotState == PreparedBallotState.Requested ? _preparedBallot : Ballot.Zero;
        private set
        {
            if (!value.IsZero)
            {
                _preparedBallot = value;
                _preparedBallotState = PreparedBallotState.Requested;
            }
            else if (_preparedBallotState == PreparedBallotState.Requested)
            {
                ClearPreparedBallot();
            }
        }
    }

    public void StartProposal(IOperation<TValue> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        CurrentOperation = operation;
        _runtime.Diagnostics.OnProposalStarted(this, ProposerId, Ballot);
        BeginOperation();
    }

    /// <summary>
    /// Starts above locally observed ballots, retaining a certified prepared ballot while the
    /// local acceptor still permits it. Value recovery remains governed by quorum evidence.
    /// </summary>
    public void StartProposal(IOperation<TValue> operation, Ballot acceptedBallot, Ballot promisedBallot)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (CurrentOperation is not null)
            throw new InvalidOperationException("A local observation can seed only an idle proposer.");

        if (PreparedBallot is { } prepared && _modePolicy.CanReusePreparedBallot(prepared, ProposerId)
            && acceptedBallot < prepared && promisedBallot <= prepared)
        {
            StartProposal(operation);
            return;
        }

        Ballot = Ballot.Max(Ballot, Ballot.Max(acceptedBallot, promisedBallot));
        StartProposal(operation);
    }

    public void StartCurrentRequest()
    {
        if (CurrentOperation is not null)
        {
            BeginOperation();
        }
    }

    public void LearnCommittedValue(Ballot acceptedBallot, TValue value, Ballot preparedBallot = default)
    {
        if (acceptedBallot.IsZero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(acceptedBallot),
                acceptedBallot,
                "A learned committed value must include a non-zero accepted ballot.");
        }

        if (CurrentOperation is not null)
        {
            return;
        }

        if (acceptedBallot < Ballot)
        {
            return;
        }

        ClearAcceptState();
        Ballot = acceptedBallot;
        SetCachedValue(value);
        var nextPreparedBallot = PreparedBallot is { } existingPreparedBallot
            ? Ballot.Max(existingPreparedBallot, preparedBallot)
            : preparedBallot;
        PreparedBallot = nextPreparedBallot > Ballot ? nextPreparedBallot : null;
        _modePolicy.LearnCommittedValue(acceptedBallot, preparedBallot);
    }

    public void HandlePreparePromised(PreparePromise<TValue, TRoute> message)
        => HandlePrepareResponse(message.Round, message.Acceptor, new PrepareResponse(message.AcceptedBallot, message.AcceptedValue, Ballot.Zero));

    public void HandlePrepareRejected(PrepareRejection<TValue, TRoute> message)
        => HandlePrepareResponse(message.Round, message.Acceptor, new PrepareResponse(message.AcceptedBallot, message.AcceptedValue, message.ConflictBallot));

    private void HandlePrepareResponse(int round, TRoute acceptor, PrepareResponse response)
    {
        if (round != Ballot.Round || CurrentOperation is null)
        {
            // Late responses and responses from a completed request must not mutate cached state or affect the
            // next request. Once the client has already been replied to, thr round is finished.
            return;
        }

        _prepareResponses[acceptor] = response;
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // Once prepare has already advanced this round into accept, late prepare responses are
            // informational only. Accept results now determine whether the round commits or retries.
            return;
        }

        var (successResponses, failureResponses, maxAccepted, maxConflictBallot) = AggregatePrepareResponses();

        // Every prepare response already reports durable acceptor state, so we can refresh our
        // best-known local cache immediately even though quorum is still required before acting.
        AdoptValue(maxAccepted.Value);
        if (HasQuorum(Ballot, successResponses))
        {
            // A successful prepare recovers the freshest durable value any acceptor reported and
            // then immediately re-applies the caller's operation on top of that recovered base.
            // AcceptedBallot/AcceptedValue report durable acceptor state regardless of whether this
            // prepare promised or rejected; only ConflictBallot is rejection-specific.
            if (!TryResolvePreparedValue(maxAccepted, out var preparedValue, out var conflictBallot))
            {
                _runtime.Diagnostics.OnPrepareFailed(this, ProposerId, Ballot, conflictBallot);
                RetryPrepare(conflictBallot);
                return;
            }

            AdoptValue(preparedValue);
            _runtime.Diagnostics.OnPrepareSucceeded(this, ProposerId, Ballot);
            ContinueCurrentRequest();
            return;
        }

        if (HasQuorum(Ballot, failureResponses) || !IsQuorumPossible(Ballot, successResponses, failureResponses))
        {
            // Once this round is lost, abandon it and retry above the highest conflicting ballot.
            _runtime.Diagnostics.OnPrepareFailed(this, ProposerId, Ballot, maxConflictBallot);
            RetryPrepare(maxConflictBallot);
        }
    }

    public void HandleAcceptAccepted(AcceptAccepted<TRoute> message) => HandleAcceptResponse(message.Round, message.Acceptor, new AcceptResponse(IsAccepted: true, message.PromisedBallot));

    public void HandleAcceptRejected(AcceptRejected<TRoute> message) => HandleAcceptResponse(message.Round, message.Acceptor, new AcceptResponse(IsAccepted: false, message.ConflictingBallot));

    /// <summary>
    /// Called by the host when a response timeout fires. If the engine is still waiting for
    /// responses in the current round (prepare or accept), forces a retry with a higher ballot.
    /// Returns true if a retry was triggered; false if the timeout was stale (wrong round or
    /// no pending operation).
    /// </summary>
    public bool HandleResponseTimeout(int round)
    {
        if (round != Ballot.Round || CurrentOperation is null)
        {
            return false;
        }

        // Determine whether we're waiting in prepare or accept phase
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // In accept phase — check if we already have quorum or are already retrying
            var successResponses = 0;
            var failureResponses = 0;
            var maxConflict = Ballot.Zero;
            foreach (var current in _acceptResponses.Values)
            {
                if (current.IsAccepted) ++successResponses;
                else
                {
                    ++failureResponses;
                    if (current.Ballot > maxConflict) maxConflict = current.Ballot;
                }
            }

            if (HasQuorum(Ballot, successResponses))
            {
                return false; // Already reached quorum, timeout is stale
            }

            _runtime.Diagnostics.OnAcceptFailed(this, ProposerId, Ballot, maxConflict);
            RetryPrepare(maxConflict);
            return true;
        }
        else
        {
            // In prepare phase
            var (successResponses, failureResponses, _, maxConflictBallot) = AggregatePrepareResponses();
            if (HasQuorum(Ballot, successResponses))
            {
                return false; // Already reached quorum
            }

            _runtime.Diagnostics.OnPrepareFailed(this, ProposerId, Ballot, maxConflictBallot);
            RetryPrepare(maxConflictBallot);
            return true;
        }
    }

    private void HandleAcceptResponse(int round, TRoute acceptor, AcceptResponse response)
    {
        if (round != Ballot.Round || CurrentOperation is null)
        {
            // Late responses and responses from a completed request must not mutate cached state or affect the
            // next request. Once the client has already been replied to, the round is finished.
            return;
        }

        _acceptResponses[acceptor] = response;

        var successResponses = 0;
        var failureResponses = 0;
        var maxConflict = Ballot.Zero;
        var preparedMatches = 0;
        var requestedNextBallot = RequestedNextBallot;

        foreach (var current in _acceptResponses.Values)
        {
            if (current.IsAccepted)
            {
                ++successResponses;
                if (!requestedNextBallot.IsZero && current.Ballot == requestedNextBallot)
                {
                    // Successful accepts may also confirm that the piggybacked next ballot was
                    // prepared, allowing a future request to skip an explicit prepare.
                    ++preparedMatches;
                }
            }
            else
            {
                ++failureResponses;
                if (current.Ballot > maxConflict)
                {
                    maxConflict = current.Ballot;
                }
            }
        }

        if (HasQuorum(Ballot, successResponses))
        {
            // Once accept reaches quorum, the value is committed for this ballot.
            var committedValue = CommitPendingAcceptValue();

            // Only a real quorum of echoed promises is reusable. Partial piggybacked prepares cannot
            // safely replace a standalone prepare on the next request.
            if (!requestedNextBallot.IsZero && HasQuorum(requestedNextBallot, preparedMatches))
            {
                PreparedBallot = requestedNextBallot;
            }

            _modePolicy.RecordAcceptQuorumSucceeded(Ballot, ProposerId);

            _runtime.Diagnostics.OnAcceptSucceeded(this, ProposerId, Ballot);
            _runtime.Diagnostics.OnValueCommitted(this, committedValue, ProposerId, Ballot);

            OnValueCommitted(new CommittedValue<TValue>(committedValue));

            CompleteCurrentRequest();
            return;
        }

        if (HasQuorum(Ballot, failureResponses) || !IsQuorumPossible(Ballot, successResponses, failureResponses))
        {
            // Once this accept can no longer win, fall back to prepare and pick a ballot above all
            // conflicts we have seen.
            _runtime.Diagnostics.OnAcceptFailed(this, ProposerId, Ballot, maxConflict);
            RetryPrepare(maxConflict);
        }
    }

    public bool HasQuorum(Ballot ballot, int responses)
    {
        // Fast rounds run without a preceding prepare, so they need a 3/4 quorum to keep any two
        // successful fast quorums intersecting. Classic rounds are serialized by prepare, so a
        // simple majority is enough.
        if (ballot.IsFastRoundBallot)
        {
            return 4 * responses >= 3 * _acceptors.Length;
        }

        return 2 * responses > _acceptors.Length;
    }

    public bool IsQuorumPossible(Ballot ballot, int successResponses, int failureResponses)
    {
        var remaining = _acceptors.Length - (successResponses + failureResponses);

        // Stop waiting once even the best-case remaining responses cannot produce either a success
        // quorum or a rejection quorum.
        return HasQuorum(ballot, successResponses + remaining) || HasQuorum(ballot, failureResponses + remaining);
    }

    protected abstract void OnSendPrepare(PrepareRequest request);
    protected abstract void OnSendAccept(AcceptRequest<TValue> request);
    protected abstract void OnProposalCompleted(ProposeResponse<TValue> response);
    protected abstract void OnValueCommitted(CommittedValue<TValue> committedValue);
    /// <summary>
    /// Reserves a newly allocated ballot before it becomes observable. Durable hosts override this
    /// to return an owned classic ballot at least as large as the candidate. Shared fast ballots
    /// retain their identity. A reserved piggyback promise is consumed at most once by this engine.
    /// </summary>
    protected virtual Ballot ReserveBallot(Ballot candidate) => candidate;
    protected virtual void OnCachedValueChanged(TValue value) { }
    protected virtual void OnPendingAcceptValueStarted(TValue value) { }
    protected virtual void OnPendingAcceptValueCommitted(TValue value) { }
    protected virtual void OnPendingAcceptValueCleared(TValue value) { }

    /// <summary>
    /// Called whenever a prepare retry is forced. <paramref name="conflictBallot"/> is non-zero when a
    /// competing ballot was observed; zero indicates a timeout-forced or operation-failed retry.
    /// Only real ballot conflicts (non-zero) count as contention for adaptive policy purposes.
    /// </summary>
    protected virtual void OnRetryPrepare(Ballot conflictBallot) { }

    public void RecordLeaderForwardFailure()
        => _modePolicy.RecordLeaderForwardFailure();

    public void RecordLeaderForwardSuccess()
        => _modePolicy.RecordLeaderForwardSuccess();

    public bool ShouldAttemptLeaderTakeover() => _modePolicy.ShouldAttemptLeaderTakeover(ProposerId);

    private void BeginOperation()
    {
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // A new local attempt abandons any previously in-flight accept and re-enters through a
            // fresh ballot. The cluster state will be re-read via prepare unless this is the
            // implicitly prepared initial fast round.
            ClearAcceptState();
        }

        // A cached prepared ballot is only useful while it is strictly ahead of the ballot we have
        // already consumed. Once we catch up, the cache no longer lets us skip prepare.
        if (PreparedBallot is { } preparedBallot && preparedBallot <= Ballot)
        {
            PreparedBallot = null;
        }

        if (PreparedBallot is { } reusableBallot
            && _modePolicy.CanReusePreparedBallot(reusableBallot, ProposerId))
        {
            // Reuse the piggybacked promise exactly once, jumping straight into operation execution
            // as if a standalone prepare for that ballot had already succeeded.
            PreparedBallot = null;
            Ballot = reusableBallot;
            ContinueCurrentRequest();
            return;
        }

        Ballot = CreateInitialBallot();
        if (_modePolicy.ShouldSkipInitialPrepare(ProposerId)
            && Ballot == Ballot.InitialFast()
            && TryStartInitialFastAccept())
        {
            return;
        }

        SendPrepareForCurrentBallot();
    }

    private Ballot CreateInitialBallot()
    {
        // New work stays on the fast path when enabled; otherwise it starts in this proposer's
        // classic ballot space. Retries are handled separately by RetryPrepare.
        return ReserveBallot(_modePolicy.CreateProposalBallot(ProposerId, Ballot));
    }

    private void SendPrepareForCurrentBallot()
    {
        _prepareResponses.Clear();
        _acceptResponses.Clear();

        _runtime.Diagnostics.OnPrepareStarted(this, ProposerId, Ballot);
        OnSendPrepare(new PrepareRequest(Ballot));
    }

    private (int SuccessResponses, int FailureResponses, (TValue Value, Ballot Ballot) MaxAccepted, Ballot MaxConflictBallot) AggregatePrepareResponses()
    {
        var successResponses = 0;
        var failureResponses = 0;
        var maxAccepted = (Value: _initialValue, Ballot: Ballot.Zero);
        var maxConflictBallot = Ballot.Zero;

        // Every prepare response reports the acceptor's last accepted ballot/value, so both
        // promises and rejections participate in value recovery. Only rejections contribute the
        // conflict ballot that the next retry must jump above.
        foreach (var response in _prepareResponses.Values)
        {
            if (response.IsPromise)
            {
                ++successResponses;
            }
            else
            {
                ++failureResponses;
                if (response.ConflictBallot > maxConflictBallot)
                {
                    maxConflictBallot = response.ConflictBallot;
                }
            }

            if (response.AcceptedBallot > maxAccepted.Ballot)
            {
                maxAccepted = (response.AcceptedValue, response.AcceptedBallot);
            }
        }

        return (successResponses, failureResponses, maxAccepted, maxConflictBallot);
    }

    private bool TryResolvePreparedValue(
        (TValue Value, Ballot Ballot) maxAccepted,
        out TValue value,
        out Ballot conflictBallot)
    {
        value = maxAccepted.Value;
        conflictBallot = Ballot.Zero;

        if (maxAccepted.Ballot.IsZero)
        {
            return true;
        }

        if (!maxAccepted.Ballot.IsFastRoundBallot)
        {
            // Classic accepted values were already serialized by prepare, so the highest accepted
            // classic ballot can be adopted directly.
            return true;
        }

        // A fast ballot may have accepted competing values concurrently. Recover by considering only
        // values from that highest fast ballot. If one value could still have reached a fast quorum
        // given the responses we have seen, we must preserve it. Otherwise the fast round cannot
        // have committed any value, so the classic recovery ballot may safely pick a winner and
        // move forward.
        (value, var hasConflict) = ChooseValue(maxAccepted);
        if (!hasConflict)
        {
            return true;
        }

        conflictBallot = maxAccepted.Ballot;
        return false;
    }

    private void AdoptValue(TValue value)
    {
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // Late prepare responses can still arrive after we have already moved into accept. The
            // slim engine keeps only one TValue slot, so once that slot holds the pending accept
            // value we intentionally stop refreshing the cached view until the round finishes.
            return;
        }

        if (!EqualityComparer<TValue>.Default.Equals(value, default)
            && !EqualityComparer<TValue>.Default.Equals(ReadCachedValueOrDefault("compare recovered values"), value))
        {
            SetCachedValue(value);
        }
    }

    private (TValue Value, bool HasConflict) ChooseValue((TValue Value, Ballot Ballot) maxAccepted)
    {
        // Recovery only cares about values accepted at the single highest fast ballot we saw.
        // Anything from lower ballots has already been superseded by `maxAccepted`.
        List<(TValue Value, int Count)> values = [];
        foreach (var response in _prepareResponses.Values)
        {
            if (response.AcceptedBallot != maxAccepted.Ballot)
            {
                continue;
            }

            var found = false;
            for (var i = 0; i < values.Count; ++i)
            {
                var (value, count) = values[i];
                if (!EqualityComparer<TValue>.Default.Equals(value, response.AcceptedValue))
                {
                    continue;
                }

                values[i] = (value, count + 1);
                found = true;
                break;
            }

            if (!found)
            {
                values.Add((response.AcceptedValue, 1));
            }
        }

        // If the highest fast ballot is already single-valued, recovery is deterministic.
        if (values.Count <= 1)
        {
            return (maxAccepted.Value, HasConflict: false);
        }

        // Pick the value with the highest occurrence. If the fast round results in a tie, this is the
        // value we will carry forward into the classic recovery round.
        var (chosenValue, _) = values.MaxBy(static v => v.Count);

        // A candidate is viable only if the prepare responses we have not seen yet could
        // still lift it to a full fast quorum. If no value is viable, then the split fast ballot
        // cannot possibly have committed anything and classic recovery may safely move forward.
        var remainingResponses = _acceptors.Length - _prepareResponses.Count;
        var fastQuorumSize = (3 * _acceptors.Length + 3) / 4;
        var viableChoices = 0;
        foreach (var (_, count) in values)
        {
            if (count + remainingResponses >= fastQuorumSize)
            {
                viableChoices++;
            }
        }

        // Only keep retrying when more than one value could still be the true fast-round winner.
        // Zero or one viable choices means the fast ballot is no longer ambiguous.
        return (chosenValue, HasConflict: viableChoices > 1);
    }

    private void ContinueCurrentRequest()
    {
        // Apply the current operation against the latest recovered value. Success produces the
        // next accept value, NotApplicable establishes the current value through an identity
        // accept, and Failed restarts with a fresh prepare before trying again.
        var currentValue = ReadCachedValueOrDefault("apply the current operation");
        var (operationStatus, acceptValue) = RequireCurrentOperation().Apply(currentValue);
        if (operationStatus == OperationStatus.Success)
        {
            StartAcceptWithValue(acceptValue);
            return;
        }

        if (operationStatus == OperationStatus.NotApplicable)
        {
            // Quorum acceptance makes a recovered value durable across later reads and checks
            // that a reused piggybacked promise still permits this value to be chosen.
            StartAcceptWithValue(currentValue);
            return;
        }

        RetryPrepare(Ballot.Zero);
    }

    private bool TryStartInitialFastAccept()
    {
        // The cluster starts with the initial fast ballot implicitly prepared, but only operations
        // that can produce an accept value from the proposer's current local view should skip the
        // explicit prepare. Reads and failed precondition checks still need prepare-based recovery.
        var (operationStatus, acceptValue) = RequireCurrentOperation().Apply(ReadCachedValueOrDefault("apply the current operation"));
        if (operationStatus != OperationStatus.Success)
        {
            return false;
        }

        StartAcceptWithValue(acceptValue);
        return true;
    }

    private void StartAcceptWithValue(TValue acceptValue)
    {
        CurrentValue = acceptValue;
        CurrentValueStatus = ProposerValueStatus.PendingAccept;
        OnPendingAcceptValueStarted(acceptValue);

        // Every accept can optionally piggyback a prepare for the "likely next" ballot. If a
        // quorum echoes that promise back, the next request can skip prepare entirely.
        RequestedNextBallot = ChooseNextBallotToPrepare();

        _prepareResponses.Clear();
        _acceptResponses.Clear();

        _runtime.Diagnostics.OnAcceptStarted(this, ProposerId, Ballot);
        OnSendAccept(new AcceptRequest<TValue>(
            Ballot,
            acceptValue,
            RequestedNextBallot));
    }

    private void RetryPrepare(Ballot maxConflict)
    {
        ClearAcceptState();
        PreparedBallot = null;

        var oldBallot = Ballot;

        if (!maxConflict.IsZero)
        {
            _runtime.Diagnostics.OnConflictDetected(this, ProposerId, maxConflict);
        }

        _modePolicy.RecordConflictObserved(Ballot, maxConflict, ProposerId);

        // Retries always move into a proposer-owned classic ballot and then jump above any
        // conflicting ballot we learned about, ensuring the retry does not immediately lose again.
        var nextBallot = Ballot.IsZero
            ? Ballot.InitialClassic(ProposerId)
            : Ballot.NextRound(ProposerId);
        if (nextBallot <= maxConflict)
        {
            nextBallot = maxConflict.NextRound(ProposerId);
        }

        nextBallot = ReserveBallot(nextBallot);
        Ballot = nextBallot;
        _runtime.Diagnostics.OnPrepareRetried(this, ProposerId, oldBallot, nextBallot, maxConflict);
        SendPrepareForCurrentBallot();
    }

    private Ballot ChooseNextBallotToPrepare()
    {
        // Piggybacked prepare is purely a latency optimization. Fast mode keeps asking acceptors
        // to line up the next shared fast ballot; leader mode lines up this proposer's next
        // classic ballot so consecutive operations can remain leader-owned.
        var candidate = _modePolicy.ChooseNextPrepareBallot(Ballot, ProposerId);
        return candidate.IsZero ? candidate : ReserveBallot(candidate);
    }

    private void ClearPreparedBallot()
    {
        _preparedBallot = Ballot.Zero;
        _preparedBallotState = PreparedBallotState.None;
    }

    private IOperation<TValue> RequireCurrentOperation() =>
        CurrentOperation ?? throw new InvalidOperationException(
            $"Proposer '{ProposerId}' expected an active operation but found none.");

    private TValue CommitPendingAcceptValue()
    {
        if (CurrentValueStatus != ProposerValueStatus.PendingAccept)
        {
            throw new InvalidOperationException(
                $"Proposer '{ProposerId}' expected a pending accept value but found state '{CurrentValueStatus}'.");
        }

        var value = CurrentValue;
        CurrentValueStatus = ProposerValueStatus.Cached;
        OnPendingAcceptValueCommitted(value);
        return value;
    }

    private TValue ReadCachedValueOrDefault(string operation) =>
        CurrentValueStatus switch
        {
            ProposerValueStatus.None => _initialValue,
            ProposerValueStatus.Cached => CurrentValue,
            ProposerValueStatus.PendingAccept => throw new InvalidOperationException(
                $"Proposer '{ProposerId}' cannot {operation} while the accept value is still pending."),
            _ => throw new InvalidOperationException(
                $"Proposer '{ProposerId}' is in an unknown value state '{CurrentValueStatus}'."),
        };

    private void SetCachedValue(TValue value)
    {
        CurrentValue = value;
        CurrentValueStatus = ProposerValueStatus.Cached;
        OnCachedValueChanged(value);
        _runtime.Diagnostics.OnValueAdopted(this, value, ProposerId);
    }

    private void ClearAcceptState()
    {
        _prepareResponses.Clear();
        _acceptResponses.Clear();
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            var value = CurrentValue;
            CurrentValue = _initialValue;
            CurrentValueStatus = ProposerValueStatus.None;
            OnPendingAcceptValueCleared(value);
        }

        RequestedNextBallot = Ballot.Zero;
    }

    private void CompleteCurrentRequest()
    {
        var committedValue = ReadCachedValueOrDefault("complete the current request");
        var response = new ProposeResponse<TValue>(Ballot.Round, committedValue);

        _runtime.Diagnostics.OnProposalCompleted(this, committedValue, ProposerId);
        try
        {
            OnProposalCompleted(response);
        }
        finally
        {
            CurrentOperation = null;
            ClearAcceptState();
        }
    }

    private readonly record struct PrepareResponse(
        Ballot AcceptedBallot,
        TValue AcceptedValue,
        Ballot ConflictBallot)
    {
        public bool IsPromise => ConflictBallot.IsZero;
    }

    private readonly record struct AcceptResponse(
        bool IsAccepted,
        Ballot Ballot);

    private enum PreparedBallotState : byte
    {
        None = 0,
        Prepared = 1,
        Requested = 2,
    }
}
