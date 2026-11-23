using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Protocol;

/// <summary>
/// Drives the proposer side of Fast/Classic CASPaxos for a single proposer.
/// It recovers the latest durable value during prepare, applies caller operations on top of that
/// recovered state, and remembers piggybacked promises that can let a later request skip prepare.
/// </summary>
public abstract class ProposerEngine<TValue, TRoute>
    where TRoute : notnull
{
    private readonly ImmutableArray<TRoute> _acceptors;
    private readonly HashSet<TRoute> _acceptorSet;
    private readonly ImmutableArray<TRoute> _previousAcceptors;
    private readonly HashSet<TRoute>? _previousAcceptorSet;
    private readonly ImmutableArray<TRoute> _prepareAcceptors;
    private readonly Dictionary<TRoute, PrepareResponse> _prepareResponses = [];
    private readonly Dictionary<TRoute, AcceptResponse> _acceptResponses = [];
    private readonly ProposerRuntime _runtime;
    private protected readonly TValue _initialValue;
    private Ballot _preparedBallot;
    private PreparedBallotState _preparedBallotState;

    public ProposerEngine(
        ProposerRuntime runtime,
        TValue initialValue,
        ImmutableArray<TRoute> acceptors,
        ProposerModePolicy modePolicy,
        long configuration = 1,
        ImmutableArray<TRoute> previousAcceptors = default)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        if (acceptors.IsDefault)
        {
            throw new ArgumentNullException(nameof(acceptors));
        }

        ArgumentNullException.ThrowIfNull(modePolicy);
        ArgumentOutOfRangeException.ThrowIfLessThan(configuration, 1);
        modePolicy.Validate();

        _runtime = runtime;
        ModePolicy = modePolicy.CreateRuntimeCopy();
        _initialValue = initialValue;
        CurrentValue = initialValue;
        Configuration = configuration;
        _acceptors = acceptors;
        _acceptorSet = [.. acceptors];

        if (!previousAcceptors.IsDefaultOrEmpty)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(configuration, 2);
            _previousAcceptors = previousAcceptors;
            _previousAcceptorSet = [.. previousAcceptors];
            var union = ImmutableArray.CreateBuilder<TRoute>(acceptors.Length + previousAcceptors.Length);
            HashSet<TRoute> seen = [];
            foreach (var route in previousAcceptors)
            {
                if (seen.Add(route)) union.Add(route);
            }

            foreach (var route in acceptors)
            {
                if (seen.Add(route)) union.Add(route);
            }

            _prepareAcceptors = union.DrainToImmutable();
        }
        else
        {
            _previousAcceptors = [];
            _prepareAcceptors = acceptors;
        }
    }

    protected IOperation<TValue>? CurrentOperation { get; private set; }

    public abstract int ProposerId { get; }

    public long Configuration { get; }

    public bool IsHandOff => _previousAcceptorSet is not null;

    public ImmutableArray<TRoute> Acceptors => _acceptors;

    public ImmutableArray<TRoute> PrepareAcceptors => _prepareAcceptors;

    public Ballot? ObservedHigherConfigurationBallot { get; private set; }

    public ProposerModePolicy ModePolicy { get; }

    public bool HasPendingProposal => CurrentOperation is not null;

    public Ballot Ballot { get; private set; }

    public int ConflictDebt => ModePolicy.ConflictDebt;

    public int LeaderMisses => ModePolicy.LeaderMisses;

    public int? LeaderHintProposerId => ModePolicy.LeaderHintProposerId;

    public bool ShouldForwardToLeader => ModePolicy.ShouldForwardToLeader(ProposerId);

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

        var highestObserved = Ballot.Max(acceptedBallot, promisedBallot);
        if (highestObserved.Configuration > Configuration)
        {
            RecordHigherConfigurationObserved(highestObserved);
            return;
        }

        if (!IsHandOff && PreparedBallot is { } prepared && prepared.Configuration == Configuration
            && CanReusePreparedBallot(prepared)
            && acceptedBallot < prepared && promisedBallot <= prepared)
        {
            StartProposal(operation);
            return;
        }

        if (highestObserved.Configuration == Configuration)
        {
            Ballot = Ballot.Max(Ballot, highestObserved);
        }

        StartProposal(operation);
    }

    public void StartCurrentRequest()
    {
        if (CurrentOperation is not null)
        {
            BeginOperation();
        }
    }

    /// <summary>Establishes the value through a fresh owned classic prepare after an unavailable local hint.</summary>
    public void StartProposalWithRecovery(IOperation<TValue> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (CurrentOperation is not null) throw new InvalidOperationException("Recovery requires an idle proposer.");
        CurrentOperation = operation;
        _runtime.Diagnostics.OnProposalStarted(this, ProposerId, Ballot);
        var floor = Ballot.Max(Ballot, PreparedBallot ?? Ballot.Zero);
        ClearAcceptState();
        PreparedBallot = null;
        Ballot = ReserveBallot(floor.IsZero || floor.Configuration < Configuration
            ? Ballot.InitialClassic(Configuration, ProposerId)
            : floor.NextRound(ProposerId));
        SendPrepareForCurrentBallot();
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

        if (acceptedBallot.Configuration != Configuration
            || (!preparedBallot.IsZero && preparedBallot.Configuration != Configuration))
        {
            return;
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
        ModePolicy.LearnCommittedValue(acceptedBallot, preparedBallot);
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
            // next request. Once the client has already been replied to, the round is finished.
            return;
        }

        if (!response.IsPromise && response.ConflictBallot.Configuration > Configuration)
        {
            AbortForHigherConfiguration(response.ConflictBallot);
            return;
        }

        if (!_acceptorSet.Contains(acceptor) && (_previousAcceptorSet is null || !_previousAcceptorSet.Contains(acceptor)))
        {
            return;
        }

        _prepareResponses[acceptor] = response;
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // Once prepare has already advanced this round into accept, late prepare responses are
            // informational only. Accept results now determine whether the round commits or retries.
            return;
        }

        var aggregated = AggregatePrepareResponses();

        // Every prepare response already reports durable acceptor state, so we can refresh our
        // best-known local cache immediately even though quorum is still required before acting.
        AdoptValue(aggregated.MaxAccepted.Value);
        if (HasPrepareQuorum(aggregated))
        {
            // A successful prepare recovers the freshest durable value any acceptor reported and
            // then immediately re-applies the caller's operation on top of that recovered base.
            // AcceptedBallot/AcceptedValue report durable acceptor state regardless of whether this
            // prepare promised or rejected; only ConflictBallot is rejection-specific.
            if (!TryResolvePreparedValue(aggregated.MaxAccepted, out var preparedValue, out var conflictBallot))
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

        if (IsPrepareQuorumLost(aggregated))
        {
            // Once this round is lost, abandon it and retry above the highest conflicting ballot.
            _runtime.Diagnostics.OnPrepareFailed(this, ProposerId, Ballot, aggregated.MaxConflictBallot);
            RetryPrepare(aggregated.MaxConflictBallot);
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
            var aggregated = AggregatePrepareResponses();
            if (HasPrepareQuorum(aggregated))
            {
                return false; // Already reached quorum
            }

            _runtime.Diagnostics.OnPrepareFailed(this, ProposerId, Ballot, aggregated.MaxConflictBallot);
            RetryPrepare(aggregated.MaxConflictBallot);
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

        if (!response.IsAccepted && response.Ballot.Configuration > Configuration)
        {
            AbortForHigherConfiguration(response.Ballot);
            return;
        }

        // Votes for Accept count strictly from voters of V(Configuration).
        if (!_acceptorSet.Contains(acceptor))
        {
            return;
        }

        if (response.IsAccepted && response.Ballot.Configuration > Configuration)
        {
            // An exact-duplicate replay below a raised floor still counts toward our ballot's quorum,
            // while signaling the host that a newer configuration exists.
            RecordHigherConfigurationObserved(response.Ballot);
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
                if (!requestedNextBallot.IsZero
                    && requestedNextBallot.Configuration == Configuration
                    && current.Ballot == requestedNextBallot)
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
            if (!requestedNextBallot.IsZero
                && requestedNextBallot.Configuration == Configuration
                && HasQuorum(requestedNextBallot, preparedMatches))
            {
                PreparedBallot = requestedNextBallot;
            }

            ModePolicy.RecordAcceptQuorumSucceeded(Ballot, ProposerId);

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
    /// <summary>Hosted engines also establish ownership of a learned classic reservation.</summary>
    protected virtual bool CanReusePreparedBallot(Ballot ballot) => ModePolicy.CanReusePreparedBallot(ballot, ProposerId);
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

    /// <summary>
    /// Called when an acceptor reports a ballot or floor from a configuration strictly higher than
    /// <see cref="Configuration"/>, indicating this engine's configuration has been superseded.
    /// </summary>
    protected virtual void OnHigherConfigurationObserved(Ballot observedBallot) { }

    public void RecordLeaderForwardFailure()
        => ModePolicy.RecordLeaderForwardFailure();

    public void RecordLeaderForwardSuccess()
        => ModePolicy.RecordLeaderForwardSuccess();

    public bool ShouldAttemptLeaderTakeover() => ModePolicy.ShouldAttemptLeaderTakeover(ProposerId);

    private void BeginOperation()
    {
        if (CurrentValueStatus == ProposerValueStatus.PendingAccept)
        {
            // A new local attempt abandons any previously in-flight accept and re-enters through a
            // fresh ballot. The cluster state will be re-read via prepare unless this is the
            // implicitly prepared initial fast round.
            ClearAcceptState();
        }

        // A cached prepared ballot is only useful while it is in this configuration and strictly
        // ahead of the ballot we have already consumed.
        if (PreparedBallot is { } preparedBallot
            && (preparedBallot.Configuration != Configuration || preparedBallot <= Ballot))
        {
            PreparedBallot = null;
        }

        if (!IsHandOff
            && PreparedBallot is { } reusableBallot
            && CanReusePreparedBallot(reusableBallot))
        {
            // Reuse the piggybacked promise exactly once, jumping straight into operation execution
            // as if a standalone prepare for that ballot had already succeeded.
            PreparedBallot = null;
            Ballot = reusableBallot;
            ContinueCurrentRequest();
            return;
        }

        Ballot = CreateInitialBallot();
        PreparedBallot = null;
        if (Configuration == 1
            && !IsHandOff
            && ModePolicy.ShouldSkipInitialPrepare(ProposerId)
            && Ballot == Ballot.InitialFast()
            && TryStartInitialFastAccept())
        {
            return;
        }

        SendPrepareForCurrentBallot();
    }

    private Ballot CreateInitialBallot()
    {
        var allowInitialFast = Configuration == 1 && !IsHandOff;
        var candidate = ModePolicy.CreateProposalBallot(ProposerId, Ballot, Configuration, allowInitialFast);
        if (PreparedBallot is { } observedPromise
            && observedPromise.Configuration == Configuration
            && candidate <= observedPromise)
        {
            candidate = ModePolicy.CreateProposalBallot(ProposerId, observedPromise, Configuration, allowInitialFast);
        }

        return ReserveBallot(candidate);
    }

    private void SendPrepareForCurrentBallot()
    {
        _prepareResponses.Clear();
        _acceptResponses.Clear();

        _runtime.Diagnostics.OnPrepareStarted(this, ProposerId, Ballot);
        OnSendPrepare(new PrepareRequest(Ballot));
    }

    private readonly record struct PrepareAggregation(
        int CurrentSuccess,
        int CurrentFailure,
        int PreviousSuccess,
        int PreviousFailure,
        (TValue Value, Ballot Ballot) MaxAccepted,
        Ballot MaxConflictBallot);

    private PrepareAggregation AggregatePrepareResponses()
    {
        var currentSuccess = 0;
        var currentFailure = 0;
        var previousSuccess = 0;
        var previousFailure = 0;
        var maxAccepted = (Value: _initialValue, Ballot: Ballot.Zero);
        var maxConflictBallot = Ballot.Zero;

        // Every prepare response reports the acceptor's last accepted ballot/value, so both
        // promises and rejections participate in value recovery. Only rejections contribute the
        // conflict ballot that the next retry must jump above.
        foreach (var (acceptor, response) in _prepareResponses)
        {
            var inCurrent = _acceptorSet.Contains(acceptor);
            var inPrevious = _previousAcceptorSet is not null && _previousAcceptorSet.Contains(acceptor);

            if (response.IsPromise)
            {
                if (inCurrent) ++currentSuccess;
                if (inPrevious) ++previousSuccess;
            }
            else
            {
                if (inCurrent) ++currentFailure;
                if (inPrevious) ++previousFailure;
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

        return new PrepareAggregation(
            currentSuccess,
            currentFailure,
            previousSuccess,
            previousFailure,
            maxAccepted,
            maxConflictBallot);
    }

    private bool HasPrepareQuorum(PrepareAggregation aggregated)
    {
        if (!HasQuorum(Ballot, aggregated.CurrentSuccess))
        {
            return false;
        }

        return !IsHandOff || 2 * aggregated.PreviousSuccess > _previousAcceptors.Length;
    }

    private bool IsPrepareQuorumLost(PrepareAggregation aggregated)
    {
        if (HasQuorum(Ballot, aggregated.CurrentFailure)
            || !IsQuorumPossible(Ballot, aggregated.CurrentSuccess, aggregated.CurrentFailure))
        {
            return true;
        }

        if (!IsHandOff)
        {
            return false;
        }

        var prevRemaining = _previousAcceptors.Length - (aggregated.PreviousSuccess + aggregated.PreviousFailure);
        return 2 * (aggregated.PreviousSuccess + prevRemaining) <= _previousAcceptors.Length;
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

        var minRelevantCfg = IsHandOff ? Configuration - 1 : Configuration;
        var hCfg = maxAccepted.Ballot.Configuration;
        if (hCfg < minRelevantCfg)
        {
            // Because minRelevantCfg is complete and our prepare quorum intersects V(minRelevantCfg),
            // any value chosen in < minRelevantCfg was already transferred into >= minRelevantCfg.
            // A lower-config remnant is unchosen and has zero viable fast winners.
            return true;
        }

        var (hVoterSet, hVoterCount) = GetVoterSetForConfiguration(hCfg);
        if (!maxAccepted.Ballot.IsFastRoundBallot)
        {
            // Classic accepted values were already serialized by prepare; prefer a member of V(hCfg)
            // when present.
            foreach (var (acceptor, response) in _prepareResponses)
            {
                if (response.AcceptedBallot == maxAccepted.Ballot && hVoterSet.Contains(acceptor))
                {
                    value = response.AcceptedValue;
                    return true;
                }
            }

            return true;
        }

        // A fast ballot may have accepted competing values concurrently. Scope both frequency counts
        // and viability strictly to V(hCfg).
        (value, var hasConflict) = ChooseValue(maxAccepted, hVoterSet, hVoterCount);
        if (!hasConflict)
        {
            return true;
        }

        conflictBallot = maxAccepted.Ballot;
        return false;
    }

    private (HashSet<TRoute> Voters, int Count) GetVoterSetForConfiguration(long configuration)
    {
        if (configuration == Configuration)
        {
            return (_acceptorSet, _acceptors.Length);
        }

        if (IsHandOff && configuration == Configuration - 1)
        {
            return (_previousAcceptorSet!, _previousAcceptors.Length);
        }

        return (_acceptorSet, _acceptors.Length);
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

    private (TValue Value, bool HasConflict) ChooseValue(
        (TValue Value, Ballot Ballot) maxAccepted,
        HashSet<TRoute> hVoterSet,
        int hVoterCount)
    {
        // Recovery only cares about values accepted at the single highest fast ballot by voters of V(hCfg).
        List<(TValue Value, int Count)> values = [];
        var seenInHCfg = 0;
        foreach (var (acceptor, response) in _prepareResponses)
        {
            if (!hVoterSet.Contains(acceptor))
            {
                continue;
            }

            seenInHCfg++;
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

        // If the highest fast ballot is already single-valued (or absent among V(hCfg)), recovery is deterministic.
        if (values.Count <= 1)
        {
            return (values.Count == 1 ? values[0].Value : maxAccepted.Value, HasConflict: false);
        }

        // Pick the value with the highest occurrence. If the fast round results in a tie, this is the
        // value we will carry forward into the classic recovery round.
        var (chosenValue, _) = values.MaxBy(static v => v.Count);

        // A candidate is viable only if the prepare responses we have not seen yet from V(hCfg)
        // could still lift it to a full fast quorum of V(hCfg).
        var remainingInHCfg = hVoterCount - seenInHCfg;
        var fastQuorumSize = (3 * hVoterCount + 3) / 4;
        var viableChoices = 0;
        foreach (var (_, count) in values)
        {
            if (count + remainingInHCfg >= fastQuorumSize)
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
        if (maxConflict.Configuration > Configuration)
        {
            AbortForHigherConfiguration(maxConflict);
            return;
        }

        ClearAcceptState();
        PreparedBallot = null;

        var oldBallot = Ballot;

        if (!maxConflict.IsZero)
        {
            _runtime.Diagnostics.OnConflictDetected(this, ProposerId, maxConflict);
        }

        ModePolicy.RecordConflictObserved(Ballot, maxConflict, ProposerId);

        // Retries always move into a proposer-owned classic ballot in this configuration and jump
        // above any conflicting ballot in this configuration.
        var nextBallot = Ballot.IsZero || Ballot.Configuration < Configuration
            ? Ballot.InitialClassic(Configuration, ProposerId)
            : Ballot.NextRound(ProposerId);
        if (maxConflict.Configuration == Configuration && nextBallot <= maxConflict)
        {
            nextBallot = maxConflict.NextRound(ProposerId);
        }

        nextBallot = ReserveBallot(nextBallot);
        Ballot = nextBallot;
        _runtime.Diagnostics.OnPrepareRetried(this, ProposerId, oldBallot, nextBallot, maxConflict);
        SendPrepareForCurrentBallot();
    }

    private void RecordHigherConfigurationObserved(Ballot observedBallot)
    {
        ObservedHigherConfigurationBallot = ObservedHigherConfigurationBallot is { } existing
            ? Ballot.Max(existing, observedBallot)
            : observedBallot;
        OnHigherConfigurationObserved(observedBallot);
    }

    private void AbortForHigherConfiguration(Ballot observedBallot)
    {
        CurrentOperation = null;
        ClearAcceptState();
        PreparedBallot = null;
        RecordHigherConfigurationObserved(observedBallot);
    }

    private Ballot ChooseNextBallotToPrepare()
    {
        if (IsHandOff)
        {
            return Ballot.Zero;
        }

        // Piggybacked prepare is purely a latency optimization. Fast mode keeps asking acceptors
        // to line up the next shared fast ballot; leader mode lines up this proposer's next
        // classic ballot so consecutive operations can remain leader-owned.
        var candidate = ModePolicy.ChooseNextPrepareBallot(Ballot, ProposerId);
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
