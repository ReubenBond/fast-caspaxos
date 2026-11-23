using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;

namespace Orleans.Consensus.CasPaxos.Server;

/// <summary>
/// Executes the four-step idempotent, resumable voter reconfiguration procedure
/// (Choose -> Fence c -> Install -> Activate) and implements stale-configuration discovery.
/// </summary>
public sealed class VoterReconfigurationDriver
{
    private readonly IProposerBallotSource _ballots;
    private readonly Func<VoterDescriptor, IKeyedAcceptor> _peerResolver;
    private readonly RegisterServiceOptions _options;

    public VoterReconfigurationDriver(
        IProposerBallotSource ballots,
        Func<VoterDescriptor, IKeyedAcceptor> peerResolver,
        RegisterServiceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(ballots);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ballots.ProposerId);
        ArgumentNullException.ThrowIfNull(peerResolver);
        _ballots = ballots;
        _peerResolver = peerResolver;
        _options = options ?? new RegisterServiceOptions();
    }

    /// <summary>
    /// Reconfigures from <paramref name="currentConfiguration"/> (<paramref name="currentVoters"/>) to
    /// <c>currentConfiguration + 1</c> (<paramref name="nextVoters"/>). Idempotent and resumable after crashes.
    /// </summary>
    public async Task<VoterConfiguration> ReconfigureAsync(
        ImmutableArray<VoterDescriptor> currentVoters,
        long currentConfiguration,
        ImmutableArray<VoterDescriptor> nextVoters,
        CancellationToken cancellationToken = default)
    {
        var sourceConfig = new VoterConfiguration(currentConfiguration, currentVoters, VoterConfigurationState.Active);
        sourceConfig.Validate();
        var desiredNext = new VoterConfiguration(
            checked(currentConfiguration + 1),
            nextVoters,
            VoterConfigurationState.Pending,
            sourceConfig.CanonicalVoters);
        desiredNext.Validate();

        // Step 1: Choose (classic CompareExchange on K_cfg in configuration c).
        var chosen = await ChooseNextConfigurationAsync(sourceConfig, desiredNext, cancellationToken).ConfigureAwait(false);
        if (chosen.State == VoterConfigurationState.Active)
        {
            return chosen;
        }

        // Steps 2 -> 3 -> 4: Fence c, Install all keys via Vertical Paxos I hand-off, Activate c + 1.
        return await CompletePendingConfigurationAsync(sourceConfig, chosen, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resumes an interrupted reconfiguration from any known configuration seed.
    /// Whenever <c>K_cfg</c> is still <see cref="VoterConfigurationState.Pending"/>, resumes from Step 2 (Fence)
    /// through Step 3 (Install) and Step 4 (Activate) so no key is left untransferred.
    /// </summary>
    public async Task<VoterConfiguration> ResumeAsync(
        ImmutableArray<VoterDescriptor> seedVoters,
        long seedConfiguration,
        CancellationToken cancellationToken = default)
    {
        var seed = new VoterConfiguration(seedConfiguration, seedVoters, VoterConfigurationState.Active);
        seed.Validate();

        var discovery = await DiscoverConfigurationChainAsync(
            seed,
            ResolvePeers(seed.CanonicalVoters),
            _peerResolver,
            _options,
            cancellationToken).ConfigureAwait(false);

        if (discovery.Latest.State == VoterConfigurationState.Active)
        {
            return discovery.Latest;
        }

        var previous = !discovery.Latest.PreviousVoters.IsDefaultOrEmpty
            ? new VoterConfiguration(discovery.Latest.Number - 1, discovery.Latest.PreviousVoters, VoterConfigurationState.Active)
            : discovery.Previous;

        if (previous is null || previous.Number != discovery.Latest.Number - 1)
        {
            throw new RegisterUnavailableException(new InvalidOperationException(
                $"Cannot resume pending configuration {discovery.Latest.Number} without its predecessor configuration {discovery.Latest.Number - 1}."));
        }

        return await CompletePendingConfigurationAsync(previous, discovery.Latest, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Runs Vertical Paxos I hand-off recovery for a single key from <paramref name="previousPeers"/> (<c>V(c)</c>)
    /// into <paramref name="nextPeers"/> (<c>V(c + 1)</c>) using a classic identity transition (<c>next = zero</c>).
    /// </summary>
    public async Task<RegisterValue> TransferKeyHandOffAsync(
        string key,
        long nextConfiguration,
        ImmutableArray<AcceptorPeer> previousPeers,
        ImmutableArray<AcceptorPeer> nextPeers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(nextConfiguration, 2);

        using var deadline = new CancellationTokenSource(_options.OperationTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var token = linked.Token;

        var previousMap = previousPeers.ToDictionary(static p => p.Id, static p => p.Acceptor, StringComparer.Ordinal);
        var nextMap = nextPeers.ToDictionary(static p => p.Id, static p => p.Acceptor, StringComparer.Ordinal);
        var allMap = new Dictionary<string, IKeyedAcceptor>(previousMap, StringComparer.Ordinal);
        foreach (var (id, acceptor) in nextMap)
        {
            allMap[id] = acceptor;
        }

        ImmutableArray<string> prevIds = [.. previousPeers.Select(static p => p.Id)];
        ImmutableArray<string> nextIds = [.. nextPeers.Select(static p => p.Id)];
        var engine = new HandOffEngine(_ballots, nextConfiguration, nextIds, prevIds);
        engine.StartProposal(new IdentityOperation());

        var retries = 0;
        while (engine.Result is null)
        {
            token.ThrowIfCancellationRequested();
            if (engine.ObservedHigherConfigurationBallot is { } higher)
            {
                throw new RegisterUnavailableException(new InvalidOperationException(
                    $"Hand-off for configuration {nextConfiguration} was superseded by {higher}."));
            }

            var outgoing = engine.Outgoing
                ?? throw new InvalidOperationException("Hand-off engine has no pending protocol message.");

            using var phase = CancellationTokenSource.CreateLinkedTokenSource(token);
            var timeout = TimeProviderDelay.DelayAsync(_options.TimeProvider, _options.AttemptTimeout, phase.Token);
            try
            {
                if (outgoing.Prepare is { } prepare)
                {
                    var targets = engine.PrepareAcceptors;
                    var pending = targets.Select(id => InvokeAsync(
                        id,
                        t => allMap[id].PrepareAsync(key, prepare, t),
                        phase.Token)).ToList();

                    while (pending.Count > 0 && engine.Result is null && ReferenceEquals(engine.Outgoing, outgoing))
                    {
                        var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                        token.ThrowIfCancellationRequested();
                        if (finished == timeout) break;
                        var task = (Task<(string Id, AcceptorPrepareResult<ByteString?> Result, Exception? Error)>)finished;
                        pending.Remove(task);
                        var (id, result, error) = await task.ConfigureAwait(false);
                        if (error is not null) continue;

                        var value = result.AcceptedBallot.IsZero
                            ? RegisterEncoding.Absent
                            : result.AcceptedValue ?? throw new InvalidDataException("An accepted ballot must include accepted bytes.");
                        RegisterEncoding.Decode(value);
                        if (result.IsPromised)
                        {
                            engine.HandlePreparePromised(new PreparePromise<ByteString, string>(
                                prepare.Ballot.Round, id, result.AcceptedBallot, value));
                        }
                        else
                        {
                            engine.HandlePrepareRejected(new PrepareRejection<ByteString, string>(
                                prepare.Ballot.Round, id, result.AcceptedBallot, value, result.ConflictingBallot));
                        }
                    }
                }
                else
                {
                    var accept = outgoing.Accept!.Value;
                    // §4.3 / §4.4: Accept is sent strictly to V(c + 1).
                    var targets = engine.Acceptors;
                    var pending = targets.Select(id => InvokeAsync(
                        id,
                        t => nextMap[id].AcceptAsync(key, accept, t),
                        phase.Token)).ToList();

                    while (pending.Count > 0 && engine.Result is null && ReferenceEquals(engine.Outgoing, outgoing))
                    {
                        var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                        token.ThrowIfCancellationRequested();
                        if (finished == timeout) break;
                        var task = (Task<(string Id, AcceptorAcceptResult Result, Exception? Error)>)finished;
                        pending.Remove(task);
                        var (id, result, error) = await task.ConfigureAwait(false);
                        if (error is not null) continue;

                        if (result.IsAccepted)
                        {
                            engine.HandleAcceptAccepted(new AcceptAccepted<string>(
                                accept.Ballot.Round, id, result.PromisedBallot));
                        }
                        else
                        {
                            engine.HandleAcceptRejected(new AcceptRejected<string>(
                                accept.Ballot.Round, id, result.ConflictingBallot));
                        }
                    }
                }

                if (engine.Result is null && ReferenceEquals(engine.Outgoing, outgoing))
                {
                    engine.HandleResponseTimeout(outgoing.Round);
                }
            }
            finally
            {
                phase.Cancel();
            }

            if (engine.Result is null && engine.Outgoing?.Prepare is not null)
            {
                await BackoffAsync(retries++, token).ConfigureAwait(false);
            }
        }

        return RegisterEncoding.Decode(engine.Result.Value);
    }

    private async Task<VoterConfiguration> ChooseNextConfigurationAsync(
        VoterConfiguration sourceConfig,
        VoterConfiguration desiredNext,
        CancellationToken cancellationToken)
    {
        var sourcePeers = ResolvePeers(sourceConfig.CanonicalVoters);
        await using var service = CreateClassicService(sourcePeers, sourceConfig.Number);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RegisterValue current;
            try
            {
                current = await service.ReadAsync(VoterConfiguration.RegisterKey, cancellationToken).ConfigureAwait(false);
            }
            catch (RegisterUnavailableException) when (service.LastObservedHigherConfiguration is not null)
            {
                // A concurrent operator already fenced c (Step 2). Discover the chosen configuration via §4.6.
                var discovered = await DiscoverConfigurationChainAsync(
                    sourceConfig, sourcePeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return ValidateOrAcceptDiscovered(discovered.Latest, desiredNext);
            }

            ByteString? expectedVersion;
            if (!current.IsPresent)
            {
                if (sourceConfig.Number != 1)
                {
                    throw new InvalidOperationException(
                        $"Configuration register '{VoterConfiguration.RegisterKey}' is absent in configuration {sourceConfig.Number}.");
                }

                expectedVersion = null;
            }
            else
            {
                var existing = VoterConfigurationEncoding.Decode(current.Value!.Value);
                if (existing.Number == desiredNext.Number)
                {
                    return ValidateOrAcceptDiscovered(existing, desiredNext);
                }

                if (existing.Number > desiredNext.Number)
                {
                    return existing;
                }

                if (existing.Number != sourceConfig.Number || existing.State != VoterConfigurationState.Active)
                {
                    throw new InvalidOperationException(
                        $"Expected active configuration {sourceConfig.Number}, but observed ({existing.Number}, {existing.State}).");
                }

                expectedVersion = current.Version;
            }

            var encodedNext = VoterConfigurationEncoding.Encode(desiredNext);
            CompareExchangeResult cas;
            try
            {
                cas = await service.CompareExchangeAsync(
                    VoterConfiguration.RegisterKey, expectedVersion, encodedNext, cancellationToken).ConfigureAwait(false);
            }
            catch (RegisterUnavailableException) when (service.LastObservedHigherConfiguration is not null)
            {
                // A concurrent operator already fenced c before our Prepare could succeed.
                var discovered = await DiscoverConfigurationChainAsync(
                    sourceConfig, sourcePeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return ValidateOrAcceptDiscovered(discovered.Latest, desiredNext);
            }

            if (cas.Status == CompareExchangeStatus.Applied)
            {
                return desiredNext;
            }

            if (cas.Status == CompareExchangeStatus.Mismatch && cas.Current is { IsPresent: true } mismatchCurrent)
            {
                var winner = VoterConfigurationEncoding.Decode(mismatchCurrent.Value!.Value);
                if (winner.Number == desiredNext.Number)
                {
                    return ValidateOrAcceptDiscovered(winner, desiredNext);
                }

                if (winner.Number > desiredNext.Number)
                {
                    return winner;
                }
            }

            if (service.LastObservedHigherConfiguration is not null)
            {
                var discovered = await DiscoverConfigurationChainAsync(
                    sourceConfig, sourcePeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return ValidateOrAcceptDiscovered(discovered.Latest, desiredNext);
            }

            // Unknown or transient mismatch — loop and re-read K_cfg in configuration c.
        }
    }

    private static VoterConfiguration ValidateOrAcceptDiscovered(
        VoterConfiguration observed,
        VoterConfiguration desiredNext)
    {
        if (observed.Number == desiredNext.Number && !observed.HasSameVoters(desiredNext.Voters))
        {
            throw new ReconfigurationConflictException(observed);
        }

        return observed;
    }

    private async Task<VoterConfiguration> CompletePendingConfigurationAsync(
        VoterConfiguration previousConfig,
        VoterConfiguration pendingConfig,
        CancellationToken cancellationToken)
    {
        var previousPeers = ResolvePeers(previousConfig.CanonicalVoters);
        var nextPeers = ResolvePeers(pendingConfig.CanonicalVoters);
        var targetFloor = Ballot.Floor(pendingConfig.Number);

        // Step 2: Fence c (and ensure all new voters in V(c + 1) \ V(c) have durable floor >= (c + 1, 0, 0)).
        var previousIds = new HashSet<string>(previousPeers.Select(static p => p.Id), StringComparer.Ordinal);
        var newPeers = nextPeers.Where(p => !previousIds.Contains(p.Id)).ToImmutableArray();
        await RaiseFloorOnNewVotersAsync(newPeers, targetFloor, cancellationToken).ConfigureAwait(false);
        var fencedPrevious = await FenceClassicQuorumAsync(previousPeers, targetFloor, cancellationToken).ConfigureAwait(false);

        // Step 3: Install — enumerate all keys across a fenced classic quorum F, transfer K_cfg last.
        var keys = await EnumerateKeysFromFencedQuorumAsync(
            previousPeers, fencedPrevious, targetFloor, cancellationToken).ConfigureAwait(false);
        if (!keys.Contains(VoterConfiguration.RegisterKey))
        {
            throw new InvalidOperationException(
                $"Configuration key '{VoterConfiguration.RegisterKey}' was not found on the fenced quorum of configuration {previousConfig.Number}.");
        }

        RegisterValue transferredConfigValue;
        try
        {
            foreach (var key in keys)
            {
                if (string.Equals(key, VoterConfiguration.RegisterKey, StringComparison.Ordinal)) continue;
                await TransferKeyHandOffAsync(key, pendingConfig.Number, previousPeers, nextPeers, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Transfer K_cfg last as operational hygiene (§5 Step 3).
            transferredConfigValue = await TransferKeyHandOffAsync(
                VoterConfiguration.RegisterKey, pendingConfig.Number, previousPeers, nextPeers, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RegisterUnavailableException)
        {
            // If a concurrent operator already activated c + 1 and advanced further, discover the active configuration.
            var discovered = await DiscoverConfigurationChainAsync(
                pendingConfig, nextPeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
            if (discovered.Latest.Number > pendingConfig.Number
                || (discovered.Latest.Number == pendingConfig.Number && discovered.Latest.State == VoterConfigurationState.Active))
            {
                return discovered.Latest;
            }

            throw;
        }
        if (!transferredConfigValue.IsPresent)
        {
            throw new InvalidDataException($"Configuration key '{VoterConfiguration.RegisterKey}' became absent during hand-off.");
        }

        var transferredConfig = VoterConfigurationEncoding.Decode(transferredConfigValue.Value!.Value);
        if (transferredConfig.Number > pendingConfig.Number
            || (transferredConfig.Number == pendingConfig.Number && transferredConfig.State == VoterConfigurationState.Active))
        {
            // A concurrent operator already completed Step 4.
            return transferredConfig;
        }

        // Step 4: Activate in c + 1 using V(c + 1) quorums only.
        var activeConfig = pendingConfig with { State = VoterConfigurationState.Active, PreviousVoters = default };
        var encodedActive = VoterConfigurationEncoding.Encode(activeConfig);
        await using var nextService = CreateClassicService(nextPeers, pendingConfig.Number);
        var expectedVersion = transferredConfigValue.Version;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompareExchangeResult cas;
            try
            {
                cas = await nextService.CompareExchangeAsync(
                    VoterConfiguration.RegisterKey, expectedVersion, encodedActive, cancellationToken).ConfigureAwait(false);
            }
            catch (RegisterUnavailableException) when (nextService.LastObservedHigherConfiguration is not null)
            {
                var discovered = await DiscoverConfigurationChainAsync(
                    pendingConfig, nextPeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return discovered.Latest;
            }
            if (cas.Status == CompareExchangeStatus.Applied)
            {
                return activeConfig;
            }

            if (cas.Status == CompareExchangeStatus.Mismatch && cas.Current is { IsPresent: true } current)
            {
                var observed = VoterConfigurationEncoding.Decode(current.Value!.Value);
                if (observed.Number > pendingConfig.Number
                    || (observed.Number == pendingConfig.Number && observed.State == VoterConfigurationState.Active))
                {
                    return observed;
                }

                expectedVersion = current.Version;
                continue;
            }

            if (nextService.LastObservedHigherConfiguration is not null)
            {
                var discovered = await DiscoverConfigurationChainAsync(
                    pendingConfig, nextPeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return discovered.Latest;
            }

            // Unknown — re-read K_cfg in c + 1 to check whether activation committed.
            RegisterValue read;
            try
            {
                read = await nextService.ReadAsync(VoterConfiguration.RegisterKey, cancellationToken).ConfigureAwait(false);
            }
            catch (RegisterUnavailableException) when (nextService.LastObservedHigherConfiguration is not null)
            {
                var discovered = await DiscoverConfigurationChainAsync(
                    pendingConfig, nextPeers, _peerResolver, _options, cancellationToken).ConfigureAwait(false);
                return discovered.Latest;
            }
            if (read.IsPresent)
            {
                var observed = VoterConfigurationEncoding.Decode(read.Value!.Value);
                if (observed.Number > pendingConfig.Number
                    || (observed.Number == pendingConfig.Number && observed.State == VoterConfigurationState.Active))
                {
                    return observed;
                }

                expectedVersion = read.Version;
            }
        }
    }

    private async Task RaiseFloorOnNewVotersAsync(
        ImmutableArray<AcceptorPeer> newPeers,
        Ballot targetFloor,
        CancellationToken cancellationToken)
    {
        if (newPeers.IsDefaultOrEmpty) return;

        using var phase = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var timeout = new CancellationTokenSource(_options.AttemptTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(phase.Token, timeout.Token);

        var tasks = newPeers.Select(peer => InvokeAsync(
            peer.Id,
            t => peer.Acceptor.SetFloorAsync(targetFloor, t),
            linked.Token)).ToArray();

        await Task.WhenAll(tasks).ConfigureAwait(false);
        // Best-effort for unreachable new voters: classic quorum of V(c + 1) is verified in Install.
    }

    private async Task<ImmutableArray<AcceptorPeer>> FenceClassicQuorumAsync(
        ImmutableArray<AcceptorPeer> peers,
        Ballot targetFloor,
        CancellationToken cancellationToken)
    {
        var classicQuorum = peers.Length / 2 + 1;
        using var deadline = new CancellationTokenSource(_options.OperationTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var token = linked.Token;

        var fenced = new Dictionary<string, AcceptorPeer>(StringComparer.Ordinal);
        var retries = 0;
        while (fenced.Count < classicQuorum)
        {
            token.ThrowIfCancellationRequested();
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(token);
            var timeout = TimeProviderDelay.DelayAsync(_options.TimeProvider, _options.AttemptTimeout, attempt.Token);
            var remaining = peers.Where(p => !fenced.ContainsKey(p.Id)).ToArray();
            var pending = remaining.Select(peer => InvokeAsync(
                peer.Id,
                t => peer.Acceptor.SetFloorAsync(targetFloor, t),
                attempt.Token)).ToList();

            try
            {
                while (pending.Count > 0 && fenced.Count < classicQuorum)
                {
                    var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                    token.ThrowIfCancellationRequested();
                    if (finished == timeout) break;
                    var task = (Task<(string Id, Ballot Result, Exception? Error)>)finished;
                    pending.Remove(task);
                    var (id, result, error) = await task.ConfigureAwait(false);
                    if (error is null && result >= targetFloor)
                    {
                        fenced[id] = peers.First(p => string.Equals(p.Id, id, StringComparison.Ordinal));
                    }
                }
            }
            finally
            {
                attempt.Cancel();
            }

            if (fenced.Count < classicQuorum)
            {
                await BackoffAsync(retries++, token).ConfigureAwait(false);
            }
        }

        return [.. fenced.Values];
    }

    private async Task<SortedSet<string>> EnumerateKeysFromFencedQuorumAsync(
        ImmutableArray<AcceptorPeer> allPreviousPeers,
        ImmutableArray<AcceptorPeer> initialFencedPeers,
        Ballot targetFloor,
        CancellationToken cancellationToken)
    {
        var classicQuorum = allPreviousPeers.Length / 2 + 1;
        using var deadline = new CancellationTokenSource(_options.OperationTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var token = linked.Token;

        var fenced = initialFencedPeers.ToDictionary(static p => p.Id, static p => p, StringComparer.Ordinal);
        var enumerated = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var retries = 0;

        while (enumerated.Count < classicQuorum)
        {
            token.ThrowIfCancellationRequested();

            foreach (var peer in allPreviousPeers)
            {
                if (enumerated.ContainsKey(peer.Id)) continue;
                token.ThrowIfCancellationRequested();

                // Ensure the peer is fenced before relying on its key list (§9.2 survivor re-fencing).
                if (!fenced.ContainsKey(peer.Id))
                {
                    using var fenceTimeout = new CancellationTokenSource(_options.AttemptTimeout, _options.TimeProvider);
                    using var fenceAttempt = CancellationTokenSource.CreateLinkedTokenSource(token, fenceTimeout.Token);
                    var (_, floor, fenceError) = await InvokeAsync(
                        peer.Id,
                        t => peer.Acceptor.SetFloorAsync(targetFloor, t),
                        fenceAttempt.Token).ConfigureAwait(false);
                    if (fenceError is not null || floor < targetFloor) continue;
                    fenced[peer.Id] = peer;
                }

                var peerKeys = new List<string>();
                string? cursor = null;
                var succeeded = true;
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    using var listTimeout = new CancellationTokenSource(_options.AttemptTimeout, _options.TimeProvider);
                    using var listAttempt = CancellationTokenSource.CreateLinkedTokenSource(token, listTimeout.Token);
                    var (_, page, listError) = await InvokeAsync(
                        peer.Id,
                        t => peer.Acceptor.ListKeysAsync(prefix: "", afterKey: cursor, limit: 128, t),
                        listAttempt.Token).ConfigureAwait(false);
                    if (listError is not null)
                    {
                        succeeded = false;
                        break;
                    }

                    foreach (var entry in page.Entries)
                    {
                        peerKeys.Add(entry.Key);
                    }

                    if (page.ContinuationKey is null) break;
                    cursor = page.ContinuationKey;
                }

                if (succeeded)
                {
                    enumerated[peer.Id] = peerKeys;
                    if (enumerated.Count >= classicQuorum) break;
                }
            }

            if (enumerated.Count < classicQuorum)
            {
                await BackoffAsync(retries++, token).ConfigureAwait(false);
            }
        }

        var keys = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var peerKeys in enumerated.Values)
        {
            foreach (var key in peerKeys)
            {
                keys.Add(key);
            }
        }

        return keys;
    }

    /// <summary>
    /// Discovers the latest reachable configuration starting from <paramref name="seedPeers"/> using
    /// ballot-free <c>Read(K_cfg)</c> per §4.6, following the chain until reaching the highest configuration.
    /// </summary>
    internal static Task<DiscoveredConfigurationChain> DiscoverConfigurationChainAsync(
        long seedConfiguration,
        ImmutableArray<AcceptorPeer> seedPeers,
        Func<VoterDescriptor, IKeyedAcceptor> peerResolver,
        RegisterServiceOptions options,
        CancellationToken cancellationToken) =>
        DiscoverConfigurationChainAsync(
            new VoterConfiguration(
                seedConfiguration,
                [.. seedPeers.Select(static p => new VoterDescriptor(p.Id))],
                VoterConfigurationState.Active),
            seedPeers,
            peerResolver,
            options,
            cancellationToken);

    internal static async Task<DiscoveredConfigurationChain> DiscoverConfigurationChainAsync(
        VoterConfiguration seedConfig,
        ImmutableArray<AcceptorPeer> seedPeers,
        Func<VoterDescriptor, IKeyedAcceptor> peerResolver,
        RegisterServiceOptions options,
        CancellationToken cancellationToken)
    {
        var currentConfig = seedConfig;
        var currentPeers = seedPeers;
        VoterConfiguration? previous = null;
        VoterConfiguration? latest = null;
        var visited = new HashSet<long>();

        while (visited.Add(currentConfig.Number))
        {
            var observations = await ReadConfigKeyFromQuorumAsync(
                currentPeers, options, confirmActiveConfiguration: null, cancellationToken).ConfigureAwait(false);

            // Order candidate configurations with Number > currentConfig.Number from highest downward,
            // falling back to c + 1 if a higher candidate is unreachable or unconfirmed (§4.6 step 2).
            var candidates = observations
                .Where(o => o.Config is not null && o.Config.Number > currentConfig.Number)
                .Select(o => o.Config!)
                .GroupBy(c => c.Number)
                .Select(g => g.First())
                .OrderByDescending(c => c.Number)
                .ToList();

            var advanced = false;
            foreach (var candidate in candidates)
            {
                var candidatePeers = ResolvePeers(candidate.CanonicalVoters, peerResolver);
                try
                {
                    var candidateObs = await ReadConfigKeyFromQuorumAsync(
                        candidatePeers, options, confirmActiveConfiguration: candidate.Number, cancellationToken).ConfigureAwait(false);
                    var matchingActive = candidateObs.Count(o =>
                        o.Config is not null
                        && o.Config.Number == candidate.Number
                        && o.Config.State == VoterConfigurationState.Active
                        && o.Config.HasSameVoters(candidate.Voters));
                    var classicQuorum = candidatePeers.Length / 2 + 1;

                    // Predecessor of candidate is the configuration we just traversed from (preserving endpoints).
                    previous = currentConfig with { State = VoterConfigurationState.Active, PreviousVoters = default };

                    if (matchingActive >= classicQuorum)
                    {
                        latest = candidate with { State = VoterConfigurationState.Active, PreviousVoters = default };
                    }
                    else
                    {
                        var highestOnCandidate = candidateObs
                            .Where(o => o.Config is not null && o.Config.Number >= candidate.Number)
                            .OrderByDescending(o => o.Config!.Number)
                            .ThenByDescending(o => (int)o.Config!.State)
                            .Select(o => o.Config!)
                            .FirstOrDefault();
                        latest = highestOnCandidate ?? candidate;
                    }

                    currentConfig = candidate;
                    currentPeers = candidatePeers;
                    advanced = true;
                    break;
                }
                catch (RegisterUnavailableException)
                {
                    // Try lower candidate in fallback order (§4.6 step 2).
                }
            }

            if (!advanced)
            {
                if (latest is null)
                {
                    var matching = observations
                        .Where(o => o.Config is not null && o.Config.Number == currentConfig.Number)
                        .OrderByDescending(o => (int)o.Config!.State)
                        .Select(o => o.Config!)
                        .FirstOrDefault();
                    latest = matching ?? currentConfig;
                }

                break;
            }
        }

        return new DiscoveredConfigurationChain(previous, latest!);
    }

    internal static async Task<List<ConfigReadObservation>> ReadConfigKeyFromQuorumAsync(
        ImmutableArray<AcceptorPeer> peers,
        RegisterServiceOptions options,
        long? confirmActiveConfiguration,
        CancellationToken cancellationToken)
    {
        var classicQuorum = peers.Length / 2 + 1;
        using var deadline = new CancellationTokenSource(options.OperationTimeout, options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var token = linked.Token;

        var retries = 0;
        var lastCount = 0;
        while (!token.IsCancellationRequested)
        {
            using var phase = CancellationTokenSource.CreateLinkedTokenSource(token);
            var timeout = TimeProviderDelay.DelayAsync(options.TimeProvider, options.AttemptTimeout, phase.Token);
            var pending = peers.Select(peer => InvokeAsync(
                peer.Id,
                t => peer.Acceptor.ReadAsync(VoterConfiguration.RegisterKey, cancellationToken: t),
                phase.Token)).ToList();

            var observations = new List<ConfigReadObservation>(peers.Length);
            var activeMatches = 0;
            try
            {
                while (pending.Count > 0)
                {
                    var finished = await Task.WhenAny(pending.Cast<Task>().Append(timeout)).ConfigureAwait(false);
                    if (finished == timeout) break;
                    var task = (Task<(string Id, AcceptorReadResult Result, Exception? Error)>)finished;
                    pending.Remove(task);
                    var (id, result, error) = await task.ConfigureAwait(false);
                    if (error is not null) continue;

                    switch (result)
                    {
                        case AcceptorMissing missing:
                            observations.Add(new ConfigReadObservation(id, missing.PromisedBallot, Ballot.Zero, null));
                            break;
                        case KeyedAcceptorState state:
                            var register = RegisterEncoding.Decode(state.AcceptedValue);
                            var cfg = register.IsPresent
                                ? VoterConfigurationEncoding.Decode(register.Value!.Value)
                                : null;
                            observations.Add(new ConfigReadObservation(id, state.PromisedBallot, state.AcceptedBallot, cfg));
                            if (confirmActiveConfiguration is { } target
                                && cfg is not null
                                && cfg.Number == target
                                && cfg.State == VoterConfigurationState.Active)
                            {
                                activeMatches++;
                            }
                            break;
                    }

                    if (confirmActiveConfiguration is not null)
                    {
                        if (activeMatches >= classicQuorum || observations.Count == peers.Length) break;
                    }
                    else if (observations.Count >= classicQuorum)
                    {
                        break;
                    }
                }
            }
            finally
            {
                phase.Cancel();
            }

            lastCount = observations.Count;
            if (observations.Count >= classicQuorum)
            {
                return observations;
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (deadline.IsCancellationRequested) break;

            var backoff = TimeSpan.FromTicks(options.RetryBackoff.Ticks * (1 + (retries++ % 7)));
            try
            {
                await TimeProviderDelay.DelayAsync(options.TimeProvider, backoff, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new RegisterUnavailableException(new InvalidOperationException(
            $"Could not read '{VoterConfiguration.RegisterKey}' from a classic quorum ({lastCount}/{classicQuorum})."));
    }

    private ReplicatedRegisterService CreateClassicService(ImmutableArray<AcceptorPeer> peers, long configuration) =>
        new(
            peers,
            _ballots,
            new RegisterServiceOptions
            {
                Mode = ProposerModePolicy.StaticClassic,
                TimeProvider = _options.TimeProvider,
                OperationTimeout = _options.OperationTimeout,
                AttemptTimeout = _options.AttemptTimeout,
                RetryBackoff = _options.RetryBackoff,
                Capacity = _options.Capacity,
                InitialConfiguration = configuration,
                PeerResolver = null,
            });

    private ImmutableArray<AcceptorPeer> ResolvePeers(ImmutableArray<VoterDescriptor> voters) =>
        ResolvePeers(voters, _peerResolver);

    internal static ImmutableArray<AcceptorPeer> ResolvePeers(
        ImmutableArray<VoterDescriptor> voters,
        Func<VoterDescriptor, IKeyedAcceptor> peerResolver) =>
        [.. voters.Select(v => new AcceptorPeer(v.Id, peerResolver(v)))];

    private Task BackoffAsync(int attempt, CancellationToken token)
    {
        var factor = 1 + ((_ballots.ProposerId % 7 + attempt % 7) % 7);
        return TimeProviderDelay.DelayAsync(_options.TimeProvider, TimeSpan.FromTicks(_options.RetryBackoff.Ticks * factor), token);
    }

    private static async Task<(string Id, T Result, Exception? Error)> InvokeAsync<T>(
        string id,
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken token)
    {
        try
        {
            return (id, await operation(token).ConfigureAwait(false), null);
        }
        catch (Exception error)
        {
            return (id, default!, error);
        }
    }

    internal sealed record ConfigReadObservation(
        string PeerId,
        Ballot PromisedBallot,
        Ballot AcceptedBallot,
        VoterConfiguration? Config);

    internal sealed record DiscoveredConfigurationChain(
        VoterConfiguration? Previous,
        VoterConfiguration Latest);

    private sealed record HandOffOutbound(PrepareRequest? Prepare, AcceptRequest<ByteString>? Accept)
    {
        public int Round => Prepare?.Ballot.Round ?? Accept!.Value.Ballot.Round;
    }

    private sealed class IdentityOperation : IOperation<ByteString>
    {
        public (OperationStatus Status, ByteString Result) Apply(ByteString current) =>
            (OperationStatus.NotApplicable, current);
    }

    private sealed class HandOffEngine(
        IProposerBallotSource source,
        long configuration,
        ImmutableArray<string> nextPeers,
        ImmutableArray<string> previousPeers)
        : ProposerEngine<ByteString, string>(
            new ProposerRuntime(),
            RegisterEncoding.Absent,
            nextPeers,
            ProposerModePolicy.StaticClassic,
            configuration,
            previousPeers)
    {
        public override int ProposerId => source.ProposerId;
        public HandOffOutbound? Outgoing { get; private set; }
        public ByteString? Result { get; private set; }

        protected override Ballot ReserveBallot(Ballot candidate) =>
            source.Reserve(candidate.Configuration, candidate.Round);

        protected override void OnSendPrepare(PrepareRequest request) => Outgoing = new HandOffOutbound(request, null);

        protected override void OnSendAccept(AcceptRequest<ByteString> request) => Outgoing = new HandOffOutbound(null, request);

        protected override void OnProposalCompleted(ProposeResponse<ByteString> response) => Result = response.CommittedValue;

        protected override void OnValueCommitted(CommittedValue<ByteString> committedValue) { }
    }
}
