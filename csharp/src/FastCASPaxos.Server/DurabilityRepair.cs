using System.Collections.Immutable;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using System.Runtime.ExceptionServices;

namespace FastCASPaxos.Server;

/// <summary>
/// Compares bounded inventory ranges and restores replicas through classic identity proposals.
/// Dispose after every pass has completed.
/// </summary>
public sealed class DurabilityRepair : IDisposable
{
    private readonly ImmutableArray<RepairPeer> _peers;
    private readonly ImmutableArray<string> _peerIds;
    private readonly SemaphoreSlim[] _slots;
    private readonly SemaphoreSlim _scan = new(1);
    private readonly IProposerBallotSource _ballots;
    private readonly DurabilityRepairOptions _options;
    private readonly int _local;
    public int TargetReplicas { get; }

    public DurabilityRepair(string localId, ImmutableArray<RepairPeer> peers,
        IProposerBallotSource ballots, DurabilityRepairOptions? options = null)
    {
        if (peers.IsDefault) throw new ArgumentNullException(nameof(peers));
        ArgumentNullException.ThrowIfNull(ballots);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ballots.ProposerId);
        _peers = peers;
        var ids = new HashSet<string>(StringComparer.Ordinal);
        _local = -1;
        for (var index = 0; index < _peers.Length; index++)
        {
            var peer = _peers[index];
            ArgumentNullException.ThrowIfNull(peer);
            ArgumentException.ThrowIfNullOrEmpty(peer.Id);
            ArgumentNullException.ThrowIfNull(peer.Acceptor);
            ArgumentNullException.ThrowIfNull(peer.Digests);
            if (!ids.Add(peer.Id)) throw new ArgumentException("Repair requires distinct fixed acceptor identities.");
            if (peer.Id == localId) _local = index;
        }
        _peerIds = [.. _peers.Select(peer => peer.Id)];
        if (_local < 0) throw new ArgumentException("Repair membership must include the local acceptor.");
        _ballots = ballots;
        _options = options ?? new();
        ArgumentNullException.ThrowIfNull(_options.TimeProvider);
        ArgumentOutOfRangeException.ThrowIfNegative(_options.TargetReplicas);
        TargetReplicas = _options.TargetReplicas == 0 ? _peers.Length / 2 + 1 : _options.TargetReplicas;
        if (TargetReplicas > _peers.Length) throw new ArgumentOutOfRangeException(nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.PageSize);
        ValidateDuration(_options.OperationTimeout);
        ValidateDuration(_options.AttemptTimeout);
        ValidateDuration(_options.RetryBackoff);
        _slots = _peers.Select(_ => new SemaphoreSlim(1)).ToArray();
    }

    /// <summary>
    /// Visits the union of reachable inventories. A departing pass visits local keys and counts remote replicas.
    /// Calls serialize; cancellation ends waiting while already selected durable transitions retain their outcome.
    /// </summary>
    public async Task<RepairPassResult> RunPassAsync(bool departing = false, CancellationToken cancellationToken = default)
    {
        await _scan.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long examined = 0, repaired = 0, pending = 0;
            var errors = new Dictionary<int, Exception>();
            Exception? lastRepairError = null;
            string? cursor = null;
            do
            {
                var pages = await Task.WhenAll(Enumerable.Range(0, _peers.Length).Select(index =>
                    InvokeAsync(index, (peer, token) => peer.Digests.ListDigestsAsync(
                        afterKey: cursor, limit: _options.PageSize, cancellationToken: token), cancellationToken)))
                    .ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                RecordErrors(pages, errors);
                if (pages[_local].Value is null)
                    throw new RegisterUnavailableException(new AggregateException(errors.Values));
                string? frontier = null;
                foreach (var page in pages)
                {
                    // Departure follows the sealed local key set while other members keep accepting keys.
                    if (departing && page.Index != _local) continue;
                    if (page.Value?.ContinuationKey is { } next
                        && (frontier is null || StringComparer.Ordinal.Compare(next, frontier) < 0))
                        frontier = next;
                }
                var candidates = new SortedDictionary<string, (int Source, AcceptedDigest Digest)>(StringComparer.Ordinal);
                for (var index = 0; index < pages.Length; index++)
                {
                    if (departing && index != _local) continue;
                    if (pages[index].Value is not { } page) continue;
                    foreach (var entry in page.Entries)
                    {
                        if (frontier is null || StringComparer.Ordinal.Compare(entry.Key, frontier) <= 0)
                        {
                            if (index == _local) candidates[entry.Key] = (index, entry.Digest);
                            else candidates.TryAdd(entry.Key, (index, entry.Digest));
                        }
                    }
                }
                var inventories = pages.Select(page => page.Value?.Entries.ToDictionary(e => e.Key, e => e.Digest, StringComparer.Ordinal)).ToArray();
                foreach (var (key, candidate) in candidates)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    examined++;
                    var matching = 0;
                    var eligible = 0;
                    var divergent = false;
                    for (var index = 0; index < inventories.Length; index++)
                    {
                        if (inventories[index] is not { } inventory) continue;
                        if (inventory.TryGetValue(key, out var digest) && digest == candidate.Digest)
                        {
                            matching++;
                            if (!departing || index != _local) eligible++;
                        }
                        else divergent = true;
                    }
                    var commitQuorum = candidate.Digest.AcceptedBallot.IsFastRoundBallot
                        ? (3 * _peers.Length + 3) / 4 : _peers.Length / 2 + 1;
                    // Departure needs fresh protocol acknowledgements: readable digests may belong
                    // to peers which have themselves sealed mutation admission for shutdown.
                    if (!departing && !divergent && matching >= commitQuorum && eligible >= TargetReplicas) continue;

                    var seed = await InvokeAsync(candidate.Source,
                        (peer, token) => peer.Acceptor.ReadAsync(key, cancellationToken: token), cancellationToken).ConfigureAwait(false);
                    if (seed.Error is { } seedError)
                    {
                        errors[candidate.Source] = seedError;
                        pending++;
                        continue;
                    }
                    if (seed.Value is not KeyedAcceptorState state)
                        throw new InvalidDataException("A discovered accepted key must retain its accepted state.");
                    try
                    {
                        await RepairKeyAsync(key, state.AcceptedValue, departing, errors, cancellationToken).ConfigureAwait(false);
                        repaired++;
                    }
                    catch (RegisterUnavailableException error)
                    {
                        lastRepairError = error;
                        pending++;
                    }
                }
                cursor = frontier;
            } while (cursor is not null);
            var reported = ImmutableArray.CreateBuilder<Exception>(errors.Count + (lastRepairError is null ? 0 : 1));
            reported.AddRange(errors.Values);
            if (lastRepairError is not null) reported.Add(lastRepairError);
            return new(examined, repaired, pending, reported.MoveToImmutable());
        }
        finally { _scan.Release(); }
    }

    /// <summary>Releases the pass gate. Detached peer attempts retain their own slots until they finish.</summary>
    public void Dispose() => _scan.Dispose();

    private async Task RepairKeyAsync(string key, ByteString seed, bool departing,
        Dictionary<int, Exception> errors, CancellationToken cancellationToken)
    {
        using var deadline = new CancellationTokenSource(_options.OperationTimeout, _options.TimeProvider);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
        var token = operation.Token;
        var engine = new Engine(_ballots, _peerIds, seed);
        engine.StartProposal(new Identity());
        try
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                var request = engine.Outgoing ?? throw new InvalidOperationException("Repair has no protocol phase.");
                var retryDelay = _options.RetryBackoff * (1 + _ballots.ProposerId % 7);
                if (request.Prepare is { } prepare)
                {
                    var responses = await Task.WhenAll(Enumerable.Range(0, _peers.Length).Select(index =>
                        InvokeAsync(index, (peer, attempt) => peer.Acceptor.PrepareAsync(key, prepare, attempt), token))).ConfigureAwait(false);
                    RecordErrors(responses, errors);
                    if (responses.Count(r => r.Error is null && r.Value.IsPromised) <= _peers.Length / 2)
                        retryDelay = _options.AttemptTimeout;
                    var conflict = responses.Where(r => r.Error is null && !r.Value.IsPromised)
                        .Select(r => r.Value.ConflictingBallot).DefaultIfEmpty(Ballot.Zero).Max();
                    if (!conflict.IsZero)
                    {
                        engine = new Engine(_ballots, _peerIds, seed, checked(conflict.Round + 1));
                        engine.StartProposal(new Identity());
                    }
                    else
                    {
                        foreach (var response in responses)
                        {
                            if (response.Error is not null) continue;
                            var result = response.Value;
                            var value = result.AcceptedBallot.IsZero ? seed
                                : result.AcceptedValue ?? throw new InvalidDataException("Accepted ballots require accepted bytes.");
                            engine.HandlePreparePromised(new(prepare.Ballot.Round, _peers[response.Index].Id,
                                result.AcceptedBallot, value));
                        }
                    }
                }
                else
                {
                    var accept = request.Accept!.Value;
                    var responses = await Task.WhenAll(Enumerable.Range(0, _peers.Length).Select(index =>
                        InvokeAsync(index, (peer, attempt) => peer.Acceptor.AcceptAsync(key, accept, attempt), token))).ConfigureAwait(false);
                    RecordErrors(responses, errors);
                    var eligible = 0;
                    var accepted = 0;
                    foreach (var response in responses)
                    {
                        if (response.Error is not null) continue;
                        var result = response.Value;
                        if (result.IsAccepted)
                        {
                            accepted++;
                            if (!departing || response.Index != _local) eligible++;
                            engine.HandleAcceptAccepted(new(accept.Ballot.Round, _peers[response.Index].Id, result.PromisedBallot));
                        }
                        else engine.HandleAcceptRejected(new(accept.Ballot.Round, _peers[response.Index].Id, result.ConflictingBallot));
                    }
                    var conflict = responses.Where(r => r.Error is null && !r.Value.IsAccepted)
                        .Select(r => r.Value.ConflictingBallot).DefaultIfEmpty(Ballot.Zero).Max();
                    if (accepted > _peers.Length / 2 && eligible >= TargetReplicas && (departing || conflict.IsZero)) return;
                    if (responses.Any(r => r.Error is not null)) retryDelay = _options.AttemptTimeout;
                    // The same classic proposal is safe to retry after a lost reply. A fresh recovery
                    // handles superseding writes when the required remote acknowledgements are missing.
                    if (engine.Completed || !conflict.IsZero)
                        engine = new Engine(_ballots, _peerIds, seed,
                            conflict.IsZero ? 1 : checked(conflict.Round + 1));
                    if (!engine.HasPendingProposal) engine.StartProposal(new Identity());
                }
                if (ReferenceEquals(request, engine.Outgoing)) engine.HandleResponseTimeout(request.Round);
                if (engine.Outgoing?.Prepare is not null)
                    await Task.Delay(retryDelay, _options.TimeProvider, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && deadline.IsCancellationRequested)
        {
            throw new RegisterUnavailableException(new AggregateException(errors.Values));
        }
    }

    private async Task<PeerResult<T>> InvokeAsync<T>(int index,
        Func<RepairPeer, CancellationToken, ValueTask<T>> operation, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (!_slots[index].Wait(0, CancellationToken.None)) return new(index, default!, new RegisterBusyException());
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(token);
        var task = ExecuteAsync();
        try
        {
            var result = await task.WaitAsync(_options.AttemptTimeout, _options.TimeProvider, token).ConfigureAwait(false);
            if (index == _local && result.Error is { } error
                && error is not TimeoutException && error is not RegisterBusyException)
                ExceptionDispatchInfo.Capture(error).Throw();
            return result;
        }
        catch (TimeoutException error) { return new(index, default!, error); }
        finally { attempt.Cancel(); }

        async Task<PeerResult<T>> ExecuteAsync()
        {
            try { return new(index, await operation(_peers[index], attempt.Token).ConfigureAwait(false), null); }
            catch (Exception error) { return new(index, default!, error); }
            finally { _slots[index].Release(); }
        }
    }

    private static void RecordErrors<T>(ReadOnlySpan<PeerResult<T>> responses, Dictionary<int, Exception> errors)
    {
        foreach (var response in responses)
            if (response.Error is { } error) errors[response.Index] = error;
    }

    private static void ValidateDuration(TimeSpan value)
    {
        if (value <= TimeSpan.Zero || value > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(value), "Repair durations must be positive and at most one day.");
    }

    private sealed record PeerResult<T>(int Index, T Value, Exception? Error);
    private sealed record Outbound(PrepareRequest? Prepare, AcceptRequest<ByteString>? Accept)
    {
        public int Round => Prepare?.Ballot.Round ?? Accept!.Value.Ballot.Round;
    }
    private sealed class Identity : IOperation<ByteString>
    {
        public (OperationStatus Status, ByteString Result) Apply(ByteString current) => (OperationStatus.NotApplicable, current);
    }
    private sealed class Engine(IProposerBallotSource source, ImmutableArray<string> peers, ByteString seed, int minimumRound = 1)
        : ProposerEngine<ByteString, string>(new ProposerRuntime(), seed, peers, ProposerModePolicy.Parse("classic"))
    {
        public override int ProposerId => source.ProposerId;
        public Outbound? Outgoing { get; private set; }
        public bool Completed { get; private set; }
        protected override Ballot ReserveBallot(Ballot candidate) => source.Reserve(Math.Max(candidate.Round, minimumRound));
        protected override void OnSendPrepare(PrepareRequest request) => Outgoing = new(request, null);
        protected override void OnSendAccept(AcceptRequest<ByteString> request) => Outgoing = new(null, request);
        protected override void OnProposalCompleted(ProposeResponse<ByteString> response) => Completed = true;
        protected override void OnValueCommitted(CommittedValue<ByteString> committedValue) { }
    }
}
