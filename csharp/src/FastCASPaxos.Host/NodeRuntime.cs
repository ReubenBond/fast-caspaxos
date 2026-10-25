using System.Collections.Immutable;
using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;
using Grpc.Core.Interceptors;
using System.Threading.Channels;

namespace FastCASPaxos.Host;

/// <summary>Owns one durable acceptor, one proposer allocator, and its fixed-peer register service.</summary>
public sealed partial class NodeRuntime : IRepairScheduler, IRegisterLearner, IRegisterDigestLearner, IAsyncDisposable
{
    private readonly List<GrpcChannel> _channels = [];
    private SqliteProposerBallotSource? _ballots;
    private SqliteAcceptorServer? _acceptor;
    private ReplicatedRegisterService? _registers;
    private IKeyedAcceptor? _observedAcceptor;
    private QuiescingAcceptor? _admission;
    private DurabilityRepair? _repair;
    private readonly List<GrpcRepairScheduler> _remoteSchedulers = [];
    private readonly List<LearningPeer> _learners = [];
    private CancellationTokenSource _learningStopping = new();
    private CancellationTokenSource _generationStopping = new();
    private readonly CancellationTokenSource _maintenanceStopping = new();
    private readonly CancellationTokenSource _shutdownAborted = new();
    private readonly Channel<bool> _staged = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        FullMode = BoundedChannelFullMode.DropWrite,
        SingleReader = true,
    });
    private readonly TimeProvider _clock;
    private readonly TimeSpan _repairInterval;
    private readonly TimeSpan _repairRetryInterval;
    private readonly TimeSpan _initialDelay;
    private Task? _periodic;
    private Task? _shutdown;
    private bool _stopping;
    private RepairObservation? _lastRepair;
    private readonly Lock _gate = new();
    private Task? _disposal;
    private readonly NodeOptions _options;
    private NodeAdminStateStore? _admin;
    private readonly SemaphoreSlim _administration = new(1);
    private EpochAcceptor? _epochAcceptor;
    private RegisterGeneration? _registerGeneration;
    private ImmutableArray<AcceptorPeer> _peers;
    private readonly BoundedReadExecutor _localReads;

    private int _eventStreams;
    private NodeRuntime(NodeOptions options, TimeProvider timeProvider)
    {
        _options = options;
        _localReads = new(options.Capacity);
        _clock = timeProvider;
        _repairInterval = options.RepairInterval;
        _repairRetryInterval = options.RepairRetryInterval;
        ShutdownMode = options.ShutdownMode;
        var ordinal = options.Peers.Select(p => p.Id).Order(StringComparer.Ordinal).ToList().IndexOf(options.AcceptorId);
        _initialDelay = TimeSpan.FromTicks(Math.Max(1, options.RepairInterval.Ticks / options.Peers.Count * (ordinal + 1)));
        Diagnostics = new(options.AcceptorId, options.EventHistoryCapacity, timeProvider);
        ProposerId = options.ProposerId;
        Peers = [.. options.Peers.Select(peer =>
        {
            var origin = new Uri(peer.HttpEndpoint).GetLeftPart(UriPartial.Authority);
            return new AdminPeer(peer.Id, peer.Endpoint, origin, origin + "/v1/admin/events");
        })];
        HeartbeatInterval = options.HeartbeatInterval;
        MaxEventStreams = options.MaxEventStreams;
        Acceptor = new RuntimeAcceptor(this);
        Registers = new RuntimeRegisters(this);
        Digests = new RuntimeDigests(this);
    }

    public IKeyedAcceptor Acceptor { get; }
    public IRegisterService Registers { get; }
    public IRegisterLearner Learner => this;
    public ILocalAcceptorInspection Inspection => _acceptor ?? throw new InvalidOperationException("The node is not open.");
    public IAcceptorDigestSource Digests { get; }
    private DurabilityRepair Repair => _repair ?? throw new InvalidOperationException("The node is not open.");
    public int TargetReplicas => Repair.TargetReplicas;
    public RepairObservation? LastRepair => Volatile.Read(ref _lastRepair);
    public bool IsDraining => Volatile.Read(ref _stopping);
    public bool IsShutdownComplete => Volatile.Read(ref _shutdown)?.IsCompletedSuccessfully == true;
    public NodeShutdownMode ShutdownMode { get; }
    public NodeEventJournal Diagnostics { get; }
    public int ProposerId { get; }
    public ImmutableArray<AdminPeer> Peers { get; }
    public TimeSpan HeartbeatInterval { get; }
    public int MaxEventStreams { get; }

    public static async Task<NodeRuntime> OpenAsync(
        NodeOptions options, TimeProvider? timeProvider = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        var clock = timeProvider ?? TimeProvider.System;
        var runtime = new NodeRuntime(options, clock);
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(options.AcceptorDatabase))!);
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(options.ProposerDatabase))!);
            runtime._acceptor = await SqliteAcceptorServer.OpenAsync(options.AcceptorDatabase,
                options.QueueCapacity, options.MaxBatchSize, runtime.Diagnostics, cancellationToken).ConfigureAwait(false);
            runtime._ballots = new SqliteProposerBallotSource(options.ProposerDatabase, options.ProposerId);
            runtime._admin = new(options.ProposerDatabase, options.ProposerSettings ?? new() { Mode = options.Mode });
            runtime.OpenGeneration(runtime._admin.State.Epoch);
            if (runtime._admin.State.Phase != "active")
                await runtime.QuiesceGenerationAsync().ConfigureAwait(false);
            return runtime;
        }
        catch (Exception error)
        {
            try { await runtime.DisposeAsync().ConfigureAwait(false); }
            catch (Exception cleanupError) { throw new AggregateException(error, cleanupError); }
            throw;
        }
    }

    private void OpenGeneration(string epoch)
    {
        var options = _options;
        _epochAcceptor = new(epoch, _acceptor!, _acceptor!);
        _admission = new QuiescingAcceptor(_epochAcceptor);
        _observedAcceptor = new ObservedAcceptor(_admission, Diagnostics, "inbound", null);
        var peers = ImmutableArray.CreateBuilder<AcceptorPeer>(options.Peers.Count);
        var repairPeers = ImmutableArray.CreateBuilder<RepairPeer>(options.Peers.Count);
        foreach (var peer in options.Peers)
        {
            IKeyedAcceptor acceptor;
            IAcceptorDigestSource digests;
            if (peer.Id == options.AcceptorId)
            {
                acceptor = _admission;
                digests = _epochAcceptor;
            }
            else
            {
                var channel = GrpcChannel.ForAddress(peer.Endpoint, new GrpcChannelOptions
                {
                    MaxReceiveMessageSize = options.MaxReceiveMessageBytes,
                    MaxSendMessageSize = options.MaxSendMessageBytes,
                });
                _channels.Add(channel);
                var invoker = channel.CreateCallInvoker().Intercept(
                    new DiagnosticClientInterceptor(), new EpochClientInterceptor(epoch));
                acceptor = new GrpcKeyedAcceptor(invoker, options.PeerCallTimeout, _clock);
                digests = new GrpcAcceptorDigestSource(invoker, options.PeerCallTimeout, _clock);
                _remoteSchedulers.Add(new GrpcRepairScheduler(invoker, options.PeerCallTimeout, _clock));
                _learners.Add(new(peer.Id, new GrpcRegisterLearner(invoker, options.PeerCallTimeout, _clock),
                    new LatestLearningQueue(options.Capacity)));
            }
            peers.Add(new(peer.Id, new ObservedAcceptor(acceptor, Diagnostics,
                peer.Id == options.AcceptorId ? "local" : "outbound", peer.Id)));
            repairPeers.Add(new(peer.Id, peer.Id == options.AcceptorId
                ? new ObservedAcceptor(_epochAcceptor, Diagnostics, "local", peer.Id)
                : peers[^1].Acceptor, digests));
        }
        _peers = peers.MoveToImmutable();
        OpenRegisters(epoch, _admin!.State.Settings);
        foreach (var peer in _learners)
            peer.Worker = Task.WhenAll(Enumerable.Range(0, options.Capacity).Select(_ => BroadcastCommitsAsync(peer)));
        _repair = new(options.AcceptorId, repairPeers.MoveToImmutable(), _ballots!, new()
        {
            TargetReplicas = options.TargetReplicationFactor,
            PageSize = options.RepairPageSize,
            OperationTimeout = options.OperationTimeout,
            AttemptTimeout = options.AttemptTimeout,
            RetryBackoff = options.RetryBackoff,
            TimeProvider = _clock,
        });
    }

    private void OpenRegisters(string epoch, ProposerSettings settings)
    {
        _registers = new(_peers, _ballots!, _options.RegisterOptions(_clock, settings), _acceptor, Diagnostics, _localReads);
        _registers.Committed += QueueCommit;
        var observed = new ObservedRegisterService(_registers, Diagnostics);
        Volatile.Write(ref _registerGeneration, new(epoch, observed, _registers));
    }

    public void Learn(RegisterCommit commit)
    {
        ArgumentNullException.ThrowIfNull(commit);
        var context = DiagnosticContext.Current;
        var callId = context?.CallId ?? Guid.NewGuid().ToString("N");
        ObserveLearning(commit, "inbound", "request-received", context?.NodeId, callId);
        try
        {
            var generation = GetServingRegisters();
            using var lease = generation.Admission.Enter();
            generation.Core.Learn(commit);
            ObserveLearning(commit, "inbound", "response-produced", context?.NodeId, callId);
        }
        catch (Exception error)
        {
            ObserveLearning(commit, "inbound", "fault-observed", context?.NodeId, callId, error.GetType().Name);
            throw;
        }
    }

    private void QueueCommit(RegisterCommit commit)
    {
        if (_learners.Count == 0) return;
        var notification = new LearningNotification(commit,
            new(commit.AcceptedBallot, AcceptedDigest.Hash(commit.Value)), DiagnosticContext.Current?.OperationId);
        foreach (var peer in _learners)
        {
            if (!peer.Queue.TryWrite(notification))
                ObserveLearning(commit, "outbound", "fault-observed", peer.Id, Guid.NewGuid().ToString("N"),
                    "Commit learning key capacity is full; the peer will recover through a quorum.");
        }
    }

    private async Task BroadcastCommitsAsync(LearningPeer peer)
    {
        var token = _learningStopping.Token;
        try
        {
            while (true)
            {
                var notification = await peer.Queue.ReadAsync(token).ConfigureAwait(false);
                var commit = notification.Commit;
                var callId = Guid.NewGuid().ToString("N");
                DiagnosticContext.Current = new(Diagnostics.NodeId, notification.OperationId, callId);
                var compact = commit.AcknowledgedPeers.Contains(peer.Id)
                    ? new RegisterCommitDigest(commit.Key, notification.Digest, commit.PreparedBallot) : null;
                if (compact is null) ObserveLearning(commit, "outbound", "request-initiated", peer.Id, callId);
                else ObserveCompactLearning(compact, "outbound", "request-initiated", peer.Id, callId);
                try
                {
                    if (compact is null)
                    {
                        await peer.Client.LearnAsync(commit, token).ConfigureAwait(false);
                        ObserveLearning(commit, "outbound", "response-observed", peer.Id, callId);
                    }
                    else
                    {
                        var learned = await peer.Client.LearnDigestAsync(compact, token).ConfigureAwait(false);
                        ObserveCompactLearning(compact, "outbound", "response-observed", peer.Id, callId,
                            learned ? "committed" : "stale");
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    if (compact is null) ObserveLearning(commit, "outbound", "fault-observed", peer.Id, callId, error.GetType().Name);
                    else ObserveCompactLearning(compact, "outbound", "fault-observed", peer.Id, callId, error: error.GetType().Name);
                }
                finally
                {
                    DiagnosticContext.Current = null;
                    peer.Queue.Complete(commit.Key);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private void ObserveLearning(RegisterCommit commit, string direction, string phase, string? peer, string callId,
        string? error = null) =>
        Diagnostics.Publish("protocol", new ProtocolObservation("learn", direction, phase, peer, callId,
            DiagnosticContext.Current?.OperationId, KeyPreview.From(commit.Key), commit.AcceptedBallot,
            commit.PreparedBallot, error is null ? "committed" : null, AcceptedBallot: commit.AcceptedBallot,
            Value: PayloadPreview.From(commit.Value), Error: error));

    internal bool TryOpenEventStream()
    {
        if (Interlocked.Increment(ref _eventStreams) <= MaxEventStreams) return true;
        Interlocked.Decrement(ref _eventStreams);
        return false;
    }

    internal void CloseEventStream() => Interlocked.Decrement(ref _eventStreams);

    public void StageRepair()
    {
        lock (_gate)
        {
            if (_stopping || _disposal is not null)
                throw new RegisterUnavailableException(new InvalidOperationException("The node is stopping."));
            using var lease = _epochAcceptor!.Admission.Enter();
            PublishRepair(new("staged", false, Repair.TargetReplicas));
            _staged.Writer.TryWrite(true);
        }
    }

    public async Task StageClusterRepairAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var lease = _epochAcceptor!.Admission.Enter();
        StageRepair();
        await Task.WhenAll(_remoteSchedulers.Select(peer => peer.StageRepairAsync(cancellationToken))).ConfigureAwait(false);
    }

    internal Task RunPeriodicRepairAsync(ILogger logger, CancellationToken stoppingToken)
    {
        lock (_gate) return _periodic ??= PeriodicCoreAsync(logger, stoppingToken);
    }

    private async Task PeriodicCoreAsync(ILogger logger, CancellationToken stoppingToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _maintenanceStopping.Token);
        var token = linked.Token;
        var delay = _initialDelay;
        try
        {
            while (true)
            {
                using (var trigger = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    try { await _staged.Reader.ReadAsync(trigger.Token).AsTask().WaitAsync(delay, _clock, token).ConfigureAwait(false); }
                    catch (TimeoutException) { } // The interval stages the same bounded pass as an explicit command.
                    finally { trigger.Cancel(); }
                }
                token.ThrowIfCancellationRequested();
                PublishRepair(new("running", false, Repair.TargetReplicas));
                try
                {
                    await _administration.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        using var lease = _epochAcceptor!.Admission.Enter();
                        using var passCancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _generationStopping.Token);
                        var result = await Repair.RunPassAsync(cancellationToken: passCancellation.Token).ConfigureAwait(false);
                        ReportRepair(result, false);
                        foreach (var error in result.Errors) logger.RepairPeerUnavailable(error);
                    }
                    finally { _administration.Release(); }
                }
                catch (RegisterUnavailableException error)
                {
                    logger.RepairInventoryUnavailable(error);
                    PublishRepair(new("failed", false, Repair.TargetReplicas, Error: error.Message));
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    PublishRepair(new("quiesced", false, Repair.TargetReplicas));
                }
                delay = _repairInterval;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception error)
        {
            PublishRepair(new("failed", false, Repair.TargetReplicas, Error: error.Message));
            throw;
        }
    }

    /// <summary>Drains admitted mutations, then establishes remote replicas in departure mode.</summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _stopping = true;
            _maintenanceStopping.Cancel();
            return (_shutdown ??= StopCoreAsync(cancellationToken)).WaitAsync(cancellationToken);
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        await _administration.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { await StopServingAsync(cancellationToken).ConfigureAwait(false); }
        finally { _administration.Release(); }
    }

    private async Task StopServingAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownAborted.Token);
        cancellationToken = linked.Token;
        var departing = ShutdownMode == NodeShutdownMode.Departure && _admin!.State.Phase == "active";
        _maintenanceStopping.Cancel();
        PublishRepair(new("draining", departing, Repair.TargetReplicas));
        var admitted = _admission!.QuiesceAsync();
        var serving = GetRegisters().Admission.CloseAsync();
        var registers = _registers!.DisposeAsync().AsTask();
        try
        {
            if (_periodic is not null) await _periodic.WaitAsync(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(admitted, registers, serving).WaitAsync(cancellationToken).ConfigureAwait(false);
            _learningStopping.Cancel();
            await Task.WhenAll(_learners.Select(peer => peer.Worker)).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!departing)
            {
                PublishRepair(new("complete", false, Repair.TargetReplicas));
                return;
            }
            while (true)
            {
                PublishRepair(new("running", true, Repair.TargetReplicas));
                try
                {
                    var result = await Repair.RunPassAsync(departing: true, cancellationToken).ConfigureAwait(false);
                    ReportRepair(result, true);
                    if (result.Pending == 0) return;
                }
                catch (RegisterUnavailableException error)
                {
                    PublishRepair(new("failed", true, Repair.TargetReplicas, Error: error.Message));
                }
                await Task.Delay(_repairRetryInterval, _clock, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            PublishRepair(new(
                cancellationToken.IsCancellationRequested ? "cancelled" : "failed", departing, Repair.TargetReplicas, Error: error.Message));
            throw;
        }
    }

    private void ReportRepair(RepairPassResult result, bool departing) =>
        PublishRepair(new(result.Pending == 0 ? "complete" : "pending",
            departing, Repair.TargetReplicas, result.Examined, result.Repaired, result.Pending,
            result.Errors.Length == 0 ? null : string.Join("; ", result.Errors.Select(error => error.Message))));

    private void PublishRepair(RepairObservation observation)
    {
        observation = observation with { Timestamp = _clock.GetUtcNow() };
        Volatile.Write(ref _lastRepair, observation);
        Diagnostics.Publish("repair", observation);
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate) return new ValueTask(_disposal ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        List<Exception> errors = [];
        _shutdownAborted.Cancel();
        _maintenanceStopping.Cancel();
        _learningStopping.Cancel();
        _generationStopping.Cancel();
        if (_shutdown is not null)
        {
            try { await _shutdown.ConfigureAwait(false); }
            catch (OperationCanceledException) when (_shutdownAborted.IsCancellationRequested) { }
            catch (Exception error) { errors.Add(error); }
        }
        if (_periodic is not null)
        {
            try { await _periodic.ConfigureAwait(false); }
            catch (Exception error) { errors.Add(error); }
        }
        await _clusterAdministration.WaitAsync().ConfigureAwait(false);
        await _administration.WaitAsync().ConfigureAwait(false);
        if (_registers is not null)
        {
            try { await _registers.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { errors.Add(error); }
        }
        try { await Task.WhenAll(_learners.Select(peer => peer.Worker)).ConfigureAwait(false); }
        catch (Exception error) { errors.Add(error); }
        _repair?.Dispose();
        foreach (var channel in _channels)
        {
            try { channel.Dispose(); }
            catch (Exception error) { errors.Add(error); }
        }
        if (_ballots is not null)
        {
            try { _ballots.Dispose(); }
            catch (Exception error) { errors.Add(error); }
        }
        _admin?.Dispose();
        _adminHttp.Dispose();
        if (_acceptor is not null)
        {
            try { await _acceptor.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { errors.Add(error); }
        }
        Diagnostics.Dispose();
        _administration.Release();
        _clusterAdministration.Release();
        if (errors.Count > 0) throw new AggregateException("Node shutdown failed.", errors);
    }

}

public sealed record RepairObservation(
    string State, bool Departing, int TargetReplicas, long Examined = 0, long Repaired = 0, long Pending = 0,
    string? Error = null, DateTimeOffset Timestamp = default);
