using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;
using Grpc.Core.Interceptors;
using System.Threading.Channels;

namespace FastCASPaxos.Host;

/// <summary>Owns one durable acceptor, one proposer allocator, and its fixed-peer register service.</summary>
public sealed class NodeRuntime : IRepairScheduler, IAsyncDisposable
{
    private readonly List<GrpcChannel> _channels = [];
    private SqliteProposerBallotSource? _ballots;
    private SqliteAcceptorServer? _acceptor;
    private ReplicatedRegisterService? _registers;
    private IKeyedAcceptor? _observedAcceptor;
    private IRegisterService? _observedRegisters;
    private QuiescingAcceptor? _admission;
    private DurabilityRepair? _repair;
    private readonly List<GrpcRepairScheduler> _remoteSchedulers = [];
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
    private readonly object _gate = new();
    private Task? _disposal;

    private int _eventStreams;
    private NodeRuntime(NodeOptions options, TimeProvider timeProvider)
    {
        _clock = timeProvider;
        _repairInterval = options.RepairInterval;
        _repairRetryInterval = options.RepairRetryInterval;
        var ordinal = options.Peers.Select(p => p.Id).Order(StringComparer.Ordinal).ToList().IndexOf(options.AcceptorId);
        _initialDelay = TimeSpan.FromTicks(Math.Max(1, options.RepairInterval.Ticks / options.Peers.Count * (ordinal + 1)));
        Diagnostics = new(options.AcceptorId, options.EventHistoryCapacity, timeProvider);
        ProposerId = options.ProposerId;
        Peers = Array.AsReadOnly(options.Peers.Select(peer =>
        {
            var origin = new Uri(peer.HttpEndpoint).GetLeftPart(UriPartial.Authority);
            return new AdminPeer(peer.Id, peer.Endpoint, origin, origin + "/v1/admin/events");
        }).ToArray());
        HeartbeatInterval = options.HeartbeatInterval;
        MaxEventStreams = options.MaxEventStreams;
    }

    public IKeyedAcceptor Acceptor => _observedAcceptor ?? throw new InvalidOperationException("The node is not open.");
    public IRegisterService Registers => _observedRegisters ?? throw new InvalidOperationException("The node is not open.");
    public ILocalAcceptorInspection Inspection => _acceptor ?? throw new InvalidOperationException("The node is not open.");
    public IAcceptorDigestSource Digests => _acceptor ?? throw new InvalidOperationException("The node is not open.");
    public DurabilityRepair Repair => _repair ?? throw new InvalidOperationException("The node is not open.");
    public NodeEventJournal Diagnostics { get; }
    public int ProposerId { get; }
    public IReadOnlyList<AdminPeer> Peers { get; }
    public TimeSpan HeartbeatInterval { get; }
    public int MaxEventStreams { get; }

    public static async Task<NodeRuntime> OpenAsync(
        NodeOptions options, CancellationToken cancellationToken = default, TimeProvider? timeProvider = null)
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
                options.QueueCapacity, options.MaxBatchSize, cancellationToken, runtime.Diagnostics).ConfigureAwait(false);
            runtime._admission = new QuiescingAcceptor(runtime._acceptor);
            runtime._observedAcceptor = new ObservedAcceptor(runtime._admission, runtime.Diagnostics, "inbound", null);
            runtime._ballots = new SqliteProposerBallotSource(options.ProposerDatabase, options.ProposerId);
            var peers = new List<AcceptorPeer>();
            var repairPeers = new List<RepairPeer>();
            foreach (var peer in options.Peers)
            {
                IKeyedAcceptor acceptor;
                IAcceptorDigestSource digests;
                if (peer.Id == options.AcceptorId)
                {
                    acceptor = runtime._admission;
                    digests = runtime._acceptor;
                }
                else
                {
                    var channel = GrpcChannel.ForAddress(peer.Endpoint, new GrpcChannelOptions
                    {
                        MaxReceiveMessageSize = options.MaxReceiveMessageBytes,
                        MaxSendMessageSize = options.MaxSendMessageBytes,
                    });
                    runtime._channels.Add(channel);
                    var invoker = channel.CreateCallInvoker().Intercept(new DiagnosticClientInterceptor());
                    acceptor = new GrpcKeyedAcceptor(invoker, options.PeerCallTimeout, clock);
                    digests = new GrpcAcceptorDigestSource(invoker, options.PeerCallTimeout, clock);
                    runtime._remoteSchedulers.Add(new GrpcRepairScheduler(invoker, options.PeerCallTimeout, clock));
                }
                peers.Add(new AcceptorPeer(peer.Id, new ObservedAcceptor(acceptor, runtime.Diagnostics,
                    peer.Id == options.AcceptorId ? "local" : "outbound", peer.Id)));
                repairPeers.Add(new(peer.Id, peer.Id == options.AcceptorId
                    ? new ObservedAcceptor(runtime._acceptor, runtime.Diagnostics, "repair", peer.Id)
                    : peers[^1].Acceptor, digests));
            }
            runtime._registers = new ReplicatedRegisterService(peers, runtime._ballots, options.RegisterOptions(clock));
            runtime._observedRegisters = new ObservedRegisterService(runtime._registers, runtime.Diagnostics);
            runtime._repair = new DurabilityRepair(options.AcceptorId, repairPeers, runtime._ballots, new()
            {
                TargetReplicas = options.TargetReplicationFactor,
                PageSize = options.RepairPageSize,
                OperationTimeout = options.OperationTimeout,
                AttemptTimeout = options.AttemptTimeout,
                RetryBackoff = options.RetryBackoff,
                TimeProvider = clock,
            });
            return runtime;
        }
        catch (Exception error)
        {
            try { await runtime.DisposeAsync().ConfigureAwait(false); }
            catch (Exception cleanupError) { throw new AggregateException(error, cleanupError); }
            throw;
        }
    }

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
            _staged.Writer.TryWrite(true);
        }
        Diagnostics.Publish("repair", new RepairObservation("staged", false, Repair.TargetReplicas));
    }

    public async Task StageClusterRepairAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
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
                Diagnostics.Publish("repair", new RepairObservation("running", false, Repair.TargetReplicas));
                var result = await Repair.RunPassAsync(cancellationToken: token).ConfigureAwait(false);
                ReportRepair(result, false);
                foreach (var error in result.Errors) logger.LogWarning(error, "Durability repair observed an unavailable peer or key.");
                delay = _repairInterval;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception error)
        {
            Diagnostics.Publish("repair", new RepairObservation("failed", false, Repair.TargetReplicas, Error: error.Message));
            throw;
        }
    }

    /// <summary>Seals mutation admission and establishes the target count on remote peers, until canceled.</summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _stopping = true;
            return (_shutdown ??= StopCoreAsync(cancellationToken)).WaitAsync(cancellationToken);
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownAborted.Token);
        cancellationToken = linked.Token;
        _maintenanceStopping.Cancel();
        var admitted = _admission!.QuiesceAsync();
        var registers = _registers!.DisposeAsync().AsTask();
        try
        {
            if (_periodic is not null) await _periodic.WaitAsync(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(admitted, registers).WaitAsync(cancellationToken).ConfigureAwait(false);
            while (true)
            {
                Diagnostics.Publish("repair", new RepairObservation("running", true, Repair.TargetReplicas));
                var result = await Repair.RunPassAsync(departing: true, cancellationToken).ConfigureAwait(false);
                ReportRepair(result, true);
                if (result.Pending == 0) return;
                await Task.Delay(_repairRetryInterval, _clock, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            Diagnostics.Publish("repair", new RepairObservation(
                cancellationToken.IsCancellationRequested ? "cancelled" : "failed", true, Repair.TargetReplicas, Error: error.Message));
            throw;
        }
    }

    private void ReportRepair(RepairPassResult result, bool departing) =>
        Diagnostics.Publish("repair", new RepairObservation(result.Pending == 0 ? "complete" : "pending",
            departing, Repair.TargetReplicas, result.Examined, result.Repaired, result.Pending,
            result.Errors.Count == 0 ? null : string.Join("; ", result.Errors.Select(error => error.Message))));

    public ValueTask DisposeAsync()
    {
        lock (_gate) return new ValueTask(_disposal ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        List<Exception> errors = [];
        _shutdownAborted.Cancel();
        _maintenanceStopping.Cancel();
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
        if (_registers is not null)
        {
            try { await _registers.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { errors.Add(error); }
        }
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
        if (_acceptor is not null)
        {
            try { await _acceptor.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { errors.Add(error); }
        }
        Diagnostics.Dispose();
        if (errors.Count > 0) throw new AggregateException("Node shutdown failed.", errors);
    }
}

public sealed record RepairObservation(
    string State, bool Departing, int TargetReplicas, long Examined = 0, long Repaired = 0, long Pending = 0, string? Error = null);
