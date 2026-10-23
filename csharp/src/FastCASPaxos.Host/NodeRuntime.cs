using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;
using Grpc.Core.Interceptors;

namespace FastCASPaxos.Host;

/// <summary>Owns one durable acceptor, one proposer allocator, and its fixed-peer register service.</summary>
public sealed class NodeRuntime : IAsyncDisposable
{
    private readonly List<GrpcChannel> _channels = [];
    private SqliteProposerBallotSource? _ballots;
    private SqliteAcceptorServer? _acceptor;
    private ReplicatedRegisterService? _registers;
    private IKeyedAcceptor? _observedAcceptor;
    private IRegisterService? _observedRegisters;
    private readonly object _gate = new();
    private Task? _disposal;

    private int _eventStreams;
    private NodeRuntime(NodeOptions options, TimeProvider timeProvider)
    {
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
            runtime._observedAcceptor = new ObservedAcceptor(runtime._acceptor, runtime.Diagnostics, "inbound", null);
            runtime._ballots = new SqliteProposerBallotSource(options.ProposerDatabase, options.ProposerId);
            var peers = new List<AcceptorPeer>();
            foreach (var peer in options.Peers)
            {
                IKeyedAcceptor acceptor;
                if (peer.Id == options.AcceptorId) acceptor = runtime._acceptor;
                else
                {
                    var channel = GrpcChannel.ForAddress(peer.Endpoint, new GrpcChannelOptions
                    {
                        MaxReceiveMessageSize = options.MaxReceiveMessageBytes,
                        MaxSendMessageSize = options.MaxSendMessageBytes,
                    });
                    runtime._channels.Add(channel);
                    acceptor = new GrpcKeyedAcceptor(channel.CreateCallInvoker().Intercept(new DiagnosticClientInterceptor()),
                        options.PeerCallTimeout, clock);
                }
                peers.Add(new AcceptorPeer(peer.Id, new ObservedAcceptor(acceptor, runtime.Diagnostics,
                    peer.Id == options.AcceptorId ? "local" : "outbound", peer.Id)));
            }
            runtime._registers = new ReplicatedRegisterService(peers, runtime._ballots, options.RegisterOptions(clock));
            runtime._observedRegisters = new ObservedRegisterService(runtime._registers, runtime.Diagnostics);
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

    public ValueTask DisposeAsync()
    {
        lock (_gate) return new ValueTask(_disposal ??= DisposeCoreAsync());
    }

    private async Task DisposeCoreAsync()
    {
        List<Exception> errors = [];
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
