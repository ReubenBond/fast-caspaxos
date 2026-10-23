using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;

namespace FastCASPaxos.Host;

/// <summary>Owns one durable acceptor, one proposer allocator, and its fixed-peer register service.</summary>
public sealed class NodeRuntime : IAsyncDisposable
{
    private readonly List<GrpcChannel> _channels = [];
    private SqliteProposerBallotSource? _ballots;
    private SqliteAcceptorServer? _acceptor;
    private ReplicatedRegisterService? _registers;
    private readonly object _gate = new();
    private Task? _disposal;

    private NodeRuntime() { }

    public IKeyedAcceptor Acceptor => _acceptor ?? throw new InvalidOperationException("The node is not open.");
    public IRegisterService Registers => _registers ?? throw new InvalidOperationException("The node is not open.");

    public static async Task<NodeRuntime> OpenAsync(NodeOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        var runtime = new NodeRuntime();
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(options.AcceptorDatabase))!);
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(options.ProposerDatabase))!);
            runtime._acceptor = await SqliteAcceptorServer.OpenAsync(options.AcceptorDatabase,
                options.QueueCapacity, options.MaxBatchSize, cancellationToken).ConfigureAwait(false);
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
                    acceptor = new GrpcKeyedAcceptor(channel.CreateCallInvoker(), options.PeerCallTimeout, TimeProvider.System);
                }
                peers.Add(new AcceptorPeer(peer.Id, acceptor));
            }
            runtime._registers = new ReplicatedRegisterService(peers, runtime._ballots, options.RegisterOptions());
            return runtime;
        }
        catch (Exception error)
        {
            try { await runtime.DisposeAsync().ConfigureAwait(false); }
            catch (Exception cleanupError) { throw new AggregateException(error, cleanupError); }
            throw;
        }
    }

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
        if (errors.Count > 0) throw new AggregateException("Node shutdown failed.", errors);
    }
}
