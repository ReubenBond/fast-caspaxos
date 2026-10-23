using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Core;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

// File-backed end-to-end domain/protobuf composition, deliberately no HTTP transport.
internal sealed class SqliteContractCluster(TestDirectory directory) : IAsyncDisposable
{
    private readonly List<SqliteAcceptorServer> _sqlite = [];
    private readonly List<ReplicatedRegisterService> _registers = [];
    private readonly List<SqliteProposerBallotSource> _ballots = [];
    public List<DelegateAcceptor> Endpoints { get; } = [];
    public List<GrpcKeyedAcceptor> Clients { get; } = [];

    public static async Task<SqliteContractCluster> OpenAsync(TestDirectory directory)
    {
        var cluster = new SqliteContractCluster(directory);
        try
        {
            for (var index = 0; index < 3; index++)
            {
                var sqlite = await SqliteAcceptorServer.OpenAsync(directory.Database($"acceptor-{index}"))
                    .AsTask().WaitAsync(Watchdog);
                cluster._sqlite.Add(sqlite);
                var endpoint = new DelegateAcceptor
                {
                    Prepare = sqlite.PrepareAsync,
                    Accept = sqlite.AcceptAsync,
                    List = sqlite.ListKeysAsync,
                };
                cluster.Endpoints.Add(endpoint);
                cluster.Clients.Add(new(new ContractCallInvoker(Service(endpoint))));
            }
            return cluster;
        }
        catch { await cluster.DisposeAsync(); throw; }
    }

    public GrpcRegisterService CreateRegister(int proposer, string mode = "classic", int? offlinePeer = null)
    {
        var ballots = new SqliteProposerBallotSource(directory.Database($"proposer-{proposer}"), proposer);
        _ballots.Add(ballots);
        var offline = new GrpcKeyedAcceptor(StubCallInvoker.Fail(
            new RpcException(new Status(StatusCode.Unavailable, "Peer is offline."))));
        var routes = Clients.Select((client, index) =>
            new AcceptorPeer($"peer-{index}", index == offlinePeer ? offline : client));
        var service = new ReplicatedRegisterService(routes, ballots, new()
        {
            Mode = ProposerModePolicy.Parse(mode),
            OperationTimeout = TimeSpan.FromSeconds(30),
            AttemptTimeout = TimeSpan.FromSeconds(10),
            RetryBackoff = TimeSpan.FromMilliseconds(1),
        });
        _registers.Add(service);
        return new(new ContractCallInvoker(register: Service(service)));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var service in _registers) await service.DisposeAsync().AsTask().WaitAsync(Watchdog);
        foreach (var ballots in _ballots) ballots.Dispose();
        foreach (var sqlite in _sqlite) await sqlite.DisposeAsync().AsTask().WaitAsync(Watchdog);
    }
}
