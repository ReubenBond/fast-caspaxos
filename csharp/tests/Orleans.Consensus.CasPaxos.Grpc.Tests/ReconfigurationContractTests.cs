using System.Collections.Immutable;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Sqlite;
using Xunit;
using Bytes = Orleans.Consensus.CasPaxos.Server.ByteString;
using DomainBallot = Orleans.Consensus.CasPaxos.Model.Ballot;

namespace Orleans.Consensus.CasPaxos.Grpc.Tests;

public sealed class ReconfigurationContractTests
{
    [Fact]
    public async Task SetFloorAndConfigurationQualifiedBallotsRoundTripOverGrpc()
    {
        var directory = Path.Combine(Path.GetTempPath(), "fc-grpc-reconfig-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            await using var sqlite = await SqliteAcceptorServer.OpenAsync(
                Path.Combine(directory, "acceptor.db"), ContractTestSupport.FatalErrors);
            var acceptorService = ContractTestSupport.Service(sqlite);
            var digestService = new AcceptorDigestGrpcService(sqlite, sqlite, NullLogger<AcceptorDigestGrpcService>.Instance);
            var client = new GrpcKeyedAcceptor(new ContractCallInvoker(acceptor: acceptorService, digests: digestService));

            var payload = new Bytes("value-c1"u8);
            Assert.True((await client.AcceptAsync("k", new(DomainBallot.InitialFast(), payload, new DomainBallot(1, 2, 0)))).IsAccepted);

            // SetFloor RPC raises floor monotonically and rejects non-floor ballots.
            Assert.Equal(DomainBallot.Floor(2), await client.SetFloorAsync(DomainBallot.Floor(2)));
            Assert.Equal(DomainBallot.Floor(2), await client.SetFloorAsync(DomainBallot.Floor(1)));
            var invalid = await Assert.ThrowsAsync<RpcException>(() =>
                acceptorService.SetFloor(new Wire.SetFloorRequest { Floor = ContractTestSupport.Ballot(1, 0, 2) },
                    new TestServerCallContext(new CallOptions())));
            Assert.Equal(StatusCode.InvalidArgument, invalid.StatusCode);

            // Prepare & Accept rejections carry floor ballot (2, 0, 0) cleanly over the wire.
            var prepReject = await client.PrepareAsync("k", new(new DomainBallot(1, 4, 1)));
            Assert.False(prepReject.IsPromised);
            Assert.Equal(DomainBallot.Floor(2), prepReject.ConflictingBallot);
            Assert.Equal(payload, prepReject.AcceptedValue);

            var accReject = await client.AcceptAsync("k", new(new DomainBallot(1, 2, 0), new Bytes("other"u8), DomainBallot.Zero));
            Assert.False(accReject.IsAccepted);
            Assert.Equal(DomainBallot.Floor(2), accReject.ConflictingBallot);

            // Exact-duplicate replay below floor echoes floor over gRPC.
            var dup = await client.AcceptAsync("k", new(DomainBallot.InitialFast(), payload, new DomainBallot(1, 2, 0)));
            Assert.True(dup.IsAccepted);
            Assert.Equal(DomainBallot.Floor(2), dup.PromisedBallot);

            // Ballot-free Read and ListKeys work after floor is raised and return effPromised = (2, 0, 0).
            var read = ContractTestSupport.ReadState(await client.ReadAsync("k"));
            Assert.Equal(DomainBallot.Floor(2), read.PromisedBallot);
            Assert.Equal(payload, read.AcceptedValue);

            var listed = Assert.Single((await client.ListKeysAsync()).Entries);
            Assert.Equal(DomainBallot.Floor(2), listed.PromisedBallot);

            // Config 2 prepare & accept succeed over gRPC.
            var prep2 = await client.PrepareAsync("k", new(DomainBallot.InitialClassic(2, 1)));
            Assert.True(prep2.IsPromised);
            var acc2 = await client.AcceptAsync("k", new(DomainBallot.InitialClassic(2, 1), new Bytes("value-c2"u8), new DomainBallot(2, 2, 0)));
            Assert.True(acc2.IsAccepted);
            Assert.Equal(new DomainBallot(2, 2, 0), acc2.PromisedBallot);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task EndToEndReconfigurationOverSqliteAndGrpcReplacesVoter()
    {
        var directory = Path.Combine(Path.GetTempPath(), "fc-e2e-reconfig-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var servers = new Dictionary<string, SqliteAcceptorServer>(StringComparer.Ordinal);
        var clients = new Dictionary<string, IKeyedAcceptor>(StringComparer.Ordinal);
        try
        {
            foreach (var (id, joinCfg) in new[] { ("v1", 1L), ("v2", 1L), ("v3", 1L), ("v4", 2L) })
            {
                var dbPath = Path.Combine(directory, $"{id}.db");
                var sqlite = await SqliteAcceptorServer.OpenAsync(dbPath, ContractTestSupport.FatalErrors);
                await SqliteVoterRegistry.OpenAsync(sqlite, id, joinConfiguration: joinCfg);
                servers[id] = sqlite;
                var invoker = new ContractCallInvoker(
                    acceptor: ContractTestSupport.Service(sqlite),
                    digests: new AcceptorDigestGrpcService(sqlite, sqlite, NullLogger<AcceptorDigestGrpcService>.Instance));
                clients[id] = new GrpcKeyedAcceptor(invoker);
            }

            IKeyedAcceptor Resolve(VoterDescriptor d) => clients[d.Id];
            ImmutableArray<VoterDescriptor> c1 = [new("v1"), new("v2"), new("v3")];
            ImmutableArray<VoterDescriptor> c2 = [new("v1"), new("v2"), new("v4")];

            using var clientBallots = new SqliteProposerBallotSource(Path.Combine(directory, "p1.db"), 1, ContractTestSupport.FatalErrors);
            using var opBallots = new SqliteProposerBallotSource(Path.Combine(directory, "p2.db"), 2, ContractTestSupport.FatalErrors);

            await using var register = new ReplicatedRegisterService(
                [.. c1.Select(v => new AcceptorPeer(v.Id, clients[v.Id]))],
                clientBallots,
                new RegisterServiceOptions
                {
                    PeerResolver = Resolve,
                    OperationTimeout = TimeSpan.FromSeconds(10),
                    AttemptTimeout = TimeSpan.FromSeconds(1),
                    RetryBackoff = TimeSpan.FromMilliseconds(5),
                });

            var created = await register.CompareExchangeAsync("doc", null, new Bytes("hello"u8));
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);

            var driver = new VoterReconfigurationDriver(opBallots, Resolve);
            var active = await driver.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active.Number);
            Assert.Equal(VoterConfigurationState.Active, active.State);

            // Stale gRPC register client automatically refreshes to c = 2 and updates with preserved version.
            var updated = await register.CompareExchangeAsync("doc", created.Current!.Version, new Bytes("world"u8));
            Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
            Assert.Equal(2, register.ActiveConfiguration);
            Assert.Equal(new Bytes("world"u8), (await register.ReadAsync("doc")).Value);
        }
        finally
        {
            foreach (var server in servers.Values) await server.DisposeAsync();
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }
}
