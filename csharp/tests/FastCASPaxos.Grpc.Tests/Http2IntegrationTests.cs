using System.Net;
using FastCASPaxos.Host;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using Wire = FastCASPaxos.Grpc.Wire;
using DomainBallot = FastCASPaxos.Model.Ballot;

namespace FastCASPaxos.Grpc.Tests;

public sealed class Http2IntegrationTests
{
    [Fact]
    public async Task MultiEndpointClientOverHttp2_HealthyFastContentionHasOneWinner()
    {
        using var directory = new TestDirectory();
        var acceptors = new List<SqliteAcceptorServer>();
        var ballots = new List<SqliteProposerBallotSource>();
        var registers = new List<ReplicatedRegisterService>();
        var endpoints = new List<Http2Endpoint>();
        var entered = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            for (var index = 0; index < 5; index++)
                acceptors.Add(await SqliteAcceptorServer.OpenAsync(directory.Database($"member-{index}")));
            for (var index = 0; index < 3; index++)
            {
                var allocator = new SqliteProposerBallotSource(directory.Database($"proposer-{index}"), index + 1);
                ballots.Add(allocator);
                var service = new ReplicatedRegisterService(
                    acceptors.Select((acceptor, id) => new AcceptorPeer($"member-{id}", acceptor)), allocator,
                    new() { Mode = ProposerModePolicy.Parse("fast"), Capacity = 64 });
                registers.Add(service);
                var delayed = new DelegateRegister
                {
                    Read = service.ReadAsync,
                    List = service.ListKeysAsync,
                    CompareExchange = async (key, expected, value, token) =>
                    {
                        if (Interlocked.Increment(ref entered) == 32) release.TrySetResult();
                        await release.Task.WaitAsync(Watchdog);
                        return await service.CompareExchangeAsync(key, expected, value, token);
                    },
                };
                endpoints.Add(await Http2Endpoint.StartAsync(acceptors[0], delayed));
            }
            using var client = new GrpcRegisterClient(endpoints.Select(endpoint => new Uri(endpoint.Address)),
                new() { CallTimeout = Watchdog, AttemptTimeout = Watchdog });
            ByteString? version = null;
            for (var round = 0; round < 2; round++)
            {
                var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ =>
                    client.CompareExchangeAsync("contended", version, Payload).AsTask())).WaitAsync(Watchdog);
                var applied = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
                Assert.Equal(31, results.Count(result => result.Status == CompareExchangeStatus.Mismatch));
                Assert.All(results, result => Assert.Equal(applied.Current, result.Current));
                Assert.NotEqual(version, applied.Current!.Version);
                version = applied.Current.Version;
                entered = 0;
                release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            Assert.Equal(version, (await client.ReadAsync("contended")).Version);
        }
        finally
        {
            release.TrySetResult();
            foreach (var endpoint in endpoints) await endpoint.DisposeAsync();
            foreach (var register in registers) await register.DisposeAsync();
            foreach (var allocator in ballots) allocator.Dispose();
            foreach (var acceptor in acceptors) await acceptor.DisposeAsync();
        }
    }

    [Fact]
    public async Task MultiEndpointClientOverHttp2_ReadsSurvivorAndWritesThroughServerProposer()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        await using var surviving = await Http2Endpoint.StartAsync(node.Runtime.Acceptor, node.Runtime.Registers);
        var created = await node.Endpoint.Register.CompareExchangeAsync("multi", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        using var client = new GrpcRegisterClient(
            [new Uri(node.Endpoint.Address), new Uri(surviving.Address)]);
        await node.Endpoint.StopAsync();

        Assert.Equal(created.Current, await client.ReadAsync("multi"));
        var update = await client.CompareExchangeAsync("multi", created.Current!.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, update.Status);
        Assert.NotEqual(created.Current.Version, update.Current!.Version);
        Assert.Equal(ByteString.Empty, update.Current.Value);
        Assert.Equal(["multi"], (await client.ListKeysAsync()).Keys);
        Assert.Equal(update.Current, await surviving.Register.ReadAsync("multi"));
    }

    [Fact]
    public async Task MultiEndpointClientOverHttp2_EnforcesConfiguredMessageBounds()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        using (var client = new GrpcRegisterClient([new Uri(node.Endpoint.Address)],
            new() { MaxSendMessageBytes = 1 }))
        {
            var result = await client.CompareExchangeAsync("oversize", null, Payload);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Equal(StatusCode.ResourceExhausted, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
            Assert.False((await node.Endpoint.Register.ReadAsync("oversize")).IsPresent);
        }

        Assert.Equal(CompareExchangeStatus.Applied,
            (await node.Endpoint.Register.CompareExchangeAsync("present", null, Payload)).Status);
        using var bounded = new GrpcRegisterClient([new Uri(node.Endpoint.Address)],
            new() { MaxReceiveMessageBytes = 1 });
        var error = await Assert.ThrowsAsync<RemoteCallException>(() => bounded.ReadAsync("present").AsTask());
        Assert.Equal(StatusCode.ResourceExhausted, error.StatusCode);
    }

    [Fact]
    public async Task ProtocolOverHttp2_PreservesBallotsRetriesAndAbsentEmpty()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        var client = node.Endpoint.Acceptor;
        const string key = "protocol/\0\U0001F600";
        var ballot = new DomainBallot(12, 7);
        var next = new DomainBallot(19, 8);

        var absent = await client.PrepareAsync(key, new(ballot));
        Assert.True(absent.IsPromised);
        Assert.Equal(DomainBallot.Zero, absent.AcceptedBallot);
        Assert.Null(absent.AcceptedValue);
        Assert.Empty((await client.ListKeysAsync()).Entries);

        var accepted = await client.AcceptAsync(key, new(ballot, ByteString.Empty, next));
        Assert.True(accepted.IsAccepted);
        Assert.Equal(next, accepted.PromisedBallot);
        var retry = await client.AcceptAsync(key, new(ballot, ByteString.Empty, new(99, 0)));
        Assert.True(retry.IsAccepted);
        Assert.Equal(next, retry.PromisedBallot);
        var rejected = await client.PrepareAsync(key, new(ballot));
        Assert.False(rejected.IsPromised);
        Assert.Equal(next, rejected.ConflictingBallot);
        Assert.Equal(ballot, rejected.AcceptedBallot);
        Assert.Equal(ByteString.Empty, rejected.AcceptedValue);
        var conflict = await client.AcceptAsync(key, new(ballot, Payload, DomainBallot.Zero));
        Assert.False(conflict.IsAccepted);
        Assert.Equal(next, conflict.ConflictingBallot);
        var entry = Assert.Single((await client.ListKeysAsync("protocol/", limit: 1)).Entries);
        Assert.Equal(key, entry.Key);
        Assert.Equal(ballot, entry.AcceptedBallot);
        Assert.Equal(next, entry.PromisedBallot);
        Assert.Equal(ByteString.Empty, entry.AcceptedValue);

        var highest = new DomainBallot(int.MaxValue, int.MaxValue);
        Assert.True((await client.PrepareAsync("maximum", new(highest))).IsPromised);
        Assert.True((await client.AcceptAsync("maximum", new(highest, Payload, DomainBallot.Zero))).IsAccepted);
        Assert.Equal(highest, (await client.PrepareAsync("maximum", new(highest))).AcceptedBallot);
    }

    [Fact]
    public async Task RegisterOverHttp2_ConcurrentCasHasOneWinnerAndIdenticalBytesGetFreshVersion()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        var first = node.Endpoint.Register;
        using var otherChannel = GrpcChannel.ForAddress(node.Endpoint.Address);
        var second = new GrpcRegisterService(otherChannel.CreateCallInvoker());
        var absent = await first.ReadAsync("cas");
        Assert.Null(absent.Value);
        Assert.Null(absent.Version);
        var results = await Task.WhenAll(
            first.CompareExchangeAsync("cas", null, ByteString.Empty).AsTask(),
            second.CompareExchangeAsync("cas", null, ByteString.Empty).AsTask()).WaitAsync(Watchdog);
        var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
        var loser = Assert.Single(results, result => result.Status == CompareExchangeStatus.Mismatch);
        Assert.Equal(winner.Current, loser.Current);
        Assert.Equal(ByteString.Empty, winner.Current!.Value);
        Assert.NotNull(winner.Current.Version);

        var updated = await second.CompareExchangeAsync("cas", winner.Current.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.NotEqual(winner.Current.Version, updated.Current!.Version);
        Assert.Equal(ByteString.Empty, updated.Current.Value);
        Assert.Equal(updated.Current, await first.ReadAsync("cas"));
        var stale = await first.CompareExchangeAsync("cas", winner.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(updated.Current, stale.Current);
    }

    [Fact]
    public async Task ListingOverHttp2_RoundTripsUnicodeNulEmptyAndExclusiveOrdinalPages()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        string[] keys = ["", "\0", "catalog/\0", "catalog/%", "catalog/_", "catalog/\U0001F600", "catalog/\uE000", "catalog/z"];
        foreach (var key in keys)
            Assert.Equal(CompareExchangeStatus.Applied,
                (await node.Endpoint.Register.CompareExchangeAsync(key, null, ByteString.Empty)).Status);

        var first = await node.Endpoint.Register.ListKeysAsync(limit: 1);
        Assert.Equal([""], first.Keys);
        Assert.Equal("", first.ContinuationKey);
        var found = new List<string>();
        string? cursor = null;
        do
        {
            var page = await node.Endpoint.Register.ListKeysAsync(afterKey: cursor, limit: 1);
            found.AddRange(page.Keys);
            cursor = page.ContinuationKey;
        } while (cursor is not null && found.Count <= keys.Length);
        Assert.Null(cursor);
        Assert.Equal(keys.Order(StringComparer.Ordinal), found);
        var suffix = await node.Endpoint.Register.ListKeysAsync(afterKey: "");
        Assert.Equal(keys.Where(key => key.Length > 0).Order(StringComparer.Ordinal), suffix.Keys);

        var expected = keys.Where(key => key.StartsWith("catalog/", StringComparison.Ordinal)).Order(StringComparer.Ordinal).ToArray();
        var logical = await node.Endpoint.Register.ListKeysAsync("catalog/");
        Assert.Equal(expected, logical.Keys);
        Assert.Null(logical.ContinuationKey);
        var physical = await node.Endpoint.Acceptor.ListKeysAsync("catalog/", limit: 2);
        Assert.Equal(expected.Take(2), physical.Entries.Select(entry => entry.Key));
        Assert.Equal(expected[1], physical.ContinuationKey);
        var remaining = await node.Endpoint.Acceptor.ListKeysAsync("catalog/", physical.ContinuationKey);
        Assert.Equal(expected.Skip(2), remaining.Entries.Select(entry => entry.Key));
        Assert.Null(remaining.ContinuationKey);
    }

    [Fact]
    public async Task RestartOverHttp2_RetainsValuesVersionsAndDurablePromises()
    {
        using var directory = new TestDirectory();
        RegisterValue retained;
        await using (var node = await RunningNode.StartAsync(directory))
        {
            var result = await node.Endpoint.Register.CompareExchangeAsync("register", null, Payload);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            retained = result.Current!;
            Assert.True((await node.Endpoint.Acceptor.PrepareAsync("promise", new(new(100, 9)))).IsPromised);
        }
        await using (var restarted = await RunningNode.StartAsync(directory))
        {
            Assert.Equal(retained, await restarted.Endpoint.Register.ReadAsync("register"));
            var promise = await restarted.Endpoint.Acceptor.PrepareAsync("promise", new(new(1, 0)));
            Assert.False(promise.IsPromised);
            Assert.Equal(new DomainBallot(100, 9), promise.ConflictingBallot);
            Assert.Null(promise.AcceptedValue);
            var next = await restarted.Endpoint.Register.CompareExchangeAsync("register", retained.Version, Payload);
            Assert.Equal(CompareExchangeStatus.Applied, next.Status);
            Assert.NotEqual(retained.Version, next.Current!.Version);
            Assert.Equal(Payload, next.Current.Value);
        }
    }

    [Fact]
    public async Task RemotePeerQuorumOverHttp2_ContinuesWithOneOfThreeEndpointsStopped()
    {
        using var directory = new TestDirectory();
        await using var a = await RunningNode.StartAsync(directory, "a", 1);
        await using var b = await RunningNode.StartAsync(directory, "b", 2);
        await using var c = await RunningNode.StartAsync(directory, "c", 3);
        using var ballots = new SqliteProposerBallotSource(directory.Database("external-proposer"), 4);
        await using var registers = new ReplicatedRegisterService(
            [new("a", a.Endpoint.Acceptor), new("b", b.Endpoint.Acceptor), new("c", c.Endpoint.Acceptor)],
            ballots, new() { AttemptTimeout = TimeSpan.FromSeconds(1), RetryBackoff = TimeSpan.FromMilliseconds(1) });
        await using var frontend = await Http2Endpoint.StartAsync(a.Runtime.Acceptor, registers);
        Assert.Equal(CompareExchangeStatus.Applied,
            (await frontend.Register.CompareExchangeAsync("before", null, Payload)).Status);
        await c.Endpoint.StopAsync();

        Assert.Equal(CompareExchangeStatus.Applied,
            (await frontend.Register.CompareExchangeAsync("degraded", null, ByteString.Empty)).Status);
        Assert.Equal(ByteString.Empty, (await frontend.Register.ReadAsync("degraded")).Value);
        var page = await frontend.Register.ListKeysAsync();
        Assert.Equal(["before", "degraded"], page.Keys);
        Assert.Null(page.ContinuationKey);
        Assert.Single((await b.Endpoint.Acceptor.ListKeysAsync("degraded")).Entries);
    }

    [Fact]
    public async Task ValidationOverHttp2_RejectsMalformedMessagesAndPreservesTypedFailures()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory);
        var invoker = node.Endpoint.Channel.CreateCallInvoker();
        var protocol = new Wire.Acceptor.AcceptorClient(invoker);
        var register = new Wire.Register.RegisterClient(invoker);
        var missingKey = await Assert.ThrowsAsync<RpcException>(() =>
            protocol.PrepareAsync(new Wire.PrepareRequest { Ballot = new() { Round = 1 } }).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, missingKey.StatusCode);
        Assert.Equal("invalid", missingKey.Trailers.GetValue("fastcaspaxos-failure"));
        var missingBallot = await Assert.ThrowsAsync<RpcException>(() =>
            protocol.PrepareAsync(new Wire.PrepareRequest { Key = "" }).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, missingBallot.StatusCode);
        var invalidBallot = await Assert.ThrowsAsync<RpcException>(() =>
            protocol.AcceptAsync(new Wire.AcceptRequest
            {
                Key = "invalid", Ballot = new() { Round = -1 }, Value = Google.Protobuf.ByteString.Empty,
            }).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, invalidBallot.StatusCode);
        var missingValue = await Assert.ThrowsAsync<RpcException>(() =>
            register.CompareExchangeAsync(new Wire.CompareExchangeRequest { Key = "invalid" }).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, missingValue.StatusCode);
        var invalidPage = await Assert.ThrowsAsync<RpcException>(() =>
            protocol.ListKeysAsync(new Wire.ListKeysRequest { Limit = 0 }).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, invalidPage.StatusCode);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            node.Endpoint.Register.CompareExchangeAsync("invalid", ByteString.Empty, Payload).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() =>
            node.Endpoint.Register.CompareExchangeAsync("\uD800", null, Payload).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() =>
            node.Endpoint.Acceptor.ListKeysAsync(afterKey: "\uDC00").AsTask());
        Assert.Empty((await node.Endpoint.Acceptor.ListKeysAsync()).Entries);

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            node.Endpoint.Register.CompareExchangeAsync("cancelled", null, Payload, cancelled.Token).AsTask());
        Assert.False((await node.Endpoint.Register.ReadAsync("cancelled")).IsPresent);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostReplyOverHttp2_AfterDurableCommitReturnsUnknown(bool deadline)
    {
        using var directory = new TestDirectory();
        var committed = new TaskCompletionSource<CompareExchangeResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var node = await RunningNode.StartAsync(directory, decorate: service => new DelegateRegister
        {
            Read = service.ReadAsync,
            List = service.ListKeysAsync,
            CompareExchange = async (key, expected, value, token) =>
            {
                var result = await service.CompareExchangeAsync(key, expected, value, token);
                committed.TrySetResult(result);
                await release.Task;
                return result;
            },
        });
        await node.Endpoint.Register.ReadAsync("warmup");
        var client = new GrpcRegisterService(node.Endpoint.Channel.CreateCallInvoker(),
            deadline ? TimeSpan.FromSeconds(3) : TimeSpan.FromSeconds(10));
        using var cancellation = new CancellationTokenSource();
        try
        {
            var pending = client.CompareExchangeAsync("committed", null, Payload, cancellation.Token).AsTask();
            var durable = await committed.Task.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, durable.Status);
            if (!deadline) cancellation.Cancel();
            var result = await pending.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Null(result.Current);
            if (deadline)
                Assert.Equal(StatusCode.DeadlineExceeded, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
            else
                Assert.Equal(cancellation.Token, Assert.IsType<OperationCanceledException>(result.Error).CancellationToken);
            release.TrySetResult();
            Assert.Equal(durable.Current, await node.Endpoint.Register.ReadAsync("committed"));
        }
        finally { release.TrySetResult(); }
    }

    [Fact]
    public async Task UnknownOverHttp2_PreservesLaterObservationAndTypedDiagnostic()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.StartAsync(directory, decorate: service => new DelegateRegister
        {
            Read = service.ReadAsync,
            List = service.ListKeysAsync,
            CompareExchange = async (key, expected, value, token) =>
            {
                var result = await service.CompareExchangeAsync(key, expected, value, token);
                return new(CompareExchangeStatus.Unknown, result.Current, new TimeoutException("reply uncertain"));
            },
        });
        var unknown = await node.Endpoint.Register.CompareExchangeAsync("unknown", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Unknown, unknown.Status);
        Assert.NotNull(unknown.Current!.Version);
        Assert.Equal(ByteString.Empty, unknown.Current.Value);
        var error = Assert.IsType<RemoteCallException>(unknown.Error);
        Assert.Equal(StatusCode.DeadlineExceeded, error.StatusCode);
        Assert.Equal("reply uncertain", error.Message);
        Assert.Equal(unknown.Current, await node.Endpoint.Register.ReadAsync("unknown"));
    }

    private sealed class RunningNode(NodeRuntime runtime, Http2Endpoint endpoint) : IAsyncDisposable
    {
        public NodeRuntime Runtime { get; } = runtime;
        public Http2Endpoint Endpoint { get; } = endpoint;

        public static async Task<RunningNode> StartAsync(TestDirectory directory, string id = "a", int proposer = 1,
            Func<IRegisterService, IRegisterService>? decorate = null)
        {
            var runtime = await NodeRuntime.OpenAsync(new()
            {
                AcceptorId = id,
                AcceptorDatabase = directory.Database($"acceptor-{id}"),
                ProposerId = proposer,
                ProposerDatabase = directory.Database($"proposer-{id}"),
                Peers = [new() { Id = id, Endpoint = "http://127.0.0.1:1", HttpEndpoint = "http://127.0.0.1:11" }],
            });
            try
            {
                var endpoint = await Http2Endpoint.StartAsync(runtime.Acceptor,
                    decorate is null ? runtime.Registers : decorate(runtime.Registers));
                return new(runtime, endpoint);
            }
            catch { await runtime.DisposeAsync(); throw; }
        }

        public async ValueTask DisposeAsync()
        {
            try { await Endpoint.DisposeAsync(); }
            finally { await Runtime.DisposeAsync(); }
        }
    }

    private sealed class Http2Endpoint(WebApplication application, string address) : IAsyncDisposable
    {
        private bool _stopped;
        public string Address { get; } = address;
        public GrpcChannel Channel { get; } = GrpcChannel.ForAddress(address);
        public GrpcKeyedAcceptor Acceptor => new(Channel.CreateCallInvoker(), TimeSpan.FromSeconds(5));
        public GrpcRegisterService Register => new(Channel.CreateCallInvoker(), TimeSpan.FromSeconds(10));

        public static async Task<Http2Endpoint> StartAsync(IKeyedAcceptor acceptor, IRegisterService registers)
        {
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Testing" });
            builder.Configuration.Sources.Clear();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(server =>
                server.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(acceptor);
            builder.Services.AddSingleton<IAcceptorDigestSource>(acceptor as IAcceptorDigestSource ?? new DelegateDigests());
            builder.Services.AddSingleton(registers);
            var app = builder.Build();
            app.MapGrpcService<AcceptorGrpcService>();
            app.MapGrpcService<AcceptorDigestGrpcService>();
            app.MapGrpcService<RegisterGrpcService>();
            try
            {
                using var timeout = new CancellationTokenSource(Watchdog);
                await app.StartAsync(timeout.Token);
                var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!;
                return new(app, Assert.Single(addresses.Addresses));
            }
            catch { await app.DisposeAsync(); throw; }
        }

        public async Task StopAsync()
        {
            if (_stopped) return;
            _stopped = true;
            using var timeout = new CancellationTokenSource(Watchdog);
            await application.StopAsync(timeout.Token);
        }

        public async ValueTask DisposeAsync()
        {
            Channel.Dispose();
            try { await StopAsync(); }
            finally { await application.DisposeAsync(); }
        }
    }
}
