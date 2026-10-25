using System.Net;
using FastCASPaxos.Host;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

// Uses real HTTP/2 only for the lifecycle/remote-replica boundary.
// Keeps the existing REST, diagnostics, and launcher test harnesses untouched.
internal sealed class RepairEndpoint(WebApplication app, string address) : IAsyncDisposable
{
    public WebApplication App { get; } = app;
    public string Address { get; } = address;
    public GrpcChannel Channel { get; } = GrpcChannel.ForAddress(address);
    public GrpcAcceptorDigestSource Digests => new(Channel.CreateCallInvoker());

    public static async Task<RepairEndpoint> StartAsync(
        IKeyedAcceptor acceptor, IAcceptorDigestSource digests, IRepairScheduler scheduler, NodeRuntime? node = null,
        IRegisterLearner? learner = null, Func<NodeRuntime>? epochNode = null,
        Func<object, ServerCallContext, Task>? beforeLearning = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.Configuration.Sources.Clear();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(server =>
            server.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc(options =>
        {
            if (epochNode is not null)
            {
                options.Interceptors.Add<DiagnosticServerInterceptor>();
                options.Interceptors.Add<EpochServerInterceptor>();
            }
            if (beforeLearning is not null) options.Interceptors.Add<LearningHookInterceptor>();
        });
        if (epochNode is not null) builder.Services.AddSingleton(_ => new EpochServerInterceptor(epochNode()));
        if (beforeLearning is not null) builder.Services.AddSingleton(new LearningHookInterceptor(beforeLearning));
        builder.Services.AddSingleton(acceptor);
        builder.Services.AddSingleton(digests);
        builder.Services.AddSingleton(scheduler);
        if (learner is not null) builder.Services.AddSingleton(learner);
        if (node is not null)
        {
            builder.Services.AddSingleton(node);
            builder.Services.AddHostedService<NodeRepairService>();
        }

        var app = builder.Build();
        app.MapGrpcService<AcceptorGrpcService>();
        app.MapGrpcService<AcceptorDigestGrpcService>();
        app.MapGrpcService<RepairGrpcService>();
        if (learner is not null) app.MapGrpcService<RegisterLearnerGrpcService>();
        try
        {
            await app.StartAsync().WaitAsync(Watchdog);
            var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!;
            return new(app, Assert.Single(addresses.Addresses));
        }
        catch { await app.DisposeAsync(); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        // Tests explicitly exercise StopAsync. Cleanup must not initiate another replication barrier.
        await App.DisposeAsync().AsTask().WaitAsync(Watchdog);
    }
}

internal sealed class LearningHookInterceptor(Func<object, ServerCallContext, Task> beforeLearning) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        if (request is Wire.RegisterCommit or Wire.RegisterCommitDigest)
            await beforeLearning(request, context);
        return await continuation(request, context);
    }
}

internal sealed class RepairHostCluster : IAsyncDisposable
{
    public SqliteAcceptorServer[] Stores { get; } = new SqliteAcceptorServer[2];
    public DelegateAcceptor[] RemoteAcceptors { get; } = new DelegateAcceptor[2];
    public CountingRepairScheduler[] Schedulers { get; } = [new(), new()];
    public RepairEndpoint[] Endpoints { get; } = new RepairEndpoint[2];
    public NodeRuntime Node { get; private set; } = null!;
    public NodeOptions Options { get; private set; } = null!;
    public RepairClock Clock { get; } = new();

    public static async Task<RepairHostCluster> OpenAsync(
        TestDirectory directory, int target = 0, NodeShutdownMode shutdownMode = NodeShutdownMode.Departure)
    {
        var cluster = new RepairHostCluster();
        try
        {
            for (var index = 0; index < 2; index++)
            {
                var store = cluster.Stores[index] = await SqliteAcceptorServer.OpenAsync(directory.Database($"remote-{index}"));
                var acceptor = cluster.RemoteAcceptors[index] = new DelegateAcceptor
                {
                    Read = store.ReadAsync,
                    Prepare = store.PrepareAsync,
                    Accept = store.AcceptAsync,
                    List = store.ListKeysAsync
                };
                cluster.Endpoints[index] = await RepairEndpoint.StartAsync(acceptor: acceptor, digests: store,
                    scheduler: cluster.Schedulers[index]);
            }
            cluster.Options = DiagnosticHost.Options(directory);
            cluster.Options.Peers.AddRange(cluster.Endpoints.Select((endpoint, index) => new PeerOptions
            {
                Id = index == 0 ? "b" : "c",
                Endpoint = endpoint.Address,
                HttpEndpoint = $"http://127.0.0.1:{12 + index}"
            }));
            cluster.Options.TargetReplicationFactor = target;
            cluster.Options.ShutdownMode = shutdownMode;
            cluster.Options.RepairPageSize = 2;
            cluster.Node = await NodeRuntime.OpenAsync(cluster.Options, timeProvider: cluster.Clock).WaitAsync(Watchdog);
            return cluster;
        }
        catch { await cluster.DisposeAsync(); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        if (Node is not null) await Node.DisposeAsync().AsTask().WaitAsync(Watchdog);
        foreach (var endpoint in Endpoints)
            if (endpoint is not null) await endpoint.DisposeAsync();
        foreach (var store in Stores)
            if (store is not null) await store.DisposeAsync().AsTask().WaitAsync(Watchdog);
    }
}

internal sealed class RepairClock : TimeProvider
{
    private readonly DiagnosticClock _inner = new();
    // HTTP/2 interprets absolute UTC deadlines using the machine clock; retain manual durations
    // without sending DiagnosticClock's UnixEpoch deadlines to a real transport.
    private readonly DateTimeOffset _origin = DateTimeOffset.UtcNow;
    public override long TimestampFrequency => _inner.TimestampFrequency;
    public override long GetTimestamp() => _inner.GetTimestamp();
    public override DateTimeOffset GetUtcNow() => _origin.AddTicks(GetTimestamp());
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        _inner.CreateTimer(callback, state, dueTime, period);
    public void Advance(TimeSpan duration) => _inner.Advance(duration);
    public async Task WaitForTimerAsync(TimeSpan duration)
    {
        for (var index = 0; index < 100; index++)
            if (await _inner.TimerCreated.Reader.ReadAsync().AsTask().WaitAsync(Watchdog) == duration) return;
        throw new InvalidOperationException("Expected repair timer was not scheduled in the bounded trace.");
    }
}
