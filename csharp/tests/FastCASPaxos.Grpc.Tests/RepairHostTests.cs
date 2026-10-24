using System.Net;
using System.Text.Json;
using FastCASPaxos.Host;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class RepairHostTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepairStatusApi_ReportsExactSchemaAndDistinguishesCompletedFromCancelledDrain(bool cancel)
    {
        using var directory = new TestDirectory();
        var clock = new RepairClock();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory), clock);
        using (var response = await host.Client.GetAsync("/v1/admin/repair"))
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.True(response.Headers.CacheControl!.NoStore);
            var initial = await DiagnosticHost.Parse(response);
            Assert.Equal(new[] { "bootId", "draining", "lastRepair", "nodeId", "schemaVersion", "shutdownComplete", "targetReplicas" },
                initial.EnumerateObject().Select(property => property.Name).Order(StringComparer.Ordinal));
            Assert.Equal(1, initial.GetProperty("schemaVersion").GetInt32());
            Assert.Equal("a", initial.GetProperty("nodeId").GetString());
            Assert.Equal(host.Node.Diagnostics.BootId, initial.GetProperty("bootId").GetString());
            Assert.Equal(1, initial.GetProperty("targetReplicas").GetInt32());
            Assert.False(initial.GetProperty("draining").GetBoolean());
            Assert.False(initial.GetProperty("shutdownComplete").GetBoolean());
            Assert.Equal(JsonValueKind.Null, initial.GetProperty("lastRepair").ValueKind);
        }
        using (var staged = await host.Client.PostAsync("/v1/admin/repair", null))
            Assert.Equal(HttpStatusCode.Accepted, staged.StatusCode);
        var stagedState = host.Node.LastRepair!;
        Assert.Equal("staged", stagedState.State);
        Assert.False(stagedState.Departing);
        Assert.Equal(clock.GetUtcNow(), stagedState.Timestamp);
        clock.Advance(TimeSpan.FromSeconds(1));
        if (cancel) await host.Node.Acceptor.AcceptAsync("retain", new(new(1, 0), Payload, new()));
        using var cancellation = new CancellationTokenSource();
        var cursor = host.Node.Diagnostics.Cursor;
        var stopped = host.Node.StopAsync(cancellation.Token);
        if (cancel)
        {
            // One-member fixed membership cannot establish even one remote copy.
            await clock.WaitForTimerAsync(TimeSpan.FromMilliseconds(20));
            var draining = await host.Json("/v1/admin/repair");
            Assert.True(draining.GetProperty("draining").GetBoolean());
            Assert.False(draining.GetProperty("shutdownComplete").GetBoolean());
            Assert.Equal("running", draining.GetProperty("lastRepair").GetProperty("state").GetString());
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stopped.WaitAsync(Watchdog));
            await NextRepairAsync(host.Node, cursor, "cancelled", departing: true);
        }
        else await stopped.WaitAsync(Watchdog);

        var final = await host.Json("/v1/admin/repair");
        Assert.True(final.GetProperty("draining").GetBoolean());
        Assert.Equal(!cancel, final.GetProperty("shutdownComplete").GetBoolean());
        Assert.Equal(!cancel, host.Node.IsShutdownComplete);
        var last = final.GetProperty("lastRepair");
        Assert.Equal(cancel ? "cancelled" : "complete", last.GetProperty("state").GetString());
        Assert.True(last.GetProperty("departing").GetBoolean());
        Assert.Equal(1, last.GetProperty("targetReplicas").GetInt32());
        Assert.Equal(clock.GetUtcNow(), last.GetProperty("timestamp").GetDateTimeOffset());
        Assert.Equal("staged", stagedState.State); // Prior observations are immutable snapshots.
        Assert.True(stagedState.Timestamp < host.Node.LastRepair!.Timestamp);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepairWorkers_UnavailableLocalInventory_RetryAtConfiguredIntervalUntilCancelled(bool departing)
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        var clock = new RepairClock();
        var node = await NodeRuntime.OpenAsync(options, timeProvider: clock);
        using var service = new NodeRepairService(node, NullLogger<NodeRepairService>.Instance);
        using var cancellation = new CancellationTokenSource();
        var unavailable = new RegisterUnavailableException(new IOException("injected inventory unavailability"));
        var store = Assert.IsType<SqliteAcceptorServer>(node.Inspection);
        store.BeforeCommit = (_, _, _) => throw unavailable;
        Task work = Task.CompletedTask;
        try
        {
            var cursor = node.Diagnostics.Cursor;
            if (departing) work = node.StopAsync(cancellation.Token);
            else
            {
                await service.StartAsync(CancellationToken.None);
                // Consume the initial one-member timer before staging: it has the same
                // duration as subsequent intervals and must not be mistaken for a retry.
                await clock.WaitForTimerAsync(options.RepairInterval);
                node.StageRepair();
                work = service.ExecuteTask!;
            }
            var first = await NextRepairAsync(node, cursor, "failed", departing);
            Assert.False(work.IsCompleted);
            var interval = departing ? options.RepairRetryInterval : options.RepairInterval;
            await clock.WaitForTimerAsync(interval);
            clock.Advance(interval);
            var second = await NextRepairAsync(node, first.Id, "failed", departing);
            Assert.Equal(interval, second.Timestamp - first.Timestamp);
            Assert.False(work.IsCompleted);
            Assert.False(node.IsShutdownComplete);
            Assert.Equal(departing, node.IsDraining);
            if (departing)
            {
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => work.WaitAsync(Watchdog));
            }
            else await service.StopAsync(CancellationToken.None).WaitAsync(Watchdog);
        }
        finally
        {
            cancellation.Cancel();
            await service.StopAsync(CancellationToken.None).WaitAsync(Watchdog);
            // The injected writer failure retires this fixture's store. Observe that known
            // cleanup failure rather than pretending the damaged fixture can resume storage.
            var cleanup = await Assert.ThrowsAsync<AggregateException>(() => node.DisposeAsync().AsTask().WaitAsync(Watchdog));
            Assert.Same(unavailable, Assert.Single(cleanup.InnerExceptions));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StopAsync_RemoteOnlyTarget_BarrierCompletesBeforeHostedTransportStops(bool hosted)
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        var node = cluster.Node;
        await node.Acceptor.AcceptAsync("local", new(new(1, 0), Payload, new()));
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstRemoteCommitted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.RemoteAcceptors[0].Accept = async (key, request, token) =>
        {
            var result = await cluster.Stores[0].AcceptAsync(key, request, token);
            firstRemoteCommitted.TrySetResult(result.IsAccepted);
            return result;
        };
        cluster.RemoteAcceptors[1].Accept = async (key, request, _) =>
        {
            arrived.TrySetResult();
            await release.Task;
            return await cluster.Stores[1].AcceptAsync(key, request);
        };
        await using var endpoint = await RepairEndpoint.StartAsync(node.Acceptor, node.Digests, node, hosted ? node : null);
        using var watchdog = new CancellationTokenSource(Watchdog);
        try
        {
            var stopping = hosted ? endpoint.App.StopAsync(watchdog.Token) : node.StopAsync(watchdog.Token);
            await arrived.Task.WaitAsync(Watchdog);
            // Independent HTTP/2 requests can arrive in either order. The blocked second
            // remote proves nothing about the first remote's durable acceptance.
            Assert.True(await firstRemoteCommitted.Task.WaitAsync(Watchdog));
            Assert.False(stopping.IsCompleted);
            Assert.Equal(2, node.TargetReplicas);
            Assert.True(node.IsDraining);
            Assert.False(node.IsShutdownComplete);
            Assert.Equal("running", node.LastRepair!.State);
            Assert.True(node.LastRepair.Departing);
            var surviving = await cluster.Stores[0].ReadAsync("local", null);
            Assert.Equal(Payload, ReadState(surviving).AcceptedValue);
            Assert.True(await cluster.Stores[1].ReadAsync("local") is AcceptorMissing);
            // This real HTTP/2 read must succeed while IHostedLifecycleService.StoppingAsync
            // is blocked. Calling node.StopAsync only after Kestrel.StopAsync would fail here.
            Assert.Equal("local", Assert.Single((await endpoint.Digests.ListDigestsAsync()).Entries).Key);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() =>
                node.Acceptor.AcceptAsync("late", new(new(2, 1), Payload, new())).AsTask());
            await Assert.ThrowsAsync<RegisterUnavailableException>(() =>
                node.Acceptor.PrepareAsync("late", new(new(2, 1))).AsTask());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => node.Registers.ReadAsync("local").AsTask());

            release.TrySetResult();
            await stopping.WaitAsync(Watchdog);
            Assert.True(node.IsDraining);
            Assert.True(node.IsShutdownComplete);
            Assert.Equal("complete", node.LastRepair!.State);
            Assert.Equal(0, node.LastRepair.Pending);
            Assert.Equal(cluster.Clock.GetUtcNow(), node.LastRepair.Timestamp);
            foreach (var store in cluster.Stores)
                Assert.Equal(Payload, ReadState(await store.ReadAsync("local")).AcceptedValue);
            Assert.Null(await node.Inspection.InspectAsync("late"));
            if (hosted)
                Assert.Single(endpoint.App.Services.GetServices<IHostedService>().OfType<NodeRepairService>());
        }
        finally { release.TrySetResult(); }
    }

    [Fact]
    public async Task StopAsync_InFlightDurableAdmission_DrainsBeforeFrozenInventory()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        var store = Assert.IsType<SqliteAcceptorServer>(cluster.Node.Inspection);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var commits = 0;
        store.BeforeCommit = (_, _, _) =>
        {
            if (Interlocked.Increment(ref commits) != 1) return;
            entered.TrySetResult();
            if (!release.Wait(Watchdog)) throw new TimeoutException("Test did not release the admitted commit.");
        };
        using var watchdog = new CancellationTokenSource(Watchdog);
        try
        {
            var accepted = cluster.Node.Acceptor.AcceptAsync("in-flight", new(new(1, 0), Payload, new())).AsTask();
            await entered.Task.WaitAsync(Watchdog);
            var stopped = cluster.Node.StopAsync(watchdog.Token);
            Assert.True(cluster.Node.IsDraining);
            Assert.False(cluster.Node.IsShutdownComplete);
            Assert.Equal("draining", cluster.Node.LastRepair!.State);
            Assert.False(stopped.IsCompleted);
            Assert.False(accepted.IsCompleted);
            Assert.All(cluster.RemoteAcceptors, peer => Assert.Equal(0, Volatile.Read(ref peer.Calls)));
            await Assert.ThrowsAsync<RegisterUnavailableException>(() =>
                cluster.Node.Acceptor.AcceptAsync("too-late", new(new(1, 0), Payload, new())).AsTask());

            release.Set();
            Assert.True((await accepted.WaitAsync(Watchdog)).IsAccepted);
            await stopped.WaitAsync(Watchdog);
            Assert.True(cluster.Node.IsShutdownComplete);
            foreach (var remote in cluster.Stores)
            {
                Assert.Equal(Payload, ReadState(await remote.ReadAsync("in-flight")).AcceptedValue);
                Assert.True(await remote.ReadAsync("too-late") is AcceptorMissing);
            }
        }
        finally { release.Set(); store.BeforeCommit = null; }
    }

    [Fact]
    public async Task StopAsync_CancellationDuringAdmissionDrain_ReturnedTaskCutsOffWithoutLosingAcceptedWrite()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        var store = Assert.IsType<SqliteAcceptorServer>(cluster.Node.Inspection);
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.BeforeCommit = (_, _, _) =>
        {
            entered.TrySetResult();
            if (!release.Wait(Watchdog)) throw new TimeoutException("Test did not release the admitted commit.");
        };
        try
        {
            var accepted = cluster.Node.Acceptor.AcceptAsync("in-flight", new(new(1, 0), Payload, new())).AsTask();
            await entered.Task.WaitAsync(Watchdog);
            var stopped = cluster.Node.StopAsync(cancellation.Token);
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stopped.WaitAsync(Watchdog));
            Assert.True(cluster.Node.IsDraining);
            Assert.False(cluster.Node.IsShutdownComplete);
            Assert.False(accepted.IsCompleted);
            Assert.False(release.IsSet);
            Assert.Equal(0, cluster.Clock.GetTimestamp());
            Assert.All(cluster.RemoteAcceptors, peer => Assert.Equal(0, Volatile.Read(ref peer.Calls)));
            store.BeforeCommit = null;
            release.Set();
            Assert.True((await accepted.WaitAsync(Watchdog)).IsAccepted);
            Assert.Equal(Payload, ReadState(await store.ReadAsync("in-flight")).AcceptedValue);
        }
        finally { release.Set(); store.BeforeCommit = null; }
    }

    [Fact]
    public async Task StopAsync_CancellationDuringReplication_DoesNotWaitForRemoteAcknowledgement()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        await cluster.Node.Acceptor.AcceptAsync("local", new(new(1, 0), Payload, new()));
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        cluster.RemoteAcceptors[1].Accept = async (key, request, _) =>
        {
            arrived.TrySetResult();
            try
            {
                await release.Task; // Lost reply / remote work can outlive StopAsync's token.
                return await cluster.Stores[1].AcceptAsync(key, request);
            }
            finally { returned.TrySetResult(); }
        };
        try
        {
            var stopped = cluster.Node.StopAsync(cancellation.Token);
            await arrived.Task.WaitAsync(Watchdog);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stopped.WaitAsync(Watchdog));
            Assert.True(cluster.Node.IsDraining);
            Assert.False(cluster.Node.IsShutdownComplete);
            Assert.False(release.Task.IsCompleted);
            Assert.Equal(0, cluster.Clock.GetTimestamp());
            Assert.Equal(Payload, ReadState(await cluster.Node.Acceptor.ReadAsync("local")).AcceptedValue);
            Assert.Throws<RegisterUnavailableException>(cluster.Node.StageRepair);
        }
        finally
        {
            release.TrySetResult();
            await returned.Task.WaitAsync(Watchdog);
        }
    }

    [Fact]
    public async Task PeriodicRepair_UsesStaggeredInitialTimerThenFullInterval()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        await cluster.Node.Acceptor.AcceptAsync("local", new(new(1, 0), Payload, new()));
        var before = cluster.Node.Diagnostics.Cursor;
        await using var endpoint = await RepairEndpoint.StartAsync(
            cluster.Node.Acceptor, cluster.Node.Digests, cluster.Node, cluster.Node);
        var initial = TimeSpan.FromSeconds(20); // First of three ordinal member IDs, one-minute interval.
        await cluster.Clock.WaitForTimerAsync(initial);
        Assert.All(cluster.RemoteAcceptors, peer => Assert.Equal(0, Volatile.Read(ref peer.Calls)));
        cluster.Clock.Advance(initial);
        var first = await NextCompleteAsync(cluster.Node, before);
        Assert.Equal(1, Assert.IsType<RepairObservation>(first.Data).Repaired);
        foreach (var store in cluster.Stores)
            Assert.Equal(Payload, ReadState(await store.ReadAsync("local")).AcceptedValue);

        await cluster.Clock.WaitForTimerAsync(cluster.Options.RepairInterval);
        var calls = cluster.RemoteAcceptors.Select(peer => Volatile.Read(ref peer.Calls)).ToArray();
        cluster.Clock.Advance(cluster.Options.RepairInterval);
        var second = await NextCompleteAsync(cluster.Node, first.Id);
        var observation = Assert.IsType<RepairObservation>(second.Data);
        Assert.Equal((1L, 0L, 0L), (observation.Examined, observation.Repaired, observation.Pending));
        Assert.Equal(calls, cluster.RemoteAcceptors.Select(peer => Volatile.Read(ref peer.Calls)));
    }

    [Fact]
    public async Task StageClusterRepair_CallsEveryRemoteAndCoalescesLocalRequestsBeforePeriodicStart()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        await cluster.Node.Acceptor.AcceptAsync("local", new(new(1, 0), Payload, new()));
        var before = cluster.Node.Diagnostics.Cursor;
        await cluster.Node.StageClusterRepairAsync().WaitAsync(Watchdog);
        cluster.Node.StageRepair();
        cluster.Node.StageRepair();
        Assert.All(cluster.Schedulers, scheduler => Assert.Equal(1, Volatile.Read(ref scheduler.Calls)));

        await using var endpoint = await RepairEndpoint.StartAsync(
            cluster.Node.Acceptor, cluster.Node.Digests, cluster.Node, cluster.Node);
        var completed = await NextCompleteAsync(cluster.Node, before);
        Assert.Equal(1, Assert.IsType<RepairObservation>(completed.Data).Repaired);
        Assert.Equal(0, cluster.Clock.GetTimestamp()); // No interval advance was needed for the staged pass.
        await cluster.Clock.WaitForTimerAsync(cluster.Options.RepairInterval);
        // Exactly one prepare/accept per remote: requests queued before the worker starts coalesce.
        Assert.All(cluster.RemoteAcceptors, peer => Assert.Equal(2, Volatile.Read(ref peer.Calls)));
        foreach (var store in cluster.Stores)
            Assert.Equal(Payload, ReadState(await store.ReadAsync("local")).AcceptedValue);
    }

    [Fact]
    public async Task Repair_AfterRuntimeRestart_ReservesHigherDurableBallot()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        await cluster.Node.Acceptor.AcceptAsync("key", new(new(1, 0), Payload, new()));
        await cluster.Node.StopAsync().WaitAsync(Watchdog);
        Assert.Equal(1, cluster.Node.LastRepair!.Repaired);
        var before = ReadState(await cluster.Node.Acceptor.ReadAsync("key"));
        await cluster.Node.DisposeAsync().AsTask().WaitAsync(Watchdog);

        await using var reopened = await NodeRuntime.OpenAsync(cluster.Options, timeProvider: cluster.Clock);
        await reopened.StopAsync().WaitAsync(Watchdog);
        Assert.Equal(1, reopened.LastRepair!.Repaired);

        var after = ReadState(await reopened.Acceptor.ReadAsync("key"));
        Assert.True(after.AcceptedBallot > before.AcceptedBallot);
        Assert.Equal(cluster.Options.ProposerId, after.AcceptedBallot.Proposer);
        Assert.Equal(Payload, after.AcceptedValue);
        foreach (var store in cluster.Stores)
            Assert.Equal(after.AcceptedBallot, ReadState(await store.ReadAsync("key")).AcceptedBallot);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotInitiateReplication_AndReopenRetainsLocalBytes()
    {
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory);
        await cluster.Node.Acceptor.AcceptAsync("retain", new(new(1, 0), Payload, new()));

        await cluster.Node.DisposeAsync().AsTask().WaitAsync(Watchdog);

        Assert.All(cluster.RemoteAcceptors, peer => Assert.Equal(0, Volatile.Read(ref peer.Calls)));
        Assert.All(cluster.Schedulers, scheduler => Assert.Equal(0, Volatile.Read(ref scheduler.Calls)));
        foreach (var store in cluster.Stores) Assert.Empty((await store.ListDigestsAsync()).Entries);
        await using var reopened = await SqliteAcceptorServer.OpenAsync(cluster.Options.AcceptorDatabase);
        Assert.Equal(Payload, ReadState(await reopened.ReadAsync("retain")).AcceptedValue);
    }

    [Theory]
    [InlineData("negative-target")]
    [InlineData("oversized-target")]
    [InlineData("zero-page")]
    [InlineData("negative-page")]
    [InlineData("zero-interval")]
    [InlineData("excessive-interval")]
    [InlineData("zero-retry")]
    [InlineData("excessive-retry")]
    public void RepairOptions_InvalidBounds_RejectBeforeStorage(string scenario)
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        switch (scenario)
        {
            case "negative-target": options.TargetReplicationFactor = -1; break;
            case "oversized-target": options.TargetReplicationFactor = 2; break;
            case "zero-page": options.RepairPageSize = 0; break;
            case "negative-page": options.RepairPageSize = -1; break;
            case "zero-interval": options.RepairInterval = TimeSpan.Zero; break;
            case "excessive-interval": options.RepairInterval = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
            case "zero-retry": options.RepairRetryInterval = TimeSpan.Zero; break;
            case "excessive-retry": options.RepairRetryInterval = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
        }
        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task RepairOptions_DefaultsAndExplicitTarget_ReachRuntime()
    {
        var defaults = new NodeOptions();
        Assert.Equal(0, defaults.TargetReplicationFactor);
        Assert.Equal(128, defaults.RepairPageSize);
        Assert.Equal(TimeSpan.FromMinutes(1), defaults.RepairInterval);
        Assert.Equal(TimeSpan.FromSeconds(1), defaults.RepairRetryInterval);
        using var directory = new TestDirectory();
        await using var cluster = await RepairHostCluster.OpenAsync(directory, target: 3);
        Assert.Equal(3, cluster.Node.TargetReplicas);
    }

    private static Task<NodeEvent> NextCompleteAsync(NodeRuntime node, string after) =>
        NextRepairAsync(node, after, "complete", departing: false);

    private static async Task<NodeEvent> NextRepairAsync(NodeRuntime node, string after, string state, bool departing)
    {
        using var watchdog = new CancellationTokenSource(Watchdog);
        await foreach (var item in node.Diagnostics.ReadAsync(after, TimeSpan.FromHours(1), watchdog.Token))
            if (item.Data is RepairObservation observation && observation.State == state && observation.Departing == departing) return item;
        throw new InvalidOperationException("Node stopped before the expected repair completed.");
    }
}
