using System.Net;
using System.Text.Json;
using System.Threading.Channels;
using FastCASPaxos.Host;
using FastCASPaxos.Model;
using FastCASPaxos.Rest;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Data.Sqlite;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class DiagnosticHostTests
{
    public enum BadConfig
    {
        Missing, Relative, Scheme, Path, Query, Fragment, Credentials, Wildcard, AnyAddress, Duplicate,
        NullOrigins, InvalidTrusted, ZeroHistory, NegativeHistory, ZeroStreams, NegativeStreams,
        ZeroHeartbeat, NegativeHeartbeat, ExcessiveHeartbeat
    }

    [Theory]
    [InlineData(BadConfig.Missing)]
    [InlineData(BadConfig.Relative)]
    [InlineData(BadConfig.Scheme)]
    [InlineData(BadConfig.Path)]
    [InlineData(BadConfig.Query)]
    [InlineData(BadConfig.Fragment)]
    [InlineData(BadConfig.Credentials)]
    [InlineData(BadConfig.Wildcard)]
    [InlineData(BadConfig.AnyAddress)]
    [InlineData(BadConfig.Duplicate)]
    [InlineData(BadConfig.NullOrigins)]
    [InlineData(BadConfig.InvalidTrusted)]
    [InlineData(BadConfig.ZeroHistory)]
    [InlineData(BadConfig.NegativeHistory)]
    [InlineData(BadConfig.ZeroStreams)]
    [InlineData(BadConfig.NegativeStreams)]
    [InlineData(BadConfig.ZeroHeartbeat)]
    [InlineData(BadConfig.NegativeHeartbeat)]
    [InlineData(BadConfig.ExcessiveHeartbeat)]
    public void Options_RejectInvalidAdvertisedOriginsAndDiagnosticBoundsBeforeStorage(BadConfig scenario)
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        switch (scenario)
        {
            case BadConfig.Missing: options.Peers[0].HttpEndpoint = ""; break;
            case BadConfig.Relative: options.Peers[0].HttpEndpoint = "/node"; break;
            case BadConfig.Scheme: options.Peers[0].HttpEndpoint = "ftp://example.test"; break;
            case BadConfig.Path: options.Peers[0].HttpEndpoint = "http://example.test/path"; break;
            case BadConfig.Query: options.Peers[0].HttpEndpoint = "http://example.test/?key=value"; break;
            case BadConfig.Fragment: options.Peers[0].HttpEndpoint = "http://example.test/#fragment"; break;
            case BadConfig.Credentials: options.Peers[0].HttpEndpoint = "http://user:password@example.test"; break;
            case BadConfig.Wildcard: options.Peers[0].HttpEndpoint = "http://*:80"; break;
            case BadConfig.AnyAddress: options.Peers[0].HttpEndpoint = "http://0.0.0.0:80"; break;
            case BadConfig.Duplicate:
                options.Peers.Add(new() { Id = "b", Endpoint = "http://127.0.0.1:2", HttpEndpoint = "http://127.0.0.1:11/" }); break;
            case BadConfig.NullOrigins: options.TrustedAdminOrigins = null!; break;
            case BadConfig.InvalidTrusted: options.TrustedAdminOrigins = ["https://admin.test/path"]; break;
            case BadConfig.ZeroHistory: options.EventHistoryCapacity = 0; break;
            case BadConfig.NegativeHistory: options.EventHistoryCapacity = -1; break;
            case BadConfig.ZeroStreams: options.MaxEventStreams = 0; break;
            case BadConfig.NegativeStreams: options.MaxEventStreams = -1; break;
            case BadConfig.ZeroHeartbeat: options.HeartbeatInterval = TimeSpan.Zero; break;
            case BadConfig.NegativeHeartbeat: options.HeartbeatInterval = TimeSpan.FromTicks(-1); break;
            case BadConfig.ExcessiveHeartbeat: options.HeartbeatInterval = TimeSpan.FromDays(1).Add(TimeSpan.FromTicks(1)); break;
        }
        Assert.ThrowsAny<ArgumentException>(options.Validate);
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public void Options_DefaultsAndBoundaryValuesAreAccepted()
    {
        var defaults = new NodeOptions();
        Assert.Equal(1024, defaults.EventHistoryCapacity);
        Assert.Equal(64, defaults.MaxEventStreams);
        Assert.Equal(TimeSpan.FromSeconds(15), defaults.HeartbeatInterval);
        Assert.Empty(defaults.TrustedAdminOrigins);
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.Peers[0].HttpEndpoint = "https://node.test/";
        options.TrustedAdminOrigins = ["https://admin.test"];
        options.EventHistoryCapacity = options.MaxEventStreams = 1;
        foreach (var duration in new[] { TimeSpan.FromTicks(1), TimeSpan.FromDays(1) })
        {
            options.HeartbeatInterval = duration;
            options.Validate();
        }
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task Discovery_UsesOptionsSnapshotAndFreshOperationEventWithoutPeerProbes()
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.Peers.Add(new() { Id = "offline", Endpoint = "http://127.0.0.1:2", HttpEndpoint = "https://offline.test" });
        options.EventHistoryCapacity = 7;
        var clock = new DiagnosticClock();
        await using var host = await DiagnosticHost.Start(options, clock);
        options.Peers[1].HttpEndpoint = "https://changed.test";
        options.Peers.Clear();
        options.EventHistoryCapacity = 99;
        options.HeartbeatInterval = TimeSpan.FromSeconds(99);
        var cursor = host.Node.Diagnostics.Cursor;
        var status = await host.Json("/v1/admin/node");
        Assert.Equal(1, status.GetProperty("schemaVersion").GetInt32());
        Assert.Equal("a", status.GetProperty("nodeId").GetString());
        Assert.Equal(1, status.GetProperty("proposerId").GetInt32());
        Assert.Equal(host.Node.Diagnostics.BootId, status.GetProperty("bootId").GetString());
        Assert.Equal(DateTimeOffset.UnixEpoch, status.GetProperty("startedAt").GetDateTimeOffset());
        Assert.Equal(cursor, status.GetProperty("cursor").GetString());
        Assert.Equal("ready", status.GetProperty("storage").GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, status.GetProperty("lastOperation").ValueKind);
        Assert.Equal(7, status.GetProperty("historyCapacity").GetInt32());
        Assert.Equal(15000, status.GetProperty("heartbeatMilliseconds").GetDouble());
        var peers = status.GetProperty("peers").EnumerateArray().ToArray();
        Assert.Equal(2, peers.Length);
        Assert.Equal("offline", peers[1].GetProperty("id").GetString());
        Assert.Equal("http://127.0.0.1:2", peers[1].GetProperty("grpcEndpoint").GetString());
        Assert.Equal("https://offline.test", peers[1].GetProperty("httpEndpoint").GetString());
        Assert.Equal("https://offline.test/v1/admin/events", peers[1].GetProperty("eventsUrl").GetString());
        using var live = await host.Client.GetAsync("/health/live");
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal(cursor, host.Node.Diagnostics.Cursor);

        clock.Advance(TimeSpan.FromSeconds(3));
        host.Node.Diagnostics.Publish("operation", new OperationObservation("read", "operation", null, "fault", Error: "IOException"));
        var operationCursor = host.Node.Diagnostics.Cursor;
        clock.Advance(TimeSpan.FromSeconds(2));
        host.Node.Diagnostics.Publish("storage", new StorageObservation("failed", "SqliteException"));
        status = await host.Json("/v1/admin/node");
        var operation = status.GetProperty("lastOperation");
        Assert.Equal(operationCursor, operation.GetProperty("id").GetString());
        Assert.Equal("2", operation.GetProperty("sequence").GetString());
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(3), operation.GetProperty("timestamp").GetDateTimeOffset());
        Assert.Equal("fault", operation.GetProperty("data").GetProperty("outcome").GetString());
        Assert.Equal("failed", status.GetProperty("storage").GetProperty("status").GetString());
        using var stillLive = await host.Client.GetAsync("/health/live");
        Assert.Equal(HttpStatusCode.OK, stillLive.StatusCode);
        using var stream = await host.Events();
        using var reader = new StreamReader(await stream.Content.ReadAsStreamAsync());
        var reset = await Frame(reader, "reset");
        Assert.Equal("initial", reset.GetProperty("data").GetProperty("reason").GetString());
        Assert.Equal(host.Node.Diagnostics.Cursor, reset.GetProperty("id").GetString());
    }

    [Fact]
    public async Task StateSnapshots_AreReadOnlyIncludePromiseAndSerializeCompleteMaximumBallots()
    {
        using var directory = new TestDirectory();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        var missing = await host.Json("/v1/admin/state?key=absent");
        Assert.Equal(JsonValueKind.Null, missing.GetProperty("entry").ValueKind);
        Assert.Empty((await host.Node.Inspection.InspectKeysAsync()).Entries);
        var maximum = new FastCASPaxos.Model.Ballot(int.MaxValue, int.MaxValue);
        Assert.True((await host.Node.Acceptor.PrepareAsync("promise", new(maximum))).IsPromised);
        Assert.True((await host.Node.Acceptor.AcceptAsync("binary", new(new(12, 7), new([0, 255]), new(19, 8)))).IsAccepted);
        var cursor = host.Node.Diagnostics.Cursor;
        var promise = await host.Json("/v1/admin/state?key=promise");
        Assert.Equal(cursor, promise.GetProperty("cursor").GetString());
        AssertBallot(promise.GetProperty("entry").GetProperty("promisedBallot"), int.MaxValue, int.MaxValue);
        AssertBallot(promise.GetProperty("entry").GetProperty("acceptedBallot"), 0, 0);
        Assert.Equal(JsonValueKind.Null, promise.GetProperty("entry").GetProperty("acceptedValue").ValueKind);
        var page = await host.Json("/v1/admin/states?limit=1");
        var entry = Assert.Single(page.GetProperty("entries").EnumerateArray());
        Assert.Equal("binary", entry.GetProperty("key").GetString());
        AssertBallot(entry.GetProperty("promisedBallot"), 19, 8);
        AssertBallot(entry.GetProperty("acceptedBallot"), 12, 7);
        Assert.Equal("AP8=", entry.GetProperty("acceptedValue").GetProperty("base64").GetString());
        Assert.Equal("opaque", entry.GetProperty("register").GetProperty("format").GetString());
        Assert.Equal("binary", page.GetProperty("continuation").GetString());
        var rest = await host.Json("/v1/admin/states?prefix=p&after=binary&limit=100");
        Assert.Equal("promise", Assert.Single(rest.GetProperty("entries").EnumerateArray()).GetProperty("key").GetString());
        Assert.Equal(JsonValueKind.Null, rest.GetProperty("continuation").ValueKind);
        Assert.Equal(cursor, host.Node.Diagnostics.Cursor);
        Assert.Equal("binary", Assert.Single((await host.Node.Acceptor.ListKeysAsync()).Entries).Key);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Snapshot_PreReadWatermarkReplaysBlockedWriteAndRefetchesCommittedState(bool list)
    {
        using var directory = new TestDirectory();
        using var barrier = new SnapshotCommitBarrier();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        var sqlite = Assert.IsType<SqliteAcceptorServer>(host.Node.Inspection);
        Assert.True((await sqlite.AcceptAsync("key", new(new(2, 1), new([7]), new(3, 1)))).IsAccepted);
        var beforeRead = host.Node.Diagnostics.Cursor;
        var commits = 0;
        sqlite.BeforeCommit = (_, _, count) =>
        {
            if (Interlocked.Increment(ref commits) == 1) barrier.Arrive(count);
        };
        var path = list ? "/v1/admin/states?prefix=key" : "/v1/admin/state?key=key";
        try
        {
            var snapshotRequest = host.Json(path);
            Assert.Equal(1, await barrier.Entered.WaitAsync(Watchdog));
            Assert.False(snapshotRequest.IsCompleted);

            // The HTTP read has materialized the old row, but cannot respond before its
            // transaction commits. The observed write publishes its request immediately
            // and queues behind that read, guaranteeing a cursor advance during the await.
            var write = host.Node.Acceptor.AcceptAsync(
                "key", new(new(4, 1), new([8, 255]), new(5, 1))).AsTask();
            var requestCursor = host.Node.Diagnostics.Cursor;
            Assert.NotEqual(beforeRead, requestCursor);
            Assert.False(write.IsCompleted);
            Assert.False(snapshotRequest.IsCompleted);

            barrier.Release();
            var snapshot = await snapshotRequest.WaitAsync(Watchdog);
            Assert.Equal(beforeRead, snapshot.GetProperty("cursor").GetString());
            var oldEntry = Entry(snapshot);
            Assert.Equal("key", oldEntry.GetProperty("key").GetString());
            Assert.Equal("Bw==", oldEntry.GetProperty("acceptedValue").GetProperty("base64").GetString());
            AssertBallot(oldEntry.GetProperty("acceptedBallot"), 2, 1);
            AssertBallot(oldEntry.GetProperty("promisedBallot"), 3, 1);
            Assert.True((await write.WaitAsync(Watchdog)).IsAccepted);
            var committedCursor = host.Node.Diagnostics.Cursor;

            // Replay uses the cursor returned by HTTP, not a test-owned substitute.
            // Capturing it after the awaited inspection would omit at least this request.
            using var replay = await host.Events(after: snapshot.GetProperty("cursor").GetString());
            using var reader = new StreamReader(await replay.Content.ReadAsStreamAsync());
            var requested = await Frame(reader, "protocol");
            Assert.Equal(requestCursor, requested.GetProperty("id").GetString());
            Assert.Equal("request-received", requested.GetProperty("data").GetProperty("phase").GetString());
            Assert.Equal("key", requested.GetProperty("data").GetProperty("key").GetProperty("text").GetString());
            var changed = await Frame(reader, "acceptor-state");
            Assert.Equal("key", changed.GetProperty("data").GetProperty("key").GetProperty("text").GetString());
            Assert.Equal("CP8=", changed.GetProperty("data").GetProperty("acceptedValue").GetProperty("base64").GetString());
            AssertBallot(changed.GetProperty("data").GetProperty("acceptedBallot"), 4, 1);
            AssertBallot(changed.GetProperty("data").GetProperty("promisedBallot"), 5, 1);
            var completed = await Frame(reader, "protocol");
            Assert.Equal(committedCursor, completed.GetProperty("id").GetString());
            Assert.Equal("response-produced", completed.GetProperty("data").GetProperty("phase").GetString());
            Assert.Equal("accepted", completed.GetProperty("data").GetProperty("outcome").GetString());
            Assert.Equal(requested.GetProperty("data").GetProperty("callId").GetString(),
                completed.GetProperty("data").GetProperty("callId").GetString());

            var refetched = await host.Json(path);
            Assert.Equal(committedCursor, refetched.GetProperty("cursor").GetString());
            var newEntry = Entry(refetched);
            Assert.Equal("CP8=", newEntry.GetProperty("acceptedValue").GetProperty("base64").GetString());
            AssertBallot(newEntry.GetProperty("acceptedBallot"), 4, 1);
            AssertBallot(newEntry.GetProperty("promisedBallot"), 5, 1);
            Assert.Equal(committedCursor, host.Node.Diagnostics.Cursor);
        }
        finally { barrier.Release(); }

        JsonElement Entry(JsonElement snapshot) => list
            ? Assert.Single(snapshot.GetProperty("entries").EnumerateArray())
            : snapshot.GetProperty("entry");
    }

    [Theory]
    [InlineData("/v1/admin/state")]
    [InlineData("/v1/admin/state?key=a&key=b")]
    [InlineData("/v1/admin/state?unknown=a")]
    [InlineData("/v1/admin/states?limit=0")]
    [InlineData("/v1/admin/states?limit=101")]
    [InlineData("/v1/admin/states?limit=-1")]
    [InlineData("/v1/admin/states?limit=text")]
    [InlineData("/v1/admin/states?limit=1&limit=2")]
    [InlineData("/v1/admin/states?prefix=a&prefix=b")]
    [InlineData("/v1/admin/events?unknown=1")]
    [InlineData("/v1/admin/events?after=a&after=b")]
    public async Task Admin_InvalidQueryIsExplicitBadRequestBeforeInspection(string path)
    {
        using var directory = new TestDirectory();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        var sqlite = Assert.IsType<SqliteAcceptorServer>(host.Node.Inspection);
        var inspections = 0;
        sqlite.BeforeCommit = (_, _, count) => Interlocked.Add(ref inspections, count);
        var before = host.Node.Diagnostics.Cursor;
        using var response = await host.Client.GetAsync(path);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await DiagnosticHost.Parse(response);
        Assert.Equal("invalid", problem.GetProperty("title").GetString());
        Assert.Equal(before, host.Node.Diagnostics.Cursor);
        Assert.Equal(0, Volatile.Read(ref inspections));
        // This valid read both proves the hook is active and drains any earlier
        // mistakenly enqueued work before the exact final work-count assertion.
        Assert.Null(await sqlite.InspectAsync("sentinel"));
        Assert.Equal(1, Volatile.Read(ref inspections));
    }

    [Fact]
    public async Task States_DefaultPageIsCappedAtOneHundred()
    {
        using var directory = new TestDirectory();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        for (var i = 0; i < 101; i++)
            await host.Node.Acceptor.PrepareAsync($"key-{i:D3}", new(new FastCASPaxos.Model.Ballot(2, 1)));
        var first = await host.Json("/v1/admin/states");
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"key-{i:D3}"),
            first.GetProperty("entries").EnumerateArray().Select(e => e.GetProperty("key").GetString()));
        Assert.Equal("key-099", first.GetProperty("continuation").GetString());
        var last = await host.Json("/v1/admin/states?after=key-099");
        Assert.Equal("key-100", Assert.Single(last.GetProperty("entries").EnumerateArray()).GetProperty("key").GetString());
        Assert.Equal(JsonValueKind.Null, last.GetProperty("continuation").ValueKind);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidProviderUtf16_RejectsStateConversionAndHttpInventoryWithoutAliasing(bool lowSurrogate)
    {
        using var directory = new TestDirectory();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        var key = lowSurrogate ? "a\udc00b" : "a\ud800b";
        Assert.True((await host.Node.Acceptor.PrepareAsync(key, new(new FastCASPaxos.Model.Ballot(9, 3)))).IsPromised);
        var local = Assert.IsType<LocalAcceptorState>(await host.Node.Inspection.InspectAsync(key));
        Assert.Equal(key, local.Key); // Local storage's UTF16 domain is intentionally unchanged.
        var error = Assert.Throws<InvalidOperationException>(() => LocalStateView.From(local));
        Assert.IsType<System.Text.EncoderFallbackException>(error.InnerException);
        using var response = await host.Client.GetAsync("/v1/admin/states");
        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("invalid-text", (await DiagnosticHost.Parse(response)).GetProperty("title").GetString());
        var replacement = await host.Json("/v1/admin/state?key=a%EF%BF%BDb");
        Assert.Equal(JsonValueKind.Null, replacement.GetProperty("entry").ValueKind);
        Assert.Equal(local, await host.Node.Inspection.InspectAsync(key));
    }

    [Theory]
    [InlineData("/v1/admin/state?key=%")]
    [InlineData("/v1/admin/state?key=%GG")]
    [InlineData("/v1/admin/state?key=%FF")]
    [InlineData("/v1/admin/state?key=%C0%AF")]
    [InlineData("/v1/admin/state?key=%ED%A0%80")]
    [InlineData("/v1/admin/state?key=a&%6Bey=b")]
    [InlineData("/v1/admin/states?prefix=%E2%82")]
    [InlineData("/v1/admin/states?after=%FF")]
    [InlineData("/v1/admin/events?after=%FF")]
    public async Task StrictQuery_MalformedPercentUtf8AndEncodedDuplicatesReturn400BeforeInspection(string path)
    {
        using var directory = new TestDirectory();
        await using var host = await DiagnosticHost.Start(DiagnosticHost.Options(directory));
        var sqlite = Assert.IsType<SqliteAcceptorServer>(host.Node.Inspection);
        var inspections = 0;
        sqlite.BeforeCommit = (_, _, count) => Interlocked.Add(ref inspections, count);
        var before = host.Node.Diagnostics.Cursor;
        var uri = new Uri(host.Address + path, new UriCreationOptions { DangerousDisablePathAndQueryCanonicalization = true });
        using var response = await host.Client.GetAsync(uri);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("invalid", (await DiagnosticHost.Parse(response)).GetProperty("title").GetString());
        Assert.Equal(before, host.Node.Diagnostics.Cursor);
        Assert.Equal(0, Volatile.Read(ref inspections));
        Assert.Empty((await host.Node.Inspection.InspectKeysAsync()).Entries);
        Assert.Equal(1, Volatile.Read(ref inspections));
    }

    [Fact]
    public async Task Sse_InitialReplayGapAndHeartbeatUseExactFramesAndHeaders()
    {
        using var directory = new TestDirectory();
        var clock = new DiagnosticClock();
        var options = DiagnosticHost.Options(directory);
        options.EventHistoryCapacity = 3;
        await using var host = await DiagnosticHost.Start(options, clock);
        using var initial = await host.Events();
        Assert.Equal("text/event-stream", initial.Content.Headers.ContentType!.MediaType);
        Assert.Equal("no-store, no-transform", initial.Headers.CacheControl!.ToString());
        Assert.Equal("no", Assert.Single(initial.Headers.GetValues("X-Accel-Buffering")));
        using var reader = new StreamReader(await initial.Content.ReadAsStreamAsync());
        var reset = await Frame(reader, "reset");
        Assert.Equal(host.Node.Diagnostics.Cursor, reset.GetProperty("id").GetString());
        Assert.Equal("initial", reset.GetProperty("data").GetProperty("reason").GetString());
        var cursor = host.Node.Diagnostics.Cursor;
        await clock.TimerCreated.Reader.ReadAsync().AsTask().WaitAsync(Watchdog);
        clock.Advance(options.HeartbeatInterval);
        var heartbeat = await Frame(reader, "heartbeat", hasId: false);
        Assert.Equal(cursor, heartbeat.GetProperty("id").GetString());
        Assert.Equal("ready", heartbeat.GetProperty("data").GetProperty("storageStatus").GetString());
        host.Node.Diagnostics.Publish("sample", new { number = 2 });
        var live = await Frame(reader, "sample");
        Assert.Equal(2, live.GetProperty("data").GetProperty("number").GetInt32());

        // Last-Event-ID takes precedence over ?after=.
        using var replay = await host.Events("?after=invalid", cursor);
        using var replayReader = new StreamReader(await replay.Content.ReadAsStreamAsync());
        Assert.Equal(live.GetProperty("id").GetString(), (await Frame(replayReader, "sample")).GetProperty("id").GetString());
        for (var i = 3; i <= 6; i++) host.Node.Diagnostics.Publish("sample", new { number = i });
        using var gap = await host.Events("?after=" + Uri.EscapeDataString(cursor));
        using var gapReader = new StreamReader(await gap.Content.ReadAsStreamAsync());
        var lost = await Frame(gapReader, "reset");
        Assert.Equal("history-unavailable", lost.GetProperty("data").GetProperty("reason").GetString());
        Assert.Equal(host.Node.Diagnostics.Cursor, lost.GetProperty("id").GetString());
    }

    [Fact]
    public async Task Sse_StreamCapacityDisconnectAndShutdownReleaseResources()
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.MaxEventStreams = 1;
        await using var host = await DiagnosticHost.Start(options);
        var first = await host.Events(testId: "first");
        using (var reader = new StreamReader(await first.Content.ReadAsStreamAsync()))
        {
            await Frame(reader, "reset");
            using var denied = await host.Events(expectSuccess: false);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, denied.StatusCode);
            Assert.Equal("stream-capacity", (await DiagnosticHost.Parse(denied)).GetProperty("title").GetString());
            first.Dispose();
        }
        Assert.Equal("first", await host.CompletedStreams.Reader.ReadAsync().AsTask().WaitAsync(Watchdog));
        using var second = await host.Events(testId: "second");
        using var secondReader = new StreamReader(await second.Content.ReadAsStreamAsync());
        await Frame(secondReader, "reset");
        await host.App.StopAsync().WaitAsync(Watchdog);
        Assert.Null(await secondReader.ReadLineAsync().WaitAsync(Watchdog));
        Assert.Equal("second", await host.CompletedStreams.Reader.ReadAsync().AsTask().WaitAsync(Watchdog));
        await host.Node.DisposeAsync();
        using (File.Open(options.AcceptorDatabase, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) { }
    }

    [Fact]
    public async Task FailedStorage_Returns503WithoutLosingDiscoveryLivenessOrEventStream()
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        var host = await DiagnosticHost.Start(options);
        SqliteException? failure = null;
        try
        {
            var applied = await host.Node.Registers.CompareExchangeAsync("stable", null, Payload);
            Assert.Equal(CompareExchangeStatus.Applied, applied.Status);
            var lastOperation = host.Node.Diagnostics.LastOperation!;
            using var events = await host.Events();
            using var reader = new StreamReader(await events.Content.ReadAsStreamAsync());
            await Frame(reader, "reset");
            using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = options.AcceptorDatabase, Pooling = false
            }.ToString()))
            {
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = """
                    CREATE TRIGGER fail_diagnostic_write BEFORE INSERT ON acceptor_state
                    BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END;
                    """;
                command.ExecuteNonQuery();
            }
            failure = await Assert.ThrowsAsync<SqliteException>(() => host.Node.Acceptor.AcceptAsync(
                "new", new(new(3, 1), Payload, new(4, 1))).AsTask().WaitAsync(Watchdog));
            var request = await Frame(reader, "protocol");
            Assert.Equal("request-received", request.GetProperty("data").GetProperty("phase").GetString());
            var failed = await Frame(reader, "storage");
            Assert.Equal("failed", failed.GetProperty("data").GetProperty("status").GetString());
            Assert.Equal("SqliteException", failed.GetProperty("data").GetProperty("error").GetString());
            var fault = await Frame(reader, "protocol");
            Assert.Equal("fault-observed", fault.GetProperty("data").GetProperty("phase").GetString());
            Assert.Equal(request.GetProperty("data").GetProperty("callId").GetString(),
                fault.GetProperty("data").GetProperty("callId").GetString());
            foreach (var path in new[] { "/v1/admin/state?key=stable", "/v1/admin/states" })
            {
                using var unavailable = await host.Client.GetAsync(path);
                Assert.Equal(HttpStatusCode.ServiceUnavailable, unavailable.StatusCode);
                Assert.Equal("storage-unavailable", (await DiagnosticHost.Parse(unavailable)).GetProperty("title").GetString());
            }
            var status = await host.Json("/v1/admin/node");
            Assert.Equal("failed", status.GetProperty("storage").GetProperty("status").GetString());
            Assert.Equal(lastOperation.Id, status.GetProperty("lastOperation").GetProperty("id").GetString());
            Assert.Equal("applied", status.GetProperty("lastOperation").GetProperty("data").GetProperty("outcome").GetString());
            using var live = await host.Client.GetAsync("/health/live");
            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
            host.Node.Diagnostics.Publish("probe", new { stillServing = true });
            Assert.True((await Frame(reader, "probe")).GetProperty("data").GetProperty("stillServing").GetBoolean());
        }
        finally
        {
            var shutdown = await Record.ExceptionAsync(() => host.DisposeAsync().AsTask());
            if (failure is null) Assert.Null(shutdown);
            else Assert.Same(failure, Assert.Single(Assert.IsType<AggregateException>(shutdown).InnerExceptions));
        }
    }

    [Theory]
    [InlineData("/v1/admin/node", "https://admin.test", true)]
    [InlineData("/v1/admin/node", "http://127.0.0.1:11", true)]
    [InlineData("/v1/admin/node", "https://untrusted.test", false)]
    [InlineData("/v1/registers/value?key=key", "https://admin.test", true)]
    [InlineData("/v1/registers/value?key=key", "https://untrusted.test", false)]
    [InlineData("/v1/admin/repair", "https://admin.test", true)]
    [InlineData("/v1/admin/repair", "https://untrusted.test", false)]
    public async Task Cors_PreflightAndResponsesAllowOnlyConfiguredOrigins(string path, string origin, bool allowed)
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.TrustedAdminOrigins = ["https://admin.test"];
        await using var host = await DiagnosticHost.Start(options);
        using var preflight = new HttpRequestMessage(HttpMethod.Options, path)
        {
            Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };
        preflight.Headers.Add("Origin", origin);
        var repair = path == "/v1/admin/repair";
        preflight.Headers.Add("Access-Control-Request-Method", repair ? "POST" : path.Contains("/admin/") ? "GET" : "PUT");
        preflight.Headers.Add("Access-Control-Request-Headers", "content-type,if-match,if-none-match,last-event-id,authorization");
        using var response = await host.Client.SendAsync(preflight);
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Equal(allowed, response.Headers.Contains("Access-Control-Allow-Origin"));
        if (allowed)
        {
            Assert.Equal(origin, Assert.Single(response.Headers.GetValues("Access-Control-Allow-Origin")));
            Assert.Equal(["GET", "POST", "PUT"], Tokens(response, "Access-Control-Allow-Methods").Order());
            Assert.Equal(new[] { "authorization", "content-type", "if-match", "if-none-match", "last-event-id" },
                Tokens(response, "Access-Control-Allow-Headers").Select(x => x.ToLowerInvariant()).Order());
        }
        Assert.False(response.Headers.Contains("Access-Control-Allow-Credentials"));
        using var request = new HttpRequestMessage(repair ? HttpMethod.Post : HttpMethod.Get, path)
        {
            Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };
        request.Headers.Add("Origin", origin);
        using var actual = await host.Client.SendAsync(request);
        if (repair)
        {
            Assert.Equal(HttpStatusCode.Accepted, actual.StatusCode);
            var staged = await DiagnosticHost.Parse(actual);
            Assert.Equal("staged", staged.GetProperty("state").GetString());
            Assert.Equal(1, staged.GetProperty("targetReplicas").GetInt32());
        }
        Assert.Equal(allowed, actual.Headers.Contains("Access-Control-Allow-Origin"));
        Assert.False(actual.Headers.Contains("Access-Control-Allow-Credentials"));
        if (allowed) Assert.Equal(["ETag", "Location"], Tokens(actual, "Access-Control-Expose-Headers").Order());
    }

    internal static async Task<JsonElement> Frame(StreamReader reader, string kind, bool hasId = true)
    {
        var first = await reader.ReadLineAsync().WaitAsync(Watchdog);
        string? id = null;
        if (hasId)
        {
            Assert.StartsWith("id: ", first);
            id = first![4..];
            first = await reader.ReadLineAsync().WaitAsync(Watchdog);
        }
        Assert.Equal("event: " + kind, first);
        var data = await reader.ReadLineAsync().WaitAsync(Watchdog);
        Assert.StartsWith("data: ", data);
        Assert.Equal("", await reader.ReadLineAsync().WaitAsync(Watchdog));
        using var document = JsonDocument.Parse(data![6..]);
        var json = document.RootElement.Clone();
        Assert.Equal(kind, json.GetProperty("kind").GetString());
        Assert.Equal(1, json.GetProperty("schemaVersion").GetInt32());
        if (hasId) Assert.Equal(id, json.GetProperty("id").GetString());
        return json;
    }

    private static string[] Tokens(HttpResponseMessage response, string header) =>
        response.Headers.GetValues(header).SelectMany(value => value.Split(',', StringSplitOptions.TrimEntries)).ToArray();

    private static void AssertBallot(JsonElement ballot, int round, int proposer)
    {
        Assert.Equal(["round", "proposer"], ballot.EnumerateObject().Select(property => property.Name));
        Assert.Equal(round, ballot.GetProperty("round").GetInt32());
        Assert.Equal(proposer, ballot.GetProperty("proposer").GetInt32());
    }

    private sealed class SnapshotCommitBarrier : IDisposable
    {
        private readonly TaskCompletionSource<int> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _released = new();
        public Task<int> Entered => _entered.Task;
        public void Arrive(int count)
        {
            _entered.SetResult(count);
            if (!_released.Wait(Watchdog)) throw new TimeoutException("HTTP snapshot commit barrier was not released.");
        }
        public void Release() => _released.Set();
        public void Dispose() => _released.Dispose();
    }
}

internal sealed class DiagnosticHost(NodeRuntime node, WebApplication app, string address,
    Channel<string> completedStreams) : IAsyncDisposable
{
    public NodeRuntime Node { get; } = node;
    public WebApplication App { get; } = app;
    public string Address { get; } = address;
    public Channel<string> CompletedStreams { get; } = completedStreams;
    public HttpClient Client { get; } = new()
    {
        BaseAddress = new(address), Timeout = Watchdog,
        DefaultRequestVersion = HttpVersion.Version20, DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact
    };

    public static NodeOptions Options(TestDirectory directory, string id = "a", int proposer = 1) => new()
    {
        AcceptorId = id, ProposerId = proposer,
        AcceptorDatabase = directory.Database($"diagnostic-acceptor-{id}"),
        ProposerDatabase = directory.Database($"diagnostic-proposer-{id}"),
        Peers = [new() { Id = id, Endpoint = "http://127.0.0.1:1", HttpEndpoint = "http://127.0.0.1:11" }]
    };

    public static async Task<DiagnosticHost> Start(NodeOptions options, TimeProvider? clock = null)
    {
        var node = await NodeRuntime.OpenAsync(options, timeProvider: clock).WaitAsync(Watchdog);
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.Configuration.Sources.Clear();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(server =>
            server.Listen(IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc(grpc => grpc.Interceptors.Add<DiagnosticServerInterceptor>());
        builder.Services.AddAdminCors(options);
        builder.Services.AddSingleton(node.Acceptor);
        builder.Services.AddSingleton(node.Digests);
        builder.Services.AddSingleton(node.Registers);
        var app = builder.Build();
        var completed = Channel.CreateUnbounded<string>();
        app.Use(async (context, next) =>
        {
            try { await next(context); }
            finally
            {
                if (context.Request.Headers.TryGetValue("X-Test-Stream", out var id))
                    completed.Writer.TryWrite(id.ToString());
            }
        });
        app.UseCors();
        app.MapAdminApi(node);
        app.MapRegisterApi().RequireCors(AdminEndpoints.CorsPolicy);
        app.MapGrpcService<AcceptorGrpcService>();
        app.MapGrpcService<AcceptorDigestGrpcService>();
        app.MapGrpcService<RegisterGrpcService>();
        app.MapGet("/health/live", () => Results.Text("Serving register requests."));
        try
        {
            await app.StartAsync().WaitAsync(Watchdog);
            var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!;
            return new(node, app, Assert.Single(addresses.Addresses), completed);
        }
        catch
        {
            await app.DisposeAsync();
            await node.DisposeAsync();
            throw;
        }
    }

    public async Task<JsonElement> Json(string path)
    {
        using var response = await Client.GetAsync(path);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(response.Headers.CacheControl!.NoStore);
        return await Parse(response);
    }

    public static async Task<JsonElement> Parse(HttpResponseMessage response)
    {
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.RootElement.Clone();
    }

    public async Task<HttpResponseMessage> Events(string suffix = "", string? after = null,
        string? testId = null, bool expectSuccess = true)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "/v1/admin/events" + suffix)
        {
            Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };
        if (after is not null) request.Headers.Add("Last-Event-ID", after);
        if (testId is not null) request.Headers.Add("X-Test-Stream", testId);
        var response = await Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        if (expectSuccess) Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return response;
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        try { await App.StopAsync().WaitAsync(Watchdog); }
        finally
        {
            await App.DisposeAsync();
            await Node.DisposeAsync();
        }
    }
}

// A one-shot timer barrier makes heartbeat tests independent of scheduler speed and wall time.
internal sealed class DiagnosticClock : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<Timer> _timers = [];
    private long _ticks;
    public Channel<TimeSpan> TimerCreated { get; } = Channel.CreateUnbounded<TimeSpan>();
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    public override long GetTimestamp() { lock (_gate) return _ticks; }
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(GetTimestamp());
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new Timer(this, callback, state);
        lock (_gate)
        {
            _timers.Add(timer);
            timer.Change(dueTime, period);
            TimerCreated.Writer.TryWrite(dueTime);
        }
        return timer;
    }
    public void Advance(TimeSpan duration)
    {
        Timer[] due;
        lock (_gate)
        {
            _ticks += duration.Ticks;
            due = _timers.Where(t => t.Due <= _ticks).ToArray();
            foreach (var timer in due) timer.Due = long.MaxValue;
        }
        foreach (var timer in due) timer.Fire();
    }
    private sealed class Timer(DiagnosticClock owner, TimerCallback callback, object? state) : ITimer
    {
        public long Due = long.MaxValue;
        private bool _disposed;
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._gate)
            {
                if (_disposed) return false;
                Due = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : owner._ticks + dueTime.Ticks;
                return true;
            }
        }
        public void Fire() { if (!_disposed) callback(state); }
        public void Dispose() { lock (owner._gate) { _disposed = true; owner._timers.Remove(this); } }
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }
}
