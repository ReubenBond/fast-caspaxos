using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using FastCASPaxos.Grpc;
using FastCASPaxos.Host;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using global::Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;
using static FastCASPaxos.Rest.Tests.RestTestSupport;

namespace FastCASPaxos.Rest.Tests;

public sealed class HttpIntegrationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InspectorGetRepeatedRefreshPreservesDurableBallotsAndDoesNotCreateMissingRows(bool present)
    {
        // Execute the real REST endpoint and register/SQLite stack in process, without a socket.
        using var directory = new TestDirectory();
        await using var a = await SqliteAcceptorServer.OpenAsync(System.IO.Path.Combine(directory.Path, "a.db"));
        await using var b = await SqliteAcceptorServer.OpenAsync(System.IO.Path.Combine(directory.Path, "b.db"));
        await using var c = await SqliteAcceptorServer.OpenAsync(System.IO.Path.Combine(directory.Path, "c.db"));
        var stores = new[] { a, b, c };
        var version = new ByteString([0, 0, 0, 9, 0, 0, 0, 1]);
        var envelope = new ByteString([.. "FCR1"u8, 1, .. version.ToArray(), .. Payload.ToArray()]);
        if (present)
            foreach (var store in stores)
                Assert.True((await store.AcceptAsync("inspect", new(new(3, 9), envelope, new(8, 9)))).IsAccepted);
        using var ballots = new SqliteProposerBallotSource(System.IO.Path.Combine(directory.Path, "ballots.db"), 1);
        Assert.Equal(new FastCASPaxos.Model.Ballot(1, 1), ballots.Reserve(1));
        await using var registers = new ReplicatedRegisterService([new("a", a), new("b", b), new("c", c)], ballots);
        var observed = new ControlledService { Read = registers.ReadAsync };
        await using var endpoint = new EndpointHarness(observed);

        for (var refresh = 0; refresh < 3; refresh++)
        {
            var request = endpoint.Request(query: "?key=inspect");
            await endpoint.Invoke(request);
            Assert.Equal(present ? 200 : 404, request.Response.StatusCode);
            RestTestSupport.NoStore(request);
            if (present) Assert.Equal(Payload.ToArray(), Bytes(request));
            else Assert.Equal("missing", Problem(request, 404, "missing").GetProperty("code").GetString());
            Assert.Equal(present ? Tag(version) : "", request.Response.Headers.ETag.ToString());
        }

        Assert.Equal(["read", "read", "read"], observed.Calls.Select(call => call.Operation));
        Assert.Equal(new FastCASPaxos.Model.Ballot(2, 1), ballots.Reserve(1)); // No hidden allocation during any GET.
        foreach (var store in stores)
        {
            var state = await store.InspectAsync("inspect");
            if (present)
            {
                Assert.Equal(new FastCASPaxos.Model.Ballot(8, 9), state!.PromisedBallot);
                Assert.Equal(new FastCASPaxos.Model.Ballot(3, 9), state.AcceptedBallot);
                Assert.Equal(envelope, state.AcceptedValue);
            }
            else Assert.Null(state);
        }
    }

    [Fact]
    public async Task Http1AndGrpcShareDurableStateAndRetainPayloadAndEtagAfterRestart()
    {
        using var directory = new TestDirectory();
        const string key = "rest/../literal%2F +\0\U0001F600";
        string retainedTag;
        ByteString retainedVersion;
        var finalPayload = new ByteString([255, 0, 254, 128, 127]);
        await using (var node = await RunningNode.Start(directory.Options()))
        {
            using var missing = await node.Http.GetAsync(ValueUrl(key));
            Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
            Assert.Null(missing.Headers.ETag);
            NoStore(missing);

            using var created = await Put(node.Http, key, Payload);
            Assert.Equal(HttpStatusCode.Created, created.StatusCode);
            Assert.Equal(HttpVersion.Version11, created.Version);
            Assert.Equal(ValueUrl(key), created.Headers.Location!.OriginalString);
            NoStore(created);
            var initialTag = created.Headers.ETag!.ToString();
            var fromGrpc = await node.Grpc.ReadAsync(key);
            Assert.Equal(Payload, fromGrpc.Value);
            Assert.Equal(initialTag, Tag(fromGrpc.Version!.Value));

            using var readLocation = await node.Http.GetAsync(created.Headers.Location);
            Assert.Equal(Payload.ToArray(), await readLocation.Content.ReadAsByteArrayAsync());
            Assert.Equal(initialTag, readLocation.Headers.ETag!.ToString());
            Assert.Equal("application/octet-stream", readLocation.Content.Headers.ContentType!.MediaType);

            using var duplicate = await Put(node.Http, key, ByteString.Empty);
            Assert.Equal(HttpStatusCode.PreconditionFailed, duplicate.StatusCode);
            Assert.Equal(initialTag, duplicate.Headers.ETag!.ToString());
            NoStore(duplicate);

            var grpcUpdate = await node.Grpc.CompareExchangeAsync(key, fromGrpc.Version, ByteString.Empty);
            Assert.Equal(CompareExchangeStatus.Applied, grpcUpdate.Status);
            var grpcTag = Tag(grpcUpdate.Current!.Version!.Value);
            Assert.NotEqual(initialTag, grpcTag);
            using var emptyRead = await node.Http.GetAsync(ValueUrl(key));
            Assert.Equal(HttpStatusCode.OK, emptyRead.StatusCode);
            Assert.Empty(await emptyRead.Content.ReadAsByteArrayAsync());
            Assert.Equal(grpcTag, emptyRead.Headers.ETag!.ToString());
            NoStore(emptyRead);

            using var stale = await Put(node.Http, key, finalPayload, initialTag);
            Assert.Equal(HttpStatusCode.PreconditionFailed, stale.StatusCode);
            Assert.Equal(grpcTag, stale.Headers.ETag!.ToString());
            using var updated = await Put(node.Http, key, finalPayload, grpcTag);
            Assert.Equal(HttpStatusCode.NoContent, updated.StatusCode);
            Assert.Empty(await updated.Content.ReadAsByteArrayAsync());
            retainedTag = updated.Headers.ETag!.ToString();
            Assert.NotEqual(grpcTag, retainedTag);
            NoStore(updated);
            var last = await node.Grpc.ReadAsync(key);
            Assert.Equal(finalPayload, last.Value);
            retainedVersion = last.Version!.Value;
            Assert.Equal(retainedTag, Tag(retainedVersion));
        }

        await using (var restarted = await RunningNode.Start(directory.Options()))
        {
            using var read = await restarted.Http.GetAsync(ValueUrl(key));
            Assert.Equal(HttpStatusCode.OK, read.StatusCode);
            Assert.Equal(retainedTag, read.Headers.ETag!.ToString());
            Assert.Equal(finalPayload.ToArray(), await read.Content.ReadAsByteArrayAsync());
            NoStore(read);
            Assert.Equal(new RegisterValue(finalPayload, retainedVersion), await restarted.Grpc.ReadAsync(key));
            using var update = await Put(restarted.Http, key, ByteString.Empty, retainedTag);
            Assert.Equal(HttpStatusCode.NoContent, update.StatusCode);
            Assert.NotEqual(retainedTag, update.Headers.ETag!.ToString());
            Assert.Equal(ByteString.Empty, (await restarted.Grpc.ReadAsync(key)).Value);
        }
    }

    [Fact]
    public async Task Http1ListUsesExclusiveEmptyCursorAndOrdinalPrefixPages()
    {
        using var directory = new TestDirectory();
        await using var node = await RunningNode.Start(directory.Options());
        string[] keys = ["", "\0", "prefix/ ", "prefix/%", "prefix/../", "prefix/\U0001F600", "prefix/\uE000"];
        foreach (var key in keys)
        {
            using var response = await Put(node.Http, key, ByteString.Empty);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        }
        var first = await Page(node.Http, "/v1/registers?limit=1");
        Assert.Equal<string>([""], first.Keys);
        Assert.Equal("", first.ContinuationKey);
        var afterEmpty = await Page(node.Http, "/v1/registers?after=&limit=100");
        Assert.Equal(keys.Where(key => key.Length != 0).Order(StringComparer.Ordinal), afterEmpty.Keys);
        Assert.Null(afterEmpty.ContinuationKey);

        var collected = new List<string>();
        string? cursor = null;
        // A fixed upper bound exposes non-progressing cursors without retrying until green.
        for (var index = 0; index < keys.Length; index++)
        {
            var page = await Page(node.Http, "/v1/registers?prefix=prefix%2F&limit=2"
                + (cursor is null ? "" : "&after=" + Uri.EscapeDataString(cursor)));
            Assert.InRange(page.Keys.Length, 1, 2);
            collected.AddRange(page.Keys);
            cursor = page.ContinuationKey;
            if (cursor is null) break;
            Assert.Equal(page.Keys[^1], cursor);
        }
        Assert.Null(cursor);
        Assert.Equal(keys.Where(key => key.StartsWith("prefix/", StringComparison.Ordinal)).Order(StringComparer.Ordinal), collected);
        var empty = await Page(node.Http, "/v1/registers?prefix=missing");
        Assert.Empty(empty.Keys);
        Assert.Null(empty.ContinuationKey);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Http1BodyLimitRejectsFixedAndChunkedRequestsBeforeMutation(bool chunked)
    {
        using var directory = new TestDirectory();
        var options = directory.Options();
        options.MaxRequestBodyBytes = 8;
        await using var node = await RunningNode.Start(options);
        byte[] oversized = [0, 1, 2, 3, 4, 5, 6, 7, 8];
        using var request = new HttpRequestMessage(HttpMethod.Put, ValueUrl("bounded"))
        {
            Content = chunked ? new ChunkedBytes(oversized) : new ByteArrayContent(oversized),
        };
        request.Headers.TryAddWithoutValidation("If-None-Match", "*");
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (chunked) request.Headers.TransferEncodingChunked = true;
        using var rejected = await node.Http.SendAsync(request);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, rejected.StatusCode);
        Assert.Equal(HttpVersion.Version11, rejected.Version);
        NoStore(rejected);
        Assert.Null(rejected.Headers.ETag);
        using var problem = JsonDocument.Parse(await rejected.Content.ReadAsByteArrayAsync());
        Assert.Equal("about:blank", problem.RootElement.GetProperty("type").GetString());
        Assert.Equal(413, problem.RootElement.GetProperty("status").GetInt32());
        Assert.Equal("body-read", problem.RootElement.GetProperty("code").GetString());
        Assert.Equal("not-applied", problem.RootElement.GetProperty("outcome").GetString());
        using var absent = await node.Http.GetAsync(ValueUrl("bounded"));
        Assert.Equal(HttpStatusCode.NotFound, absent.StatusCode);
        Assert.Null(absent.Headers.ETag);

        var boundary = new ByteString(oversized.AsSpan(0, 8));
        using var accepted = await Put(node.Http, "bounded", boundary);
        Assert.Equal(HttpStatusCode.Created, accepted.StatusCode);
        using var retained = await node.Http.GetAsync(ValueUrl("bounded"));
        Assert.Equal(HttpStatusCode.OK, retained.StatusCode);
        Assert.Equal(boundary.ToArray(), await retained.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task Http1BodyLimitIsIndependentOfGrpcMessageLimits()
    {
        using var directory = new TestDirectory();
        var options = directory.Options();
        options.MaxRequestBodyBytes = 1024;
        options.MaxReceiveMessageBytes = options.MaxSendMessageBytes = 1024 * 1024;
        var observed = new ControlledService();
        await using var node = await RunningNode.Start(options, observed);
        var payload = new ByteString(Enumerable.Range(0, 2048).Select(i => (byte)i).ToArray());

        using var rejected = await Put(node.Http, "independent-limits", payload);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, rejected.StatusCode);
        Assert.Empty(observed.Calls);
        using var problem = JsonDocument.Parse(await rejected.Content.ReadAsByteArrayAsync());
        Assert.Equal("body-read", problem.RootElement.GetProperty("code").GetString());
        Assert.Equal("not-applied", problem.RootElement.GetProperty("outcome").GetString());

        var created = await node.Grpc.CompareExchangeAsync("independent-limits", null, payload);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(payload, created.Current!.Value);
        var call = Assert.Single(observed.Calls);
        Assert.Equal("write", call.Operation);
        Assert.Equal(payload, call.Value);
        Assert.Null(call.Expected);
        Assert.Equal(created.Current, await node.Grpc.ReadAsync("independent-limits"));
        using var read = await node.Http.GetAsync(ValueUrl("independent-limits"));
        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        Assert.Equal(payload.ToArray(), await read.Content.ReadAsByteArrayAsync());
        Assert.Equal(Tag(created.Current.Version!.Value), read.Headers.ETag!.ToString());
    }

    [Fact]
    public void NodeOptionsRequestBodyLimitDefaultsAndRejectsNonpositiveValues()
    {
        Assert.Equal(4L * 1024 * 1024, new NodeOptions().MaxRequestBodyBytes);
        using var directory = new TestDirectory();
        var options = directory.Options();
        foreach (var invalid in new[] { 0L, -1L, long.MinValue })
        {
            options.MaxRequestBodyBytes = invalid;
            var error = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
            Assert.Equal(nameof(NodeOptions.MaxRequestBodyBytes), error.ParamName);
        }
        foreach (var valid in new[] { 1L, (long)int.MaxValue + 1, long.MaxValue })
        {
            options.MaxRequestBodyBytes = valid;
            options.Validate();
        }
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    private static string ValueUrl(string key) => "/v1/registers/value?key=" + Uri.EscapeDataString(key);
    private static void NoStore(HttpResponseMessage response) => Assert.Equal("no-store", response.Headers.CacheControl!.ToString());

    private static async Task<HttpResponseMessage> Put(HttpClient client, string key, ByteString value, string? tag = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, ValueUrl(key))
        {
            Content = new ByteArrayContent(value.ToArray()),
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        request.Headers.TryAddWithoutValidation(tag is null ? "If-None-Match" : "If-Match", tag ?? "*");
        return await client.SendAsync(request);
    }

    private static async Task<RegisterKeyPage> Page(HttpClient client, string url)
    {
        using var response = await client.GetAsync(url);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        NoStore(response);
        using var json = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync());
        return new([.. json.RootElement.GetProperty("keys").EnumerateArray().Select(key => key.GetString()!)],
            json.RootElement.GetProperty("continuation").GetString());
    }

    private sealed class TestDirectory : IDisposable
    {
        internal string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "fast-caspaxos-rest-" + Guid.NewGuid().ToString("N"));
        internal TestDirectory() => Directory.CreateDirectory(Path);
        internal NodeOptions Options() => new()
        {
            AcceptorId = "a",
            AcceptorDatabase = System.IO.Path.Combine(Path, "acceptor.db"),
            ProposerId = 1,
            ProposerDatabase = System.IO.Path.Combine(Path, "proposer.db"),
            Peers = [new() { Id = "a", Endpoint = "http://127.0.0.1:1", HttpEndpoint = "http://127.0.0.1:11" }],
        };
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }

    private sealed class RunningNode(NodeRuntime runtime, WebApplication app, string restAddress,
        string grpcAddress) : IAsyncDisposable
    {
        private readonly GrpcChannel _channel = GrpcChannel.ForAddress(grpcAddress);
        internal HttpClient Http { get; } = new()
        {
            BaseAddress = new Uri(restAddress),
            DefaultRequestVersion = HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Timeout = Watchdog,
        };
        internal GrpcRegisterService Grpc => new(_channel.CreateCallInvoker(), Watchdog);

        internal static async Task<RunningNode> Start(NodeOptions options, ControlledService? observed = null)
        {
            var runtime = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
            WebApplication? app = null;
            try
            {
                var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Testing" });
                builder.Configuration.Sources.Clear();
                builder.Logging.ClearProviders();
                ListenOptions? restListener = null;
                ListenOptions? grpcListener = null;
                builder.WebHost.ConfigureKestrel(server =>
                {
                    server.Listen(IPAddress.Loopback, 0, listen =>
                    {
                        listen.Protocols = HttpProtocols.Http1;
                        restListener = listen;
                    });
                    server.Listen(IPAddress.Loopback, 0, listen =>
                    {
                        listen.Protocols = HttpProtocols.Http2;
                        grpcListener = listen;
                    });
                });
                IRegisterService registers = runtime.Registers;
                if (observed is not null)
                {
                    observed.Read = registers.ReadAsync;
                    observed.Write = registers.CompareExchangeAsync;
                    observed.List = registers.ListKeysAsync;
                    registers = observed;
                }
                builder.Services.AddSingleton(registers);
                builder.Services.AddGrpc(grpc =>
                {
                    grpc.MaxReceiveMessageSize = options.MaxReceiveMessageBytes;
                    grpc.MaxSendMessageSize = options.MaxSendMessageBytes;
                });
                app = builder.Build();
                app.MapRegisterApi(maxRequestBodyBytes: options.MaxRequestBodyBytes);
                app.MapGrpcService<RegisterGrpcService>();
                using var timeout = new CancellationTokenSource(Watchdog);
                await app.StartAsync(timeout.Token);
                return new(runtime, app, $"http://{restListener!.IPEndPoint}", $"http://{grpcListener!.IPEndPoint}");
            }
            catch
            {
                if (app is not null) await app.DisposeAsync();
                await runtime.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Http.Dispose();
            _channel.Dispose();
            try
            {
                using var timeout = new CancellationTokenSource(Watchdog);
                await app.StopAsync(timeout.Token);
            }
            finally
            {
                try { await app.DisposeAsync(); }
                finally { await runtime.DisposeAsync(); }
            }
        }
    }

    private sealed class ChunkedBytes(byte[] bytes) : HttpContent
    {
        protected override bool TryComputeLength(out long length) { length = 0; return false; }
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            await stream.WriteAsync(bytes);
    }
}
