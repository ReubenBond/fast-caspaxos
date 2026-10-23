using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using FastCASPaxos.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FastCASPaxos.Rest.Tests;

public enum ApiOperation { Read, Write, List }

internal static class RestTestSupport
{
    internal static readonly TimeSpan Watchdog = TimeSpan.FromSeconds(30);
    internal static readonly ByteString Payload = new([0, 255, 128, 13, 10, 34, 92]);
    internal static readonly ByteString OpaqueVersion = new([255, 0, 17, 128, 42]);
    internal const string Secret = "private-database-path-and-secret";
    internal static string Tag(ByteString version) => $"\"{Convert.ToBase64String(version.ToArray())}\"";
    internal static TaskCompletionSource<T> Barrier<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal static DefaultHttpContext RequestFor(this EndpointHarness harness, ApiOperation operation,
        CancellationToken token = default) =>
        harness.Request(operation == ApiOperation.Write ? "PUT" : "GET",
            list: operation == ApiOperation.List, query: operation == ApiOperation.List ? "" : "?key=k", token: token);

    internal static void NoStore(HttpContext context) =>
        Assert.Equal("no-store", context.Response.Headers.CacheControl.ToString());

    internal static byte[] Bytes(HttpContext context) => ((MemoryStream)context.Response.Body).ToArray();

    internal static JsonElement Json(HttpContext context)
    {
        using var document = JsonDocument.Parse(Bytes(context));
        return document.RootElement.Clone();
    }

    internal static JsonElement Problem(HttpContext context, int status, string code, string outcome = "not-applied")
    {
        NoStore(context);
        Assert.Equal(status, context.Response.StatusCode);
        Assert.StartsWith("application/problem+json", context.Response.ContentType);
        var problem = Json(context);
        Assert.Equal("about:blank", problem.GetProperty("type").GetString());
        Assert.Equal(status, problem.GetProperty("status").GetInt32());
        Assert.Equal(code, problem.GetProperty("code").GetString());
        Assert.Equal(code, problem.GetProperty("title").GetString());
        Assert.False(string.IsNullOrWhiteSpace(problem.GetProperty("detail").GetString()));
        Assert.Equal(outcome, problem.GetProperty("outcome").GetString());
        Assert.DoesNotContain(Secret, Encoding.UTF8.GetString(Bytes(context)), StringComparison.Ordinal);
        return problem;
    }

    internal sealed record Call(string Operation, string Key, ByteString? Expected, ByteString? Value,
        string? After, int Limit, CancellationToken Token);

    internal sealed class ControlledService : IRegisterService
    {
        internal ConcurrentQueue<Call> Calls { get; } = new();
        internal Func<string, CancellationToken, ValueTask<RegisterValue>> Read { get; set; } =
            (_, _) => throw new InvalidOperationException("Unconfigured read.");
        internal Func<string, ByteString?, ByteString, CancellationToken, ValueTask<CompareExchangeResult>> Write { get; set; } =
            (_, _, _, _) => throw new InvalidOperationException("Unconfigured write.");
        internal Func<string, string?, int, CancellationToken, ValueTask<RegisterKeyPage>> List { get; set; } =
            (_, _, _, _) => throw new InvalidOperationException("Unconfigured list.");

        public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue(new("read", key, null, null, null, 0, cancellationToken));
            return Read(key, cancellationToken);
        }

        public ValueTask<CompareExchangeResult> CompareExchangeAsync(string key, ByteString? expectedVersion,
            ByteString value, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue(new("write", key, expectedVersion, value, null, 0, cancellationToken));
            return Write(key, expectedVersion, value, cancellationToken);
        }

        public ValueTask<RegisterKeyPage> ListKeysAsync(string prefix = "", string? afterKey = null,
            int limit = 100, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue(new("list", prefix, null, null, afterKey, limit, cancellationToken));
            return List(prefix, afterKey, limit, cancellationToken);
        }
    }

    internal sealed class EndpointHarness : IAsyncDisposable
    {
        private readonly WebApplication _app;
        internal ControlledService Service { get; }
        internal RecordingLogs Logs { get; } = new();
        internal RouteEndpoint[] Endpoints { get; }
        internal string Pattern { get; }

        internal EndpointHarness(ControlledService? service = null, string pattern = "/v1/registers",
            bool authorize = false)
        {
            Service = service ?? new();
            Pattern = pattern;
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Testing" });
            builder.Configuration.Sources.Clear();
            builder.Logging.ClearProviders();
            builder.Logging.AddProvider(Logs);
            builder.Services.AddSingleton<IRegisterService>(Service);
            _app = builder.Build();
            var group = _app.MapRegisterApi(pattern);
            group.AddEndpointFilter(async (invocation, next) =>
            {
                var result = await next(invocation);
                return result is IResult response ? new ObservableResult(response) : result;
            });
            if (authorize) group.RequireAuthorization("register-writers");
            Endpoints = ((IEndpointRouteBuilder)_app).DataSources
                .SelectMany(source => source.Endpoints).Cast<RouteEndpoint>().ToArray();
        }

        internal DefaultHttpContext Request(string method = "GET", bool list = false, string query = "?key=k",
            byte[]? body = null, CancellationToken token = default)
        {
            var context = new DefaultHttpContext { RequestServices = _app.Services, RequestAborted = token };
            context.Request.Method = method;
            context.Request.Path = Pattern + (list ? "" : "/value");
            context.Request.QueryString = new QueryString(query);
            context.Request.Body = new MemoryStream(body ?? []);
            context.Response.Body = new MemoryStream();
            if (method == "PUT")
            {
                context.Request.ContentType = "application/octet-stream";
                context.Request.Headers.IfNoneMatch = "*";
            }
            return context;
        }

        internal async Task Invoke(DefaultHttpContext context)
        {
            var endpoint = Assert.Single(Endpoints, endpoint =>
                endpoint.RoutePattern.RawText!.TrimEnd('/') == context.Request.Path.Value!.TrimEnd('/')
                && endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods.Contains(context.Request.Method));
            context.SetEndpoint(endpoint);
            await endpoint.RequestDelegate!(context).WaitAsync(Watchdog);
        }

        public ValueTask DisposeAsync() => _app.DisposeAsync();

        // Keep the real request token throughout the handler. Once it has selected an IResult,
        // decouple in-memory serialization from a disconnected socket, so tests can inspect
        // the selected problem body. This does not promise delivery over an aborted network.
        private sealed class ObservableResult(IResult result) : IResult
        {
            public async Task ExecuteAsync(HttpContext context)
            {
                var requestToken = context.RequestAborted;
                try
                {
                    context.RequestAborted = CancellationToken.None;
                    await result.ExecuteAsync(context);
                }
                finally { context.RequestAborted = requestToken; }
            }
        }
    }

    internal sealed record LogEntry(LogLevel Level, Exception? Exception, string Message);

    internal sealed class RecordingLogs : ILoggerProvider
    {
        internal ConcurrentQueue<LogEntry> Entries { get; } = new();
        public ILogger CreateLogger(string categoryName) => new Recorder(this);
        public void Dispose() { }

        private sealed class Recorder(RecordingLogs owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                owner.Entries.Enqueue(new(logLevel, exception, formatter(state, exception)));
        }
    }
}
