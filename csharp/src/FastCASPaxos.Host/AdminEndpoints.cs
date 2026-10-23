using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using FastCASPaxos.Model;
using FastCASPaxos.Rest;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using Microsoft.AspNetCore.Http.Features;

namespace FastCASPaxos.Host;

public sealed record AdminPeer(string Id, string GrpcEndpoint, string HttpEndpoint, string EventsUrl);
public sealed record NodeStatus(
    int SchemaVersion, string NodeId, int ProposerId, string BootId, DateTimeOffset StartedAt,
    string Cursor, IReadOnlyList<AdminPeer> Peers, StorageObservation Storage, NodeEvent? LastOperation,
    double HeartbeatMilliseconds, int HistoryCapacity);
public sealed record LocalStateView(
    string Key, Ballot PromisedBallot, Ballot AcceptedBallot, PayloadPreview? AcceptedValue, RegisterPreview? Register)
{
    public static LocalStateView From(LocalAcceptorState state)
    {
        HttpQuery.ValidateResponseText(state.Key);
        return new(state.Key, state.PromisedBallot, state.AcceptedBallot, PayloadPreview.From(state.AcceptedValue),
            state.AcceptedValue is null ? null : RegisterPreview.FromAccepted(state.AcceptedValue));
    }
}
public sealed record StateSnapshot(
    int SchemaVersion, string NodeId, string BootId, string Cursor, DateTimeOffset Timestamp, LocalStateView? Entry);
public sealed record StatePage(
    int SchemaVersion, string NodeId, string BootId, string Cursor, DateTimeOffset Timestamp,
    IReadOnlyList<LocalStateView> Entries, string? Continuation);

public static class AdminEndpoints
{
    public const string CorsPolicy = "FastCASPaxos.Admin";
    private static readonly JsonSerializerOptions Json = CreateJsonOptions();

    public static IServiceCollection AddAdminCors(this IServiceCollection services, NodeOptions options)
    {
        options.Validate();
        var origins = options.TrustedAdminOrigins.Concat(options.Peers.Select(peer => peer.HttpEndpoint))
            .Select(origin => NodeOptions.ValidateHttpOrigin(origin).GetLeftPart(UriPartial.Authority))
            .Distinct(StringComparer.Ordinal).ToArray();
        services.AddCors(cors => cors.AddPolicy(CorsPolicy, policy => policy
            .WithOrigins(origins).WithMethods("GET", "PUT")
            .WithHeaders("Content-Type", "If-Match", "If-None-Match", "Last-Event-ID", "Authorization")
            .WithExposedHeaders("ETag", "Location")));
        return services;
    }

    /// <summary>Maps one node's direct stream and local snapshots with application-owned authorization metadata.</summary>
    public static RouteGroupBuilder MapAdminApi(this IEndpointRouteBuilder endpoints, NodeRuntime node)
    {
        var group = endpoints.MapGroup("/v1/admin").WithTags("Node diagnostics").RequireCors(CorsPolicy);
        group.MapGet("/node", (HttpContext context) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            var journal = node.Diagnostics;
            return Results.Json(new NodeStatus(1, journal.NodeId, node.ProposerId, journal.BootId, journal.StartedAt,
                journal.Cursor, node.Peers, journal.Storage, journal.LastOperation,
                node.HeartbeatInterval.TotalMilliseconds, journal.Capacity), Json);
        });
        group.MapGet("/state", (HttpContext context, ILoggerFactory logging) => StateAsync(context, node, logging, false));
        group.MapGet("/states", (HttpContext context, ILoggerFactory logging) => StateAsync(context, node, logging, true));
        group.MapGet("/events", (HttpContext context, IHostApplicationLifetime lifetime, ILoggerFactory logging) =>
            EventsAsync(context, node, lifetime, logging));
        return group;
    }

    private static async Task<IResult> StateAsync(HttpContext context, NodeRuntime node, ILoggerFactory logging, bool list)
    {
        context.Response.Headers.CacheControl = "no-store";
        try
        {
            var query = Query(context, list ? ["prefix", "after", "limit"] : ["key"]);
            var journal = node.Diagnostics;
            // A pre-read watermark conservatively retains every invalidation which might race with the snapshot.
            var cursor = journal.Cursor;
            if (!list)
            {
                if (!query.TryGetValue("key", out var key)) throw new ArgumentException("A key is required.");
                var state = await node.Inspection.InspectAsync(key, context.RequestAborted);
                return Results.Json(new StateSnapshot(1, journal.NodeId, journal.BootId, cursor,
                    journal.Clock.GetUtcNow(), state is null ? null : LocalStateView.From(state)), Json);
            }
            var limit = 100;
            if (query.TryGetValue("limit", out var supplied)
                && (!int.TryParse(supplied, NumberStyles.None, CultureInfo.InvariantCulture, out limit) || limit is < 1 or > 100))
                throw new ArgumentException("limit must be a decimal integer from 1 through 100.");
            var page = await node.Inspection.InspectKeysAsync(
                query.GetValueOrDefault("prefix", ""), query.GetValueOrDefault("after"), limit, context.RequestAborted);
            return Results.Json(new StatePage(1, journal.NodeId, journal.BootId, cursor, journal.Clock.GetUtcNow(),
                page.Entries.Select(LocalStateView.From).ToArray(), page.ContinuationKey), Json);
        }
        catch (ArgumentException error) { return Results.Problem(statusCode: 400, title: "invalid", detail: error.Message); }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return Results.Problem(statusCode: 499, title: "cancelled");
        }
        catch (InvalidOperationException error) when (error.InnerException is System.Text.EncoderFallbackException)
        {
            logging.CreateLogger(typeof(AdminEndpoints)).LogError(error, "Local key cannot be represented as HTTP Unicode text.");
            return Results.Problem(statusCode: 500, title: "invalid-text",
                detail: "A local key cannot be represented as HTTP Unicode text; use the local inspection API.");
        }
        catch (Exception error)
        {
            logging.CreateLogger(typeof(AdminEndpoints)).LogError(error, "Local acceptor inspection failed.");
            return Results.Problem(statusCode: 503, title: "storage-unavailable", detail: "Consult node storage health and server logs.");
        }
    }

    private static async Task EventsAsync(
        HttpContext context, NodeRuntime node, IHostApplicationLifetime lifetime, ILoggerFactory logging)
    {
        context.Response.Headers.CacheControl = "no-store, no-transform";
        string? after;
        try
        {
            var query = Query(context, ["after"]);
            var header = context.Request.Headers["Last-Event-ID"];
            if (header.Count > 1) throw new ArgumentException("Last-Event-ID must appear once.");
            after = header.Count == 1 ? header[0] : query.GetValueOrDefault("after");
        }
        catch (ArgumentException error)
        {
            await Results.Problem(statusCode: 400, title: "invalid", detail: error.Message).ExecuteAsync(context);
            return;
        }
        if (!node.TryOpenEventStream())
        {
            await Results.Problem(statusCode: 503, title: "stream-capacity").ExecuteAsync(context);
            return;
        }
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, lifetime.ApplicationStopping);
        try
        {
            context.Response.ContentType = "text/event-stream";
            context.Response.Headers["X-Accel-Buffering"] = "no";
            context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
            await context.Response.StartAsync(cancellation.Token);
            await foreach (var item in node.Diagnostics.ReadAsync(after, node.HeartbeatInterval, cancellation.Token))
            {
                var id = item.Kind == "heartbeat" ? "" : $"id: {item.Id}\n";
                await context.Response.WriteAsync(
                    $"{id}event: {item.Kind}\ndata: {JsonSerializer.Serialize(item, Json)}\n\n", cancellation.Token);
                await context.Response.Body.FlushAsync(cancellation.Token);
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
        catch (IOException error)
        {
            logging.CreateLogger(typeof(AdminEndpoints)).LogDebug(error, "Diagnostic stream disconnected.");
        }
        finally { node.CloseEventStream(); }
    }

    private static Dictionary<string, string> Query(HttpContext context, string[] allowed)
        => HttpQuery.Parse(context.Request, allowed);

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new BallotConverter());
        return options;
    }

    private sealed class BallotConverter : JsonConverter<Ballot>
    {
        public override Ballot Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotSupportedException("Diagnostics serialize protocol ballots.");

        public override void Write(Utf8JsonWriter writer, Ballot value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteNumber("round", value.Round);
            writer.WriteNumber("proposer", value.Proposer);
            writer.WriteEndObject();
        }
    }
}
