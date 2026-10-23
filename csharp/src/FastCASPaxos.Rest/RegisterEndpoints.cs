using System.Globalization;
using System.Text;
using FastCASPaxos.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;

namespace FastCASPaxos.Rest;

public sealed record RegisterKeyList(IReadOnlyList<string> Keys, string? Continuation);

public static class RegisterEndpoints
{
    private static readonly UTF8Encoding Utf8 = new(false, true);

    /// <summary>Maps the conditional register API using an application-owned IRegisterService.</summary>
    public static RouteGroupBuilder MapRegisterApi(
        this IEndpointRouteBuilder endpoints, string pattern = "/v1/registers")
    {
        var group = endpoints.MapGroup(pattern).WithTags("Registers");
        var read = group.MapGet("/value", (HttpContext context, IRegisterService service, ILoggerFactory logging) =>
                ServeAsync(context, service, logging, Operation.Read))
            .Produces(StatusCodes.Status200OK, contentType: "application/octet-stream")
            .ProducesProblem(StatusCodes.Status404NotFound);
        var write = group.MapPut("/value", (HttpContext context, IRegisterService service, ILoggerFactory logging) =>
                ServeAsync(context, service, logging, Operation.Write))
            .Accepts<byte[]>("application/octet-stream")
            .Produces(StatusCodes.Status201Created)
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status412PreconditionFailed)
            .ProducesProblem(StatusCodes.Status413PayloadTooLarge)
            .ProducesProblem(StatusCodes.Status415UnsupportedMediaType)
            .ProducesProblem(StatusCodes.Status428PreconditionRequired);
        var list = group.MapGet("", (HttpContext context, IRegisterService service, ILoggerFactory logging) =>
                ServeAsync(context, service, logging, Operation.List))
            .Produces<RegisterKeyList>();
        foreach (var route in new[] { read, write, list })
            route.ProducesProblem(StatusCodes.Status400BadRequest)
                .ProducesProblem(StatusCodes.Status499ClientClosedRequest)
                .ProducesProblem(StatusCodes.Status500InternalServerError)
                .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
        return group;
    }

    private static async Task<IResult> ServeAsync(
        HttpContext context, IRegisterService service, ILoggerFactory logging, Operation operation)
    {
        context.Response.Headers.CacheControl = "no-store";
        var logger = logging.CreateLogger(typeof(RegisterEndpoints));
        var dispatched = false;
        try
        {
            context.RequestAborted.ThrowIfCancellationRequested();
            var query = ParseQuery(context.Request, operation);
            ValidateConditionalHeaders(context.Request, operation);
            if (operation == Operation.List)
            {
                var limit = 100;
                if (query.TryGetValue("limit", out var suppliedLimit)
                    && (!int.TryParse(suppliedLimit, NumberStyles.None, CultureInfo.InvariantCulture, out limit) || limit <= 0))
                    throw Invalid("limit must be a positive decimal integer.");
                dispatched = true;
                var page = await service.ListKeysAsync(
                    query.GetValueOrDefault("prefix", ""), query.GetValueOrDefault("after"), limit, context.RequestAborted);
                foreach (var key in page.Keys) ValidateResponseText(key);
                if (page.ContinuationKey is { } continuation) ValidateResponseText(continuation);
                return Results.Ok(new RegisterKeyList(page.Keys, page.ContinuationKey));
            }

            if (!query.TryGetValue("key", out var registerKey)) throw Invalid("A key query parameter is required.");
            if (operation == Operation.Read)
            {
                dispatched = true;
                var current = await service.ReadAsync(registerKey, context.RequestAborted);
                if (!current.IsPresent)
                    return Problem(404, "missing", "The register is absent.");
                context.Response.Headers.ETag = ETag(current.Version!);
                return Results.Bytes(current.Value!.ToArray(), "application/octet-stream");
            }

            var expected = ExpectedVersion(context.Request);
            if (!MediaTypeHeaderValue.TryParse(context.Request.ContentType, out var mediaType)
                || !string.Equals(mediaType.MediaType.Value, "application/octet-stream", StringComparison.OrdinalIgnoreCase)
                || context.Request.Headers.ContainsKey(HeaderNames.ContentEncoding))
                throw new RequestFailure(415, "Use application/octet-stream with an unencoded body.");
            if (context.Request.Headers.ContainsKey(HeaderNames.ContentRange))
                throw Invalid("PUT supplies the complete register value.");

            // Kestrel enforces the configured body limit for both Content-Length and streamed bodies.
            using var body = new MemoryStream();
            await context.Request.Body.CopyToAsync(body, context.RequestAborted);
            context.RequestAborted.ThrowIfCancellationRequested();
            var payload = new ByteString(body.GetBuffer().AsSpan(0, checked((int)body.Length)));
            dispatched = true;
            var result = await service.CompareExchangeAsync(registerKey, expected, payload, context.RequestAborted);
            switch (result.Status)
            {
                case CompareExchangeStatus.Applied:
                    context.Response.Headers.ETag = ETag(result.Current!.Version!);
                    if (expected is not null) return Results.NoContent();
                    context.Response.Headers.Location =
                        $"{context.Request.PathBase}{context.Request.Path}?key={Uri.EscapeDataString(registerKey)}";
                    return Results.StatusCode(StatusCodes.Status201Created);
                case CompareExchangeStatus.Mismatch:
                    if (result.Current!.IsPresent) context.Response.Headers.ETag = ETag(result.Current.Version!);
                    return Problem(412, "mismatch", "The register's established state failed the write precondition.");
                case CompareExchangeStatus.Unknown:
                    if (result.Error is not null) logger.LogWarning(result.Error, "Register write outcome is unknown.");
                    var extensions = new Dictionary<string, object?>();
                    if (result.Current is { IsPresent: true })
                        extensions["currentEtag"] = ETag(result.Current.Version!);
                    if (result.Error is not null)
                        extensions["cause"] = result.Error switch
                        {
                            OperationCanceledException => "cancelled",
                            TimeoutException => "deadline",
                            _ => "internal",
                        };
                    return Problem(503, "unknown",
                        "The write may have applied. Reconcile the register using application semantics.",
                        "unknown", extensions);
                default:
                    throw new InvalidOperationException("The register service returned an unrecognized write outcome.");
            }
        }
        catch (RequestFailure error)
        {
            return Problem(error.StatusCode, "invalid", error.Message);
        }
        catch (BadHttpRequestException error) when (!dispatched)
        {
            return Problem(error.StatusCode, "body-read", "The HTTP request body could not be accepted.");
        }
        catch (IOException error) when (!dispatched)
        {
            logger.LogInformation(error, "HTTP request body failed before register dispatch.");
            return Problem(400, "body-read", "The HTTP request body could not be read.");
        }
        catch (ArgumentException error) when (dispatched)
        {
            logger.LogInformation(error, "The register service rejected an argument before mutation.");
            return Problem(400, "invalid", "The register service rejected a request argument or version token.");
        }
        catch (RegisterBusyException)
        {
            return Problem(503, "busy", "The register service is at its configured capacity.");
        }
        catch (RegisterUnavailableException error)
        {
            logger.LogWarning(error, "Register operation reached its deadline before mutation.");
            return Problem(503, "unavailable", "A register quorum was unavailable before the operation deadline.");
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            return Problem(499, "cancelled", "The request was cancelled before mutation.");
        }
        catch (Exception error)
        {
            logger.LogError(error, "HTTP register operation failed.");
            return Problem(500, "internal", "The operation failed; consult the server log.",
                operation == Operation.Write && dispatched ? "unknown" : "not-applied");
        }
    }

    private static ByteString? ExpectedVersion(HttpRequest request)
    {
        var create = request.Headers.ContainsKey(HeaderNames.IfNoneMatch);
        var update = request.Headers.ContainsKey(HeaderNames.IfMatch);
        if (!create && !update)
            throw new RequestFailure(428, "Supply If-None-Match: * or one strong If-Match ETag.");
        if (create && update) throw Invalid("Supply exactly one write precondition.");
        var values = request.Headers[create ? HeaderNames.IfNoneMatch : HeaderNames.IfMatch];
        if (values.Count != 1) throw Invalid("Supply exactly one write precondition.");
        var tag = values[0]!.Trim();
        if (create)
        {
            if (tag != "*") throw Invalid("If-None-Match must be *.");
            return null;
        }

        if (tag.Length < 2 || tag[0] != '"' || tag[^1] != '"')
            throw Invalid("If-Match must be one strong quoted Base64 ETag.");
        var encoded = tag[1..^1];
        var bytes = new byte[encoded.Length];
        if (!Convert.TryFromBase64String(encoded, bytes, out var count)
            || Convert.ToBase64String(bytes, 0, count) != encoded)
            throw Invalid("If-Match must contain canonical Base64 version bytes.");
        return new ByteString(bytes.AsSpan(0, count));
    }

    private static void ValidateConditionalHeaders(HttpRequest request, Operation operation)
    {
        if (request.Headers.ContainsKey(HeaderNames.IfModifiedSince)
            || request.Headers.ContainsKey(HeaderNames.IfUnmodifiedSince)
            || request.Headers.ContainsKey(HeaderNames.IfRange)
            || (operation != Operation.Write && (request.Headers.ContainsKey(HeaderNames.IfMatch)
                || request.Headers.ContainsKey(HeaderNames.IfNoneMatch))))
            throw Invalid("Conditional headers are supported only as If-Match or If-None-Match on PUT.");
    }

    private static Dictionary<string, string> ParseQuery(HttpRequest request, Operation operation)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var raw = request.QueryString.Value;
        if (string.IsNullOrEmpty(raw)) return result;
        foreach (var pair in raw.AsSpan(1).ToString().Split('&'))
        {
            var separator = pair.IndexOf('=');
            if (separator < 0) throw Invalid("Query parameters require name=value.");
            var name = DecodeQueryComponent(pair[..separator]);
            var value = DecodeQueryComponent(pair[(separator + 1)..]);
            var allowed = operation == Operation.List ? name is "prefix" or "after" or "limit" : name == "key";
            if (!allowed || !result.TryAdd(name, value))
                throw Invalid("Query parameter names must be supported and appear once.");
        }
        return result;
    }

    private static string DecodeQueryComponent(string encoded)
    {
        try
        {
            var bytes = Utf8.GetBytes(encoded);
            var length = 0;
            for (var i = 0; i < bytes.Length; i++)
            {
                var value = bytes[i];
                if (value == '%')
                {
                    if (i + 2 >= bytes.Length || Hex(bytes[i + 1]) < 0 || Hex(bytes[i + 2]) < 0)
                        throw Invalid("Query parameters require valid percent-encoded UTF-8.");
                    value = (byte)((Hex(bytes[i + 1]) << 4) | Hex(bytes[i + 2]));
                    i += 2;
                }
                else if (value == '+') value = (byte)' ';
                bytes[length++] = value;
            }
            return Utf8.GetString(bytes, 0, length);
        }
        catch (EncoderFallbackException) { throw Invalid("Query parameters require well-formed Unicode."); }
        catch (DecoderFallbackException) { throw Invalid("Query parameters require valid percent-encoded UTF-8."); }
    }

    private static int Hex(byte value) => value switch
    {
        >= (byte)'0' and <= (byte)'9' => value - '0',
        >= (byte)'A' and <= (byte)'F' => value - 'A' + 10,
        >= (byte)'a' and <= (byte)'f' => value - 'a' + 10,
        _ => -1,
    };

    private static void ValidateResponseText(string value)
    {
        try { Utf8.GetByteCount(value); }
        catch (EncoderFallbackException error)
        {
            throw new InvalidOperationException("A listed key cannot be represented as HTTP Unicode text.", error);
        }
    }

    private static string ETag(ByteString version) => $"\"{Convert.ToBase64String(version.ToArray())}\"";

    private static IResult Problem(int status, string code, string detail, string outcome = "not-applied",
        Dictionary<string, object?>? extensions = null)
    {
        extensions ??= [];
        extensions["code"] = code;
        extensions["outcome"] = outcome;
        return Results.Problem(new ProblemDetails
        {
            Type = "about:blank",
            Status = status,
            Title = code,
            Detail = detail,
            Extensions = extensions,
        });
    }

    private static RequestFailure Invalid(string message) => new(400, message);
    private sealed class RequestFailure(int statusCode, string message) : Exception(message)
    {
        public int StatusCode { get; } = statusCode;
    }
    private enum Operation { Read, Write, List }
}
