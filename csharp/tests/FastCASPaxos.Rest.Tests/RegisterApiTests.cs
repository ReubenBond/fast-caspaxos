using FastCASPaxos.Server;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Xunit;
using static FastCASPaxos.Rest.Tests.RestTestSupport;

namespace FastCASPaxos.Rest.Tests;

public sealed class RegisterApiTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Get_PreservesRawBytesAndOpaqueEtag(bool empty)
    {
        var payload = empty ? ByteString.Empty : Payload;
        var version = empty ? ByteString.Empty : OpaqueVersion;
        var service = new ControlledService { Read = (_, _) => ValueTask.FromResult(new RegisterValue(payload, version)) };
        await using var harness = new EndpointHarness(service);
        using var cancellation = new CancellationTokenSource();
        var context = harness.Request(token: cancellation.Token);
        await harness.Invoke(context);
        Assert.Equal(200, context.Response.StatusCode);
        Assert.Equal("application/octet-stream", context.Response.ContentType);
        Assert.Equal(payload.ToArray(), Bytes(context));
        Assert.Equal(empty ? "\"\"" : "\"/wARgCo=\"", context.Response.Headers.ETag.ToString());
        NoStore(context);
        var call = Assert.Single(service.Calls);
        Assert.Equal("k", call.Key);
        Assert.Equal(cancellation.Token, call.Token);
    }

    [Fact]
    public async Task Get_AbsentIsNotAnEmptyRegister()
    {
        var service = new ControlledService { Read = (_, _) => ValueTask.FromResult(new RegisterValue(null, null)) };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request();
        await harness.Invoke(context);
        Problem(context, 404, "missing");
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
        Assert.Single(service.Calls);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Put_CreateAndUpdatePreserveBytesVersionsAndLocation(bool update, bool empty)
    {
        var payload = empty ? ByteString.Empty : Payload;
        var expected = empty ? ByteString.Empty : OpaqueVersion;
        var next = new ByteString([4, 3, 2, 1]);
        var service = new ControlledService
        {
            Write = (_, _, _, _) => ValueTask.FromResult(
                new CompareExchangeResult(CompareExchangeStatus.Applied, new(payload, next))),
        };
        await using var harness = new EndpointHarness(service);
        const string key = "/.././literal%2f +\0\U0001F600";
        using var cancellation = new CancellationTokenSource();
        var context = harness.Request("PUT", query: "?key=" + Uri.EscapeDataString(key),
            body: payload.ToArray(), token: cancellation.Token);
        context.Request.PathBase = "/gateway";
        if (update)
        {
            context.Request.Headers.Remove("If-None-Match");
            context.Request.Headers.IfMatch = Tag(expected);
        }
        await harness.Invoke(context);
        Assert.Equal(update ? 204 : 201, context.Response.StatusCode);
        Assert.Equal(Tag(next), context.Response.Headers.ETag.ToString());
        Assert.Empty(Bytes(context));
        NoStore(context);
        var call = Assert.Single(service.Calls);
        Assert.Equal(key, call.Key);
        Assert.Equal(update ? expected : null, call.Expected);
        Assert.Equal(payload, call.Value);
        Assert.Equal(cancellation.Token, call.Token);
        Assert.Equal(update ? "" : "/gateway/v1/registers/value?key=" + Uri.EscapeDataString(key),
            context.Response.Headers.Location.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Put_MismatchUsesOnlyEstablishedCurrentEtag(bool present)
    {
        var current = present ? new RegisterValue(Payload, OpaqueVersion) : new RegisterValue(null, null);
        var service = new ControlledService
        {
            Write = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult(CompareExchangeStatus.Mismatch, current)),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT");
        await harness.Invoke(context);
        Problem(context, 412, "mismatch");
        Assert.Equal(present ? Tag(OpaqueVersion) : "", context.Response.Headers.ETag.ToString());
        Assert.False(context.Response.Headers.ContainsKey("Location"));
        Assert.Single(service.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Put_OpaqueVersionValidationRemainsServiceOwned(bool empty)
    {
        var expected = empty ? ByteString.Empty : new ByteString([251, 255, 254]);
        var service = new ControlledService
        {
            Write = (_, _, _, _) => throw new ArgumentException(Secret),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT", body: Payload.ToArray());
        context.Request.Headers.Remove("If-None-Match");
        context.Request.Headers.IfMatch = empty ? "\"\"" : "\"+//+\"";
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        var call = Assert.Single(service.Calls);
        Assert.Equal(expected, call.Expected);
        Assert.Equal(Payload, call.Value);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum PageCase { Defaults, EmptyCursor, PrefixAndCursor, MaxLimit, LeadingZeroLimit, EmptyPage }

    [Theory]
    [InlineData(PageCase.Defaults)]
    [InlineData(PageCase.EmptyCursor)]
    [InlineData(PageCase.PrefixAndCursor)]
    [InlineData(PageCase.MaxLimit)]
    [InlineData(PageCase.LeadingZeroLimit)]
    [InlineData(PageCase.EmptyPage)]
    public async Task List_PreservesArgumentsOrderAndCursor(PageCase scenario)
    {
        const string prefix = "dir/../% +\0\U0001F600";
        const string cursor = "cursor/.\0\U0001F680";
        var query = scenario switch
        {
            PageCase.EmptyCursor => "?after=&limit=1",
            PageCase.PrefixAndCursor => "?prefix=" + Uri.EscapeDataString(prefix) + "&after=" + Uri.EscapeDataString(cursor) + "&limit=2",
            PageCase.MaxLimit => "?limit=2147483647",
            PageCase.LeadingZeroLimit => "?limit=0002",
            _ => "",
        };
        string[] keys = scenario == PageCase.EmptyPage ? [] : ["z", "", "a/\0\U0001F600"];
        string? continuation = scenario switch { PageCase.EmptyCursor => "", PageCase.PrefixAndCursor => cursor, _ => null };
        var service = new ControlledService { List = (_, _, _, _) => ValueTask.FromResult(new RegisterKeyPage(keys, continuation)) };
        await using var harness = new EndpointHarness(service);
        using var cancellation = new CancellationTokenSource();
        var context = harness.Request(list: true, query: query, token: cancellation.Token);
        await harness.Invoke(context);
        Assert.Equal(200, context.Response.StatusCode);
        Assert.StartsWith("application/json", context.Response.ContentType);
        NoStore(context);
        var json = Json(context);
        Assert.Equal(["keys", "continuation"], json.EnumerateObject().Select(property => property.Name));
        Assert.Equal(keys, json.GetProperty("keys").EnumerateArray().Select(item => item.GetString()));
        Assert.Equal(continuation, json.GetProperty("continuation").GetString());
        var call = Assert.Single(service.Calls);
        Assert.Equal(scenario == PageCase.PrefixAndCursor ? prefix : "", call.Key);
        Assert.Equal(scenario switch { PageCase.EmptyCursor => "", PageCase.PrefixAndCursor => cursor, _ => null }, call.After);
        Assert.Equal(scenario switch { PageCase.EmptyCursor => 1, PageCase.PrefixAndCursor or PageCase.LeadingZeroLimit => 2,
            PageCase.MaxLimit => int.MaxValue, _ => 100 }, call.Limit);
        Assert.Equal(cancellation.Token, call.Token);
    }

    [Fact]
    public async Task MapRegisterApi_SupportsCustomPrefixAuthorizationAndResponseMetadata()
    {
        await using var harness = new EndpointHarness(pattern: "/custom/durable", authorize: true);
        Assert.Equal(3, harness.Endpoints.Length);
        foreach (var endpoint in harness.Endpoints)
        {
            Assert.StartsWith("/custom/durable", endpoint.RoutePattern.RawText);
            Assert.Equal("register-writers", Assert.Single(endpoint.Metadata.GetOrderedMetadata<IAuthorizeData>()).Policy);
            var responses = endpoint.Metadata.GetOrderedMetadata<IProducesResponseTypeMetadata>();
            Assert.All(new[] { 400, 499, 500, 503 }, status => Assert.Contains(responses, response =>
                response.StatusCode == status && response.ContentTypeIs("application/problem+json")));
            var method = Assert.Single(endpoint.Metadata.GetMetadata<HttpMethodMetadata>()!.HttpMethods);
            if (method == "PUT")
            {
                Assert.All(new[] { 201, 204, 412, 413, 415, 428 },
                    status => Assert.Contains(responses, response => response.StatusCode == status));
                Assert.Contains("application/octet-stream", endpoint.Metadata.GetMetadata<IAcceptsMetadata>()!.ContentTypes);
            }
            else if (endpoint.RoutePattern.RawText!.EndsWith("/value", StringComparison.Ordinal))
                Assert.Contains(responses, response => response.StatusCode == 200 && response.ContentTypeIs("application/octet-stream"));
            else
                Assert.Contains(responses, response => response.StatusCode == 200 && response.Type == typeof(RegisterKeyList));
        }
    }
}

internal static class ResponseMetadataAssertions
{
    internal static bool ContentTypeIs(this IProducesResponseTypeMetadata metadata, string expected) =>
        metadata.ContentTypes.Contains(expected);
}
