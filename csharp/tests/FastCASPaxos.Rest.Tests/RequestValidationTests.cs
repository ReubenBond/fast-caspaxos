using FastCASPaxos.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Xunit;
using static FastCASPaxos.Rest.Tests.RestTestSupport;

namespace FastCASPaxos.Rest.Tests;

public sealed class RequestValidationTests
{
    public enum Precondition
    {
        Missing, DuplicateMatch, MatchList, Weak, Wildcard, Both, EmptyMatch, Unquoted,
        BrokenQuote, InvalidBase64, MissingPadding, ExcessPadding, NoncanonicalPadBits,
        EmbeddedWhitespace, UrlBase64, NoneMatchTag, NoneMatchList, DuplicateNoneMatch,
        EmptyNoneMatch, ModifiedSince, UnmodifiedSince, IfRange,
    }

    public static IEnumerable<object[]> Preconditions() =>
        Enum.GetValues<Precondition>().Select(value => new object[] { value });

    [Theory]
    [MemberData(nameof(Preconditions))]
    public async Task Put_InvalidPreconditionsNeverDispatch(Precondition scenario)
    {
        await using var harness = new EndpointHarness();
        var context = harness.Request("PUT");
        context.Request.Headers.Remove("If-None-Match");
        var tag = Tag(OpaqueVersion);
        switch (scenario)
        {
            case Precondition.Missing: break;
            case Precondition.DuplicateMatch: context.Request.Headers.IfMatch = new StringValues([tag, tag]); break;
            case Precondition.MatchList: context.Request.Headers.IfMatch = tag + ", " + tag; break;
            case Precondition.Weak: context.Request.Headers.IfMatch = "W/" + tag; break;
            case Precondition.Wildcard: context.Request.Headers.IfMatch = "*"; break;
            case Precondition.Both: context.Request.Headers.IfMatch = tag; context.Request.Headers.IfNoneMatch = "*"; break;
            case Precondition.EmptyMatch: context.Request.Headers.IfMatch = ""; break;
            case Precondition.Unquoted: context.Request.Headers.IfMatch = "AA=="; break;
            case Precondition.BrokenQuote: context.Request.Headers.IfMatch = "\"AA=="; break;
            case Precondition.InvalidBase64: context.Request.Headers.IfMatch = "\"!!!!\""; break;
            case Precondition.MissingPadding: context.Request.Headers.IfMatch = "\"AA\""; break;
            case Precondition.ExcessPadding: context.Request.Headers.IfMatch = "\"AA===\""; break;
            case Precondition.NoncanonicalPadBits: context.Request.Headers.IfMatch = "\"AB==\""; break;
            case Precondition.EmbeddedWhitespace: context.Request.Headers.IfMatch = "\"A A==\""; break;
            case Precondition.UrlBase64: context.Request.Headers.IfMatch = "\"_w==\""; break;
            case Precondition.NoneMatchTag: context.Request.Headers.IfNoneMatch = tag; break;
            case Precondition.NoneMatchList: context.Request.Headers.IfNoneMatch = "*, *"; break;
            case Precondition.DuplicateNoneMatch: context.Request.Headers.IfNoneMatch = new StringValues(["*", "*"]); break;
            case Precondition.EmptyNoneMatch: context.Request.Headers.IfNoneMatch = ""; break;
            case Precondition.ModifiedSince:
            case Precondition.UnmodifiedSince:
            case Precondition.IfRange:
                context.Request.Headers.IfNoneMatch = "*";
                context.Request.Headers[scenario switch
                {
                    Precondition.ModifiedSince => "If-Modified-Since",
                    Precondition.UnmodifiedSince => "If-Unmodified-Since",
                    _ => "If-Range",
                }] = "Thu, 01 Jan 1970 00:00:00 GMT";
                break;
        }
        await harness.Invoke(context);
        Problem(context, scenario == Precondition.Missing ? 428 : 400, "invalid");
        Assert.Empty(harness.Service.Calls);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum Conditional { Match, NoneMatch, ModifiedSince, UnmodifiedSince, IfRange }

    public static IEnumerable<object[]> ReadConditions() =>
        from operation in new[] { ApiOperation.Read, ApiOperation.List }
        from header in Enum.GetValues<Conditional>()
        select new object[] { operation, header };

    [Theory]
    [MemberData(nameof(ReadConditions))]
    public async Task Get_ConditionalHeadersNeverDispatch(ApiOperation operation, Conditional header)
    {
        await using var harness = new EndpointHarness();
        var context = harness.RequestFor(operation);
        context.Request.Headers[header switch
        {
            Conditional.Match => "If-Match",
            Conditional.NoneMatch => "If-None-Match",
            Conditional.ModifiedSince => "If-Modified-Since",
            Conditional.UnmodifiedSince => "If-Unmodified-Since",
            _ => "If-Range",
        }] = header is Conditional.Match or Conditional.NoneMatch ? Tag(OpaqueVersion) : "Thu, 01 Jan 1970 00:00:00 GMT";
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        Assert.Empty(harness.Service.Calls);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum BadQuery
    {
        UnknownName, OtherEndpointName, DuplicateName, EncodedDuplicateName, MissingEquals, EmptyPair, EmptyName,
        PercentOnly, ShortEscape, NonHexEscape, InvalidUtf8, OverlongUtf8, TruncatedUtf8,
        Utf8Surrogate, AboveUnicodeLimit, LoneHighSurrogate, LoneLowSurrogate, BadNameEncoding,
    }

    public static IEnumerable<object[]> BadQueries() =>
        from operation in Enum.GetValues<ApiOperation>()
        from scenario in Enum.GetValues<BadQuery>()
        select new object[] { operation, scenario };

    [Theory]
    [MemberData(nameof(BadQueries))]
    public async Task InvalidQueriesNeverDispatch(ApiOperation operation, BadQuery scenario)
    {
        await using var harness = new EndpointHarness();
        var context = harness.RequestFor(operation);
        var name = operation == ApiOperation.List ? "prefix" : "key";
        var raw = scenario switch
        {
            BadQuery.UnknownName => "?unsupported=x",
            BadQuery.OtherEndpointName => operation == ApiOperation.List ? "?key=x" : "?key=x&prefix=p",
            BadQuery.DuplicateName => $"?{name}=a&{name}=b",
            BadQuery.EncodedDuplicateName => $"?{name}=a&%{(int)name[0]:X2}{name[1..]}=b",
            BadQuery.MissingEquals => "?" + name,
            BadQuery.EmptyPair => $"?{name}=a&",
            BadQuery.EmptyName => "?=a",
            BadQuery.BadNameEncoding => "?%FF=x",
            _ => $"?{name}=" + (scenario switch
            {
                BadQuery.PercentOnly => "%",
                BadQuery.ShortEscape => "%2",
                BadQuery.NonHexEscape => "%GG",
                BadQuery.InvalidUtf8 => "%FF",
                BadQuery.OverlongUtf8 => "%C0%AF",
                BadQuery.TruncatedUtf8 => "%E2%82",
                BadQuery.Utf8Surrogate => "%ED%A0%80",
                BadQuery.AboveUnicodeLimit => "%F4%90%80%80",
                BadQuery.LoneHighSurrogate => "\uD800",
                BadQuery.LoneLowSurrogate => "\uDC00",
                _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
            }),
        };
        context.Request.QueryString = new QueryString(raw);
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        Assert.Empty(harness.Service.Calls);
    }

    [Theory]
    [InlineData(ApiOperation.Read)]
    [InlineData(ApiOperation.Write)]
    public async Task Value_MissingKeyNeverDispatches(ApiOperation operation)
    {
        await using var harness = new EndpointHarness();
        var context = harness.RequestFor(operation);
        context.Request.QueryString = QueryString.Empty;
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        Assert.Empty(harness.Service.Calls);
    }

    public enum KeyCase { Empty, PathSegments, EncodedOnce, PlusAndSpace, NulSupplementary, LiteralUnicode }
    public static IEnumerable<object[]> Keys() =>
        from operation in new[] { ApiOperation.Read, ApiOperation.Write }
        from key in Enum.GetValues<KeyCase>()
        select new object[] { operation, key };

    [Theory]
    [MemberData(nameof(Keys))]
    public async Task Keys_AreDecodedExactlyOnceWithoutPathNormalization(ApiOperation operation, KeyCase scenario)
    {
        var (encoded, expected) = scenario switch
        {
            KeyCase.Empty => ("", ""),
            KeyCase.PathSegments => ("%2f..%2f.%2f%5cfile.txt", "/.././\\file.txt"),
            KeyCase.EncodedOnce => ("%252F%2525", "%2F%25"),
            KeyCase.PlusAndSpace => ("a+b%2Bc%20d", "a b+c d"),
            KeyCase.NulSupplementary => ("%00%F0%9F%98%80%C3%A9", "\0\U0001F600é"),
            KeyCase.LiteralUnicode => ("é\U0001F680", "é\U0001F680"),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
        var service = new ControlledService
        {
            Read = (_, _) => ValueTask.FromResult(new RegisterValue(Payload, OpaqueVersion)),
            Write = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult(CompareExchangeStatus.Applied, new(Payload, OpaqueVersion))),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.RequestFor(operation);
        context.Request.QueryString = new QueryString("?%6bey=" + encoded);
        await harness.Invoke(context);
        Assert.Equal(operation == ApiOperation.Read ? 200 : 201, context.Response.StatusCode);
        Assert.Equal(expected, Assert.Single(service.Calls).Key);
        Assert.Equal(Tag(OpaqueVersion), context.Response.Headers.ETag.ToString());
        NoStore(context);
    }

    public enum BadLimit { Empty, Zero, Negative, Overflow, PlusSign, Whitespace, Fraction, Exponent, UnicodeDigit }
    public static IEnumerable<object[]> BadLimits() =>
        Enum.GetValues<BadLimit>().Select(value => new object[] { value });

    [Theory]
    [MemberData(nameof(BadLimits))]
    public async Task List_InvalidLimitsNeverDispatch(BadLimit scenario)
    {
        var limit = scenario switch
        {
            BadLimit.Empty => "",
            BadLimit.Zero => "0",
            BadLimit.Negative => "-1",
            BadLimit.Overflow => "2147483648",
            BadLimit.PlusSign => "%2B1",
            BadLimit.Whitespace => "%201",
            BadLimit.Fraction => "1.0",
            BadLimit.Exponent => "1e2",
            BadLimit.UnicodeDigit => "%D9%A1",
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
        await using var harness = new EndpointHarness();
        var context = harness.Request(list: true, query: "?limit=" + limit);
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        Assert.Empty(harness.Service.Calls);
    }

    public enum ListQuery { DuplicateAfter, DuplicateLimit, InvalidAfterEncoding, InvalidPrefixEncoding }

    [Theory]
    [InlineData(ListQuery.DuplicateAfter)]
    [InlineData(ListQuery.DuplicateLimit)]
    [InlineData(ListQuery.InvalidAfterEncoding)]
    [InlineData(ListQuery.InvalidPrefixEncoding)]
    public async Task List_RejectsInvalidCursorAndDuplicatePagingFields(ListQuery scenario)
    {
        var query = scenario switch
        {
            ListQuery.DuplicateAfter => "?after=&after=x",
            ListQuery.DuplicateLimit => "?limit=1&limit=2",
            ListQuery.InvalidAfterEncoding => "?prefix=valid&after=%ED%A0%80",
            _ => "?prefix=%E2%82&after=valid",
        };
        await using var harness = new EndpointHarness();
        var context = harness.Request(list: true, query: query);
        await harness.Invoke(context);
        Problem(context, 400, "invalid");
        Assert.Empty(harness.Service.Calls);
    }

    public enum Representation { MissingType, TextType, InvalidType, Encoding, IdentityEncoding, Range }
    public static IEnumerable<object[]> Representations() =>
        Enum.GetValues<Representation>().Select(value => new object[] { value });

    [Theory]
    [MemberData(nameof(Representations))]
    public async Task Put_InvalidRepresentationNeverDispatch(Representation scenario)
    {
        await using var harness = new EndpointHarness();
        var context = harness.Request("PUT");
        switch (scenario)
        {
            case Representation.MissingType: context.Request.ContentType = null; break;
            case Representation.TextType: context.Request.ContentType = "text/plain"; break;
            case Representation.InvalidType: context.Request.ContentType = "invalid media type"; break;
            case Representation.Encoding: context.Request.Headers.ContentEncoding = "gzip"; break;
            case Representation.IdentityEncoding: context.Request.Headers.ContentEncoding = "identity"; break;
            case Representation.Range: context.Request.Headers.ContentRange = "bytes 0-0/1"; break;
        }
        await harness.Invoke(context);
        Problem(context, scenario == Representation.Range ? 400 : 415, "invalid");
        Assert.Empty(harness.Service.Calls);
    }
}
