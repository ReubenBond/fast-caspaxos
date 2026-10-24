using FastCASPaxos.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Xunit;
using static FastCASPaxos.Rest.Tests.RestTestSupport;

namespace FastCASPaxos.Rest.Tests;

public sealed class FailurePhaseTests
{
    public enum Failure { Busy, Unavailable, Invalid, Cancelled }
    public static IEnumerable<object[]> ServiceFailures() =>
        from operation in Enum.GetValues<ApiOperation>()
        from failure in Enum.GetValues<Failure>()
        select new object[] { operation, failure };

    [Theory]
    [MemberData(nameof(ServiceFailures))]
    public async Task ServiceFailuresHaveSanitizedDefinitiveProblems(ApiOperation operation, Failure failure)
    {
        using var cancellation = new CancellationTokenSource();
        Exception error = failure switch
        {
            Failure.Busy => new RegisterBusyException(),
            Failure.Unavailable => new RegisterUnavailableException(new TimeoutException(Secret)),
            Failure.Invalid => new ArgumentException(Secret),
            Failure.Cancelled => new OperationCanceledException(Secret, cancellation.Token),
            _ => throw new ArgumentOutOfRangeException(nameof(failure)),
        };
        void Fail()
        {
            if (failure == Failure.Cancelled) cancellation.Cancel();
            throw error;
        }
        var service = new ControlledService
        {
            Read = (_, _) => { Fail(); return default; },
            Write = (_, _, _, _) => { Fail(); return default; },
            List = (_, _, _, _) => { Fail(); return default; },
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.RequestFor(operation, cancellation.Token);
        await harness.Invoke(context);
        var (status, code) = failure switch
        {
            Failure.Busy => (503, "busy"),
            Failure.Unavailable => (503, "unavailable"),
            Failure.Invalid => (400, "invalid"),
            _ => (499, "cancelled"),
        };
        Problem(context, status, code);
        Assert.Equal(cancellation.Token, Assert.Single(service.Calls).Token);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum Unexpected { Internal, IoAfterDispatch, UnrequestedCancellation, RawTimeout }
    public static IEnumerable<object[]> UnexpectedFailures() =>
        from operation in Enum.GetValues<ApiOperation>()
        from failure in Enum.GetValues<Unexpected>()
        select new object[] { operation, failure };

    [Theory]
    [MemberData(nameof(UnexpectedFailures))]
    public async Task UnexpectedServiceFailureIsLoggedAndHasPhaseAwareOutcome(ApiOperation operation, Unexpected failure)
    {
        Exception error = failure switch
        {
            Unexpected.Internal => new InvalidOperationException(Secret),
            Unexpected.IoAfterDispatch => new IOException(Secret),
            Unexpected.UnrequestedCancellation => new OperationCanceledException(Secret),
            _ => new TimeoutException(Secret),
        };
        var service = new ControlledService
        {
            Read = (_, _) => throw error,
            Write = (_, _, _, _) => throw error,
            List = (_, _, _, _) => throw error,
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.RequestFor(operation);
        await harness.Invoke(context);
        Problem(context, 500, "internal", operation == ApiOperation.Write ? "unknown" : "not-applied");
        Assert.Single(service.Calls);
        Assert.Contains(harness.Logs.Entries, entry => entry.Level == LogLevel.Error && ReferenceEquals(entry.Exception, error));
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum Observation { None, Absent, Present, EmptyVersion }
    public enum Cause { None, Cancelled, TaskCancelled, Deadline, Unavailable, Internal }
    public static IEnumerable<object[]> Unknowns() =>
        from observation in Enum.GetValues<Observation>()
        from cause in Enum.GetValues<Cause>()
        select new object[] { observation, cause };

    [Theory]
    [MemberData(nameof(Unknowns))]
    public async Task PutUnknownDoesNotClaimLaterObservationAsResponseEtag(Observation observation, Cause cause)
    {
        RegisterValue? current = observation switch
        {
            Observation.None => null,
            Observation.Absent => new(null, null),
            Observation.EmptyVersion => new(ByteString.Empty, ByteString.Empty),
            _ => new(Payload, OpaqueVersion),
        };
        Exception? error = cause switch
        {
            Cause.None => null,
            Cause.Cancelled => new OperationCanceledException(Secret),
            Cause.TaskCancelled => new TaskCanceledException(Secret),
            Cause.Deadline => new TimeoutException(Secret),
            Cause.Unavailable => new RegisterUnavailableException(new TimeoutException(Secret)),
            _ => new InvalidOperationException(Secret),
        };
        var service = new ControlledService
        {
            Write = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult(CompareExchangeStatus.Unknown, current, error)),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT");
        await harness.Invoke(context);
        var problem = Problem(context, 503, "unknown", "unknown");
        if (current is { IsPresent: true })
            Assert.Equal(Tag(current.Version!.Value), problem.GetProperty("currentEtag").GetString());
        else
            Assert.False(problem.TryGetProperty("currentEtag", out _));
        if (cause == Cause.None)
            Assert.False(problem.TryGetProperty("cause", out _));
        else
        {
            Assert.Equal(cause switch
                { Cause.Cancelled or Cause.TaskCancelled => "cancelled", Cause.Deadline or Cause.Unavailable => "deadline", _ => "internal" },
                problem.GetProperty("cause").GetString());
            Assert.Contains(harness.Logs.Entries,
                entry => entry.Level == LogLevel.Warning && ReferenceEquals(entry.Exception, error));
        }
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
        Assert.False(context.Response.Headers.ContainsKey("Location"));
        Assert.Single(service.Calls);
    }

    [Fact]
    public async Task PutInvalidServiceStatusIsLoggedAsUnknownFailure()
    {
        var service = new ControlledService
        {
            Write = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult((CompareExchangeStatus)(-1), null)),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT");
        await harness.Invoke(context);
        Problem(context, 500, "internal", "unknown");
        Assert.Single(service.Calls);
        Assert.Contains(harness.Logs.Entries, entry => entry.Level == LogLevel.Error && entry.Exception is InvalidOperationException);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
    }

    public enum BodyFailure { Io, BadRequest, TooLarge, Unexpected }

    [Theory]
    [InlineData(BodyFailure.Io)]
    [InlineData(BodyFailure.BadRequest)]
    [InlineData(BodyFailure.TooLarge)]
    [InlineData(BodyFailure.Unexpected)]
    public async Task PutBodyReadFailureNeverDispatches(BodyFailure failure)
    {
        Exception error = failure switch
        {
            BodyFailure.Io => new IOException(Secret),
            BodyFailure.BadRequest => new BadHttpRequestException(Secret, 400),
            BodyFailure.TooLarge => new BadHttpRequestException(Secret, 413),
            _ => new InvalidOperationException(Secret),
        };
        await using var harness = new EndpointHarness();
        var context = harness.Request("PUT");
        using var stream = new ScriptedStream((_, _) => throw error);
        context.Request.Body = stream;
        await harness.Invoke(context);
        Problem(context, failure switch { BodyFailure.TooLarge => 413, BodyFailure.Unexpected => 500, _ => 400 },
            failure == BodyFailure.Unexpected ? "internal" : "body-read");
        Assert.Empty(harness.Service.Calls);
        Assert.False(context.Response.Headers.ContainsKey("ETag"));
        if (failure == BodyFailure.Unexpected)
            Assert.Contains(harness.Logs.Entries, entry => entry.Level == LogLevel.Error && ReferenceEquals(entry.Exception, error));
    }

    [Fact]
    public async Task PutWaitsForCompleteBodyBeforeDispatch()
    {
        using var cancellation = new CancellationTokenSource();
        var atTail = Barrier<CancellationToken>();
        var release = Barrier<bool>();
        var data = Payload.ToArray();
        var reads = 0;
        var reachedEof = false;
        using var stream = new ScriptedStream(async (buffer, token) =>
        {
            switch (++reads)
            {
                case 1:
                    data.AsMemory(0, 2).CopyTo(buffer);
                    return 2;
                case 2:
                    atTail.SetResult(token);
                    await release.Task;
                    data.AsMemory(2).CopyTo(buffer);
                    return data.Length - 2;
                default:
                    reachedEof = true;
                    return 0;
            }
        });
        var service = new ControlledService
        {
            Write = (_, _, _, _) =>
            {
                Assert.True(reachedEof);
                return ValueTask.FromResult(new CompareExchangeResult(CompareExchangeStatus.Applied, new(Payload, OpaqueVersion)));
            },
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT", token: cancellation.Token);
        context.Request.Body = stream;
        var pending = harness.Invoke(context);
        try
        {
            Assert.Equal(cancellation.Token, await atTail.Task.WaitAsync(Watchdog));
            Assert.Empty(service.Calls);
            Assert.False(pending.IsCompleted);
        }
        finally { release.TrySetResult(true); }
        await pending;
        Assert.Equal(Payload, Assert.Single(service.Calls).Value);
        Assert.Equal(cancellation.Token, Assert.Single(service.Calls).Token);
        Assert.Equal(201, context.Response.StatusCode);
        Assert.Equal(Tag(OpaqueVersion), context.Response.Headers.ETag.ToString());
        NoStore(context);
    }

    [Theory]
    [InlineData(ApiOperation.Read)]
    [InlineData(ApiOperation.Write)]
    [InlineData(ApiOperation.List)]
    public async Task RequestAlreadyCancelledNeverDispatches(ApiOperation operation)
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await using var harness = new EndpointHarness();
        var context = harness.RequestFor(operation, cancellation.Token);
        var reads = 0;
        using var stream = new ScriptedStream((_, _) => { reads++; return ValueTask.FromResult(0); });
        context.Request.Body = stream;
        await harness.Invoke(context);
        Problem(context, 499, "cancelled");
        Assert.Empty(harness.Service.Calls);
        Assert.Equal(0, reads);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PutCancellationDuringBodyReadNeverDispatches(bool streamThrowsCancellation)
    {
        using var cancellation = new CancellationTokenSource();
        var reading = Barrier<CancellationToken>();
        var release = Barrier<bool>();
        using var stream = new ScriptedStream(async (_, token) =>
        {
            reading.SetResult(token);
            await release.Task;
            if (streamThrowsCancellation) token.ThrowIfCancellationRequested();
            return 0;
        });
        await using var harness = new EndpointHarness();
        var context = harness.Request("PUT", token: cancellation.Token);
        context.Request.Body = stream;
        var pending = harness.Invoke(context);
        try
        {
            Assert.Equal(cancellation.Token, await reading.Task.WaitAsync(Watchdog));
            Assert.Empty(harness.Service.Calls);
            cancellation.Cancel();
        }
        finally { release.TrySetResult(true); }
        await pending;
        Problem(context, 499, "cancelled");
        Assert.Empty(harness.Service.Calls);
    }

    [Theory]
    [InlineData(CompareExchangeStatus.Applied, false)]
    [InlineData(CompareExchangeStatus.Applied, true)]
    [InlineData(CompareExchangeStatus.Unknown, false)]
    [InlineData(CompareExchangeStatus.Mismatch, false)]
    public async Task PutKnownResultAfterCancellationIsPreserved(CompareExchangeStatus status, bool update)
    {
        using var cancellation = new CancellationTokenSource();
        var entered = Barrier<CancellationToken>();
        var release = Barrier<CompareExchangeResult>();
        var service = new ControlledService
        {
            Write = async (_, _, _, token) => { entered.SetResult(token); return await release.Task; },
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request("PUT", body: Payload.ToArray(), token: cancellation.Token);
        if (update)
        {
            context.Request.Headers.Remove("If-None-Match");
            context.Request.Headers.IfMatch = Tag(ByteString.Empty);
        }
        var pending = harness.Invoke(context);
        try
        {
            Assert.Equal(cancellation.Token, await entered.Task.WaitAsync(Watchdog));
            Assert.Single(service.Calls);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
        }
        finally { release.TrySetResult(new(status, new(Payload, OpaqueVersion))); }
        await pending;
        Assert.Single(service.Calls); // Cancellation must never trigger a retry.
        NoStore(context);
        if (status == CompareExchangeStatus.Applied)
        {
            Assert.Equal(update ? 204 : 201, context.Response.StatusCode);
            Assert.Equal(Tag(OpaqueVersion), context.Response.Headers.ETag.ToString());
            Assert.Empty(Bytes(context));
        }
        else if (status == CompareExchangeStatus.Mismatch)
        {
            Problem(context, 412, "mismatch");
            Assert.Equal(Tag(OpaqueVersion), context.Response.Headers.ETag.ToString());
        }
        else
        {
            var problem = Problem(context, 503, "unknown", "unknown");
            Assert.Equal(Tag(OpaqueVersion), problem.GetProperty("currentEtag").GetString());
            Assert.False(context.Response.Headers.ContainsKey("ETag"));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ListInvalidUtf16InventoryFailsInsteadOfReplacingCharacters(bool cursor, bool lowSurrogate)
    {
        var invalid = lowSurrogate ? "\uDC00" : "\uD800";
        var service = new ControlledService
        {
            List = (_, _, _, _) => ValueTask.FromResult(
                new RegisterKeyPage(cursor ? ["safe"] : ["safe", invalid], cursor ? invalid : null)),
        };
        await using var harness = new EndpointHarness(service);
        var context = harness.Request(list: true, query: "");
        await harness.Invoke(context);
        Problem(context, 500, "internal");
        Assert.Single(service.Calls);
        Assert.Contains(harness.Logs.Entries, entry => entry.Level == LogLevel.Error && entry.Exception is not null);
    }

    private sealed class ScriptedStream(Func<Memory<byte>, CancellationToken, ValueTask<int>> read) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            read(buffer, cancellationToken);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            read(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
