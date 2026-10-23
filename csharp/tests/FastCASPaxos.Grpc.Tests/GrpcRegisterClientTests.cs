using Clockwork;
using FastCASPaxos.Server;
using Grpc.Core;
using Xunit;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class GrpcRegisterClientTests
{
    [Theory]
    [InlineData(false, StatusCode.Unavailable)]
    [InlineData(true, StatusCode.Unavailable)]
    [InlineData(false, StatusCode.DeadlineExceeded)]
    [InlineData(true, StatusCode.DeadlineExceeded)]
    public async Task ReadOnlyCalls_FailOverAndPreserveRequests(bool list, StatusCode failure)
    {
        var offline = StubCallInvoker.Fail(new RpcException(new(failure, "offline")));
        var healthy = StubCallInvoker.Reply(list
            ? new Wire.RegisterKeyPage { Keys = { "catalog/\0", "catalog/\U0001F600" }, ContinuationKey = "catalog/\U0001F600" }
            : Present());
        using var client = new GrpcRegisterClient([offline, healthy]);
        if (list)
        {
            var result = await client.ListKeysAsync("catalog/", "", 2);
            Assert.Equal(["catalog/\0", "catalog/\U0001F600"], result.Keys);
            Assert.Equal("catalog/\U0001F600", result.ContinuationKey);
            var request = Assert.IsType<Wire.ListKeysRequest>(Assert.Single(healthy.Calls).Request);
            Assert.Equal("catalog/", request.Prefix);
            Assert.True(request.HasAfterKey);
            Assert.Equal("", request.AfterKey);
            Assert.Equal(2, request.Limit);
        }
        else
        {
            Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), await client.ReadAsync("catalog/\0\U0001F600"));
            Assert.Equal("catalog/\0\U0001F600",
                Assert.IsType<Wire.ReadRequest>(Assert.Single(healthy.Calls).Request).Key);
        }
        Assert.Single(offline.Calls);
    }

    [Fact]
    public async Task EndpointAffinity_RetainsSuccessfulServerAndMovesAfterFailure()
    {
        var offline = false;
        var first = new StubCallInvoker((_, _) => offline
            ? Task.FromException<object>(new RpcException(new(StatusCode.Unavailable, "offline")))
            : Task.FromResult<object>(Absent()));
        var second = StubCallInvoker.Reply(Present());
        using var client = new GrpcRegisterClient([first, second]);
        Assert.False((await client.ReadAsync("key")).IsPresent);
        Assert.False((await client.ReadAsync("key")).IsPresent);
        offline = true;
        Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), await client.ReadAsync("key"));
        Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), await client.ReadAsync("key"));
        Assert.Equal(3, first.Calls.Count);
        Assert.Equal(2, second.Calls.Count);
    }

    [Fact]
    public async Task DefiniteUnavailable_RetriesIdenticalMutationAtNextEndpoint()
    {
        var first = StubCallInvoker.Fail(Definite(StatusCode.Unavailable, "unavailable"));
        var second = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Applied = Present() });
        using var client = new GrpcRegisterClient([first, second]);
        var result = await client.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), result.Current);
        var initial = Assert.IsType<Wire.CompareExchangeRequest>(Assert.Single(first.Calls).Request);
        var retry = Assert.IsType<Wire.CompareExchangeRequest>(Assert.Single(second.Calls).Request);
        Assert.Equal(initial, retry);
        Assert.False(retry.HasExpectedVersion);
        Assert.Equal(Payload.ToArray(), retry.Value.ToByteArray());
    }

    [Theory]
    [InlineData(StatusCode.Unavailable)]
    [InlineData(StatusCode.DeadlineExceeded)]
    [InlineData(StatusCode.Cancelled)]
    [InlineData(StatusCode.Unauthenticated)]
    [InlineData(StatusCode.PermissionDenied)]
    [InlineData(StatusCode.ResourceExhausted)]
    [InlineData(StatusCode.DataLoss)]
    [InlineData(StatusCode.Internal)]
    public async Task AmbiguousMutationFailure_ReturnsUnknownWithoutReplay(StatusCode code)
    {
        var cause = new RpcException(new(code, "uncertain"));
        var first = StubCallInvoker.Fail(cause);
        var second = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Applied = Present() });
        using var client = new GrpcRegisterClient([first, second]);
        var result = await client.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        var error = Assert.IsType<RemoteCallException>(result.Error);
        Assert.Equal(code, error.StatusCode);
        Assert.Same(cause, error.InnerException);
        Assert.Single(first.Calls);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task UnknownWithLaterObservation_PreservesOutcomeAndCause()
    {
        var first = StubCallInvoker.Reply(new Wire.CompareExchangeResponse
        {
            Unknown = new()
            {
                Current = Present(),
                Error = new() { Code = Wire.ErrorCode.Unavailable, Message = "lost acknowledgements" },
            },
        });
        var second = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Applied = Present() });
        using var client = new GrpcRegisterClient([first, second]);
        var result = await client.CompareExchangeAsync("key", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), result.Current);
        Assert.Equal(StatusCode.Unavailable, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task Mismatch_PreservesOpaqueExpectedVersionAndCurrent()
    {
        var first = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Mismatch = Present() });
        var second = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Applied = Present() });
        using var client = new GrpcRegisterClient([first, second]);
        var expected = new ByteString([1, 2, 3, 4, 5, 6, 7, 8]);
        var result = await client.CompareExchangeAsync("key", expected, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Mismatch, result.Status);
        Assert.Equal(new RegisterValue(Payload, ContractTestSupport.Version), result.Current);
        Assert.Equal(expected.ToArray(), Assert.IsType<Wire.CompareExchangeRequest>(
            Assert.Single(first.Calls).Request).ExpectedVersion.ToByteArray());
        Assert.Empty(second.Calls);
    }

    [Theory]
    [InlineData(StatusCode.ResourceExhausted, "busy", typeof(RegisterBusyException))]
    [InlineData(StatusCode.InvalidArgument, "invalid", typeof(ArgumentException))]
    [InlineData(StatusCode.Cancelled, "cancelled", typeof(OperationCanceledException))]
    public async Task DefiniteNonRetryableFailure_RemainsExplicit(StatusCode code, string kind, Type type)
    {
        var first = StubCallInvoker.Fail(Definite(code, kind));
        var second = StubCallInvoker.Reply(new Wire.CompareExchangeResponse { Applied = Present() });
        using var client = new GrpcRegisterClient([first, second]);
        Assert.IsType(type, await Record.ExceptionAsync(() => client.CompareExchangeAsync("key", null, Payload).AsTask()));
        Assert.Empty(second.Calls);
    }

    [Theory]
    [InlineData(StatusCode.Unauthenticated)]
    [InlineData(StatusCode.PermissionDenied)]
    [InlineData(StatusCode.ResourceExhausted)]
    [InlineData(StatusCode.DataLoss)]
    [InlineData(StatusCode.Internal)]
    public async Task NonRetryableReadFailure_RemainsExplicit(StatusCode code)
    {
        var first = StubCallInvoker.Fail(new RpcException(new(code, "failure")));
        var second = StubCallInvoker.Reply(Absent());
        using var client = new GrpcRegisterClient([first, second]);
        var error = await Assert.ThrowsAsync<RemoteCallException>(() => client.ReadAsync("key").AsTask());
        Assert.Equal(code, error.StatusCode);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task CorruptReplies_ProduceDataLossAndMutationUncertainty()
    {
        var first = StubCallInvoker.Reply(new Wire.RegisterState());
        var second = StubCallInvoker.Reply(Absent());
        using (var reader = new GrpcRegisterClient([first, second]))
        {
            var error = await Assert.ThrowsAsync<RemoteCallException>(() => reader.ReadAsync("key").AsTask());
            Assert.Equal(StatusCode.DataLoss, error.StatusCode);
            Assert.Empty(second.Calls);
        }
        var corruptWrite = StubCallInvoker.Reply(new Wire.CompareExchangeResponse());
        using var writer = new GrpcRegisterClient([corruptWrite, second]);
        var result = await writer.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(StatusCode.DataLoss, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task PreCancellationAndLocalValidation_SendNoRequests()
    {
        var endpoint = StubCallInvoker.Reply(Absent());
        using var client = new GrpcRegisterClient([endpoint]);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ReadAsync("key", cancelled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.CompareExchangeAsync("key", null, Payload, cancelled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ListKeysAsync(cancellationToken: cancelled.Token).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => client.ReadAsync("\uD800").AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => client.ListKeysAsync(limit: 0).AsTask());
        Assert.Empty(endpoint.Calls);
    }

    [Fact]
    public async Task InFlightCancellation_RetainsWriteUncertaintyAndCallerToken()
    {
        using var cancellation = new CancellationTokenSource();
        var first = new StubCallInvoker((_, _) =>
        {
            cancellation.Cancel();
            throw new RpcException(new(StatusCode.Cancelled, "interrupted"));
        });
        var second = StubCallInvoker.Reply(Absent());
        using var client = new GrpcRegisterClient([first, second]);
        var result = await client.CompareExchangeAsync("key", null, Payload, cancellation.Token);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(cancellation.Token, Assert.IsType<OperationCanceledException>(result.Error).CancellationToken);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task ExhaustedEndpoints_AreAttemptedOnceAndPreserveLastError()
    {
        var cause = new RpcException(new(StatusCode.Unavailable, "last endpoint"));
        var first = StubCallInvoker.Fail(new RpcException(new(StatusCode.Unavailable, "first endpoint")));
        var last = StubCallInvoker.Fail(cause);
        using var client = new GrpcRegisterClient([first, last]);
        var error = await Assert.ThrowsAsync<RemoteCallException>(() => client.ReadAsync("key").AsTask());
        Assert.Same(cause, error.InnerException);
        Assert.Single(first.Calls);
        Assert.Single(last.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Budget_LimitsEachAttemptAndStopsAtOverallDeadline(bool mutation)
    {
        var clock = new SimulationClock(new DateTimeOffset(2040, 1, 2, 3, 4, 5, TimeSpan.Zero));
        var time = new SimulationTimeProvider(new SimulationTaskQueue(clock, new()), clock);
        var first = new StubCallInvoker((_, options) =>
        {
            Assert.Equal(clock.StartDateTime.UtcDateTime.AddSeconds(5), options.Deadline);
            clock.Advance(TimeSpan.FromSeconds(4));
            throw mutation ? Definite(StatusCode.Unavailable, "unavailable")
                : new RpcException(new(StatusCode.DeadlineExceeded, "attempt expired"));
        });
        var second = new StubCallInvoker((_, options) =>
        {
            Assert.Equal(clock.StartDateTime.UtcDateTime.AddSeconds(7), options.Deadline);
            clock.Advance(TimeSpan.FromSeconds(3));
            throw mutation ? Definite(StatusCode.Unavailable, "unavailable")
                : new RpcException(new(StatusCode.DeadlineExceeded, "attempt expired"));
        });
        var third = StubCallInvoker.Reply(Absent());
        using var client = new GrpcRegisterClient([first, second, third],
            new() { CallTimeout = TimeSpan.FromSeconds(7), AttemptTimeout = TimeSpan.FromSeconds(5) }, time);
        var error = await Assert.ThrowsAsync<RemoteCallException>(() => mutation
            ? client.CompareExchangeAsync("key", null, Payload).AsTask()
            : (Task)client.ReadAsync("key").AsTask());
        Assert.Equal(StatusCode.DeadlineExceeded, error.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(7), clock.CurrentTime);
        Assert.Single(first.Calls);
        Assert.Single(second.Calls);
        Assert.Empty(third.Calls);
    }

    [Fact]
    public async Task CancellationBetweenAttempts_StopsFailover()
    {
        using var cancellation = new CancellationTokenSource();
        var first = new StubCallInvoker((_, _) =>
        {
            cancellation.Cancel();
            throw new RpcException(new(StatusCode.Unavailable, "offline"));
        });
        var second = StubCallInvoker.Reply(Absent());
        using var client = new GrpcRegisterClient([first, second]);
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ReadAsync("key", cancellation.Token).AsTask());
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Single(first.Calls);
        Assert.Empty(second.Calls);
    }

    [Fact]
    public async Task Disposal_IsIdempotentAndLeavesCallerOwnedInvokerUsable()
    {
        var endpoint = StubCallInvoker.Reply(Absent());
        var client = new GrpcRegisterClient([endpoint]);
        client.Dispose();
        client.Dispose();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.ReadAsync("key").AsTask());
        Assert.Empty(endpoint.Calls);
        Assert.False((await new GrpcRegisterService(endpoint).ReadAsync("key")).IsPresent);
    }

    [Fact]
    public void Constructors_ValidateEndpointSetAndBounds()
    {
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(Array.Empty<Uri>()));
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(new[] { new Uri("relative", UriKind.Relative) }));
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(new Uri[] { null! }));
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(Array.Empty<CallInvoker>()));
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(new CallInvoker[] { null! }));
        foreach (var invalid in new[] { "file:///tmp", "https://host/path", "https://user@host", "https://host?x=1", "https://host/#fragment" })
            Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(new[] { new Uri(invalid) }));
        Assert.Throws<ArgumentException>(() => new GrpcRegisterClient(new[] { new Uri("https://HOST"), new Uri("https://host/") }));
        var endpoint = StubCallInvoker.Reply(Absent());
        foreach (var invalid in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1) })
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new GrpcRegisterClient([endpoint], new() { CallTimeout = invalid }));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new GrpcRegisterClient([endpoint], new() { AttemptTimeout = invalid }));
        }
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GrpcRegisterClient([endpoint], new() { MaxReceiveMessageBytes = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GrpcRegisterClient([endpoint], new() { MaxSendMessageBytes = -1 }));
        using var valid = new GrpcRegisterClient([new Uri("https://one.example"), new Uri("https://two.example:443")]);
    }

    private static RpcException Definite(StatusCode code, string kind) =>
        new(new(code, "before mutation"), new Metadata { { "fastcaspaxos-failure", kind } });
}
