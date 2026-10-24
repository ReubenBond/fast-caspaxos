using FastCASPaxos.Server;
using Grpc.Core;
using Xunit;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class FailureContractTests
{
    [Theory]
    [InlineData(StatusCode.InvalidArgument, "invalid", typeof(ArgumentException))]
    [InlineData(StatusCode.ResourceExhausted, "busy", typeof(RegisterBusyException))]
    [InlineData(StatusCode.Unavailable, "unavailable", typeof(RegisterUnavailableException))]
    [InlineData(StatusCode.Cancelled, "cancelled", typeof(OperationCanceledException))]
    public async Task DefinitiveFailures_RequireMatchingStatusAndTrailer(StatusCode status, string kind, Type expectedType)
    {
        var definite = new RpcException(new Status(status, "before mutation"),
            new Metadata { { "fastcaspaxos-failure", kind } });
        var client = new GrpcRegisterService(StubCallInvoker.Fail(definite));
        var error = await Record.ExceptionAsync(() => client.CompareExchangeAsync("key", null, Payload).AsTask());
        Assert.IsType(expectedType, error);
        var readError = await Record.ExceptionAsync(() => client.ReadAsync("key").AsTask());
        Assert.IsType(expectedType, readError);

        foreach (var trailer in new[] { "", "different" })
        {
            var metadata = new Metadata();
            if (trailer.Length > 0) metadata.Add("fastcaspaxos-failure", trailer);
            var uncertain = new RpcException(new Status(status, "lost reply"), metadata);
            var remote = new GrpcRegisterService(StubCallInvoker.Fail(uncertain));
            var result = await remote.CompareExchangeAsync("key", null, Payload);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Null(result.Current);
            var cause = Assert.IsType<RemoteCallException>(result.Error);
            Assert.Equal(status, cause.StatusCode);
            Assert.Same(uncertain, cause.InnerException);
        }
        var mismatched = new GrpcRegisterService(StubCallInvoker.Fail(new RpcException(
            new Status(StatusCode.Internal, "wrong status"), new Metadata { { "fastcaspaxos-failure", kind } })));
        var mismatch = await mismatched.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, mismatch.Status);
        Assert.Equal(StatusCode.Internal, Assert.IsType<RemoteCallException>(mismatch.Error).StatusCode);
    }

    [Theory]
    [InlineData(StatusCode.DeadlineExceeded)]
    [InlineData(StatusCode.Unavailable)]
    [InlineData(StatusCode.Cancelled)]
    [InlineData(StatusCode.Internal)]
    [InlineData(StatusCode.Unknown)]
    public async Task GenericRpcFailures_AreUnknownForWritesAndErrorsForReads(StatusCode status)
    {
        var failure = new RpcException(new Status(status, "transport lost"));
        var invoker = StubCallInvoker.Fail(failure);
        var client = new GrpcRegisterService(invoker);
        var result = await client.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        var cause = Assert.IsType<RemoteCallException>(result.Error);
        Assert.Equal(status, cause.StatusCode);
        Assert.Same(failure, cause.InnerException);
        var error = await Assert.ThrowsAsync<RemoteCallException>(() => client.ReadAsync("key").AsTask());
        Assert.Equal(status, error.StatusCode);
        var acceptor = new GrpcKeyedAcceptor(invoker);
        Assert.Equal(status, (await Assert.ThrowsAsync<RemoteCallException>(() =>
            acceptor.PrepareAsync("key", new(new(1, 1))).AsTask())).StatusCode);
        Assert.Equal(status, (await Assert.ThrowsAsync<RemoteCallException>(() =>
            acceptor.AcceptAsync("key", new(new(1, 1), Payload, new())).AsTask())).StatusCode);
        Assert.Equal(status, (await Assert.ThrowsAsync<RemoteCallException>(() =>
            acceptor.ListKeysAsync().AsTask())).StatusCode);
        Assert.Equal(status, (await Assert.ThrowsAsync<RemoteCallException>(() =>
            client.ListKeysAsync().AsTask())).StatusCode);
    }

    [Fact]
    public async Task NonRpcFailureAfterDispatch_RemainsUnknownWithOriginalCause()
    {
        var failure = new IOException("reply was lost");
        var client = new GrpcRegisterService(StubCallInvoker.Fail(failure));
        var result = await client.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        Assert.Same(failure, result.Error);
    }

    [Theory]
    [InlineData(RequestValidationTests.Operation.Prepare)]
    [InlineData(RequestValidationTests.Operation.Accept)]
    [InlineData(RequestValidationTests.Operation.AcceptorList)]
    [InlineData(RequestValidationTests.Operation.Read)]
    [InlineData(RequestValidationTests.Operation.CompareExchange)]
    [InlineData(RequestValidationTests.Operation.RegisterList)]
    public async Task Deadline_UsesInjectedTimeProviderForEachCall(RequestValidationTests.Operation operation)
    {
        using var cancellation = new CancellationTokenSource();
        var timeout = TimeSpan.FromMinutes(2);
        var clock = new FixedTimeProvider(new DateTimeOffset(2040, 1, 2, 3, 4, 5, TimeSpan.Zero));
        var invoker = StubCallInvoker.Fail(new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")));
        var acceptor = new GrpcKeyedAcceptor(invoker, timeout, clock);
        var register = new GrpcRegisterService(invoker, timeout, clock);
        for (var index = 0; index < 2; index++)
        {
            var error = await Record.ExceptionAsync(() => RequestValidationTests.Invoke(operation,
                acceptor, register, token: cancellation.Token));
            var call = invoker.Calls.Last();
            Assert.Equal(index + 1, invoker.Calls.Count);
            Assert.Equal(index + 1, clock.Reads);
            Assert.Equal(cancellation.Token, call.Options.CancellationToken);
            Assert.Equal(clock.UtcNow.UtcDateTime + timeout, call.Options.Deadline);
            Assert.Equal(DateTimeKind.Utc, call.Options.Deadline!.Value.Kind);
            if (operation == RequestValidationTests.Operation.CompareExchange)
                Assert.Null(error); // Its dedicated tests require Unknown + DeadlineExceeded.
            else
                Assert.Equal(StatusCode.DeadlineExceeded, Assert.IsType<RemoteCallException>(error).StatusCode);
            clock.UtcNow += TimeSpan.FromDays(1);
        }
    }

    [Theory]
    [InlineData(RequestValidationTests.Operation.Prepare)]
    [InlineData(RequestValidationTests.Operation.Read)]
    public async Task DefaultDeadline_UsesThirtySecondsFromInjectedTimeProvider(RequestValidationTests.Operation operation)
    {
        var clock = new FixedTimeProvider(new DateTimeOffset(2040, 1, 2, 3, 4, 5, TimeSpan.Zero));
        var invoker = StubCallInvoker.Fail(new RpcException(new Status(StatusCode.Unavailable, "offline")));
        await Assert.ThrowsAsync<RemoteCallException>(() => RequestValidationTests.Invoke(operation,
            new(invoker, timeProvider: clock), new(invoker, timeProvider: clock)));
        var call = Assert.Single(invoker.Calls);
        Assert.Equal(clock.UtcNow.UtcDateTime.AddSeconds(30), call.Options.Deadline);
        Assert.Equal(DateTimeKind.Utc, call.Options.Deadline!.Value.Kind);
        Assert.Equal(1, clock.Reads);
    }

    [Fact]
    public async Task DeadlineFailureAfterDispatch_RemainsUnknown()
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fail = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        var invoker = new StubCallInvoker((_, _) => { arrived.TrySetResult(); return fail.Task; });
        var pending = new GrpcRegisterService(invoker, TimeSpan.FromMilliseconds(1))
            .CompareExchangeAsync("key", null, Payload).AsTask();
        try
        {
            await arrived.Task.WaitAsync(Watchdog);
            Assert.False(pending.IsCompleted);
            fail.TrySetException(new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline")));
            var result = await pending.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Null(result.Current);
            Assert.Equal(StatusCode.DeadlineExceeded, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
        }
        finally { fail.TrySetCanceled(); }
    }

    [Fact]
    public async Task CancellationAfterDispatch_WithoutDefinitiveTrailerRemainsUnknown()
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var invoker = new StubCallInvoker(async (_, options) =>
        {
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = options.CancellationToken.Register(() => cancelled.TrySetResult());
            arrived.TrySetResult();
            await cancelled.Task;
            throw new RpcException(new Status(StatusCode.Cancelled, "reply lost after dispatch"));
        });
        var pending = new GrpcRegisterService(invoker).CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();
        try
        {
            await arrived.Task.WaitAsync(Watchdog);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            var result = await pending.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Null(result.Current);
            Assert.Equal(cancellation.Token, Assert.IsType<OperationCanceledException>(result.Error).CancellationToken);
        }
        finally { cancellation.Cancel(); }
    }

    [Theory]
    [InlineData(RequestValidationTests.Operation.Prepare)]
    [InlineData(RequestValidationTests.Operation.Accept)]
    [InlineData(RequestValidationTests.Operation.AcceptorList)]
    [InlineData(RequestValidationTests.Operation.Read)]
    [InlineData(RequestValidationTests.Operation.RegisterList)]
    public async Task NonCasCancellationAfterDispatch_ThrowsWithCallerToken(RequestValidationTests.Operation operation)
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var invoker = new StubCallInvoker(async (_, options) =>
        {
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = options.CancellationToken.Register(() => cancelled.TrySetResult());
            arrived.TrySetResult();
            await cancelled.Task;
            throw new RpcException(new Status(StatusCode.Cancelled, "cancelled after dispatch"));
        });
        var pending = RequestValidationTests.Invoke(operation, new(invoker), new(invoker), token: cancellation.Token);
        try
        {
            await arrived.Task.WaitAsync(Watchdog);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(Watchdog));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.Equal(StatusCode.Cancelled, Assert.IsType<RpcException>(error.InnerException).StatusCode);
        }
        finally { cancellation.Cancel(); }
    }

    [Fact]
    public async Task CancellationBeforeMutation_WithDefinitiveTrailerThrows()
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var domain = new DelegateRegister
        {
            CompareExchange = async (_, _, _, token) =>
            {
                var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var registration = token.Register(() => cancelled.TrySetResult());
                arrived.TrySetResult();
                await cancelled.Task;
                token.ThrowIfCancellationRequested();
                throw new InvalidOperationException("Cancellation was not observed.");
            },
        };
        var client = new GrpcRegisterService(new ContractCallInvoker(register: Service(domain)));
        var pending = client.CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();
        try
        {
            await arrived.Task.WaitAsync(Watchdog);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(Watchdog));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            var rpc = Assert.IsType<RpcException>(error.InnerException);
            Assert.Equal(StatusCode.Cancelled, rpc.StatusCode);
            Assert.Equal("cancelled", rpc.Trailers.GetValue("fastcaspaxos-failure"));
            Assert.Equal(1, domain.Calls);
        }
        finally { cancellation.Cancel(); }
    }

    public enum ServiceFailure { Invalid, Busy, Unavailable, Internal, UnrelatedCancellation }

    [Theory]
    [InlineData(ServiceFailure.Invalid, StatusCode.InvalidArgument, "invalid")]
    [InlineData(ServiceFailure.Busy, StatusCode.ResourceExhausted, "busy")]
    [InlineData(ServiceFailure.Unavailable, StatusCode.Unavailable, "unavailable")]
    [InlineData(ServiceFailure.Internal, StatusCode.Internal, null)]
    [InlineData(ServiceFailure.UnrelatedCancellation, StatusCode.Internal, null)]
    public async Task ServiceExceptions_MapDefinitiveErrorsWithoutLeakingInternalDetails(
        ServiceFailure scenario, StatusCode status, string? trailer)
    {
        Exception cause = scenario switch
        {
            ServiceFailure.Invalid => new ArgumentException("invalid input"),
            ServiceFailure.Busy => new RegisterBusyException(),
            ServiceFailure.Unavailable => new RegisterUnavailableException(),
            ServiceFailure.UnrelatedCancellation => new OperationCanceledException("private diagnostic"),
            _ => new InvalidOperationException("private diagnostic"),
        };
        var domain = new DelegateRegister
        {
            Read = (_, _) => ValueTask.FromException<RegisterValue>(cause),
            CompareExchange = (_, _, _, _) => ValueTask.FromException<CompareExchangeResult>(cause),
            List = (_, _, _, _) => ValueTask.FromException<RegisterKeyPage>(cause),
        };
        var raw = new Wire.Register.RegisterClient(new ContractCallInvoker(register: Service(domain)));
        Func<Task>[] calls =
        [
            () => raw.ReadAsync(new() { Key = "key" }).ResponseAsync,
            () => raw.CompareExchangeAsync(new() { Key = "key", Value = Google.Protobuf.ByteString.Empty }).ResponseAsync,
            () => raw.ListKeysAsync(new() { Limit = 1 }).ResponseAsync,
        ];
        foreach (var call in calls)
        {
            var error = await Assert.ThrowsAsync<RpcException>(call);
            Assert.Equal(status, error.StatusCode);
            Assert.Equal(trailer, error.Trailers.GetValue("fastcaspaxos-failure"));
            Assert.DoesNotContain("private diagnostic", error.Status.Detail, StringComparison.Ordinal);
        }
        Assert.Equal(3, domain.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommittedWrite_WithLostOrMalformedReply_RemainsUnknownAndReadable(bool malformed)
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var inner = cluster.CreateRegister(1);
        var domain = new DelegateRegister
        {
            CompareExchange = async (key, expected, value, token) =>
            {
                var result = await inner.CompareExchangeAsync(key, expected, value, token);
                Assert.Equal(CompareExchangeStatus.Applied, result.Status);
                if (!malformed) throw new IOException("private commit diagnostic");
                return new(CompareExchangeStatus.Applied, new(null, result.Current!.Version));
            },
        };
        var client = new GrpcRegisterService(new ContractCallInvoker(register: Service(domain)));
        var result = await client.CompareExchangeAsync("committed", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        var error = Assert.IsType<RemoteCallException>(result.Error);
        Assert.Equal(StatusCode.Internal, error.StatusCode);
        Assert.DoesNotContain("private commit diagnostic", error.Message, StringComparison.Ordinal);
        var durable = await inner.ReadAsync("committed").AsTask().WaitAsync(Watchdog);
        Assert.Equal(Payload, durable.Value);
        Assert.Equal(8, durable.Version!.Value.Length);
        Assert.Equal(CompareExchangeStatus.Mismatch,
            (await inner.CompareExchangeAsync("committed", null, ByteString.Empty)).Status);
    }

    [Fact]
    public async Task CommittedWrite_CancelledReplyIsUnknownAndDurable()
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var inner = cluster.CreateRegister(1);
        var service = Service(inner);
        var committed = new TaskCompletionSource<Wire.RegisterState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var invoker = new StubCallInvoker(async (request, options) =>
        {
            var response = await service.CompareExchange((Wire.CompareExchangeRequest)request, new TestServerCallContext(options));
            Assert.Equal(Wire.CompareExchangeResponse.OutcomeOneofCase.Applied, response.OutcomeCase);
            committed.TrySetResult(response.Applied);
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = options.CancellationToken.Register(() => cancelled.TrySetResult());
            await cancelled.Task;
            // Transport, not domain service, loses the already-committed reply.
            throw new RpcException(new Status(StatusCode.Cancelled, "reply cancelled"));
        });
        var pending = new GrpcRegisterService(invoker).CompareExchangeAsync("committed", null, Payload, cancellation.Token).AsTask();
        try
        {
            var applied = await committed.Task.WaitAsync(Watchdog);
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            var result = await pending.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.Null(result.Current);
            Assert.Equal(cancellation.Token, Assert.IsType<OperationCanceledException>(result.Error).CancellationToken);
            var durable = await inner.ReadAsync("committed").AsTask().WaitAsync(Watchdog);
            Assert.Equal(Payload, durable.Value);
            Assert.Equal(new ByteString(applied.Present.Version.Span), durable.Version);
        }
        finally { cancellation.Cancel(); }
    }

    [Fact]
    public void TimeoutConfiguration_ValidatesExactBoundaries()
    {
        var invoker = StubCallInvoker.Reply(Absent());
        foreach (var invalid in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1) })
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new GrpcKeyedAcceptor(invoker, invalid));
            Assert.Throws<ArgumentOutOfRangeException>(() => new GrpcRegisterService(invoker, invalid));
        }
        foreach (var valid in new[] { TimeSpan.FromTicks(1), TimeSpan.FromDays(1) })
        {
            _ = new GrpcKeyedAcceptor(invoker, valid);
            _ = new GrpcRegisterService(invoker, valid);
        }
        Assert.Empty(invoker.Calls);
    }

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; } = utcNow;
        public int Reads { get; private set; }
        public override DateTimeOffset GetUtcNow()
        {
            Reads++;
            return UtcNow;
        }
    }
}
