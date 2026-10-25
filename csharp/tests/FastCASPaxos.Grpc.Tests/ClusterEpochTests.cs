using FastCASPaxos.Host;
using FastCASPaxos.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using static FastCASPaxos.Grpc.Tests.HostLifecycleTests;
using static FastCASPaxos.Grpc.Tests.FastHandoffContractTests;
using Wire = FastCASPaxos.Grpc.Wire;

namespace FastCASPaxos.Grpc.Tests;

// Executes the production interceptor and runtime, not Kestrel/HTTP2 registration.
// All databases belong to TestDirectory; no listeners or external peers are used.
public sealed class ClusterEpochTests
{
    public enum InternalMethod { Prepare, Accept, ListKeys, ListDigests, FetchIfDifferent, Stage, Learn, LearnDigest }
    public enum InvalidMetadata { Missing, Empty, Malformed, Duplicate, ConflictingDuplicate, LeadingZero, Future, Overflow }
    public enum HandlerOutcome { Success, Exception, Cancellation }

    [Theory]
    [InlineData(InternalMethod.Prepare)]
    [InlineData(InternalMethod.Accept)]
    [InlineData(InternalMethod.ListKeys)]
    [InlineData(InternalMethod.ListDigests)]
    [InlineData(InternalMethod.FetchIfDifferent)]
    [InlineData(InternalMethod.Stage)]
    [InlineData(InternalMethod.Learn)]
    [InlineData(InternalMethod.LearnDigest)]
    public async Task EpochInterceptorRejectsDelayedPriorGenerationCalls(InternalMethod method)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var interceptor = new EpochServerInterceptor(node);
        var calls = 0;
        Task<string> Dispatch(string epoch) => interceptor.UnaryServerHandler("request", Context(method, epoch), (_, _) =>
        {
            calls++;
            return Task.FromResult("current-generation");
        });
        Assert.Equal("current-generation", await Dispatch("0"));
        var request = NewReset();
        await node.ApplyResetPhaseAsync(request, "prepare");
        Assert.Equal(StatusCode.FailedPrecondition, (await Assert.ThrowsAsync<RpcException>(() => Dispatch("0"))).StatusCode);
        Assert.Equal(1, calls);
        await node.ApplyResetPhaseAsync(request, "clear");
        Assert.Equal(StatusCode.FailedPrecondition, (await Assert.ThrowsAsync<RpcException>(() => Dispatch("0"))).StatusCode);
        await node.ApplyResetPhaseAsync(request, "activate");
        Assert.Equal(StatusCode.FailedPrecondition, (await Assert.ThrowsAsync<RpcException>(() => Dispatch("0"))).StatusCode);
        Assert.Equal(1, calls);
        Assert.Equal("current-generation", await Dispatch("1"));
        Assert.Equal(2, calls);
        Assert.Empty((await node.Inspection.InspectKeysAsync()).Entries);
    }

    [Theory]
    [InlineData(InvalidMetadata.Missing)]
    [InlineData(InvalidMetadata.Empty)]
    [InlineData(InvalidMetadata.Malformed)]
    [InlineData(InvalidMetadata.Duplicate)]
    [InlineData(InvalidMetadata.ConflictingDuplicate)]
    [InlineData(InvalidMetadata.LeadingZero)]
    [InlineData(InvalidMetadata.Future)]
    [InlineData(InvalidMetadata.Overflow)]
    public async Task EpochInterceptorRejectsInvalidEpochMetadata(InvalidMetadata scenario)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var headers = new Metadata();
        if (scenario != InvalidMetadata.Missing)
            headers.Add(EpochClientInterceptor.Header, scenario switch
            {
                InvalidMetadata.Empty => "",
                InvalidMetadata.Malformed => "not-an-epoch",
                InvalidMetadata.LeadingZero => "00",
                InvalidMetadata.Future => "1",
                InvalidMetadata.Overflow => "18446744073709551616",
                _ => "0",
            });
        if (scenario is InvalidMetadata.Duplicate or InvalidMetadata.ConflictingDuplicate)
            headers.Add(EpochClientInterceptor.Header, scenario == InvalidMetadata.Duplicate ? "0" : "1");
        var calls = 0;
        var interceptor = new EpochServerInterceptor(node);
        var error = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(
            "request", new TestServerCallContext(new(headers), MethodName(InternalMethod.Learn)), (_, _) =>
            {
                calls++;
                return Task.FromResult("must not run");
            }));
        Assert.Equal(StatusCode.FailedPrecondition, error.StatusCode);
        Assert.Equal(0, calls);
        Assert.Equal("active", node.Administration.State);
        using var lease = node.EnterProtocol("0");
    }

    [Theory]
    [InlineData(HandlerOutcome.Success)]
    [InlineData(HandlerOutcome.Exception)]
    [InlineData(HandlerOutcome.Cancellation)]
    public async Task EpochLeaseDrainsEntireAdmittedHandlerBeforeReset(HandlerOutcome outcome)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var interceptor = new EpochServerInterceptor(node);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var handler = interceptor.UnaryServerHandler("request", Context(InternalMethod.Accept, "0", cancellation.Token), async (_, _) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(Watchdog);
            if (outcome == HandlerOutcome.Exception) throw new InvalidOperationException("injected handler failure");
            if (outcome == HandlerOutcome.Cancellation) throw new OperationCanceledException(cancellation.Token);
            return "durable-outcome";
        });
        try
        {
            await entered.Task.WaitAsync(Watchdog);
            var preparing = node.ApplyResetPhaseAsync(NewReset(), "prepare");
            Assert.Equal("preparing", node.Administration.State);
            Assert.False(preparing.IsCompleted);
            Assert.Throws<RegisterUnavailableException>(() => node.EnterProtocol("0"));
            cancellation.Cancel();
            Assert.False(preparing.IsCompleted); // Caller cancellation cannot shorten the held server lease.
            release.TrySetResult();
            if (outcome == HandlerOutcome.Exception)
                Assert.Equal("injected handler failure", (await Assert.ThrowsAsync<InvalidOperationException>(() => handler)).Message);
            else if (outcome == HandlerOutcome.Cancellation)
                Assert.Equal(cancellation.Token, (await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handler)).CancellationToken);
            else Assert.Equal("durable-outcome", await handler.WaitAsync(Watchdog));
            Assert.Equal("prepared", (await preparing.WaitAsync(Watchdog)).State);
        }
        finally { release.TrySetResult(); }
    }

    [Fact]
    public async Task PublicRegisterRpcBypassesEpochHeaderButRetainsFacadeAdmission()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var interceptor = new EpochServerInterceptor(node);
        var service = Service(node.Registers);
        var context = new TestServerCallContext(new(), "/fastcaspaxos.v1.Register/Read");
        var request = new Wire.ReadRequest { Key = "key" };
        var result = await interceptor.UnaryServerHandler(request, context, service.Read);
        Assert.Equal(Wire.RegisterState.StateOneofCase.Absent, result.StateCase);
        await node.ApplyResetPhaseAsync(NewReset(), "prepare");
        var error = await Assert.ThrowsAsync<RpcException>(() => interceptor.UnaryServerHandler(request, context, service.Read));
        Assert.Equal(StatusCode.Unavailable, error.StatusCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DelayedPreResetLearnCannotResurrectRegister(bool reopen)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        var node = await NodeRuntime.OpenAsync(options);
        try
        {
            var accepted = Envelope(1, Payload);
            Assert.True((await node.Acceptor.AcceptAsync("key", new(new(5, 0), accepted, new(6, 0)))).IsAccepted);
            node.Learn(new("key", new(5, 0), accepted, new(6, 0)));
            // Captured before reset, delivered after activation/reopen.
            var delayed = new Wire.RegisterCommit
            {
                Key = "key",
                Accepted = new() { Ballot = Ballot(5, 0), Value = Google.Protobuf.ByteString.CopyFrom(accepted.ToArray()) },
                PreparedBallot = Ballot(6, 0),
            };
            var oldContext = Context(InternalMethod.Learn, "0");
            await node.ResetClusterAsync(NewReset());
            if (reopen)
            {
                await node.DisposeAsync();
                node = await NodeRuntime.OpenAsync(options);
            }
            var interceptor = new EpochServerInterceptor(node);
            var learner = new RegisterLearnerGrpcService(node.Learner, NullLogger<RegisterLearnerGrpcService>.Instance);
            Assert.Equal(StatusCode.FailedPrecondition, (await Assert.ThrowsAsync<RpcException>(() =>
                interceptor.UnaryServerHandler(delayed, oldContext, learner.Learn))).StatusCode);
            Assert.Null(await node.Inspection.InspectAsync("key"));
            Assert.Equal(new RegisterValue(null, null), await node.Registers.ReadAsync("key"));
            var created = await node.Registers.CompareExchangeAsync("key", null, ByteString.Empty);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            Assert.Equal(ByteString.Empty, created.Current!.Value);
            Assert.Equal(new ByteString([.. Revision(1).ToArray(), .. Revision(1).ToArray()]), created.Current.Version);
            Assert.Equal(created.Current, await node.Registers.ReadAsync("key"));
        }
        finally { await node.DisposeAsync(); }
    }

    [Theory]
    [InlineData(InternalMethod.Prepare)]
    [InlineData(InternalMethod.Accept)]
    [InlineData(InternalMethod.ListKeys)]
    [InlineData(InternalMethod.ListDigests)]
    [InlineData(InternalMethod.FetchIfDifferent)]
    [InlineData(InternalMethod.Stage)]
    [InlineData(InternalMethod.Learn)]
    [InlineData(InternalMethod.LearnDigest)]
    public async Task EpochClientInterceptorKeepsCapturedEpochAndDiagnosticHeaders(InternalMethod method)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var server = new EpochServerInterceptor(node);
        var transport = new EpochTestInvoker(server);
        var oldClient = transport.Intercept(new EpochClientInterceptor(node.Administration.Epoch));
        await node.ResetClusterAsync(NewReset());
        var freshClient = transport.Intercept(new EpochClientInterceptor(node.Administration.Epoch));
        var fullName = MethodName(method).Split('/');
        var rpc = new Method<string, string>(MethodType.Unary, fullName[1], fullName[2],
            Marshallers.StringMarshaller, Marshallers.StringMarshaller);
        var headers = new Metadata { { "fcp-call-id", "diagnostic-call" } };
        var options = new CallOptions(headers);
        using var oldCall = oldClient.AsyncUnaryCall(rpc, null, options, "old");
        Assert.Equal(StatusCode.FailedPrecondition,
            (await Assert.ThrowsAsync<RpcException>(() => oldCall.ResponseAsync)).StatusCode);
        using var freshCall = freshClient.AsyncUnaryCall(rpc, null, options, "new");
        Assert.Equal("new", await freshCall.ResponseAsync);
        Assert.Equal(["0", "1"], transport.Headers.Select(item => item.GetValue(EpochClientInterceptor.Header)));
        Assert.All(transport.Headers, item => Assert.Equal("diagnostic-call", item.GetValue("fcp-call-id")));
        Assert.Single(headers); // Caller metadata was copied, not mutated.
        Assert.Equal(1, transport.Delivered);
    }

    private sealed class EpochTestInvoker(EpochServerInterceptor interceptor) : TestCallInvoker
    {
        public List<Metadata> Headers { get; } = [];
        public int Delivered { get; private set; }
        protected override Task<TResponse> Invoke<TRequest, TResponse>(
            Method<TRequest, TResponse> method, TRequest request, CallOptions options)
        {
            Headers.Add(options.Headers!);
            return interceptor.UnaryServerHandler(request, new TestServerCallContext(options, method.FullName), (value, _) =>
            {
                Delivered++;
                return Task.FromResult((TResponse)(object)value);
            });
        }
    }

    private static TestServerCallContext Context(InternalMethod method, string epoch, CancellationToken cancellationToken = default) =>
        new(new(new Metadata { { EpochClientInterceptor.Header, epoch } }, cancellationToken: cancellationToken), MethodName(method));

    private static string MethodName(InternalMethod method) => "/fastcaspaxos.v1." + (method switch
    {
        InternalMethod.ListDigests or InternalMethod.FetchIfDifferent => "AcceptorDigests/" + method,
        InternalMethod.Stage => "Repair/Stage",
        InternalMethod.Learn or InternalMethod.LearnDigest => "RegisterLearner/" + method,
        _ => "Acceptor/" + method,
    });
}
