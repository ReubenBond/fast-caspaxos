using System.Collections.Concurrent;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Bytes = FastCASPaxos.Server.ByteString;
using Wire = FastCASPaxos.Grpc.Wire;

namespace FastCASPaxos.Grpc.Tests;

internal static class ContractTestSupport
{
    public static readonly TimeSpan Watchdog = TimeSpan.FromSeconds(20);
    public static readonly Bytes Payload = new([0, 10, 255]);
    public static readonly Bytes Version = new([0, 0, 0, 1, 0, 0, 0, 2]);
    public static Wire.Ballot Ballot(int round = 3, int proposer = 1) => new() { Round = round, ProposerId = proposer };
    public static Wire.RegisterState Present(byte[]? value = null, byte[]? version = null) => new()
    {
        Present = new()
        {
            Value = Google.Protobuf.ByteString.CopyFrom(value ?? Payload.ToArray()),
            Version = Google.Protobuf.ByteString.CopyFrom(version ?? Version.ToArray()),
        },
    };
    public static Wire.RegisterState Absent() => new() { Absent = new() };
    public static Wire.AcceptorEntry Entry(string key, int promised = 3, int accepted = 2) => new()
    {
        Key = key,
        PromisedBallot = Ballot(promised),
        Accepted = new() { Ballot = Ballot(accepted), Value = Google.Protobuf.ByteString.Empty },
    };
    public static AcceptorGrpcService Service(IKeyedAcceptor acceptor) => new(acceptor, NullLogger<AcceptorGrpcService>.Instance);
    public static RegisterGrpcService Service(IRegisterService registers) => new(registers, NullLogger<RegisterGrpcService>.Instance);
    public static KeyedAcceptorState ReadState(AcceptorReadResult result) => result switch
    {
        KeyedAcceptorState state => state,
        _ => throw new Xunit.Sdk.XunitException("Expected full accepted state.")
    };
}

// This exercises real generated messages and public adapters, NOT Kestrel or HTTP/2.
// Requests and replies cross protobuf serialization so optional-field presence is tested.
internal sealed class ContractCallInvoker(
    Wire.Acceptor.AcceptorBase? acceptor = null,
    Wire.Register.RegisterBase? register = null,
    Wire.AcceptorDigests.AcceptorDigestsBase? digests = null,
    Wire.RegisterLearner.RegisterLearnerBase? learner = null) : TestCallInvoker
{
    protected override async Task<TResponse> Invoke<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CallOptions options)
    {
        var copy = Copy(request);
        var context = new TestServerCallContext(options);
        object response = copy switch
        {
            Wire.PrepareRequest value => await acceptor!.Prepare(value, context),
            Wire.AcceptRequest value => await acceptor!.Accept(value, context),
            Wire.FetchIfDifferentRequest value => await digests!.FetchIfDifferent(value, context),
            Wire.ReadRequest value => await register!.Read(value, context),
            Wire.CompareExchangeRequest value => await register!.CompareExchange(value, context),
            Wire.RegisterCommit value => await learner!.Learn(value, context),
            Wire.RegisterCommitDigest value => await learner!.LearnDigest(value, context),
            Wire.ListKeysRequest value when method.ServiceName.EndsWith(".Acceptor", StringComparison.Ordinal) =>
                await acceptor!.ListKeys(value, context),
            Wire.ListKeysRequest value => await register!.ListKeys(value, context),
            _ => throw new InvalidOperationException($"Unsupported contract method {method.FullName}."),
        };
        return Copy((TResponse)response);
    }

    private static T Copy<T>(T value)
    {
        var message = (IMessage)(object)value!;
        return (T)message.Descriptor.Parser.ParseFrom(message.ToByteArray());
    }
}

internal sealed class StubCallInvoker(Func<object, CallOptions, Task<object>> respond) : TestCallInvoker
{
    public ConcurrentQueue<(object Request, CallOptions Options)> Calls { get; } = new();

    public static StubCallInvoker Reply(object response) => new((_, _) => Task.FromResult(response));
    public static StubCallInvoker Fail(Exception error) => new((_, _) => Task.FromException<object>(error));

    protected override async Task<TResponse> Invoke<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CallOptions options)
    {
        Calls.Enqueue((request, options));
        return (TResponse)await respond(request, options);
    }
}

internal abstract class TestCallInvoker : CallInvoker
{
    protected abstract Task<TResponse> Invoke<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CallOptions options)
        where TRequest : class where TResponse : class;

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        new(Invoke(method, request, options), Task.FromResult(new Metadata()),
            () => Status.DefaultSuccess, () => new Metadata(), () => { });

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException("Only async unary calls are used by these adapters.");
    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();
    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();
}

internal sealed class TestServerCallContext(CallOptions options, string method = "contract-test") : ServerCallContext
{
    private readonly Metadata _trailers = new();
    private Status _status;
    private WriteOptions? _writeOptions;
    protected override string MethodCore => method;
    protected override string HostCore => "in-process";
    protected override string PeerCore => "in-process";
    protected override DateTime DeadlineCore => options.Deadline ?? DateTime.MaxValue;
    protected override Metadata RequestHeadersCore => options.Headers ?? new();
    protected override CancellationToken CancellationTokenCore => options.CancellationToken;
    protected override Metadata ResponseTrailersCore => _trailers;
    protected override Status StatusCore { get => _status; set => _status = value; }
    protected override WriteOptions? WriteOptionsCore { get => _writeOptions; set => _writeOptions = value; }
    protected override AuthContext AuthContextCore => new(null, new Dictionary<string, List<AuthProperty>>());
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException();
    protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
}

internal sealed class DelegateAcceptor : IKeyedAcceptor
{
    public Func<string, AcceptedDigest?, CancellationToken, ValueTask<AcceptorReadResult>> Read { get; set; } =
        (_, _, _) => ValueTask.FromException<AcceptorReadResult>(new IOException("inconclusive test probe"));
    public Func<string, PrepareRequest, CancellationToken, ValueTask<AcceptorPrepareResult<Bytes?>>> Prepare { get; set; } =
        (_, _, _) => ValueTask.FromException<AcceptorPrepareResult<Bytes?>>(new InvalidOperationException("Unexpected prepare."));
    public Func<string, AcceptRequest<Bytes>, CancellationToken, ValueTask<AcceptorAcceptResult>> Accept { get; set; } =
        (_, _, _) => ValueTask.FromException<AcceptorAcceptResult>(new InvalidOperationException("Unexpected accept."));
    public Func<string, string?, int, CancellationToken, ValueTask<KeyPage>> List { get; set; } =
        (_, _, _, _) => ValueTask.FromException<KeyPage>(new InvalidOperationException("Unexpected list."));
    public int Calls;
    public ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Read(key, known, cancellationToken);
    }
    public ValueTask<AcceptorPrepareResult<Bytes?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Prepare(key, request, cancellationToken);
    }
    public ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<Bytes> request, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Accept(key, request, cancellationToken);
    }
    public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return List(prefix, afterKey, limit, cancellationToken);
    }
}

internal sealed class DelegateRegister : IRegisterService
{
    public Func<string, CancellationToken, ValueTask<RegisterValue>> Read { get; set; } =
        (_, _) => ValueTask.FromException<RegisterValue>(new InvalidOperationException("Unexpected read."));
    public Func<string, Bytes?, Bytes, CancellationToken, ValueTask<CompareExchangeResult>> CompareExchange { get; set; } =
        (_, _, _, _) => ValueTask.FromException<CompareExchangeResult>(new InvalidOperationException("Unexpected compare exchange."));
    public Func<string, string?, int, CancellationToken, ValueTask<RegisterKeyPage>> List { get; set; } =
        (_, _, _, _) => ValueTask.FromException<RegisterKeyPage>(new InvalidOperationException("Unexpected list."));
    public int Calls;
    public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Read(key, cancellationToken);
    }
    public ValueTask<CompareExchangeResult> CompareExchangeAsync(string key, Bytes? expectedVersion, Bytes value, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return CompareExchange(key, expectedVersion, value, cancellationToken);
    }
    public ValueTask<RegisterKeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return List(prefix, afterKey, limit, cancellationToken);
    }
}

internal sealed class TestDirectory : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
        "FastCASPaxos.Grpc.Tests", Guid.NewGuid().ToString("N"));
    public TestDirectory() => Directory.CreateDirectory(Path);
    public string Database(string name) => System.IO.Path.Combine(Path, name + ".db");
    public void Dispose() => Directory.Delete(Path, recursive: true);
}
