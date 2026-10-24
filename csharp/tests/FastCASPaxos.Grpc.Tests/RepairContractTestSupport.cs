using FastCASPaxos.Server;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using ByteString = FastCASPaxos.Server.ByteString;
using Wire = FastCASPaxos.Grpc.Wire;

namespace FastCASPaxos.Grpc.Tests;

// A repair-only extension of the existing in-process protobuf boundary test harness.
internal sealed class RepairContractInvoker(IAcceptorDigestSource? source = null, IRepairScheduler? scheduler = null,
    IKeyedAcceptor? acceptor = null)
    : TestCallInvoker
{
    protected override async Task<TResponse> Invoke<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CallOptions options)
    {
        var context = new TestServerCallContext(options);
        var digests = new AcceptorDigestGrpcService(source!, acceptor ?? (IKeyedAcceptor)source!,
            NullLogger<AcceptorDigestGrpcService>.Instance);
        var repair = new RepairGrpcService(scheduler!, NullLogger<RepairGrpcService>.Instance);
        object response = Copy(request) switch
        {
            Wire.ListKeysRequest list => await digests.ListDigests(list, context),
            Wire.FetchIfDifferentRequest fetch => await digests.FetchIfDifferent(fetch, context),
            Wire.Empty stage => await repair.Stage(stage, context),
            _ => throw new InvalidOperationException($"Unexpected repair method {method.FullName}.")
        };
        return Copy((TResponse)response);
    }

    private static T Copy<T>(T value)
    {
        var message = (IMessage)(object)value!;
        return (T)message.Descriptor.Parser.ParseFrom(message.ToByteArray());
    }
}

internal sealed class DelegateDigests : IAcceptorDigestSource, IKeyedAcceptor
{
    public int Calls;
    public Func<string, string?, int, CancellationToken, ValueTask<AcceptorDigestPage>> List { get; set; } =
        (_, _, _, _) => throw new InvalidOperationException("Unexpected digest list.");
    public Func<string, AcceptedDigest?, CancellationToken, ValueTask<AcceptorReadResult>> Fetch { get; set; } =
        (_, _, _) => throw new InvalidOperationException("Unexpected conditional fetch.");

    public ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return List(prefix, afterKey, limit, cancellationToken);
    }

    public ValueTask<AcceptorReadResult> ReadAsync(
        string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Fetch(key, known, cancellationToken);
    }

    public ValueTask<FastCASPaxos.Protocol.AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, FastCASPaxos.Messages.PrepareRequest request, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Unexpected prepare on digest fixture.");
    public ValueTask<FastCASPaxos.Protocol.AcceptorAcceptResult> AcceptAsync(
        string key, FastCASPaxos.Messages.AcceptRequest<ByteString> request, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Unexpected accept on digest fixture.");
    public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null,
        int limit = 100, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Unexpected inventory on digest fixture.");
}

internal sealed class CountingRepairScheduler : IRepairScheduler
{
    public int Calls;
    public void StageRepair() => Interlocked.Increment(ref Calls);
}
