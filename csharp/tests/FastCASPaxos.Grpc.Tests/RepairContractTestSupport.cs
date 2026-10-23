using FastCASPaxos.Server;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Wire = FastCASPaxos.Grpc.Wire;

namespace FastCASPaxos.Grpc.Tests;

// A repair-only extension of the existing in-process protobuf boundary test harness.
internal sealed class RepairContractInvoker(IAcceptorDigestSource? source = null, IRepairScheduler? scheduler = null)
    : TestCallInvoker
{
    protected override async Task<TResponse> Invoke<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request, CallOptions options)
    {
        var context = new TestServerCallContext(options);
        var digests = new AcceptorDigestGrpcService(source!, NullLogger<AcceptorDigestGrpcService>.Instance);
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

internal sealed class DelegateDigests : IAcceptorDigestSource
{
    public int Calls;
    public Func<string, string?, int, CancellationToken, ValueTask<AcceptorDigestPage>> List { get; set; } =
        (_, _, _, _) => throw new InvalidOperationException("Unexpected digest list.");
    public Func<string, AcceptedDigest?, CancellationToken, ValueTask<ConditionalAcceptorState>> Fetch { get; set; } =
        (_, _, _) => throw new InvalidOperationException("Unexpected conditional fetch.");

    public ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return List(prefix, afterKey, limit, cancellationToken);
    }

    public ValueTask<ConditionalAcceptorState> FetchIfDifferentAsync(
        string key, AcceptedDigest? known, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Calls);
        return Fetch(key, known, cancellationToken);
    }
}

internal sealed class CountingRepairScheduler : IRepairScheduler
{
    public int Calls;
    public void StageRepair() => Interlocked.Increment(ref Calls);
}
