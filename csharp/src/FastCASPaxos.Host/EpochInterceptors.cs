using FastCASPaxos.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FastCASPaxos.Host;

public sealed class EpochClientInterceptor(string epoch) : Interceptor
{
    public const string Header = "fcp-epoch";

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var headers = new Metadata();
        if (context.Options.Headers is { } existing)
            foreach (var entry in existing) headers.Add(entry);
        headers.Add(Header, epoch);
        return continuation(request, new(context.Method, context.Host, context.Options.WithHeaders(headers)));
    }
}

/// <summary>Holds admission through the complete durable outcome of each peer RPC.</summary>
public sealed class EpochServerInterceptor(NodeRuntime node) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        if (context.Method.StartsWith("/fastcaspaxos.v1.Register/", StringComparison.Ordinal))
            return await continuation(request, context).ConfigureAwait(false);
        var headers = context.RequestHeaders.Where(entry => entry.Key == EpochClientInterceptor.Header).ToArray();
        if (headers.Length != 1)
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Peer RPCs require exactly one fcp-epoch header."));
        IDisposable lease;
        try { lease = node.EnterProtocol(headers[0].Value); }
        catch (RegisterUnavailableException)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "The node is fenced or the peer epoch is stale."));
        }
        using (lease)
            return await continuation(request, context).ConfigureAwait(false);
    }
}
