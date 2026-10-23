using FastCASPaxos.Server.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace FastCASPaxos.Grpc;

/// <summary>Carries diagnostic correlation separately from protocol fields and authentication.</summary>
public sealed class DiagnosticClientInterceptor : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        if (DiagnosticContext.Current is not { } diagnostic) return continuation(request, context);
        var headers = new Metadata();
        if (context.Options.Headers is { } existing)
            foreach (var entry in existing) headers.Add(entry);
        headers.Add("fcp-node-bin", System.Text.Encoding.UTF8.GetBytes(diagnostic.NodeId));
        if (diagnostic.OperationId is { } operation) headers.Add("fcp-operation", operation);
        if (diagnostic.CallId is { } call) headers.Add("fcp-call", call);
        return continuation(request, new(context.Method, context.Host, context.Options.WithHeaders(headers)));
    }
}

public sealed class DiagnosticServerInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var previous = DiagnosticContext.Current;
        var node = context.RequestHeaders.FirstOrDefault(entry => entry.Key == "fcp-node-bin")?.ValueBytes;
        // Untrusted correlation is bounded and has no role in accepting or rejecting a vote.
        DiagnosticContext.Current = new(
            node is { Length: <= 1024 } ? System.Text.Encoding.UTF8.GetString(node) : "",
            ReadId(context.RequestHeaders, "fcp-operation"), ReadId(context.RequestHeaders, "fcp-call"));
        try { return await continuation(request, context).ConfigureAwait(false); }
        finally { DiagnosticContext.Current = previous; }
    }

    private static string? ReadId(Metadata headers, string name)
    {
        var value = headers.FirstOrDefault(entry => entry.Key == name)?.Value;
        return Guid.TryParseExact(value, "N", out _) ? value : null;
    }
}
