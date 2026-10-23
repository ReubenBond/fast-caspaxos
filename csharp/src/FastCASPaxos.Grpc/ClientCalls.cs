using Grpc.Core;

namespace FastCASPaxos.Grpc;

internal sealed class ClientCalls
{
    private readonly TimeSpan _timeout;

    public ClientCalls(TimeSpan? timeout)
    {
        _timeout = timeout ?? TimeSpan.FromSeconds(30);
        if (_timeout <= TimeSpan.Zero || _timeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(timeout), "A call timeout must be positive and at most one day.");
    }

    public CallOptions Options(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return new CallOptions(deadline: DateTime.UtcNow + _timeout, cancellationToken: token);
    }

    public async Task<T> Invoke<T>(Func<CallOptions, AsyncUnaryCall<T>> start, CancellationToken token)
    {
        var options = Options(token);
        try
        {
            using var call = start(options);
            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (RpcException error) { throw RpcErrors.FromRpc(error, token); }
    }
}
