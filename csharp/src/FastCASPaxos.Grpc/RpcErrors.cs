using FastCASPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

/// <summary>A remote transport or service failure, retaining its gRPC status and diagnostic cause.</summary>
public sealed class RemoteCallException(StatusCode statusCode, string message, Exception? innerException = null)
    : Exception(message, innerException)
{
    public StatusCode StatusCode { get; } = statusCode;
}

internal static class RpcErrors
{
    private const string FailureKey = "fastcaspaxos-failure";

    public static async Task<T> Serve<T>(Func<Task<T>> action, ServerCallContext context, ILogger logger)
    {
        try { return await action().ConfigureAwait(false); }
        catch (ArgumentException error) { throw Definite(StatusCode.InvalidArgument, "invalid", error.Message); }
        catch (RegisterBusyException error) { throw Definite(StatusCode.ResourceExhausted, "busy", error.Message); }
        catch (RegisterUnavailableException error)
        {
            logger.RegisterUnavailable(error);
            throw Definite(StatusCode.Unavailable, "unavailable", error.Message);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw Definite(StatusCode.Cancelled, "cancelled", "The operation was cancelled before mutation.");
        }
        catch (Exception error)
        {
            logger.OperationFailed(error);
            throw new RpcException(new Status(StatusCode.Internal, "The operation failed; consult the server log."));
        }
    }

    private static RpcException Definite(StatusCode code, string kind, string message) =>
        new(new Status(code, message), new Metadata { { FailureKey, kind } });

    public static bool IsDefinite(RpcException error) => (error.StatusCode, error.Trailers.GetValue(FailureKey)) is
        (StatusCode.InvalidArgument, "invalid") or (StatusCode.ResourceExhausted, "busy")
        or (StatusCode.Unavailable, "unavailable") or (StatusCode.Cancelled, "cancelled");

    public static Exception FromRpc(RpcException error, CancellationToken token)
    {
        if (IsDefinite(error))
        {
            return error.Trailers.GetValue(FailureKey) switch
            {
                "invalid" => new ArgumentException(error.Status.Detail, error),
                "busy" => new RegisterBusyException(),
                "unavailable" => new RegisterUnavailableException(error),
                "cancelled" => new OperationCanceledException(error.Status.Detail, error, token),
                _ => throw new InvalidOperationException("Unrecognized definite failure."),
            };
        }
        if (error.StatusCode == StatusCode.Cancelled && token.IsCancellationRequested)
            return new OperationCanceledException(error.Status.Detail, error, token);
        return new RemoteCallException(error.StatusCode, error.Status.Detail, error);
    }

    public static Wire.OperationError ToWire(Exception error) => new()
    {
        Code = error switch
        {
            OperationCanceledException => Wire.ErrorCode.Cancelled,
            TimeoutException => Wire.ErrorCode.Deadline,
            RemoteCallException { StatusCode: StatusCode.Unavailable } => Wire.ErrorCode.Unavailable,
            _ => Wire.ErrorCode.Internal,
        },
        Message = error.Message,
    };

    public static Exception FromWire(Wire.OperationError error)
    {
        var code = error.Code switch
        {
            Wire.ErrorCode.Cancelled => StatusCode.Cancelled,
            Wire.ErrorCode.Deadline => StatusCode.DeadlineExceeded,
            Wire.ErrorCode.Unavailable => StatusCode.Unavailable,
            Wire.ErrorCode.Internal => StatusCode.Internal,
            _ => throw new ArgumentException("An operation error must specify a recognized code."),
        };
        return new RemoteCallException(code, error.Message);
    }

    public static T Response<T>(Func<T> decode)
    {
        try { return decode(); }
        catch (ArgumentException error)
        {
            throw new RemoteCallException(StatusCode.DataLoss, "The remote response violates the wire contract.", error);
        }
    }
}
