using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

internal static partial class Log
{
    [LoggerMessage(1, LogLevel.Warning, "CompareExchange has an unknown outcome.")]
    public static partial void CompareExchangeOutcomeUnknown(this ILogger logger, Exception error);

    [LoggerMessage(2, LogLevel.Warning, "Register operation reached its deadline before mutation.")]
    public static partial void RegisterUnavailable(this ILogger logger, Exception error);

    [LoggerMessage(3, LogLevel.Error, "gRPC operation failed.")]
    public static partial void OperationFailed(this ILogger logger, Exception error);
}
