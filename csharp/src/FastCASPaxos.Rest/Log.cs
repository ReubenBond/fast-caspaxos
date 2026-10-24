using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Rest;

internal static partial class Log
{
    [LoggerMessage(1, LogLevel.Warning, "Register write outcome is unknown.")]
    public static partial void WriteOutcomeUnknown(this ILogger logger, Exception error);

    [LoggerMessage(2, LogLevel.Information, "HTTP request body failed before register dispatch.")]
    public static partial void RequestBodyFailed(this ILogger logger, Exception error);

    [LoggerMessage(3, LogLevel.Information, "The register service rejected an argument before mutation.")]
    public static partial void ArgumentRejected(this ILogger logger, Exception error);

    [LoggerMessage(4, LogLevel.Warning, "Register operation reached its deadline before mutation.")]
    public static partial void RegisterUnavailable(this ILogger logger, Exception error);

    [LoggerMessage(5, LogLevel.Error, "HTTP register operation failed.")]
    public static partial void OperationFailed(this ILogger logger, Exception error);
}
