namespace FastCASPaxos.Host;

internal static partial class Log
{
    [LoggerMessage(1, LogLevel.Warning, "Cluster repair was only partially staged.")]
    public static partial void ClusterRepairPartiallyStaged(this ILogger logger, Exception error);

    [LoggerMessage(2, LogLevel.Error, "Local key cannot be represented as HTTP Unicode text.")]
    public static partial void LocalKeyNotUnicode(this ILogger logger, Exception error);

    [LoggerMessage(3, LogLevel.Error, "Local acceptor inspection failed.")]
    public static partial void InspectionFailed(this ILogger logger, Exception error);

    [LoggerMessage(4, LogLevel.Debug, "Diagnostic stream disconnected.")]
    public static partial void DiagnosticStreamDisconnected(this ILogger logger, Exception error);

    [LoggerMessage(5, LogLevel.Warning,
        "Shutdown cancellation ended the durability wait; replication may be incomplete. Retain this node's database and WAL.")]
    public static partial void DurabilityWaitCancelled(this ILogger logger);

    [LoggerMessage(6, LogLevel.Warning, "Durability repair observed an unavailable peer or key.")]
    public static partial void RepairPeerUnavailable(this ILogger logger, Exception error);

    [LoggerMessage(7, LogLevel.Warning, "Durability repair inventory is unavailable; another pass is scheduled.")]
    public static partial void RepairInventoryUnavailable(this ILogger logger, Exception error);

    [LoggerMessage(8, LogLevel.Warning,
        "Shutdown cancellation ended the restart drain. Retain this node's database and WAL.")]
    public static partial void RestartDrainCancelled(this ILogger logger);
}
