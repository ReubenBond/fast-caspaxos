using Microsoft.Coyote.IO;

namespace FastCASPaxos.Utilities;

public static class LogHelper
{
    public static void Log(this ILogger logger, string value) => logger.WriteLine(LogSeverity.Important, value);
}
