using System.ComponentModel;
using FastCASPaxos.Server;

namespace FastCASPaxos.Host;

/// <summary>
/// Ends the host after a failed local storage operation: it writes the operation, the path, and the operating-system
/// error to standard error and exits with <see cref="ExitCode"/>, leaving restart to the process supervisor.
/// </summary>
/// <remarks>
/// Reports arrive while <see cref="NodeRuntime.OpenAsync"/> runs, before the web host starts, so no graceful shutdown
/// runs against the failed storage.
/// </remarks>
internal sealed class ExitingFatalErrorHandler : IFatalErrorHandler
{
    /// <summary>The exit code after a failed local storage operation: sysexits' <c>EX_IOERR</c>.</summary>
    public const int ExitCode = 74;

    public void OnLocalStorageFailure(string operation, string path, Exception exception)
    {
        Console.Error.WriteLine(Describe(operation, path, exception));
        Environment.Exit(ExitCode);
    }

    internal static string Describe(string operation, string path, Exception exception) =>
        $"Fatal local storage failure: the {operation} of '{path}' failed: {exception.Message}"
        + (exception is Win32Exception os ? $" (operating-system error {os.NativeErrorCode})" : "");
}
