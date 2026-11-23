using System.ComponentModel;
using System.Runtime.InteropServices;
using Orleans.Consensus.CasPaxos.Server;

namespace Orleans.Consensus.CasPaxos.Host;

/// <summary>
/// Ends the host after a failed local storage operation: it writes the operation, the path, and the operating-system
/// error to standard error and terminates the process at once with <see cref="ExitCode"/>, leaving restart to the
/// process supervisor.
/// </summary>
/// <remarks>
/// Reports arrive while the node opens and while it serves. Termination runs no process-exit handlers, so the web
/// host's graceful shutdown, with its drain and departure barrier, never runs against the failed storage.
/// </remarks>
internal sealed partial class ExitingFatalErrorHandler : IFatalErrorHandler
{
    /// <summary>The exit code after a failed local storage operation: sysexits' <c>EX_IOERR</c>.</summary>
    public const int ExitCode = 74;

    public void OnLocalStorageFailure(string operation, string path, Exception exception)
    {
        Console.Error.WriteLine(Describe(operation, path, exception));
        Console.Error.Flush();
        if (OperatingSystem.IsWindows()) TerminateProcess(GetCurrentProcess(), ExitCode);
        else ExitImmediately(ExitCode);
    }

    internal static string Describe(string operation, string path, Exception exception) =>
        $"Fatal local storage failure: the {operation} of '{path}' failed: {exception.Message}"
        + (exception is Win32Exception os ? $" (operating-system error {os.NativeErrorCode})" : "");

    [LibraryImport("kernel32.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial nint GetCurrentProcess();

    [LibraryImport("kernel32.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool TerminateProcess(nint process, uint exitCode);

    [LibraryImport("libc", EntryPoint = "_exit")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial void ExitImmediately(int status);
}
