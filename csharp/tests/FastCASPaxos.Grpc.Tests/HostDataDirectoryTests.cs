using System.ComponentModel;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using FastCASPaxos.Host;
using FastCASPaxos.Testing;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

/// <summary>
/// A node makes each database directory's name, and the database names in it, durable before it answers: it creates
/// the directory under its existing parent and flushes the parent on every start, and each SQLite opener flushes its
/// database's directory. A directory which cannot be prepared fails the open, which answers nothing.
/// </summary>
public sealed class HostDataDirectoryTests
{
    // ERROR_ACCESS_DENIED on Windows; EACCES on Linux and macOS.
    private static readonly int AccessDenied = OperatingSystem.IsWindows() ? 5 : 13;

    [Fact]
    public async Task OpenCreatesEachDatabaseDirectoryUnderItsExistingParent()
    {
        using var directory = new TestDirectory();
        var options = Layout(directory);
        await using (await NodeRuntime.OpenAsync(options, FatalErrors).WaitAsync(Watchdog)) { }
        Assert.True(File.Exists(options.AcceptorDatabase));
        Assert.True(File.Exists(options.ProposerDatabase));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OpenRefusesADatabaseDirectoryWhoseParentIsMissing(bool proposer)
    {
        using var directory = new TestDirectory();
        var options = Layout(directory);
        var missing = Path.Combine(directory.Path, "missing");
        var database = Path.Combine(missing, "data", "database.db");
        if (proposer) options.ProposerDatabase = database;
        else options.AcceptorDatabase = database;
        var fatalErrors = new RecordingFatalErrorHandler();
        var error = await Assert.ThrowsAsync<DirectoryNotFoundException>(() => NodeRuntime.OpenAsync(options, fatalErrors));
        Assert.Contains($"create '{missing}'", error.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(missing));
        Assert.Empty(Directory.GetFiles(directory.Path, "*.db", SearchOption.AllDirectories));
        Assert.Empty(fatalErrors.Failures);
    }

    /// <summary>
    /// Each database directory exists, as after a first start which crashed before its flushes, so every flush
    /// repeats: the acceptor's parent, the proposer's parent, then each database's directory once SQLite has created
    /// its files. Denying one flush its open fails the open with the operating system's error, unreported, since no
    /// flush was attempted; a fresh open succeeds once the flush can be made.
    /// </summary>
    [AccessDeniableTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task OpenFailsWhenADirectoryFlushCannotOpenItsDirectory(bool proposer, bool databaseDirectory)
    {
        using var directory = new TestDirectory();
        var options = Layout(directory);
        Directory.CreateDirectory(Path.GetDirectoryName(options.AcceptorDatabase)!);
        Directory.CreateDirectory(Path.GetDirectoryName(options.ProposerDatabase)!);
        var data = Path.GetDirectoryName(proposer ? options.ProposerDatabase : options.AcceptorDatabase)!;
        var denied = databaseDirectory ? data : Path.GetDirectoryName(data)!;
        var fatalErrors = new RecordingFatalErrorHandler();
        IOException error;
        using (DenyFlush(denied, databaseDirectory))
        {
            error = await Assert.ThrowsAsync<IOException>(() => NodeRuntime.OpenAsync(options, fatalErrors).WaitAsync(Watchdog));
        }

        Assert.Equal($"The directory open of '{denied}' failed.", error.Message);
        Assert.Equal(AccessDenied, Assert.IsType<Win32Exception>(error.InnerException).NativeErrorCode);
        Assert.Empty(fatalErrors.Failures);
        Assert.Equal(databaseDirectory, File.Exists(options.AcceptorDatabase));
        Assert.Equal(proposer && databaseDirectory, File.Exists(options.ProposerDatabase));
        await using (await NodeRuntime.OpenAsync(options, FatalErrors).WaitAsync(Watchdog)) { }
    }

    [Fact]
    public void TheHostsFatalErrorMessageNamesTheOperationPathAndOperatingSystemError()
    {
        var error = new Win32Exception(5);
        Assert.Equal($"Fatal local storage failure: the directory fsync of '/data' failed: {error.Message} (operating-system error 5)",
            ExitingFatalErrorHandler.Describe("directory fsync", "/data", error));
    }

    /// <summary>Places each database in its own directory under its own existing parent.</summary>
    private static NodeOptions Layout(TestDirectory directory)
    {
        var options = HostLifecycleTests.Options(directory);
        options.AcceptorDatabase = Path.Combine(Directory.CreateDirectory(Path.Combine(directory.Path, "acceptor-parent")).FullName, "acceptor", "acceptor.db");
        options.ProposerDatabase = Path.Combine(Directory.CreateDirectory(Path.Combine(directory.Path, "proposer-parent")).FullName, "proposer", "proposer.db");
        return options;
    }

    /// <summary>
    /// Denies this process the open a flush of <paramref name="path"/> makes, leaving every other access to it intact:
    /// on Linux and macOS, reading it; on Windows, adding a subdirectory to a parent, or, for a database directory whose
    /// files SQLite still creates, the synchronization every handle takes.
    /// </summary>
    private static Restore DenyFlush(string path, bool databaseDirectory) => OperatingSystem.IsWindows()
        ? DenyWindows(path, databaseDirectory ? FileSystemRights.Synchronize : FileSystemRights.CreateDirectories)
        : DenyUnix(path);

    [UnsupportedOSPlatform("windows")]
    private static Restore DenyUnix(string path)
    {
        var mode = File.GetUnixFileMode(path);
        File.SetUnixFileMode(path, UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        return new Restore(() => File.SetUnixFileMode(path, mode));
    }

    [SupportedOSPlatform("windows")]
    private static Restore DenyWindows(string path, FileSystemRights rights)
    {
        var directory = new DirectoryInfo(path);
        var security = directory.GetAccessControl();
        // The factory keeps the mask as given; FileSystemAccessRule's public constructor drops Synchronize from a denial.
        var rule = (FileSystemAccessRule)security.AccessRuleFactory(WindowsIdentity.GetCurrent().User!, (int)rights,
            isInherited: false, InheritanceFlags.None, PropagationFlags.None, AccessControlType.Deny);
        security.AddAccessRule(rule);
        directory.SetAccessControl(security);
        return new Restore(() =>
        {
            _ = security.RemoveAccessRule(rule);
            directory.SetAccessControl(security);
        });
    }

    private sealed class Restore(Action restore) : IDisposable
    {
        public void Dispose() => restore();
    }
}

/// <summary>A theory which denies this process access, and so skips for a privileged Unix process, which bypasses it.</summary>
public sealed class AccessDeniableTheoryAttribute : TheoryAttribute
{
    public AccessDeniableTheoryAttribute()
    {
        if (!OperatingSystem.IsWindows() && Environment.IsPrivilegedProcess)
            Skip = "A privileged process bypasses denied directory access.";
    }
}
