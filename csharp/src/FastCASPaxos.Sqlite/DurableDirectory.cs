using System.ComponentModel;
using System.Runtime.InteropServices;
using FastCASPaxos.Server;
using Microsoft.Win32.SafeHandles;

namespace FastCASPaxos.Sqlite;

/// <summary>
/// Makes directory entries durable with the platform's directory flush: <c>fsync</c> of a directory descriptor on Linux
/// and macOS, and on Windows <c>FlushFileBuffers</c> of a directory handle opened with backup semantics and the right to
/// add the flushed kind of entry.
/// </summary>
/// <remarks>
/// A failed flush is reported to the fatal-error handler, since afterward the process cannot know what the device
/// holds, and is then thrown as an <see cref="IOException"/> whose inner <see cref="Win32Exception"/> carries the
/// operating system's error. Any other error, such as a missing parent or a denied open, attempts no flush, so a later
/// flush still makes the names durable; it is thrown without a report. On Windows only NTFS implements a directory flush
/// ("Other file systems return STATUS_SUCCESS and perform no other action", MS-FSA 2.1.5.7), so a directory on another
/// file system or on a remote volume is refused with <see cref="PlatformNotSupportedException"/>.
/// </remarks>
public static partial class DurableDirectory
{
    // FILE_ADD_FILE and FILE_ADD_SUBDIRECTORY: NtFlushBuffersFileEx fails a handle with neither write nor append access.
    private const uint WindowsAddFile = 0x2;
    private const uint WindowsAddSubdirectory = 0x4;

    // FILE_FLAG_BACKUP_SEMANTICS: CreateFileW opens a directory only with this flag.
    private const uint WindowsBackupSemantics = 0x02000000;

    // FileFsDeviceInformation and its FILE_REMOTE_DEVICE characteristic (wdm.h).
    private const int FileFsDeviceInformation = 4;
    private const uint RemoteDevice = 0x10;

    // MAX_PATH + 1 characters, the most GetVolumeInformationByHandleW writes.
    private const int FileSystemNameLength = 261;

    // O_CLOEXEC beside O_RDONLY (0). Every Linux architecture .NET supports uses the generic value.
    private const int LinuxCloseOnExec = 0x80000;
    private const int MacOsCloseOnExec = 0x1000000;

    /// <summary>
    /// Creates the directory under its existing parent, or accepts an existing one, and flushes the parent so the
    /// directory's name is durable. Repeating it makes durable a name which a crashed creator left unflushed.
    /// </summary>
    /// <param name="path">The directory. A volume root has no name in a parent and is accepted as it is.</param>
    /// <param name="fatalErrors">Receives a failed flush.</param>
    /// <exception cref="DirectoryNotFoundException">The parent does not exist.</exception>
    public static void Create(string path, IFatalErrorHandler fatalErrors)
    {
        ArgumentNullException.ThrowIfNull(fatalErrors);
        path = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        if (Path.GetDirectoryName(path) is not { } parent) return;
        try
        {
            _ = File.GetAttributes(parent);
        }
        catch (Exception exception) when (exception is FileNotFoundException or DirectoryNotFoundException)
        {
            throw new DirectoryNotFoundException(
                $"The parent of the directory '{path}' does not exist; create '{parent}' durably first.", exception);
        }

        Directory.CreateDirectory(path);
        Flush(parent, WindowsAddSubdirectory, fatalErrors);
    }

    /// <summary>Flushes the directory so the names of the files created in it are durable.</summary>
    /// <param name="path">The directory.</param>
    /// <param name="fatalErrors">Receives a failed flush.</param>
    public static void FlushFiles(string path, IFatalErrorHandler fatalErrors)
    {
        ArgumentNullException.ThrowIfNull(fatalErrors);
        Flush(Path.GetFullPath(path), WindowsAddFile, fatalErrors);
    }

    /// <summary>
    /// Throws <see cref="PlatformNotSupportedException"/> unless a directory's volume is local and its file system is
    /// NTFS, the only Windows file system whose directory flush makes the directory's entries durable.
    /// </summary>
    internal static void ThrowIfUnqualified(string path, string fileSystem, bool remote)
    {
        if (remote || fileSystem != "NTFS")
        {
            throw new PlatformNotSupportedException(
                $"The directory '{path}' lies on a {(remote ? "remote" : fileSystem)} volume; on Windows, durable directories lie on local NTFS volumes.");
        }
    }

    private static void Flush(string path, uint windowsAccess, IFatalErrorHandler fatalErrors)
    {
        if (OperatingSystem.IsWindows())
        {
            FlushWindows(path, windowsAccess, fatalErrors);
            return;
        }

        var closeOnExec = OperatingSystem.IsLinux() ? LinuxCloseOnExec
            : OperatingSystem.IsMacOS() ? MacOsCloseOnExec
            : throw new PlatformNotSupportedException("Durable directories require Linux, macOS, or Windows.");
        var descriptor = Open(path, closeOnExec);
        if (descriptor < 0) throw Failure("directory open", path, LastError());
        try
        {
            if (Fsync(descriptor) != 0) throw Report(fatalErrors, "directory fsync", path, LastError());
        }
        finally
        {
            // The fsync decided the flush; closing a read-only directory descriptor loses nothing.
            _ = Close(descriptor);
        }
    }

    private static void FlushWindows(string path, uint access, IFatalErrorHandler fatalErrors)
    {
        // The extended form opens a local path beyond MAX_PATH; a UNC path keeps its form and is refused as remote.
        using var handle = CreateFile(path.StartsWith(@"\\", StringComparison.Ordinal) ? path : @"\\?\" + path,
            access, FileShare.ReadWrite | FileShare.Delete, 0, FileMode.Open, WindowsBackupSemantics, 0);
        if (handle.IsInvalid) throw Failure("directory open", path, LastError());
        ThrowIfUnqualified(path, FileSystemName(handle, path), IsRemote(handle, path));
        if (!FlushFileBuffers(handle)) throw Report(fatalErrors, "directory flush", path, LastError());
    }

    private static string FileSystemName(SafeFileHandle handle, string path)
    {
        Span<char> name = stackalloc char[FileSystemNameLength];
        if (GetVolumeInformationByHandle(handle, 0, 0, 0, 0, 0, name, FileSystemNameLength) == 0)
            throw Failure("file system query", path, LastError());
        return new string(name[..name.IndexOf('\0')]);
    }

    private static bool IsRemote(SafeFileHandle handle, string path)
    {
        // IO_STATUS_BLOCK is two pointer-sized words; FILE_FS_DEVICE_INFORMATION is DeviceType, then Characteristics.
        Span<nint> status = stackalloc nint[2];
        Span<uint> device = stackalloc uint[2];
        var result = NtQueryVolumeInformationFile(handle, status, device, 2 * sizeof(uint), FileFsDeviceInformation);
        if (result < 0) throw Failure("volume query", path, new Win32Exception((int)RtlNtStatusToDosError(result)));
        return (device[1] & RemoteDevice) != 0;
    }

    private static IOException Report(IFatalErrorHandler fatalErrors, string operation, string path, Win32Exception error)
    {
        fatalErrors.OnLocalStorageFailure(operation, path, error);
        return Failure(operation, path, error);
    }

    private static IOException Failure(string operation, string path, Win32Exception error) =>
        new($"The {operation} of '{path}' failed.", error);

    private static Win32Exception LastError() => new(Marshal.GetLastPInvokeError());

    [LibraryImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial SafeFileHandle CreateFile(
        string path, uint access, FileShare share, nint securityAttributes, FileMode creationDisposition, uint flagsAndAttributes, nint template);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool FlushFileBuffers(SafeFileHandle handle);

    [LibraryImport("kernel32.dll", EntryPoint = "GetVolumeInformationByHandleW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial int GetVolumeInformationByHandle(
        SafeFileHandle handle, nint volumeName, uint volumeNameLength, nint serialNumber, nint maximumComponentLength,
        nint fileSystemFlags, Span<char> fileSystemName, uint fileSystemNameLength);

    [LibraryImport("ntdll.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial int NtQueryVolumeInformationFile(
        SafeFileHandle handle, Span<nint> status, Span<uint> information, uint length, int informationClass);

    [LibraryImport("ntdll.dll")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    private static partial uint RtlNtStatusToDosError(int status);

    [LibraryImport("libc", EntryPoint = "open", SetLastError = true, StringMarshalling = StringMarshalling.Utf8)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial int Open(string path, int flags);

    [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial int Fsync(int descriptor);

    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial int Close(int descriptor);
}
