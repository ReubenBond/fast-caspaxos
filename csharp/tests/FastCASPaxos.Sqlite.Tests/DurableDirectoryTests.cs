using System.ComponentModel;
using FastCASPaxos.Testing;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

/// <summary>
/// A failed directory flush is reported to the fatal-error handler before it is thrown, since afterward the process
/// cannot know what the device holds. On Windows only a local NTFS volume's directory flush makes entries durable.
/// </summary>
public sealed class DurableDirectoryTests
{
    [LinuxFact]
    public void AFailedFsyncIsReportedAndThrown()
    {
        // procfs implements no fsync, so a directory fsync there fails with EINVAL.
        var fatalErrors = new RecordingFatalErrorHandler();
        var error = Assert.Throws<IOException>(() => DurableDirectory.FlushFiles("/proc", fatalErrors));
        var failure = Assert.Single(fatalErrors.Failures);
        Assert.Equal("directory fsync", failure.Operation);
        Assert.Equal("/proc", failure.Path);
        Assert.Equal(22, Assert.IsType<Win32Exception>(failure.Exception).NativeErrorCode);
        Assert.Same(failure.Exception, error.InnerException);
    }

    [Theory]
    [InlineData("NTFS", false, true)]
    [InlineData("NTFS", true, false)]
    [InlineData("ReFS", false, false)]
    [InlineData("FAT32", false, false)]
    public void OnlyALocalNtfsVolumeQualifiesForAWindowsDirectoryFlush(string fileSystem, bool remote, bool qualified)
    {
        var error = Record.Exception(() => DurableDirectory.ThrowIfUnqualified(@"D:\data", fileSystem, remote));
        if (qualified) Assert.Null(error);
        else Assert.IsType<PlatformNotSupportedException>(error);
    }

    [Fact]
    public void CreateAcceptsAVolumeRootWhichHasNoParentToFlush() =>
        DurableDirectory.Create(Path.GetPathRoot(Path.GetTempPath())!, TestDatabase.FatalErrors);

    /// <summary>
    /// An opener's directory flush covers every file it creates: each open, fresh or after a clean close removed the
    /// WAL, creates the WAL before it completes, and the opener's connection holds it until disposal.
    /// </summary>
    [Fact]
    public async Task EachOpenerCreatesItsWalBeforeItsOpenCompletes()
    {
        using var database = new TestDatabase();
        var allocator = Path.Combine(Path.GetDirectoryName(database.Path)!, "allocator.db");
        for (var open = 0; open < 2; open++)
        {
            await using (await database.OpenAsync())
                Assert.True(File.Exists(database.Path + "-wal"));
            using (new SqliteProposerBallotSource(allocator, 1, TestDatabase.FatalErrors))
                Assert.True(File.Exists(allocator + "-wal"));
        }
    }
}

/// <summary>A fact which exercises a Linux file system, and so skips elsewhere.</summary>
public sealed class LinuxFactAttribute : FactAttribute
{
    public LinuxFactAttribute()
    {
        if (!OperatingSystem.IsLinux()) Skip = "The test exercises a Linux file system.";
    }
}
