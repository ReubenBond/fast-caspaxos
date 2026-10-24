using System.Globalization;
using FastCASPaxos.Server.Diagnostics;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class DiagnosticJournalTests
{
    private static readonly TimeSpan Heartbeat = TimeSpan.FromSeconds(15);

    [Fact]
    public async Task InitialResetUsesCurrentCursorAndControlledMetadata()
    {
        var clock = new ManualTime();
        using var journal = new NodeEventJournal("node-a", 3, clock);
        journal.Publish("sample", 1);
        clock.Advance(TimeSpan.FromSeconds(2));
        await using var reader = journal.ReadAsync(null, Heartbeat).GetAsyncEnumerator();
        var reset = await Next(reader);
        Assert.Equal(1, reset.SchemaVersion);
        Assert.Equal("node-a", reset.NodeId);
        Assert.Equal(journal.BootId, reset.BootId);
        Assert.Equal("1", reset.Sequence);
        Assert.Equal($"{journal.BootId}:1", reset.Id);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(2), reset.Timestamp);
        Assert.Equal(DateTimeOffset.UnixEpoch, journal.StartedAt);
        Assert.Equal("reset", reset.Kind);
        Assert.Equal("initial", Assert.IsType<ResetObservation>(reset.Data).Reason);
        journal.Publish("sample", 2);
        Assert.Equal(2, (await Next(reader)).Data);
    }

    public enum ResetCase { Invalid, Negative, Overflow, ExtraSeparator, OtherBoot, Future, Expired }

    [Theory]
    [InlineData(ResetCase.Invalid, "invalid-cursor")]
    [InlineData(ResetCase.Negative, "invalid-cursor")]
    [InlineData(ResetCase.Overflow, "invalid-cursor")]
    [InlineData(ResetCase.ExtraSeparator, "invalid-cursor")]
    [InlineData(ResetCase.OtherBoot, "restarted")]
    [InlineData(ResetCase.Future, "history-unavailable")]
    [InlineData(ResetCase.Expired, "history-unavailable")]
    public async Task InvalidOrUnavailableCursorResetsThenFollowsLive(ResetCase scenario, string reason)
    {
        using var journal = new NodeEventJournal("a", 3);
        for (var i = 1; i <= 4; i++) journal.Publish("sample", i);
        var after = scenario switch
        {
            ResetCase.Invalid => "garbage",
            ResetCase.Negative => $"{journal.BootId}:-1",
            ResetCase.Overflow => $"{journal.BootId}:9223372036854775808",
            ResetCase.ExtraSeparator => $"{journal.BootId}:1:2",
            ResetCase.OtherBoot => $"{Guid.NewGuid():N}:4",
            ResetCase.Future => $"{journal.BootId}:5",
            _ => $"{journal.BootId}:0"
        };
        await using var reader = journal.ReadAsync(after, Heartbeat).GetAsyncEnumerator();
        var reset = await Next(reader);
        Assert.Equal("reset", reset.Kind);
        Assert.Equal(reason, Assert.IsType<ResetObservation>(reset.Data).Reason);
        Assert.Equal($"{journal.BootId}:4", reset.Id);
        journal.Publish("sample", 5);
        var live = await Next(reader);
        Assert.Equal("5", live.Sequence);
        Assert.Equal(5, live.Data);
    }

    [Fact]
    public async Task ExactCapacityReplayHandoffAndWaitingPublishLoseNoEvents()
    {
        var clock = new ManualTime();
        using var journal = new NodeEventJournal("a", 3, clock);
        var origin = journal.Cursor;
        for (var i = 1; i <= 3; i++) journal.Publish("sample", i);
        await using var reader = journal.ReadAsync(origin, Heartbeat).GetAsyncEnumerator();
        Assert.Equal(1, (await Next(reader)).Data);
        // Publication while replay is suspended must neither duplicate nor skip the handoff.
        journal.Publish("sample", 4);
        foreach (var expected in new[] { 2, 3, 4 })
        {
            var item = await Next(reader);
            Assert.Equal("sample", item.Kind);
            Assert.Equal(expected.ToString(CultureInfo.InvariantCulture), item.Sequence);
            Assert.Equal(expected, item.Data);
        }
        var waiting = reader.MoveNextAsync().AsTask();
        await clock.WaitForDelayAsync(Heartbeat);
        Assert.False(waiting.IsCompleted);
        journal.Publish("sample", 5);
        Assert.True(await waiting.WaitAsync(Watchdog));
        Assert.Equal("5", reader.Current.Sequence);
        Assert.Equal(5, reader.Current.Data);
    }

    [Fact]
    public async Task SlowReaderRecoversAtCurrentCursorWithoutBlockingPublishersOrFastReader()
    {
        using var journal = new NodeEventJournal("a", 3);
        await using var slow = journal.ReadAsync(null, Heartbeat).GetAsyncEnumerator();
        await using var fast = journal.ReadAsync(null, Heartbeat).GetAsyncEnumerator();
        await Next(slow);
        await Next(fast);
        for (var i = 1; i <= 20; i++)
        {
            journal.Publish("sample", i);
            Assert.Equal(i, (await Next(fast)).Data);
        }
        var reset = await Next(slow);
        Assert.Equal("history-unavailable", Assert.IsType<ResetObservation>(reset.Data).Reason);
        Assert.Equal($"{journal.BootId}:20", reset.Id);
        journal.Publish("sample", 21);
        Assert.Equal(21, (await Next(slow)).Data);
    }

    [Fact]
    public async Task QuietHeartbeatUsesInjectedClockAndDoesNotAdvanceCursor()
    {
        var clock = new ManualTime();
        using var journal = new NodeEventJournal("a", 3, clock);
        journal.Publish("storage", new StorageObservation("ready"));
        await using var reader = journal.ReadAsync(journal.Cursor, Heartbeat).GetAsyncEnumerator();
        var pending = reader.MoveNextAsync().AsTask();
        await clock.WaitForDelayAsync(Heartbeat);
        clock.Advance(Heartbeat - TimeSpan.FromTicks(1));
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromTicks(1));
        Assert.True(await pending.WaitAsync(Watchdog));
        Assert.Equal("heartbeat", reader.Current.Kind);
        Assert.Equal("1", reader.Current.Sequence);
        Assert.Equal(journal.Cursor, reader.Current.Id);
        Assert.Equal(DateTimeOffset.UnixEpoch + Heartbeat, reader.Current.Timestamp);
        Assert.Equal("ready", Assert.IsType<HeartbeatObservation>(reader.Current.Data).StorageStatus);
        journal.Publish("sample", 2);
        Assert.Equal(2, (await Next(reader)).Data);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitingReaderCancellationOrDisposalReleasesIt(bool dispose)
    {
        var clock = new ManualTime();
        using var journal = new NodeEventJournal("a", 3, clock);
        using var cancellation = new CancellationTokenSource();
        await using var reader = journal.ReadAsync(journal.Cursor, Heartbeat, cancellation.Token).GetAsyncEnumerator();
        var pending = reader.MoveNextAsync().AsTask();
        await clock.WaitForDelayAsync(Heartbeat);
        if (dispose)
        {
            journal.Dispose();
            Assert.False(await pending.WaitAsync(Watchdog));
            journal.Publish("ignored", 7);
            Assert.Equal($"{journal.BootId}:0", journal.Cursor);
            journal.Dispose();
        }
        else
        {
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(Watchdog));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            journal.Publish("sample", 1);
            Assert.Equal($"{journal.BootId}:1", journal.Cursor);
        }
    }

    [Fact]
    public void StorageAndOperationSnapshotsRemainIndependent()
    {
        using var journal = new NodeEventJournal("a", 3);
        Assert.Equal(new StorageObservation("starting"), journal.Storage);
        Assert.Null(journal.LastOperation);
        var operation = new OperationObservation("compare-exchange", "op", null, "unknown", Error: "IOException");
        journal.Publish("operation", operation);
        var lastOperation = Assert.IsType<NodeEvent>(journal.LastOperation);
        Assert.Same(operation, lastOperation.Data);
        Assert.Equal($"{journal.BootId}:1", lastOperation.Id);
        Assert.Equal("operation", lastOperation.Kind);
        Assert.Equal("starting", journal.Storage.Status);
        foreach (var status in new[] { "ready", "failed", "stopped" })
        {
            journal.Publish("storage", new StorageObservation(status));
            Assert.Equal(status, journal.Storage.Status);
            Assert.Same(lastOperation, journal.LastOperation);
        }
        journal.Dispose();
        Assert.Null(journal.LastOperation);
    }

    [Fact]
    public async Task ValidationRejectsInvalidCapacityNodeAndHeartbeat()
    {
        Assert.Throws<ArgumentException>(() => new NodeEventJournal(" "));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NodeEventJournal("a", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NodeEventJournal("a", -1));
        using var journal = new NodeEventJournal("a");
        foreach (var interval in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1), TimeSpan.FromDays(1).Add(TimeSpan.FromTicks(1)) })
        {
            await using var reader = journal.ReadAsync(null, interval).GetAsyncEnumerator();
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reader.MoveNextAsync().AsTask());
        }
    }

    internal static async Task<NodeEvent> Next(IAsyncEnumerator<NodeEvent> reader)
    {
        Assert.True(await reader.MoveNextAsync().AsTask().WaitAsync(Watchdog));
        return reader.Current;
    }
}
