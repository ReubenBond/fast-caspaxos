using FastCASPaxos.Model;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using Microsoft.Data.Sqlite;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class DiagnosticSqliteTests
{
    [Fact]
    public async Task Inspection_IncludesPromisesAndOrdinalExclusivePagesWithoutMutation()
    {
        using var database = new TestDatabase();
        using var journal = new NodeEventJournal("a");
        await using var server = await SqliteAcceptorServer.OpenAsync(database.Path, diagnostics: journal);
        Assert.Equal("ready", journal.Storage.Status);
        var emptyCursor = journal.Cursor;
        Assert.Null(await server.InspectAsync("missing"));
        Assert.Empty((await server.InspectKeysAsync()).Entries);
        Assert.Equal(emptyCursor, journal.Cursor);
        string[] keys = ["p/\uE000", "p/\U0001F600", "p/\0", "p/%", "p/_", "p/a", "other", ""];
        foreach (var key in keys)
            Assert.True((await server.PrepareAsync(key, new(new Ballot(9, 3)))).IsPromised);
        Assert.True((await server.AcceptAsync("p/a", new(new(9, 3), ByteString.Empty, new(11, 4)))).IsAccepted);
        var before = journal.Cursor;
        var promise = await server.InspectAsync("p/%");
        Assert.Equal(new LocalAcceptorState("p/%", new(9, 3), Ballot.Zero, null), promise);
        var accepted = await server.InspectAsync("p/a");
        Assert.Equal(new LocalAcceptorState("p/a", new(11, 4), new(9, 3), ByteString.Empty), accepted);
        var first = await server.InspectKeysAsync(limit: 1);
        Assert.Equal("", Assert.Single(first.Entries).Key);
        Assert.Equal("", first.ContinuationKey);
        var found = new List<string>();
        string? after = null;
        for (var i = 0; i < keys.Length; i++)
        {
            var page = await server.InspectKeysAsync("p/", after, 2);
            found.AddRange(page.Entries.Select(entry => entry.Key));
            if (page.ContinuationKey is null) break;
            Assert.Equal(page.Entries[^1].Key, page.ContinuationKey);
            after = page.ContinuationKey;
        }
        Assert.Equal(keys.Where(k => k.StartsWith("p/", StringComparison.Ordinal)).Order(StringComparer.Ordinal), found);
        Assert.Empty((await server.InspectKeysAsync("p/", "p/\uE000")).Entries);
        Assert.Equal("p/a", Assert.Single((await server.ListKeysAsync()).Entries).Key);
        Assert.Equal(before, journal.Cursor); // Inspection must not run Prepare or change any durable ballot.
        using var observer = database.Connect();
        using var query = observer.CreateCommand();
        query.CommandText = "SELECT count(*) FROM acceptor_state;";
        Assert.Equal((long)keys.Length, query.ExecuteScalar());
        Assert.Equal(promise, await server.InspectAsync("p/%"));
    }

    [Fact]
    public async Task PostCommitPublication_PrecedesResponseButNeverPrecedesDurability()
    {
        using var database = new TestDatabase();
        using var journal = new NodeEventJournal("a");
        var server = await SqliteAcceptorServer.OpenAsync(database.Path, diagnostics: journal);
        using var barrier = new CommitBarrier();
        using var cancellation = new CancellationTokenSource();
        var before = journal.Cursor;
        server.BeforeCommit = (_, _, count) => barrier.Arrive(count);
        try
        {
            var accepting = server.AcceptAsync("key", new(new(12, 7), new([0, 255]), new(19, 8)), cancellation.Token).AsTask();
            Assert.Equal(1, await barrier.Entered.WaitAsync(TestDatabase.Timeout));
            Assert.Equal(before, journal.Cursor);
            Assert.False(accepting.IsCompleted);
            using (var connection = database.Connect())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT count(*) FROM acceptor_state;";
                Assert.Equal(0L, command.ExecuteScalar());
            }
            cancellation.Cancel();
            Assert.False(accepting.IsCompleted);
            barrier.Release();
            Assert.True((await accepting.WaitAsync(TestDatabase.Timeout)).IsAccepted);
            Assert.NotEqual(before, journal.Cursor);
            await using var events = journal.ReadAsync(before, TimeSpan.FromDays(1)).GetAsyncEnumerator();
            var item = await Next(events);
            Assert.Equal("acceptor-state", item.Kind);
            var state = Assert.IsType<AcceptorStateObservation>(item.Data);
            Assert.Equal(new KeyPreview("key", 3, false), state.Key);
            Assert.Equal(new Ballot(19, 8), state.PromisedBallot);
            Assert.Equal(new Ballot(12, 7), state.AcceptedBallot);
            Assert.Equal(new byte[] { 0, 255 }, Convert.FromBase64String(state.AcceptedValue!.Base64));
            Assert.Equal("opaque", state.Register!.Format);
            server.BeforeCommit = null;
            Assert.Equal(new ByteString([0, 255]), (await server.InspectAsync("key"))!.AcceptedValue);
            var committed = journal.Cursor;
            Assert.True((await server.AcceptAsync("key", new(new(12, 7), new([0, 255]), new(99, 9)))).IsAccepted);
            Assert.False((await server.AcceptAsync("key", new(new(12, 7), new([1]), Ballot.Zero))).IsAccepted);
            Assert.False((await server.PrepareAsync("key", new(new Ballot(11, 7)))).IsPromised);
            Assert.True((await server.PrepareAsync("key", new(new Ballot(19, 8)))).IsPromised);
            Assert.Equal(committed, journal.Cursor);
        }
        finally
        {
            barrier.Release();
            await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
        }
        Assert.Equal("stopped", journal.Storage.Status);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SelectedInspection_CancellationWaitsForCommit(bool list)
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        Assert.True((await server.PrepareAsync("key", new(new Ballot(4, 2)))).IsPromised);
        using var barrier = new CommitBarrier();
        using var cancellation = new CancellationTokenSource();
        server.BeforeCommit = (_, _, count) => barrier.Arrive(count);
        try
        {
            var read = list
                ? ReadPage()
                : server.InspectAsync("key", cancellation.Token).AsTask();
            Assert.Equal(1, await barrier.Entered.WaitAsync(TestDatabase.Timeout));
            cancellation.Cancel();
            Assert.False(read.IsCompleted);
            barrier.Release();
            Assert.Equal(new LocalAcceptorState("key", new(4, 2), Ballot.Zero, null),
                await read.WaitAsync(TestDatabase.Timeout));
        }
        finally { barrier.Release(); }

        async Task<LocalAcceptorState?> ReadPage() =>
            Assert.Single((await server.InspectKeysAsync(cancellationToken: cancellation.Token)).Entries);
    }

    [Fact]
    public async Task RolledBackBatch_PublishesOnlyStorageFailureAndFailsSelectedInspection()
    {
        using var database = new TestDatabase();
        await using (var seed = await database.OpenAsync())
            Assert.True((await seed.AcceptAsync("stable", new(new(2, 1), new([7]), new(3, 1)))).IsAccepted);
        using var connection = database.Connect();
        TestDatabase.Execute(connection, $"""
            CREATE TABLE parent (id INTEGER PRIMARY KEY);
            CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER fail_commit AFTER INSERT ON acceptor_state
            WHEN NEW.key = X'{Convert.ToHexString(TestDatabase.KeyBytes("fail"))}'
            BEGIN INSERT INTO child VALUES (42); END;
            """);
        using var journal = new NodeEventJournal("a");
        var server = await SqliteAcceptorServer.OpenAsync(database.Path, 16, 16, diagnostics: journal);
        using var first = new CommitBarrier();
        using var batch = new CommitBarrier();
        using var cancellation = new CancellationTokenSource();
        var commits = 0;
        server.BeforeCommit = (_, _, count) =>
        {
            if (Interlocked.Increment(ref commits) == 1) first.Arrive(count);
            else batch.Arrive(count);
        };
        Exception? failure = null;
        var before = journal.Cursor;
        try
        {
            var initial = server.InspectAsync("stable").AsTask();
            Assert.Equal(1, await first.Entered.WaitAsync(TestDatabase.Timeout));
            var update = server.AcceptAsync("stable", new(new(4, 1), new([70]), new(5, 1)), cancellation.Token).AsTask();
            var promise = server.PrepareAsync("promise", new(new Ballot(6, 2)), cancellation.Token).AsTask();
            var fail = server.AcceptAsync("fail", new(new(4, 1), new([99]), Ballot.Zero), cancellation.Token).AsTask();
            var read = server.InspectAsync("stable", cancellation.Token).AsTask();
            var page = server.InspectKeysAsync(cancellationToken: cancellation.Token).AsTask();
            first.Release();
            Assert.Equal(new ByteString([7]), (await initial.WaitAsync(TestDatabase.Timeout))!.AcceptedValue);
            Assert.Equal(5, await batch.Entered.WaitAsync(TestDatabase.Timeout));
            Assert.Equal(before, journal.Cursor);
            cancellation.Cancel();
            Task[] selected = [update, promise, fail, read, page];
            Assert.All(selected, task => Assert.False(task.IsCompleted));
            batch.Release();
            failure = await Assert.ThrowsAsync<SqliteException>(() => update.WaitAsync(TestDatabase.Timeout));
            Assert.Equal(787, ((SqliteException)failure).SqliteExtendedErrorCode);
            foreach (var task in selected)
                Assert.Same(failure, await Record.ExceptionAsync(() => task.WaitAsync(TestDatabase.Timeout)));
            Assert.Equal("failed", journal.Storage.Status);
            Assert.Equal("SqliteException", journal.Storage.Error);
            await using var events = journal.ReadAsync(before, TimeSpan.FromDays(1)).GetAsyncEnumerator();
            var item = await Next(events);
            Assert.Equal("storage", item.Kind);
            Assert.Equal(journal.Cursor, item.Id); // No state event before or after the sole failure observation.
            Assert.Equal(new StorageObservation("failed", "SqliteException"), item.Data);
        }
        finally
        {
            first.Release();
            batch.Release();
            var error = await Record.ExceptionAsync(() => server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout));
            Assert.Same(failure, error);
        }
        await using var recovered = await database.OpenAsync();
        var row = Assert.Single((await recovered.InspectKeysAsync()).Entries);
        Assert.Equal(new LocalAcceptorState("stable", new(3, 1), new(2, 1), new([7])), row);
        Assert.True((await recovered.AcceptAsync("stable", new(new(4, 1), new([8]), Ballot.Zero))).IsAccepted);
        Assert.Equal(new ByteString([8]), (await recovered.InspectAsync("stable"))!.AcceptedValue);
    }

    [Fact]
    public async Task Inspection_ValidationAndPreCancellationDoNotPublishOrMutate()
    {
        using var database = new TestDatabase();
        using var journal = new NodeEventJournal("a");
        await using var server = await SqliteAcceptorServer.OpenAsync(database.Path, diagnostics: journal);
        var before = journal.Cursor;
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.InspectAsync(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.InspectKeysAsync(null!).AsTask());
        foreach (var limit in new[] { 0, -1 })
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => server.InspectKeysAsync(limit: limit).AsTask());
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.InspectAsync("absent", cancellation.Token).AsTask());
        Assert.Equal(cancellation.Token, error.CancellationToken);
        error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.InspectKeysAsync(cancellationToken: cancellation.Token).AsTask());
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Empty((await server.InspectKeysAsync()).Entries);
        Assert.Equal(before, journal.Cursor);
    }

    private static async Task<NodeEvent> Next(IAsyncEnumerator<NodeEvent> reader)
    {
        Assert.True(await reader.MoveNextAsync().AsTask().WaitAsync(TestDatabase.Timeout));
        return reader.Current;
    }

    private sealed class CommitBarrier : IDisposable
    {
        private readonly TaskCompletionSource<int> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _release = new();
        public Task<int> Entered => _entered.Task;
        public void Arrive(int count)
        {
            _entered.SetResult(count);
            if (!_release.Wait(TestDatabase.Timeout)) throw new TimeoutException("Commit barrier was not released.");
        }
        public void Release() => _release.Set();
        public void Dispose() => _release.Dispose();
    }
}
