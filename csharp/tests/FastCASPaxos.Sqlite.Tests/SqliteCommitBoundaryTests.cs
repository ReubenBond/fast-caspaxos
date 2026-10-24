using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Microsoft.Data.Sqlite;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteCommitBoundaryTests
{
    [Fact]
    public async Task Read_QueuedBehindAccept_ReturnsOnlyCommittedStateAndHonorsQueuedCancellation()
    {
        using var database = new TestDatabase();
        using var gate = new CommitGate();
        using var cancellation = new CancellationTokenSource();
        var server = await database.OpenAsync();
        server.BeforeCommit = (_, _, count) =>
        {
            // Only the accept transaction uses this single-use gate, not subsequent queued reads.
            server.BeforeCommit = null;
            gate.ArriveAndWait(count);
        };
        try
        {
            var write = server.AcceptAsync("key", new(new(2, 1), new([7, 255]), new(3, 1))).AsTask();
            Assert.Equal(1, await gate.Entered.WaitAsync(TestDatabase.Timeout));
            var read = server.ReadAsync("key").AsTask();
            var canceled = server.ReadAsync("missing", cancellationToken: cancellation.Token).AsTask();
            cancellation.Cancel();
            Assert.False(read.IsCompleted);
            using (var observer = database.Connect()) Assert.Empty(ReadRows(observer));

            gate.Release();
            Assert.True((await write.WaitAsync(TestDatabase.Timeout)).IsAccepted);
            var result = await read.WaitAsync(TestDatabase.Timeout);
            Assert.Equal(new KeyedAcceptorState("key", new(3, 1), new(2, 1), new([7, 255])),
                result switch { KeyedAcceptorState state => state, _ => null });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.WaitAsync(TestDatabase.Timeout));
            Assert.Null(await server.InspectAsync("missing"));
            using var committed = database.Connect();
            AssertRow(Assert.Single(ReadRows(committed)), "key", new(3, 1), new(2, 1), [7, 255]);
        }
        finally
        {
            gate.Release();
            await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
        }
    }

    [Fact]
    public async Task CancellationAfterSelection_AwaitsSuccessfulDurableCommit()
    {
        using var database = new TestDatabase();
        using var gate = new CommitGate();
        using var cancellation = new CancellationTokenSource();
        var server = await database.OpenAsync();
        server.BeforeCommit = (_, _, count) => gate.ArriveAndWait(count);
        try
        {
            var accepting = server.AcceptAsync("key",
                new(new(2, 1), new([7, 255]), new(3, 1)), cancellation.Token).AsTask();
            Assert.Equal(1, await gate.Entered.WaitAsync(TestDatabase.Timeout));

            // Reaching BeforeCommit proves selection and SQL execution already occurred.
            // Cancellation must neither complete the response early nor undo the write.
            cancellation.Cancel();
            Assert.False(accepting.IsCompleted);
            using (var observer = database.Connect())
            {
                Assert.Empty(ReadRows(observer));
            }

            gate.Release();
            var result = await accepting.WaitAsync(TestDatabase.Timeout);
            Assert.True(result.IsAccepted);
            Assert.Equal(new Ballot(3, 1), result.PromisedBallot);
            Assert.Equal(Ballot.Zero, result.ConflictingBallot);
            using var committedObserver = database.Connect();
            AssertRow(Assert.Single(ReadRows(committedObserver)), "key",
                new(3, 1), new(2, 1), [7, 255]);
        }
        finally
        {
            gate.Release();
            await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
        }

        await using var reopened = await database.OpenAsync();
        var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal("key", state.Key);
        Assert.Equal(new Ballot(2, 1), state.AcceptedBallot);
        Assert.Equal(new Ballot(3, 1), state.PromisedBallot);
        Assert.Equal(new ByteString([7, 255]), state.AcceptedValue);
    }

    [Fact]
    public async Task MultiRequestCommitFailure_RollsBackEntireBatchDespiteCancellation()
    {
        using var database = new TestDatabase();
        await using (var seed = await database.OpenAsync())
        {
            Assert.True((await seed.AcceptAsync("stable",
                new(new(2, 1), new([7]), new(3, 1)))).IsAccepted);
        }

        using var observer = database.Connect();
        TestDatabase.Execute(observer, $"""
            CREATE TABLE batch_fault_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE batch_fault_child (
                parent_id INTEGER REFERENCES batch_fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER fail_batch AFTER INSERT ON acceptor_state
            WHEN NEW.key = X'{Convert.ToHexString(TestDatabase.KeyBytes("failure"))}'
            BEGIN
                INSERT INTO batch_fault_child VALUES (42);
            END;
            """);

        using var firstGate = new CommitGate();
        using var batchGate = new CommitGate();
        using var cancellation = new CancellationTokenSource();
        var server = await database.OpenAsync(queueCapacity: 16, maxBatchSize: 16);
        var commits = 0;
        StoredRow[]? stagedRows = null;
        long stagedForeignKeyViolations = 0;
        SqliteException? failure = null;
        server.BeforeCommit = (connection, transaction, count) =>
        {
            if (Interlocked.Increment(ref commits) == 1)
            {
                firstGate.ArriveAndWait(count);
                return;
            }

            // Capture state on its owning thread. The gate publishes this snapshot
            // only after every item in the batch has executed, before COMMIT.
            stagedRows = ReadRows(connection, transaction);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT count(*) FROM pragma_foreign_key_check;";
            stagedForeignKeyViolations = (long)command.ExecuteScalar()!;
            batchGate.ArriveAndWait(count);
        };

        try
        {
            var initialRead = server.ListKeysAsync().AsTask();
            Assert.Equal(1, await firstGate.Entered.WaitAsync(TestDatabase.Timeout));

            // The writer is parked inside the first transaction, and capacity exceeds
            // these five requests. They are all admitted before its next queue scan.
            var update = server.AcceptAsync("stable",
                new(new(4, 1), new([70]), new(5, 1)), cancellation.Token).AsTask();
            var insert = server.AcceptAsync("new",
                new(new(4, 1), new([9]), Ballot.Zero), cancellation.Token).AsTask();
            var prepare = server.PrepareAsync("promise",
                new(new Ballot(5, 1)), cancellation.Token).AsTask();
            var violatingInsert = server.AcceptAsync("failure",
                new(new(4, 1), new([99]), Ballot.Zero), cancellation.Token).AsTask();
            var stagedRead = server.ListKeysAsync(cancellationToken: cancellation.Token).AsTask();
            firstGate.Release();

            Assert.Equal("stable", Assert.Single(
                (await initialRead.WaitAsync(TestDatabase.Timeout)).Entries).Key);
            Assert.Equal(5, await batchGate.Entered.WaitAsync(TestDatabase.Timeout));
            Assert.NotNull(stagedRows);
            Assert.Collection(stagedRows,
                row => AssertRow(row, "failure", Ballot.InitialFast(), new(4, 1), [99]),
                row => AssertRow(row, "new", Ballot.InitialFast(), new(4, 1), [9]),
                row => AssertRow(row, "promise", new(5, 1), null, null),
                row => AssertRow(row, "stable", new(5, 1), new(4, 1), [70]));
            Assert.Equal(1, stagedForeignKeyViolations);
            AssertRow(Assert.Single(ReadRows(observer)), "stable", new(3, 1), new(2, 1), [7]);

            cancellation.Cancel();
            Task[] batch = [update, insert, prepare, violatingInsert, stagedRead];
            Assert.All(batch, response => Assert.False(response.IsCompleted));
            // A request admitted after selection must also fail when this writer retires.
            var pending = server.AcceptAsync("pending",
                new(Ballot.InitialFast(), new([1]), Ballot.Zero)).AsTask();
            Assert.False(pending.IsCompleted);
            batchGate.Release();

            failure = await Assert.ThrowsAsync<SqliteException>(() => update.WaitAsync(TestDatabase.Timeout));
            Assert.Equal(19, failure.SqliteErrorCode);
            Assert.Equal(787, failure.SqliteExtendedErrorCode);
            foreach (var response in batch)
            {
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    response.WaitAsync(TestDatabase.Timeout)));
            }

            Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                pending.WaitAsync(TestDatabase.Timeout)));
            Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                server.ListKeysAsync().AsTask()));
            AssertRow(Assert.Single(ReadRows(observer)), "stable", new(3, 1), new(2, 1), [7]);
            using var command = observer.CreateCommand();
            command.CommandText = "SELECT count(*) FROM batch_fault_child;";
            Assert.Equal(0L, command.ExecuteScalar());
        }
        finally
        {
            firstGate.Release();
            batchGate.Release();
            if (failure is not null)
            {
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout)));
            }
            else
            {
                await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
            }
        }

        await using var reopened = await database.OpenAsync();
        var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal("stable", state.Key);
        Assert.Equal(new Ballot(3, 1), state.PromisedBallot);
        Assert.Equal(new Ballot(2, 1), state.AcceptedBallot);
        Assert.Equal(new ByteString([7]), state.AcceptedValue);
        // The invisible, promise-only item also rolled back; recovery can make progress.
        Assert.True((await reopened.PrepareAsync("promise", new(Ballot.InitialFast()))).IsPromised);
        Assert.True((await reopened.AcceptAsync("stable",
            new(new(4, 1), new([8]), Ballot.Zero))).IsAccepted);
        Assert.Equal(new ByteString([8]), Assert.Single((await reopened.ListKeysAsync()).Entries).AcceptedValue);
    }

    [Fact]
    public async Task BeforeCommit_ObservesDedicatedWriterAndRequiredPragmas()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var observations = new List<ConnectionObservation>();
        server.BeforeCommit = (connection, transaction, count) =>
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "PRAGMA synchronous;";
            var synchronous = (long)command.ExecuteScalar()!;
            command.CommandText = "PRAGMA journal_mode;";
            var journalMode = (string)command.ExecuteScalar()!;
            command.CommandText = "PRAGMA foreign_keys;";
            var foreignKeys = (long)command.ExecuteScalar()!;
            observations.Add(new(
                Environment.CurrentManagedThreadId, count, synchronous, journalMode, foreignKeys));
        };

        // Sequential acknowledgments publish each callback's observation before the
        // next operation. No observer connection stands in for worker-local settings.
        var prepareCaller = Environment.CurrentManagedThreadId;
        Assert.True((await server.PrepareAsync("key", new(new Ballot(2, 1)))
            .AsTask().WaitAsync(TestDatabase.Timeout)).IsPromised);
        var acceptCaller = Environment.CurrentManagedThreadId;
        Assert.True((await server.AcceptAsync("key", new(new(2, 1), new([7]), new(3, 1)))
            .AsTask().WaitAsync(TestDatabase.Timeout)).IsAccepted);
        var listCaller = Environment.CurrentManagedThreadId;
        Assert.Equal("key", Assert.Single((await server.ListKeysAsync()
            .AsTask().WaitAsync(TestDatabase.Timeout)).Entries).Key);

        Assert.Equal(3, observations.Count);
        var callers = new[] { prepareCaller, acceptCaller, listCaller };
        for (var index = 0; index < observations.Count; index++)
        {
            var observation = observations[index];
            Assert.NotEqual(callers[index], observation.ThreadId);
            Assert.Equal(observations[0].ThreadId, observation.ThreadId);
            Assert.Equal(1, observation.BatchSize);
            Assert.Equal(2, observation.Synchronous);
            Assert.Equal("wal", observation.JournalMode);
            Assert.Equal(1, observation.ForeignKeys);
        }
    }

    private static StoredRow[] ReadRows(SqliteConnection connection, SqliteTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT key, promised_round, promised_proposer, accepted_round, accepted_proposer, value
            FROM acceptor_state ORDER BY key;
            """;
        using var reader = command.ExecuteReader();
        var rows = new List<StoredRow>();
        while (reader.Read())
        {
            rows.Add(new(
                (byte[])reader[0],
                new(reader.GetInt32(1), reader.GetInt32(2)),
                reader.IsDBNull(3) ? null : new Ballot(reader.GetInt32(3), reader.GetInt32(4)),
                reader.IsDBNull(5) ? null : (byte[])reader[5]));
        }

        return rows.ToArray();
    }

    private static void AssertRow(StoredRow row, string key, Ballot promise, Ballot? accepted, byte[]? value)
    {
        Assert.Equal(TestDatabase.KeyBytes(key), row.Key);
        Assert.Equal(promise, row.Promise);
        Assert.Equal(accepted, row.Accepted);
        Assert.Equal(value, row.Value);
    }

    private sealed record StoredRow(byte[] Key, Ballot Promise, Ballot? Accepted, byte[]? Value);

    private sealed record ConnectionObservation(
        int ThreadId, int BatchSize, long Synchronous, string JournalMode, long ForeignKeys);

    private sealed class CommitGate : IDisposable
    {
        private readonly TaskCompletionSource<int> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _released = new();

        public Task<int> Entered => _entered.Task;

        public void ArriveAndWait(int batchSize)
        {
            _entered.SetResult(batchSize);
            if (!_released.Wait(TestDatabase.Timeout))
            {
                throw new TimeoutException("The test did not release the before-commit barrier.");
            }
        }

        public void Release() => _released.Set();

        public void Dispose() => _released.Dispose();
    }
}
