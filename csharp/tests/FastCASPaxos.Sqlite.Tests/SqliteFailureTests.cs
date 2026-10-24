using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Microsoft.Data.Sqlite;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StatementFailureRollsBackAndFailsClosed(bool updateExisting) =>
        await VerifyFailureAsync(updateExisting, deferredCommitFailure: false);

    [Fact]
    public async Task CommitFailureRollsBackAndFailsClosed() =>
        await VerifyFailureAsync(updateExisting: true, deferredCommitFailure: true);

    private static async Task VerifyFailureAsync(bool updateExisting, bool deferredCommitFailure)
    {
        using var database = new TestDatabase();
        var server = await database.OpenAsync(queueCapacity: 1, maxBatchSize: 1);
        SqliteException? failure = null;
        try
        {
            Assert.True((await server.AcceptAsync("stable",
                new(new(2, 1), new([7]), new(3, 1)))).IsAccepted);
            using (var connection = database.Connect())
            {
                using var heldWriter = connection.BeginTransaction(deferred: false);
                if (deferredCommitFailure)
                {
                    // The AFTER trigger succeeds at statement execution. SQLite checks this
                    // deferred foreign key at COMMIT, after the acceptor row was updated.
                    TestDatabase.Execute(connection, """
                        CREATE TABLE fault_parent (id INTEGER PRIMARY KEY);
                        CREATE TABLE fault_child (
                            parent_id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
                        CREATE TRIGGER fail_write AFTER UPDATE ON acceptor_state BEGIN
                            INSERT INTO fault_child VALUES (42);
                        END;
                        """, heldWriter);
                }
                else
                {
                    var operation = updateExisting ? "UPDATE" : "INSERT";
                    TestDatabase.Execute(connection, $"""
                        CREATE TABLE fault_marker (value INTEGER);
                        CREATE TRIGGER fail_write BEFORE {operation} ON acceptor_state BEGIN
                            INSERT INTO fault_marker VALUES (1);
                            SELECT RAISE(ABORT, 'injected statement failure');
                        END;
                        """, heldWriter);
                }

                var failing = server.AcceptAsync(updateExisting ? "stable" : "doomed",
                    new(new(4, 1), new([99]), new(5, 1))).AsTask();
                var pendingAccept = server.AcceptAsync("pending",
                    new(new(4, 1), new([2]), Ballot.Zero)).AsTask();
                var pendingPrepare = server.PrepareAsync("pending-promise", new(new Ballot(4, 1))).AsTask();
                var pendingList = server.ListKeysAsync().AsTask();
                // One active item and one channel slot force later requests to wait
                // for admission, exercising failure propagation on both sides of it.
                // There can be no acknowledgment while an independent writer holds the lock.
                Assert.False(failing.IsCompleted);
                Assert.False(pendingAccept.IsCompleted);
                Assert.False(pendingPrepare.IsCompleted);
                Assert.False(pendingList.IsCompleted);
                heldWriter.Commit();

                failure = await Assert.ThrowsAsync<SqliteException>(() => failing.WaitAsync(TestDatabase.Timeout));
                Assert.Equal(19, failure.SqliteErrorCode);
                Assert.Equal(deferredCommitFailure ? 787 : 1811, failure.SqliteExtendedErrorCode);
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() => pendingAccept.WaitAsync(TestDatabase.Timeout)));
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() => pendingPrepare.WaitAsync(TestDatabase.Timeout)));
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() => pendingList.WaitAsync(TestDatabase.Timeout)));

                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    server.AcceptAsync("future", new(new(9, 1), ByteString.Empty, Ballot.Zero)).AsTask()));
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    server.PrepareAsync("future", new(new Ballot(9, 1))).AsTask()));
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() => server.ListKeysAsync().AsTask()));

                using var command = connection.CreateCommand();
                command.CommandText = deferredCommitFailure
                    ? "SELECT count(*) FROM fault_child;"
                    : "SELECT count(*) FROM fault_marker;";
                Assert.Equal(0L, command.ExecuteScalar());
                command.CommandText = "SELECT count(*) FROM acceptor_state;";
                Assert.Equal(1L, command.ExecuteScalar());
            }
        }
        finally
        {
            if (failure is not null)
            {
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout)));
                Assert.Same(failure, await Assert.ThrowsAsync<SqliteException>(() =>
                    server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout)));
            }
            else
            {
                await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
            }
        }

        // Recovery needs no hidden state or manual repair. Even with the trigger still
        // installed, the durable accepted state is precisely the pre-failure state.
        await using (var reopened = await database.OpenAsync())
        {
            var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
            Assert.Equal("stable", state.Key);
            Assert.Equal(new Ballot(2, 1), state.AcceptedBallot);
            Assert.Equal(new Ballot(3, 1), state.PromisedBallot);
            Assert.Equal(new ByteString([7]), state.AcceptedValue);
        }

        using (var connection = database.Connect())
        {
            TestDatabase.Execute(connection, "DROP TRIGGER fail_write;");
        }

        await using var recovered = await database.OpenAsync();
        // Restoring the storage assumption allows progress, while failed prepares
        // must not have left an invisible promise-only record behind.
        Assert.True((await recovered.PrepareAsync("pending-promise", new(Ballot.InitialFast()))).IsPromised);
        Assert.True((await recovered.AcceptAsync("stable",
            new(new(4, 1), new([8]), Ballot.Zero))).IsAccepted);
        var restored = Assert.Single((await recovered.ListKeysAsync()).Entries);
        Assert.Equal(new ByteString([8]), restored.AcceptedValue);
        Assert.Equal(new Ballot(4, 1), restored.AcceptedBallot);
    }
}
