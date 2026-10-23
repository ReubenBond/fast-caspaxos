using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Microsoft.Data.Sqlite;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteDigestTests
{
    private static readonly ByteString Value = new("abc"u8);
    private static readonly ByteString AbcHash = new(Convert.FromHexString(
        "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"));
    private static readonly ByteString EmptyHash = new(Convert.FromHexString(
        "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));

    [Fact]
    public async Task Accept_HashPersistsAcrossReopenAndPromiseOnlyUpdates()
    {
        using var database = new TestDatabase();
        var expected = new AcceptedDigest(new(3, 1), AbcHash);
        await using (var server = await database.OpenAsync())
        {
            Assert.True((await server.AcceptAsync("key", new(new(3, 1), Value, new(4, 1)))).IsAccepted);
            Assert.Equal(expected, Assert.Single((await server.ListDigestsAsync()).Entries).Digest);
            Assert.True((await server.PrepareAsync("key", new(new(8, 2)))).IsPromised);
            Assert.Equal(expected, Assert.Single((await server.ListDigestsAsync()).Entries).Digest);
            // Updating promise fields must not rewrite the accepted bytes or hash.
            using var connection = database.Connect();
            TestDatabase.Execute(connection, """
                CREATE TRIGGER reject_value_rewrite BEFORE UPDATE OF value, value_hash ON acceptor_state
                BEGIN SELECT RAISE(ABORT, 'accepted bytes must not be rewritten'); END;
                """);
            Assert.True((await server.PrepareAsync("key", new(new(9, 2)))).IsPromised);
            Assert.True((await server.AcceptAsync("key", new(new(3, 1), Value, new(100, 1)))).IsAccepted);
            var unchanged = await server.FetchIfDifferentAsync("key", expected);
            Assert.Equal(ConditionalAcceptorStatus.Unchanged, unchanged.Status);
            Assert.Null(unchanged.State);
        }
        await using var reopened = await database.OpenAsync();
        Assert.Equal(expected, Assert.Single((await reopened.ListDigestsAsync()).Entries).Digest);
        Assert.Equal(new KeyedAcceptorState("key", new(9, 2), new(3, 1), Value),
            (await reopened.FetchIfDifferentAsync("key", null)).State);
        using var observer = database.Connect();
        Assert.Equal(AbcHash.ToArray(), ReadHash(observer));
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("promise-only")]
    [InlineData("unknown-digest")]
    [InlineData("equal")]
    [InlineData("ballot-different")]
    [InlineData("hash-different")]
    public async Task FetchIfDifferent_ReportsExactOutcomeAndTransfersBytesOnlyWhenDifferent(string scenario)
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        if (scenario == "promise-only") await server.PrepareAsync("", new(new(4, 2)));
        else if (scenario != "missing")
            await server.AcceptAsync("", new(new(3, 0), ByteString.Empty, new(4, 2)));
        var known = scenario == "unknown-digest" ? null :
            new AcceptedDigest(scenario == "ballot-different" ? new(2, 0) : new(3, 0),
                scenario == "hash-different" ? AbcHash : EmptyHash);

        var result = await server.FetchIfDifferentAsync("", known);

        var expected = scenario is "missing" or "promise-only" ? ConditionalAcceptorStatus.Missing
            : scenario == "equal" ? ConditionalAcceptorStatus.Unchanged : ConditionalAcceptorStatus.Different;
        Assert.Equal(expected, result.Status);
        if (expected == ConditionalAcceptorStatus.Different)
            Assert.Equal(new KeyedAcceptorState("", new(4, 2), new(3, 0), ByteString.Empty), result.State);
        else Assert.Null(result.State);
        Assert.Equal(scenario is "missing" or "promise-only" ? 0 : 1, (await server.ListDigestsAsync()).Entries.Count);
    }

    [Fact]
    public async Task ListDigests_OrdinalPagingAndPrefix_ExcludePromisesAndPreserveUtf16Keys()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        // Unsafe text is data only, never xUnit metadata; SQLite's contract includes lone UTF-16 units.
        string[] expected = ["", "\0", "A", "a", "a\0", "aa", "\ud800", "\ud800\udc00", "\uffff", "\uffffx"];
        foreach (var key in expected.Reverse())
            await server.AcceptAsync(key, new(new(3, 1), Value, new(8, 2)));
        await server.PrepareAsync("promise-only", new(new(5, 1)));
        var seen = new List<string>();
        string? cursor = null;
        do
        {
            var page = await server.ListDigestsAsync(afterKey: cursor, limit: 3);
            Assert.InRange(page.Entries.Count, 1, 3);
            Assert.All(page.Entries, entry => Assert.Equal(new AcceptedDigest(new(3, 1), AbcHash), entry.Digest));
            seen.AddRange(page.Entries.Select(entry => entry.Key));
            if (page.ContinuationKey is not null) Assert.Equal(page.Entries[^1].Key, page.ContinuationKey);
            cursor = page.ContinuationKey;
        } while (cursor is not null);
        Assert.Equal(expected, seen);
        Assert.Equal(new[] { "a\0", "aa" },
            (await server.ListDigestsAsync(prefix: "a", afterKey: "a", limit: int.MaxValue)).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "\uffff", "\uffffx" },
            (await server.ListDigestsAsync(prefix: "\uffff")).Entries.Select(entry => entry.Key));
        var final = await server.ListDigestsAsync(afterKey: "\uffffx");
        Assert.Empty(final.Entries);
        Assert.Null(final.ContinuationKey);
    }

    [Fact]
    public async Task DigestReads_InvalidArgumentsAndPreCancellation_DoNotReachWriter()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var selected = 0;
        server.BeforeCommit = (_, _, _) => Interlocked.Increment(ref selected);
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.ListDigestsAsync(prefix: null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.FetchIfDifferentAsync(null!, null).AsTask());
        foreach (var limit in new[] { 0, -1 })
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => server.ListDigestsAsync(limit: limit).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => server.FetchIfDifferentAsync("key", new(new(1, 1), ByteString.Empty)).AsTask());
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.ListDigestsAsync(cancellationToken: canceled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.FetchIfDifferentAsync("key", null, canceled.Token).AsTask());
        Assert.Equal(0, Volatile.Read(ref selected));
        Assert.Empty((await server.ListDigestsAsync()).Entries); // Invalid calls do not poison the writer.
    }

    [Fact]
    public async Task Accept_CommitFailure_RollsBackValueAndHashTogether()
    {
        using var database = new TestDatabase();
        var server = await database.OpenAsync();
        var failure = new IOException("injected before commit");
        try
        {
            await server.AcceptAsync("key", new(new(3, 1), Value, new(4, 1)));
            server.BeforeCommit = (_, _, _) => throw failure;
            Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() =>
                server.AcceptAsync("key", new(new(5, 1), ByteString.Empty, new(6, 1))).AsTask().WaitAsync(TestDatabase.Timeout)));
        }
        finally
        {
            Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => server.DisposeAsync().AsTask()));
        }
        await using var reopened = await database.OpenAsync();
        Assert.Equal(new AcceptedDigest(new(3, 1), AbcHash), Assert.Single((await reopened.ListDigestsAsync()).Entries).Digest);
        Assert.Equal(new KeyedAcceptorState("key", new(4, 1), new(3, 1), Value),
            (await reopened.FetchIfDifferentAsync("key", null)).State);
    }

    [Fact]
    public async Task FetchIfDifferent_CorruptPersistedHash_FailsRatherThanTransferringBytes()
    {
        using var database = new TestDatabase();
        await using (var seed = await database.OpenAsync())
            await seed.AcceptAsync("key", new(new(3, 1), Value, new(4, 1)));
        using (var observer = database.Connect())
            TestDatabase.Execute(observer, "UPDATE acceptor_state SET value_hash=zeroblob(32);");
        var server = await database.OpenAsync();
        InvalidDataException? failure = null;
        try
        {
            failure = await Assert.ThrowsAsync<InvalidDataException>(() => server.FetchIfDifferentAsync("key", null).AsTask());
            Assert.Equal("Accepted bytes do not match their persisted SHA-256.", failure.Message);
        }
        finally
        {
            if (failure is not null)
                Assert.Same(failure, await Assert.ThrowsAsync<InvalidDataException>(() => server.DisposeAsync().AsTask()));
            else await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task Open_V1Migration_PreservesBallotsAndBytesAndBackfillsHashes()
    {
        using var database = new TestDatabase();
        CreateV1(database);
        await using (var migrated = await database.OpenAsync())
        {
            var page = await migrated.ListDigestsAsync();
            Assert.Equal(new[] { "abc", "empty" }, page.Entries.Select(entry => entry.Key));
            Assert.Equal(new AcceptedDigest(new(3, 0), AbcHash), page.Entries[0].Digest);
            Assert.Equal(new AcceptedDigest(new(4, 1), EmptyHash), page.Entries[1].Digest);
            Assert.Equal(new KeyedAcceptorState("abc", new(9, 2), new(3, 0), Value),
                (await migrated.FetchIfDifferentAsync("abc", null)).State);
            Assert.Equal(new KeyedAcceptorState("empty", new(9, 2), new(4, 1), ByteString.Empty),
                (await migrated.FetchIfDifferentAsync("empty", null)).State);
            Assert.Equal(new Ballot(9, 2), (await migrated.InspectAsync("promise"))!.PromisedBallot);
            Assert.Equal(ConditionalAcceptorStatus.Missing, (await migrated.FetchIfDifferentAsync("promise", null)).Status);
        }
        using (var observer = database.Connect())
        {
            Assert.Equal(2L, Scalar(observer, "PRAGMA user_version;"));
            Assert.Equal(0L, Scalar(observer, "SELECT count(*) FROM sqlite_schema WHERE name='acceptor_state_v1';"));
            Assert.Equal(1L, Scalar(observer, "SELECT count(*) FROM acceptor_state WHERE value_hash IS NULL AND accepted_round IS NULL;"));
        }
        await using var reopened = await database.OpenAsync();
        Assert.Equal(ConditionalAcceptorStatus.Unchanged,
            (await reopened.FetchIfDifferentAsync("abc", new(new(3, 0), AbcHash))).Status);
        Assert.True((await reopened.AcceptAsync("empty", new(new(10, 1), Value, new(11, 1)))).IsAccepted);
        Assert.Equal(new AcceptedDigest(new(10, 1), AbcHash),
            Assert.Single((await reopened.ListDigestsAsync(prefix: "empty")).Entries).Digest);
    }

    [Fact]
    public async Task Open_FailedV1Migration_RollsBackSchemaVersionAndOriginalRows()
    {
        using var database = new TestDatabase();
        CreateV1(database);
        using (var observer = database.Connect())
        {
            // Simulate a damaged legacy row; constraint failure occurs during migration's copy.
            TestDatabase.Execute(observer, """
                PRAGMA ignore_check_constraints=ON;
                INSERT INTO acceptor_state VALUES(X'01',9,2,3,0,X'07');
                """);
        }

        var failure = await Assert.ThrowsAsync<SqliteException>(() => database.OpenAsync());

        Assert.Equal(19, failure.SqliteErrorCode);
        using var connection = database.Connect();
        Assert.Equal(1L, Scalar(connection, "PRAGMA user_version;"));
        Assert.Equal(4L, Scalar(connection, "SELECT count(*) FROM acceptor_state;"));
        Assert.Equal(0L, Scalar(connection, "SELECT count(*) FROM pragma_table_info('acceptor_state') WHERE name='value_hash';"));
        Assert.Equal(0L, Scalar(connection, "SELECT count(*) FROM sqlite_schema WHERE name='acceptor_state_v1';"));
        Assert.Equal(Value.ToArray(), (byte[])Scalar(connection, "SELECT value FROM acceptor_state WHERE key=X'006100620063';"));
    }

    private static void CreateV1(TestDatabase database)
    {
        using var connection = database.Connect();
        // Independent shipped v1 schema: do not ask current production code to generate the fixture.
        TestDatabase.Execute(connection, """
            PRAGMA application_id=1178816600;
            PRAGMA user_version=1;
            CREATE TABLE acceptor_state (
                key BLOB PRIMARY KEY CHECK(length(key)%2=0),
                promised_round INTEGER NOT NULL CHECK(promised_round BETWEEN 1 AND 2147483647),
                promised_proposer INTEGER NOT NULL CHECK(promised_proposer BETWEEN 0 AND 2147483647),
                accepted_round INTEGER CHECK(accepted_round BETWEEN 1 AND 2147483647),
                accepted_proposer INTEGER CHECK(accepted_proposer BETWEEN 0 AND 2147483647),
                value BLOB,
                CHECK((accepted_round IS NULL AND accepted_proposer IS NULL AND value IS NULL)
                    OR (accepted_round IS NOT NULL AND accepted_proposer IS NOT NULL AND value IS NOT NULL))
            ) STRICT, WITHOUT ROWID;
            INSERT INTO acceptor_state VALUES(X'006100620063',9,2,3,0,X'616263');
            INSERT INTO acceptor_state VALUES(X'0065006D007000740079',9,2,4,1,X'');
            INSERT INTO acceptor_state VALUES(X'00700072006F006D006900730065',9,2,NULL,NULL,NULL);
            """);
    }

    private static object Scalar(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar()!;
    }

    private static byte[] ReadHash(SqliteConnection connection) =>
        (byte[])Scalar(connection, "SELECT value_hash FROM acceptor_state;");
}
