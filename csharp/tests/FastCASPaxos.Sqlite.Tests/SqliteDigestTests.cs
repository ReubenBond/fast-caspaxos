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
    public async Task AcceptHashPersistsAcrossReopenAndPromiseOnlyUpdates()
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
            var unchanged = await server.ReadAsync("key", expected);
            Assert.True(unchanged is AcceptorUnchanged);
        }
        await using var reopened = await database.OpenAsync();
        Assert.Equal(expected, Assert.Single((await reopened.ListDigestsAsync()).Entries).Digest);
        Assert.Equal(new KeyedAcceptorState("key", new(9, 2), new(3, 1), Value),
            ReadState(await reopened.ReadAsync("key")));
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
    public async Task ReadReportsExactOutcomeAndTransfersBytesOnlyWhenDifferent(string scenario)
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        if (scenario == "promise-only") await server.PrepareAsync("", new(new(4, 2)));
        else if (scenario != "missing")
            await server.AcceptAsync("", new(new(3, 0), ByteString.Empty, new(4, 2)));
        var known = scenario == "unknown-digest" ? null :
            new AcceptedDigest(scenario == "ballot-different" ? new(2, 0) : new(3, 0),
                scenario == "hash-different" ? AbcHash : EmptyHash);

        var result = await server.ReadAsync("", known);

        if (scenario is "missing" or "promise-only")
            Assert.True(result is AcceptorMissing);
        else if (scenario == "equal")
            Assert.True(result is AcceptorUnchanged);
        else
            Assert.Equal(new KeyedAcceptorState("", new(4, 2), new(3, 0), ByteString.Empty), ReadState(result));
        Assert.Equal(scenario is "missing" or "promise-only" ? 0 : 1, (await server.ListDigestsAsync()).Entries.Length);
    }

    [Fact]
    public async Task ReadRepeatedFullMissingAndUnchangedResultsNeverInsertOrUpdateRows()
    {
        using var database = new TestDatabase();
        await using (var server = await database.OpenAsync())
        {
            await server.AcceptAsync("value", new(new(3, 1), Value, new(9, 2)));
            await server.PrepareAsync("promise", new(new(8, 2)));
            using var observer = database.Connect();
            TestDatabase.Execute(observer, """
                CREATE TRIGGER forbid_read_insert BEFORE INSERT ON acceptor_state
                BEGIN SELECT RAISE(ABORT, 'read must not insert'); END;
                CREATE TRIGGER forbid_read_update BEFORE UPDATE ON acceptor_state
                BEGIN SELECT RAISE(ABORT, 'read must not update'); END;
                """);
            for (var iteration = 0; iteration < 3; iteration++)
            {
                Assert.Equal(new KeyedAcceptorState("value", new(9, 2), new(3, 1), Value),
                    ReadState(await server.ReadAsync("value")));
                Assert.True(await server.ReadAsync("value", new(new(3, 1), AbcHash)) is AcceptorUnchanged);
                Assert.True(await server.ReadAsync("promise") is AcceptorMissing);
                Assert.True(await server.ReadAsync("missing") is AcceptorMissing);
                Assert.Equal(2L, Scalar(observer, "SELECT count(*) FROM acceptor_state;"));
                Assert.Null(await server.InspectAsync("missing"));
                Assert.Equal(new Ballot(8, 2), (await server.InspectAsync("promise"))!.PromisedBallot);
            }
        }
        await using var reopened = await database.OpenAsync();
        Assert.Equal(new KeyedAcceptorState("value", new(9, 2), new(3, 1), Value),
            ReadState(await reopened.ReadAsync("value")));
        Assert.True(await reopened.ReadAsync("promise") is AcceptorMissing);
        Assert.Null(await reopened.InspectAsync("missing"));
    }

    [Fact]
    public async Task ListDigestsOrdinalPagingAndPrefixExcludePromisesAndPreserveUtf16Keys()
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
            Assert.InRange(page.Entries.Length, 1, 3);
            Assert.All(page.Entries, entry => Assert.Equal(new AcceptedDigest(new(3, 1), AbcHash), entry.Digest));
            seen.AddRange(page.Entries.Select(entry => entry.Key));
            if (page.ContinuationKey is not null) Assert.Equal(page.Entries[^1].Key, page.ContinuationKey);
            cursor = page.ContinuationKey;
        } while (cursor is not null);
        Assert.Equal(expected, seen);
        Assert.Equal(["a\0", "aa"],
            (await server.ListDigestsAsync(prefix: "a", afterKey: "a", limit: int.MaxValue)).Entries.Select(entry => entry.Key));
        Assert.Equal(["\uffff", "\uffffx"],
            (await server.ListDigestsAsync(prefix: "\uffff")).Entries.Select(entry => entry.Key));
        var final = await server.ListDigestsAsync(afterKey: "\uffffx");
        Assert.Empty(final.Entries);
        Assert.Null(final.ContinuationKey);
    }

    [Fact]
    public async Task DigestReadsInvalidArgumentsAndPreCancellationDoNotReachWriter()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var selected = 0;
        server.BeforeCommit = (_, _, _) => Interlocked.Increment(ref selected);
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.ListDigestsAsync(prefix: null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.ReadAsync(null!, null).AsTask());
        foreach (var limit in new[] { 0, -1 })
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => server.ListDigestsAsync(limit: limit).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => server.ReadAsync("key", new(new(1, 1), ByteString.Empty)).AsTask());
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.ListDigestsAsync(cancellationToken: canceled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => server.ReadAsync("key", null, canceled.Token).AsTask());
        Assert.Equal(0, Volatile.Read(ref selected));
        Assert.Empty((await server.ListDigestsAsync()).Entries); // Invalid calls do not poison the writer.
    }

    [Fact]
    public async Task AcceptCommitFailureRollsBackValueAndHashTogether()
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
            ReadState(await reopened.ReadAsync("key")));
    }

    [Fact]
    public async Task ReadCorruptPersistedHashFailsRatherThanTransferringBytes()
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
            failure = await Assert.ThrowsAsync<InvalidDataException>(() => server.ReadAsync("key", null).AsTask());
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
    public async Task RepairCorruptHigherBallotReplicaCannotReplaceHealthyCommittedBytes()
    {
        var databases = Enumerable.Range(0, 5).Select(_ => new TestDatabase()).ToArray();
        using var allocator = new TestDatabase();
        using var ballots = new SqliteProposerBallotSource(allocator.Path, 3);
        ballots.Reserve(10);
        var servers = new List<SqliteAcceptorServer>();
        InvalidDataException? integrityFailure = null;
        try
        {
            foreach (var database in databases) servers.Add(await database.OpenAsync());
            foreach (var server in servers)
            {
                Assert.True((await server.AcceptAsync("key", new(new(3, 1), Value, new(4, 1)))).IsAccepted);
                Assert.True((await server.PrepareAsync("key", new(new(5, 2)))).IsPromised);
            }
            Assert.True((await servers[1].AcceptAsync("key", new(new(5, 2), Value, default))).IsAccepted);
            using (var connection = databases[1].Connect())
                TestDatabase.Execute(connection, "UPDATE acceptor_state SET value=X'646566';");

            var repair = new DurabilityRepair("peer-0", [.. servers.Select((server, index) =>
                new RepairPeer($"peer-{index}", server, server))], ballots);
            var result = await repair.RunPassAsync().WaitAsync(TestDatabase.Timeout);

            foreach (var index in new[] { 0, 2, 3, 4 })
                Assert.Equal(Value, ReadState(await servers[index].ReadAsync("key")).AcceptedValue);
            integrityFailure = Assert.IsType<InvalidDataException>(Assert.Single(result.Errors));
            Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
            using var observer = databases[1].Connect();
            Assert.Equal("def"u8.ToArray(), (byte[])Scalar(observer, "SELECT value FROM acceptor_state;"));
            Assert.Equal(AbcHash.ToArray(), ReadHash(observer));
            Assert.Equal(5L, Scalar(observer, "SELECT accepted_round FROM acceptor_state;"));
        }
        finally
        {
            for (var index = 0; index < servers.Count; index++)
            {
                if (index == 1 && integrityFailure is not null)
                    Assert.Same(integrityFailure, await Assert.ThrowsAsync<InvalidDataException>(() => servers[index].DisposeAsync().AsTask()));
                else await servers[index].DisposeAsync();
            }
            foreach (var database in databases) database.Dispose();
        }
    }

    [Theory]
    [InlineData("prepare", false)]
    [InlineData("prepare", true)]
    [InlineData("accept", false)]
    [InlineData("accept", true)]
    [InlineData("retry", false)]
    [InlineData("retry", true)]
    [InlineData("list", false)]
    [InlineData("list", true)]
    [InlineData("inspect", false)]
    [InlineData("inspect", true)]
    [InlineData("inspect-list", false)]
    [InlineData("inspect-list", true)]
    public async Task AcceptedBytesCorruptionRetiresWriterBeforeReadOrTransition(string operation, bool corruptHash)
    {
        using var database = new TestDatabase();
        await using (var seed = await database.OpenAsync())
            await seed.AcceptAsync("key", new(new(3, 1), Value, new(4, 1)));
        using (var connection = database.Connect())
            TestDatabase.Execute(connection, corruptHash
                ? "UPDATE acceptor_state SET value_hash=zeroblob(32);"
                : "UPDATE acceptor_state SET value=X'646566';");
        var expectedHash = corruptHash ? new ByteString(new byte[32]) : AbcHash;
        var expectedValue = corruptHash ? Value : new ByteString("def"u8);
        var server = await database.OpenAsync();
        InvalidDataException? failure = null;
        try
        {
            // Fingerprint inventory reads metadata; consuming accepted bytes checks their integrity.
            Assert.Equal(new AcceptedDigest(new(3, 1), expectedHash),
                Assert.Single((await server.ListDigestsAsync()).Entries).Digest);
            failure = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            {
                switch (operation)
                {
                    case "prepare": await server.PrepareAsync("key", new(new(8, 2))); break;
                    case "accept": await server.AcceptAsync("key", new(new(8, 2), ByteString.Empty, default)); break;
                    case "retry": await server.AcceptAsync("key", new(new(3, 1), expectedValue, default)); break;
                    case "list": await server.ListKeysAsync(); break;
                    case "inspect": await server.InspectAsync("key"); break;
                    case "inspect-list": await server.InspectKeysAsync(); break;
                    default: throw new InvalidOperationException("Unknown fixture operation.");
                }
            });
            Assert.Equal("Accepted bytes do not match their persisted SHA-256.", failure.Message);
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidDataException>(() => server.ListDigestsAsync().AsTask()));
            using var observer = database.Connect();
            Assert.Equal(expectedValue.ToArray(), (byte[])Scalar(observer, "SELECT value FROM acceptor_state;"));
            Assert.Equal(expectedHash.ToArray(), ReadHash(observer));
            Assert.Equal(3L, Scalar(observer, "SELECT accepted_round FROM acceptor_state;"));
            Assert.Equal(4L, Scalar(observer, "SELECT promised_round FROM acceptor_state;"));
        }
        finally
        {
            if (failure is not null)
                Assert.Same(failure, await Assert.ThrowsAsync<InvalidDataException>(() => server.DisposeAsync().AsTask()));
            else await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task OpenV1MigrationPreservesBallotsAndBytesAndBackfillsHashes()
    {
        using var database = new TestDatabase();
        CreateV1(database);
        await using (var migrated = await database.OpenAsync())
        {
            var page = await migrated.ListDigestsAsync();
            Assert.Equal(["abc", "empty"], page.Entries.Select(entry => entry.Key));
            Assert.Equal(new AcceptedDigest(new(3, 0), AbcHash), page.Entries[0].Digest);
            Assert.Equal(new AcceptedDigest(new(4, 1), EmptyHash), page.Entries[1].Digest);
            Assert.Equal(new KeyedAcceptorState("abc", new(9, 2), new(3, 0), Value),
                ReadState(await migrated.ReadAsync("abc")));
            Assert.Equal(new KeyedAcceptorState("empty", new(9, 2), new(4, 1), ByteString.Empty),
                ReadState(await migrated.ReadAsync("empty")));
            Assert.Equal(new Ballot(9, 2), (await migrated.InspectAsync("promise"))!.PromisedBallot);
            Assert.True(await migrated.ReadAsync("promise") is AcceptorMissing);
        }
        using (var observer = database.Connect())
        {
            Assert.Equal(2L, Scalar(observer, "PRAGMA user_version;"));
            Assert.Equal(0L, Scalar(observer, "SELECT count(*) FROM sqlite_schema WHERE name='acceptor_state_v1';"));
            Assert.Equal(1L, Scalar(observer, "SELECT count(*) FROM acceptor_state WHERE value_hash IS NULL AND accepted_round IS NULL;"));
        }
        await using var reopened = await database.OpenAsync();
        Assert.True(await reopened.ReadAsync("abc", new(new(3, 0), AbcHash)) is AcceptorUnchanged);
        Assert.True((await reopened.AcceptAsync("empty", new(new(10, 1), Value, new(11, 1)))).IsAccepted);
        Assert.Equal(new AcceptedDigest(new(10, 1), AbcHash),
            Assert.Single((await reopened.ListDigestsAsync(prefix: "empty")).Entries).Digest);
    }

    [Fact]
    public async Task OpenFailedV1MigrationRollsBackSchemaVersionAndOriginalRows()
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

    private static KeyedAcceptorState ReadState(AcceptorReadResult result) => result switch
    {
        KeyedAcceptorState state => state,
        _ => throw new Xunit.Sdk.XunitException("Expected full accepted state.")
    };

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
