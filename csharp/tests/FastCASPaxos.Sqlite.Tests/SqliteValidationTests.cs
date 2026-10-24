using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteValidationTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 1)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    [InlineData(1, -1)]
    [InlineData(1, int.MinValue)]
    public async Task InvalidBallots_AreRejectedWithoutWriting(int round, int proposer)
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var invalid = new Ballot(round, proposer);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            server.PrepareAsync("prepare", new(invalid)).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            server.AcceptAsync("accept", new(invalid, ByteString.Empty, Ballot.Zero)).AsTask());
        if (!invalid.IsZero)
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                server.AcceptAsync("next", new(Ballot.InitialFast(), ByteString.Empty, invalid)).AsTask());
        }

        Assert.Empty((await server.ListKeysAsync()).Entries);
        using var connection = database.Connect();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM acceptor_state;";
        Assert.Equal(0L, command.ExecuteScalar());
    }

    [Fact]
    public async Task InvalidArguments_DoNotPoisonServer()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            server.PrepareAsync(null!, new(Ballot.InitialFast())).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            server.AcceptAsync(null!, new(Ballot.InitialFast(), ByteString.Empty, Ballot.Zero)).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => server.ListKeysAsync(prefix: null!).AsTask());
        foreach (var limit in new[] { 0, -1, int.MinValue })
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => server.ListKeysAsync(limit: limit).AsTask());
        }

        Assert.True((await server.AcceptAsync("", new(Ballot.InitialFast(), default, Ballot.Zero))).IsAccepted);
        Assert.Equal("", Assert.Single((await server.ListKeysAsync()).Entries).Key);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, -1)]
    public async Task InvalidOpenOptions_DoNotCreateDatabase(int capacity, int batchSize)
    {
        using var database = new TestDatabase();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            SqliteAcceptorServer.OpenAsync(database.Path, capacity, batchSize).AsTask());
        Assert.False(File.Exists(database.Path));
    }

    [Fact]
    public async Task InvalidPathAndCancelledOpen_DoNotCreateDatabase()
    {
        using var database = new TestDatabase();
        await Assert.ThrowsAsync<ArgumentNullException>(() => SqliteAcceptorServer.OpenAsync(null!).AsTask());
        foreach (var path in new[] { "", " " })
        {
            await Assert.ThrowsAsync<ArgumentException>(() => SqliteAcceptorServer.OpenAsync(path).AsTask());
        }

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            SqliteAcceptorServer.OpenAsync(database.Path, cancellationToken: cancellation.Token).AsTask());
        Assert.False(File.Exists(database.Path));
    }

    [Fact]
    public async Task NewDatabase_UsesVersionedSchemaAndWal()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        using var connection = database.Connect();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA application_id;";
        Assert.Equal(0x46435058L, command.ExecuteScalar());
        command.CommandText = "PRAGMA user_version;";
        Assert.Equal(2L, command.ExecuteScalar());
        command.CommandText = "PRAGMA journal_mode;";
        Assert.Equal("wal", command.ExecuteScalar());
        command.CommandText = "SELECT count(*) FROM acceptor_state;";
        Assert.Equal(0L, command.ExecuteScalar());
        Assert.Empty((await server.ListKeysAsync()).Entries);
    }

    [Theory]
    [InlineData("ForeignSchema")]
    [InlineData("ForeignApplication")]
    [InlineData("UnidentifiedVersion")]
    [InlineData("FutureVersion")]
    public async Task UnsupportedDatabase_IsRejectedWithoutReplacingData(string scenario)
    {
        using var database = new TestDatabase();
        if (scenario == "FutureVersion")
        {
            await using var server = await database.OpenAsync();
            Assert.True((await server.AcceptAsync("saved",
                new(Ballot.InitialFast(), new([17]), Ballot.Zero))).IsAccepted);
        }

        using (var connection = database.Connect())
        {
            TestDatabase.Execute(connection, scenario switch
            {
                "ForeignSchema" => "CREATE TABLE unrelated (value INTEGER); INSERT INTO unrelated VALUES (17);",
                "ForeignApplication" => "PRAGMA application_id=123;",
                "UnidentifiedVersion" => "PRAGMA user_version=1;",
                "FutureVersion" => "PRAGMA user_version=3;",
                _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
            });
        }

        await Assert.ThrowsAsync<InvalidDataException>(() => database.OpenAsync());
        using var observer = database.Connect();
        using var command = observer.CreateCommand();
        command.CommandText = scenario switch
        {
            "ForeignSchema" => "SELECT value FROM unrelated;",
            "ForeignApplication" => "PRAGMA application_id;",
            "UnidentifiedVersion" => "PRAGMA user_version;",
            "FutureVersion" => "PRAGMA user_version;",
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
        Assert.Equal(scenario switch
        {
            "ForeignSchema" => 17L,
            "ForeignApplication" => 123L,
            "UnidentifiedVersion" => 1L,
            "FutureVersion" => 3L,
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        }, command.ExecuteScalar());
        if (scenario == "FutureVersion")
        {
            command.CommandText = "SELECT value FROM acceptor_state;";
            Assert.Equal(new byte[] { 17 }, (byte[])command.ExecuteScalar()!);
        }
    }
}
