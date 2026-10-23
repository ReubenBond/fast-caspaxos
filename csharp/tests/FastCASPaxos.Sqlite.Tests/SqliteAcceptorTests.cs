using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteAcceptorTests
{
    [Fact]
    public async Task ExactRetry_WithFreshBytesAndAfterReopen_PreservesOriginalPromise()
    {
        using var database = new TestDatabase();
        var accepted = new Ballot(2, 7);
        var promised = new Ballot(3, 9);
        await using (var server = await database.OpenAsync())
        {
            Assert.True((await server.AcceptAsync("key", new(accepted, new([1, 2]), promised))).IsAccepted);
            var retry = await server.AcceptAsync("key", new(accepted, new([1, 2]), new(99, 1)));
            Assert.True(retry.IsAccepted);
            Assert.Equal(promised, retry.PromisedBallot);
        }

        await using var reopened = await database.OpenAsync();
        var durableRetry = await reopened.AcceptAsync("key", new(accepted, new([1, 2]), new(100, 1)));
        Assert.True(durableRetry.IsAccepted);
        Assert.Equal(promised, durableRetry.PromisedBallot);
        Assert.Equal(Ballot.Zero, durableRetry.ConflictingBallot);
        var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal(accepted, state.AcceptedBallot);
        Assert.Equal(promised, state.PromisedBallot);
        Assert.Equal(new ByteString([1, 2]), state.AcceptedValue);
        Assert.True((await reopened.PrepareAsync("key", new(promised))).IsPromised);
    }

    [Fact]
    public async Task SameBallotWithDifferentBytes_RejectsWithoutMutation()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var ballot = new Ballot(2, 1);
        Assert.True((await server.AcceptAsync("key", new(ballot, new([1]), Ballot.Zero))).IsAccepted);

        var rejected = await server.AcceptAsync("key", new(ballot, new([2]), new(99, 1)));

        Assert.False(rejected.IsAccepted);
        Assert.Equal(ballot, rejected.ConflictingBallot);
        Assert.Equal(Ballot.Zero, rejected.PromisedBallot);
        var state = Assert.Single((await server.ListKeysAsync()).Entries);
        Assert.Equal(new ByteString([1]), state.AcceptedValue);
        Assert.Equal(ballot, state.AcceptedBallot);
        Assert.Equal(Ballot.InitialFast(), state.PromisedBallot);
    }

    [Fact]
    public async Task StalePrepareAndAccept_ReturnConflictAndPreserveAcceptedState()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var accepted = new Ballot(3, 5);
        var promised = new Ballot(4, 1);
        Assert.True((await server.AcceptAsync("key", new(accepted, new([7]), promised))).IsAccepted);

        var prepare = await server.PrepareAsync("key", new(new Ballot(4, 0)));
        var accept = await server.AcceptAsync("key", new(new(4, 0), new([8]), new(20, 1)));

        Assert.False(prepare.IsPromised);
        Assert.Equal(promised, prepare.ConflictingBallot);
        Assert.Equal(accepted, prepare.AcceptedBallot);
        Assert.Equal(new ByteString([7]), prepare.AcceptedValue);
        Assert.False(accept.IsAccepted);
        Assert.Equal(promised, accept.ConflictingBallot);
        var equalPrepare = await server.PrepareAsync("key", new(promised));
        Assert.True(equalPrepare.IsPromised);
        Assert.Equal(accepted, equalPrepare.AcceptedBallot);
        Assert.Equal(new ByteString([7]), equalPrepare.AcceptedValue);
    }

    [Fact]
    public async Task AcceptWithoutPiggyback_UsesAcceptedBallotAsFloor()
    {
        using var database = new TestDatabase();
        var ballot = new Ballot(8, 4);
        await using (var server = await database.OpenAsync())
        {
            var accepted = await server.AcceptAsync("key", new(ballot, new([42]), Ballot.Zero));
            Assert.True(accepted.IsAccepted);
            Assert.Equal(Ballot.InitialFast(), accepted.PromisedBallot);
        }

        await using var reopened = await database.OpenAsync();
        var stalePrepare = await reopened.PrepareAsync("key", new(new Ballot(8, 3)));
        var staleAccept = await reopened.AcceptAsync("key", new(new(8, 3), new([0]), Ballot.Zero));
        Assert.False(stalePrepare.IsPromised);
        Assert.Equal(ballot, stalePrepare.ConflictingBallot);
        Assert.Equal(ballot, stalePrepare.AcceptedBallot);
        Assert.Equal(new ByteString([42]), stalePrepare.AcceptedValue);
        Assert.False(staleAccept.IsAccepted);
        Assert.Equal(ballot, staleAccept.ConflictingBallot);
        var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal(Ballot.InitialFast(), state.PromisedBallot);
        Assert.Equal(ballot, state.AcceptedBallot);
        Assert.Equal(new ByteString([42]), state.AcceptedValue);
    }

    [Fact]
    public async Task MaximumBallotFields_RoundTripWithoutTruncation()
    {
        using var database = new TestDatabase();
        var maximum = new Ballot(int.MaxValue, int.MaxValue);
        await using (var server = await database.OpenAsync())
        {
            Assert.True((await server.AcceptAsync("key", new(maximum, new([255]), maximum))).IsAccepted);
        }

        await using var reopened = await database.OpenAsync();
        var state = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal(maximum, state.AcceptedBallot);
        Assert.Equal(maximum, state.PromisedBallot);
        Assert.Equal(new ByteString([255]), state.AcceptedValue);
        foreach (var stale in new[] { Ballot.InitialFast(), new Ballot(int.MaxValue, int.MaxValue - 1) })
        {
            var prepare = await reopened.PrepareAsync("key", new(stale));
            var accept = await reopened.AcceptAsync("key", new(stale, new([0]), Ballot.Zero));
            Assert.False(prepare.IsPromised);
            Assert.Equal(maximum, prepare.ConflictingBallot);
            Assert.False(accept.IsAccepted);
            Assert.Equal(maximum, accept.ConflictingBallot);
        }

        Assert.True((await reopened.PrepareAsync("key", new(maximum))).IsPromised);
    }

    [Fact]
    public async Task MissingAndEmptyValues_RemainDistinctAfterReopen()
    {
        using var database = new TestDatabase();
        await using (var server = await database.OpenAsync())
        {
            var missing = await server.PrepareAsync("missing", new(Ballot.InitialFast()));
            Assert.True(missing.IsPromised);
            Assert.Equal(Ballot.Zero, missing.AcceptedBallot);
            Assert.Null(missing.AcceptedValue);
            Assert.True((await server.AcceptAsync("empty", new(Ballot.InitialFast(), ByteString.Empty, Ballot.Zero))).IsAccepted);
        }

        await using var reopened = await database.OpenAsync();
        var stillMissing = await reopened.PrepareAsync("missing", new(Ballot.InitialFast()));
        var empty = await reopened.PrepareAsync("empty", new(Ballot.InitialFast()));
        Assert.Null(stillMissing.AcceptedValue);
        Assert.Equal(Ballot.Zero, stillMissing.AcceptedBallot);
        Assert.Equal(ByteString.Empty, empty.AcceptedValue);
        Assert.Equal(Ballot.InitialFast(), empty.AcceptedBallot);
        var listed = Assert.Single((await reopened.ListKeysAsync()).Entries);
        Assert.Equal("empty", listed.Key);
        Assert.Equal(ByteString.Empty, listed.AcceptedValue);
    }

    [Fact]
    public async Task PromiseOnlyRecord_SurvivesReopenButIsNotListed()
    {
        using var database = new TestDatabase();
        var promised = new Ballot(9, 2);
        await using (var server = await database.OpenAsync())
        {
            Assert.True((await server.PrepareAsync("promise", new(promised))).IsPromised);
            Assert.Empty((await server.ListKeysAsync()).Entries);
        }

        await using var reopened = await database.OpenAsync();
        var stale = await reopened.PrepareAsync("promise", new(Ballot.InitialFast()));
        Assert.False(stale.IsPromised);
        Assert.Equal(promised, stale.ConflictingBallot);
        Assert.Equal(Ballot.Zero, stale.AcceptedBallot);
        Assert.Null(stale.AcceptedValue);
        var staleAccept = await reopened.AcceptAsync("promise", new(new(9, 1), new([1]), Ballot.Zero));
        Assert.False(staleAccept.IsAccepted);
        Assert.Equal(promised, staleAccept.ConflictingBallot);
        var page = await reopened.ListKeysAsync();
        Assert.Empty(page.Entries);
        Assert.Null(page.ContinuationKey);
    }

    [Fact]
    public async Task AcknowledgedWrite_IsVisibleToIndependentConnectionAndServer()
    {
        using var database = new TestDatabase();
        await using var writer = await database.OpenAsync();
        await using var observer = await database.OpenAsync();
        var ballot = new Ballot(3, 1);
        var next = new Ballot(4, 0);
        Assert.True((await writer.AcceptAsync("key", new(ballot, new([0, 255]), next))).IsAccepted);

        using var connection = database.Connect();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT key, promised_round, promised_proposer, accepted_round, accepted_proposer, value
            FROM acceptor_state
            """;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(TestDatabase.KeyBytes("key"), (byte[])reader[0]);
        Assert.Equal(4, reader.GetInt32(1));
        Assert.Equal(0, reader.GetInt32(2));
        Assert.Equal(3, reader.GetInt32(3));
        Assert.Equal(1, reader.GetInt32(4));
        Assert.Equal(new byte[] { 0, 255 }, (byte[])reader[5]);
        Assert.False(reader.Read());

        var state = Assert.Single((await observer.ListKeysAsync()).Entries);
        Assert.Equal(ballot, state.AcceptedBallot);
        Assert.Equal(next, state.PromisedBallot);
        Assert.Equal(new ByteString([0, 255]), state.AcceptedValue);
        var rejection = await observer.AcceptAsync("key", new(ballot, new([9]), Ballot.Zero));
        Assert.False(rejection.IsAccepted);
        Assert.Equal(next, rejection.ConflictingBallot);
    }

    [Fact]
    public async Task IndependentServers_ObserveEachOthersPromiseAndAcceptedFloor()
    {
        using var database = new TestDatabase();
        await using var first = await database.OpenAsync();
        await using var second = await database.OpenAsync();
        Assert.True((await first.PrepareAsync("key", new(new Ballot(2, 1)))).IsPromised);
        Assert.True((await second.AcceptAsync("key", new(new(3, 1), new([3]), Ballot.Zero))).IsAccepted);

        var stalePrepare = await first.PrepareAsync("key", new(new Ballot(2, 1)));
        Assert.False(stalePrepare.IsPromised);
        Assert.Equal(new Ballot(3, 1), stalePrepare.ConflictingBallot);
        Assert.Equal(new Ballot(3, 1), stalePrepare.AcceptedBallot);
        Assert.Equal(new ByteString([3]), stalePrepare.AcceptedValue);
        Assert.True((await second.PrepareAsync("key", new(new Ballot(4, 1)))).IsPromised);
        var staleAccept = await first.AcceptAsync("key", new(new(3, 2), new([4]), Ballot.Zero));
        Assert.False(staleAccept.IsAccepted);
        Assert.Equal(new Ballot(4, 1), staleAccept.ConflictingBallot);
        var state = Assert.Single((await first.ListKeysAsync()).Entries);
        Assert.Equal(new Ballot(4, 1), state.PromisedBallot);
        Assert.Equal(new Ballot(3, 1), state.AcceptedBallot);
        Assert.Equal(new ByteString([3]), state.AcceptedValue);
    }

    [Fact]
    public async Task ReadOnlyRetriesAndRejections_DoNotAttemptStorageWrites()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var accepted = new Ballot(2, 1);
        var promised = new Ballot(3, 1);
        Assert.True((await server.AcceptAsync("key", new(accepted, new([7]), promised))).IsAccepted);
        using var connection = database.Connect();
        TestDatabase.Execute(connection, """
            CREATE TRIGGER reject_insert BEFORE INSERT ON acceptor_state BEGIN
                SELECT RAISE(ABORT, 'unexpected insert');
            END;
            CREATE TRIGGER reject_update BEFORE UPDATE ON acceptor_state BEGIN
                SELECT RAISE(ABORT, 'unexpected update');
            END;
            """);

        var retry = await server.AcceptAsync("key", new(accepted, new([7]), new(99, 1)));
        Assert.True(retry.IsAccepted);
        Assert.Equal(promised, retry.PromisedBallot);
        var unchangedPrepare = await server.PrepareAsync("key", new(promised));
        Assert.True(unchangedPrepare.IsPromised);
        Assert.Equal(accepted, unchangedPrepare.AcceptedBallot);
        Assert.Equal(new ByteString([7]), unchangedPrepare.AcceptedValue);
        var stalePrepare = await server.PrepareAsync("key", new(accepted));
        Assert.False(stalePrepare.IsPromised);
        Assert.Equal(promised, stalePrepare.ConflictingBallot);
        var staleAccept = await server.AcceptAsync("key", new(accepted, new([8]), Ballot.Zero));
        Assert.False(staleAccept.IsAccepted);
        Assert.Equal(promised, staleAccept.ConflictingBallot);
        var state = Assert.Single((await server.ListKeysAsync()).Entries);
        Assert.Equal(promised, state.PromisedBallot);
        Assert.Equal(accepted, state.AcceptedBallot);
        Assert.Equal(new ByteString([7]), state.AcceptedValue);
    }
}
