using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteConcurrencyTests
{
    [Fact]
    public async Task ConcurrentDifferentKeys_PersistEveryAcknowledgedValue()
    {
        using var database = new TestDatabase();
        await using (var server = await database.OpenAsync(queueCapacity: 8, maxBatchSize: 4))
        {
            var tasks = Enumerable.Range(0, 64).Select(index => server.AcceptAsync(
                $"key-{index:D3}", new(new(2, 1), new([(byte)index]), new(3, 0))).AsTask()).ToArray();
            var results = await Task.WhenAll(tasks).WaitAsync(TestDatabase.Timeout);
            Assert.All(results, result =>
            {
                Assert.True(result.IsAccepted);
                Assert.Equal(new Ballot(3, 0), result.PromisedBallot);
            });
        }

        await using var reopened = await database.OpenAsync();
        var page = await reopened.ListKeysAsync(limit: 100);
        Assert.Equal(64, page.Entries.Count);
        Assert.Null(page.ContinuationKey);
        for (var index = 0; index < 64; index++)
        {
            Assert.Equal($"key-{index:D3}", page.Entries[index].Key);
            Assert.Equal(new ByteString([(byte)index]), page.Entries[index].AcceptedValue);
            Assert.Equal(new Ballot(2, 1), page.Entries[index].AcceptedBallot);
            Assert.Equal(new Ballot(3, 0), page.Entries[index].PromisedBallot);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentSameBallot_OnlyOneValueWins(bool separateServers)
    {
        using var database = new TestDatabase();
        await using var first = await database.OpenAsync(maxBatchSize: 4);
        await using var second = await database.OpenAsync(maxBatchSize: 4);
        var ballot = new Ballot(2, 1);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, 32).Select(async index =>
        {
            await start.Task;
            var server = separateServers && index % 2 != 0 ? second : first;
            var result = await server.AcceptAsync("shared", new(ballot, new([(byte)index]), Ballot.Zero));
            return (Index: index, Result: result);
        }).ToArray();
        start.SetResult();
        var results = await Task.WhenAll(tasks).WaitAsync(TestDatabase.Timeout);
        var winner = Assert.Single(results, result => result.Result.IsAccepted);
        Assert.Equal(Ballot.InitialFast(), winner.Result.PromisedBallot);
        Assert.All(results.Where(result => !result.Result.IsAccepted),
            result => Assert.Equal(ballot, result.Result.ConflictingBallot));
        foreach (var server in new[] { first, second })
        {
            var state = Assert.Single((await server.ListKeysAsync()).Entries);
            Assert.Equal("shared", state.Key);
            Assert.Equal(ballot, state.AcceptedBallot);
            Assert.Equal(new ByteString([(byte)winner.Index]), state.AcceptedValue);
            Assert.True((await server.AcceptAsync("shared",
                new(ballot, new([(byte)winner.Index]), new(100, 1)))).IsAccepted);
        }
    }

    [Fact]
    public async Task PreCancelledOperations_DoNotExecute()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            server.PrepareAsync("prepare", new(new Ballot(9, 1)), cancellation.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            server.AcceptAsync("accept", new(new(9, 1), new([1]), Ballot.Zero), cancellation.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            server.ListKeysAsync(cancellationToken: cancellation.Token).AsTask());

        Assert.Empty((await server.ListKeysAsync()).Entries);
        Assert.True((await server.PrepareAsync("prepare", new(Ballot.InitialFast()))).IsPromised);
        Assert.True((await server.PrepareAsync("accept", new(Ballot.InitialFast()))).IsPromised);
    }

    [Fact]
    public async Task CancellationBeforeSelection_SkipsAdmittedBacklog()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync(queueCapacity: 16, maxBatchSize: 1);
        using var connection = database.Connect();
        using var heldWriter = connection.BeginTransaction(deferred: false);
        // FIFO + one-item batches: the first operation cannot finish while this
        // transaction holds the writer lock, so no later item can be selected.
        var first = server.AcceptAsync("first",
            new(Ballot.InitialFast(), new([1]), Ballot.Zero)).AsTask();
        using var cancellation = new CancellationTokenSource();
        var skippedAccept = server.AcceptAsync("skipped-accept",
            new(new(20, 1), new([9]), Ballot.Zero), cancellation.Token).AsTask();
        var skippedPrepare = server.PrepareAsync("skipped-prepare",
            new(new Ballot(20, 1)), cancellation.Token).AsTask();
        var skippedList = server.ListKeysAsync(cancellationToken: cancellation.Token).AsTask();
        cancellation.Cancel();
        heldWriter.Commit();

        Assert.True((await first.WaitAsync(TestDatabase.Timeout)).IsAccepted);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => skippedAccept.WaitAsync(TestDatabase.Timeout));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => skippedPrepare.WaitAsync(TestDatabase.Timeout));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => skippedList.WaitAsync(TestDatabase.Timeout));
        Assert.Equal("first", Assert.Single((await server.ListKeysAsync()).Entries).Key);
        Assert.True((await server.PrepareAsync("skipped-accept", new(Ballot.InitialFast()))).IsPromised);
        Assert.True((await server.PrepareAsync("skipped-prepare", new(Ballot.InitialFast()))).IsPromised);
    }

    [Fact]
    public async Task CancellationDuringAdmission_DoesNotEnterFullQueue()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync(queueCapacity: 1, maxBatchSize: 1);
        using var connection = database.Connect();
        using var heldWriter = connection.BeginTransaction(deferred: false);
        var first = server.AcceptAsync("first", new(Ballot.InitialFast(), new([1]), Ballot.Zero)).AsTask();
        var second = server.AcceptAsync("second", new(Ballot.InitialFast(), new([2]), Ballot.Zero)).AsTask();
        using var cancellation = new CancellationTokenSource();
        // At most one item is selected and one queued while the writer is locked.
        // The third is necessarily waiting for admission, regardless of scheduling.
        var blocked = server.AcceptAsync("blocked",
            new(new(10, 1), new([3]), Ballot.Zero), cancellation.Token).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked.WaitAsync(TestDatabase.Timeout));
        heldWriter.Commit();

        Assert.True((await first.WaitAsync(TestDatabase.Timeout)).IsAccepted);
        Assert.True((await second.WaitAsync(TestDatabase.Timeout)).IsAccepted);
        Assert.Equal(new[] { "first", "second" },
            (await server.ListKeysAsync()).Entries.Select(entry => entry.Key));
        Assert.True((await server.PrepareAsync("blocked", new(Ballot.InitialFast()))).IsPromised);
    }

    [Fact]
    public async Task Dispose_DrainsAdmittedWorkAndRejectsLaterCalls()
    {
        using var database = new TestDatabase();
        var server = await database.OpenAsync(queueCapacity: 16, maxBatchSize: 1);
        try
        {
            using var connection = database.Connect();
            using var heldWriter = connection.BeginTransaction(deferred: false);
            var writes = Enumerable.Range(0, 8).Select(index => server.AcceptAsync(
                $"key-{index}", new(Ballot.InitialFast(), new([(byte)index]), Ballot.Zero)).AsTask()).ToArray();
            var disposing = server.DisposeAsync().AsTask();
            Assert.False(disposing.IsCompleted);
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                server.AcceptAsync("late", new(Ballot.InitialFast(), ByteString.Empty, Ballot.Zero)).AsTask());
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                server.PrepareAsync("late", new(Ballot.InitialFast())).AsTask());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => server.ListKeysAsync().AsTask());
            heldWriter.Commit();
            Assert.All(await Task.WhenAll(writes).WaitAsync(TestDatabase.Timeout),
                result => Assert.True(result.IsAccepted));
            await disposing.WaitAsync(TestDatabase.Timeout);
            await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
        }
        finally
        {
            await server.DisposeAsync().AsTask().WaitAsync(TestDatabase.Timeout);
        }

        await using var reopened = await database.OpenAsync();
        var entries = (await reopened.ListKeysAsync()).Entries;
        Assert.Equal(8, entries.Count);
        for (var index = 0; index < 8; index++)
        {
            Assert.Equal($"key-{index}", entries[index].Key);
            Assert.Equal(new ByteString([(byte)index]), entries[index].AcceptedValue);
        }
    }
}
