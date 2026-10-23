using FastCASPaxos.Model;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteProposerBallotSourceTests
{
    [Fact]
    public void Reserve_ReopenAndMinimumPreserveStrictOwnership()
    {
        using var database = new TestDatabase();
        using (var source = new SqliteProposerBallotSource(database.Path, 7))
        {
            Assert.Equal(7, source.ProposerId);
            Assert.Equal(new Ballot(1, 7), source.Reserve(1));
            Assert.Equal(new Ballot(2, 7), source.Reserve(1));
            Assert.Equal(new Ballot(100, 7), source.Reserve(100));
            Assert.Equal(new Ballot(101, 7), source.Reserve(100));
        }
        using var reopened = new SqliteProposerBallotSource(database.Path, 7);
        Assert.Equal(new Ballot(102, 7), reopened.Reserve(1));
        Assert.Equal(new Ballot(103, 7), reopened.Reserve(103));
        Assert.Equal(new Ballot(104, 7), reopened.Reserve(103));
    }

    [Fact]
    public async Task Reserve_ConcurrentInstancesNeverReuseRounds()
    {
        using var database = new TestDatabase();
        using var first = new SqliteProposerBallotSource(database.Path, 11);
        using var second = new SqliteProposerBallotSource(database.Path, 11);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task<Ballot[]> Run(SqliteProposerBallotSource source)
        {
            await start.Task;
            return Enumerable.Range(0, 40).Select(_ => source.Reserve(1)).ToArray();
        }
        var left = Task.Run(() => Run(first));
        var right = Task.Run(() => Run(second));
        start.SetResult();
        var sequences = await Task.WhenAll(left, right).WaitAsync(TestDatabase.Timeout);
        foreach (var sequence in sequences)
        {
            Assert.All(sequence, ballot => Assert.Equal(11, ballot.Proposer));
            Assert.True(sequence.Zip(sequence.Skip(1), (a, b) => a < b).All(value => value));
        }
        Assert.Equal(Enumerable.Range(1, 80), sequences.SelectMany(sequence => sequence).Select(ballot => ballot.Round).Order());
        using var reopened = new SqliteProposerBallotSource(database.Path, 11);
        Assert.Equal(new Ballot(81, 11), reopened.Reserve(1));
    }

    [Fact]
    public void Open_WrongIdentityCannotResetAllocator()
    {
        using var database = new TestDatabase();
        using (var owner = new SqliteProposerBallotSource(database.Path, 4))
            Assert.Equal(new Ballot(90, 4), owner.Reserve(90));
        Assert.Throws<InvalidOperationException>(() => new SqliteProposerBallotSource(database.Path, 5));
        using var reopened = new SqliteProposerBallotSource(database.Path, 4);
        Assert.Equal(new Ballot(91, 4), reopened.Reserve(1));
    }

    [Fact]
    public void Reserve_ExhaustionDoesNotWrapOrResetOnReopen()
    {
        using var database = new TestDatabase();
        using (var source = new SqliteProposerBallotSource(database.Path, 1))
        {
            Assert.Equal(new Ballot(int.MaxValue - 1, 1), source.Reserve(int.MaxValue - 1));
            Assert.Equal(new Ballot(int.MaxValue, 1), source.Reserve(1));
            Assert.Throws<OverflowException>(() => source.Reserve(1));
            Assert.Throws<OverflowException>(() => source.Reserve(int.MaxValue));
        }
        using var reopened = new SqliteProposerBallotSource(database.Path, 1);
        Assert.Throws<OverflowException>(() => reopened.Reserve(1));
    }

    [Fact]
    public void ValidationAndDisposal_DoNotConsumeReservations()
    {
        using var database = new TestDatabase();
        Assert.Throws<ArgumentException>(() => new SqliteProposerBallotSource("", 1));
        Assert.Throws<ArgumentNullException>(() => new SqliteProposerBallotSource(null!, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqliteProposerBallotSource(database.Path, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SqliteProposerBallotSource(database.Path, -1));
        var source = new SqliteProposerBallotSource(database.Path, 1);
        try
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => source.Reserve(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => source.Reserve(-1));
            Assert.Equal(new Ballot(1, 1), source.Reserve(1));
        }
        finally { source.Dispose(); }
        source.Dispose();
        Assert.Throws<ObjectDisposedException>(() => source.Reserve(1));
        using var reopened = new SqliteProposerBallotSource(database.Path, 1);
        Assert.Equal(new Ballot(2, 1), reopened.Reserve(1));
    }
}
