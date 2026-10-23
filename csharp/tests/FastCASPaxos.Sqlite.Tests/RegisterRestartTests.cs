using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class RegisterRestartTests
{
    [Theory]
    [InlineData("classic")]
    [InlineData("leader")]
    public async Task Restart_DelayedOldPrepareAndAcceptCannotReplaceNewerState(string mode)
    {
        using var ballotDatabase = new TestDatabase();
        using var firstDatabase = new TestDatabase();
        using var secondDatabase = new TestDatabase();
        using var thirdDatabase = new TestDatabase();
        PrepareRequest oldPrepare;
        AcceptRequest<ByteString> oldAccept;
        RegisterValue firstValue;
        var options = new RegisterServiceOptions { Mode = ProposerModePolicy.Parse(mode) };
        using (var ballots = new SqliteProposerBallotSource(ballotDatabase.Path, 8))
        {
            await using var a = await firstDatabase.OpenAsync();
            await using var b = await secondDatabase.OpenAsync();
            await using var c = await thirdDatabase.OpenAsync();
            var recording = new RecordingAcceptor(a);
            await using var service = new ReplicatedRegisterService(
                [new("a", recording), new("b", b), new("c", c)], ballots, options);
            var created = await service.CompareExchangeAsync("durable", null, new ByteString([1])).AsTask().WaitAsync(TestDatabase.Timeout);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            firstValue = created.Current!;
            oldPrepare = recording.Prepare!.Value;
            oldAccept = recording.Accept!.Value;
            Assert.Equal(new ByteString([1]), firstValue.Value);
        }

        // Close and reopen every durable participant, retaining the same proposer identity/database.
        using var reopenedBallots = new SqliteProposerBallotSource(ballotDatabase.Path, 8);
        await using var first = await firstDatabase.OpenAsync();
        await using var second = await secondDatabase.OpenAsync();
        await using var third = await thirdDatabase.OpenAsync();
        var afterRestart = new RecordingAcceptor(first);
        await using var restarted = new ReplicatedRegisterService(
            [new("a", afterRestart), new("b", second), new("c", third)], reopenedBallots, options);
        Assert.Equal(firstValue, await restarted.ReadAsync("durable").AsTask().WaitAsync(TestDatabase.Timeout));
        var updated = await restarted.CompareExchangeAsync("durable", firstValue.Version, new ByteString([2])).AsTask().WaitAsync(TestDatabase.Timeout);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.NotEqual(firstValue.Version, updated.Current!.Version);
        Assert.True(afterRestart.Accept!.Value.Ballot > oldAccept.Ballot);
        foreach (var peer in new[] { first, second, third })
        {
            var before = Assert.Single((await peer.ListKeysAsync()).Entries);
            var prepare = await peer.PrepareAsync("durable", oldPrepare);
            Assert.False(prepare.IsPromised);
            Assert.Equal(before.AcceptedValue, prepare.AcceptedValue);
            Assert.Equal(before.AcceptedBallot, prepare.AcceptedBallot);
            var accept = await peer.AcceptAsync("durable", oldAccept);
            Assert.False(accept.IsAccepted);
            Assert.True(accept.ConflictingBallot > oldAccept.Ballot);
            Assert.Equal(before, Assert.Single((await peer.ListKeysAsync()).Entries));
        }
        Assert.Equal(updated.Current, await restarted.ReadAsync("durable").AsTask().WaitAsync(TestDatabase.Timeout));
        Assert.Equal(new[] { "durable" }, (await restarted.ListKeysAsync()).Keys);
    }

    private sealed class RecordingAcceptor(IKeyedAcceptor inner) : IKeyedAcceptor
    {
        public PrepareRequest? Prepare;
        public AcceptRequest<ByteString>? Accept;
        public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default)
        {
            Prepare = request;
            return inner.PrepareAsync(key, request, cancellationToken);
        }
        public ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
        {
            Accept = request;
            return inner.AcceptAsync(key, request, cancellationToken);
        }
        public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken);
    }
}
