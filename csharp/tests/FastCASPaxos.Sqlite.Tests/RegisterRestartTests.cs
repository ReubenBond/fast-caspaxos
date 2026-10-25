using System.Buffers.Binary;
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
    public async Task RestartDelayedOldPrepareAndAcceptCannotReplaceNewerState(string mode)
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
            Assert.Equal(1UL, BinaryPrimitives.ReadUInt64BigEndian(firstValue.Version!.Value.Span));
            Assert.Equal("FCR2"u8.ToArray(), oldAccept.Value.Span[..4].ToArray());
            Assert.Equal(8, BinaryPrimitives.ReadInt32BigEndian(oldAccept.Value.Span[13..]));
            Assert.True(BinaryPrimitives.ReadInt32BigEndian(oldAccept.Value.Span[17..]) > 0);
            Assert.Equal(1L, BinaryPrimitives.ReadInt64BigEndian(oldAccept.Value.Span[21..]));
            Assert.Equal(new byte[] { 1 }, oldAccept.Value.Span[29..].ToArray());
            // Establish the initial accepted pair on every durable replica before restarting.
            // Service success alone proves only a majority, not that every replica caught up.
            foreach (var peer in new[] { a, b, c })
                Assert.True((await peer.AcceptAsync("durable", oldAccept)).IsAccepted);
        }

        // Close and reopen every durable participant, retaining the same proposer identity/database.
        using var reopenedBallots = new SqliteProposerBallotSource(ballotDatabase.Path, 8);
        await using var first = await firstDatabase.OpenAsync();
        await using var second = await secondDatabase.OpenAsync();
        await using var third = await thirdDatabase.OpenAsync();
        var afterRestart = new RecordingAcceptor(first);
        var partitioned = new PartitionedAcceptor(third);
        await using var restarted = new ReplicatedRegisterService(
            [new("a", afterRestart), new("b", second), new("c", partitioned)], reopenedBallots, options);
        Assert.Equal(firstValue, await restarted.ReadAsync("durable").AsTask().WaitAsync(TestDatabase.Timeout));
        var updated = await restarted.CompareExchangeAsync("durable", firstValue.Version, new ByteString([2])).AsTask().WaitAsync(TestDatabase.Timeout);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.Equal(new ByteString([2]), updated.Current!.Value);
        Assert.NotEqual(firstValue.Version, updated.Current!.Version);
        Assert.Equal(2UL, BinaryPrimitives.ReadUInt64BigEndian(updated.Current.Version!.Value.Span));
        Assert.True(afterRestart.Accept!.Value.Ballot > oldAccept.Ballot);
        var newAccept = afterRestart.Accept.Value;
        Assert.Equal(8, afterRestart.Prepare!.Value.Ballot.Proposer);
        Assert.True(afterRestart.Prepare.Value.Ballot > oldAccept.NextBallotToPrepare);
        Assert.Equal("FCR2"u8.ToArray(), newAccept.Value.Span[..4].ToArray());
        Assert.Equal(8, BinaryPrimitives.ReadInt32BigEndian(newAccept.Value.Span[13..]));
        Assert.True(BinaryPrimitives.ReadInt32BigEndian(newAccept.Value.Span[17..])
            > BinaryPrimitives.ReadInt32BigEndian(oldAccept.Value.Span[17..]));
        Assert.Equal(1L, BinaryPrimitives.ReadInt64BigEndian(newAccept.Value.Span[21..]));
        Assert.Equal(new byte[] { 2 }, newAccept.Value.Span[29..].ToArray());
        foreach (var peer in new[] { first, second })
        {
            var current = Assert.Single((await peer.ListKeysAsync()).Entries);
            Assert.Equal(newAccept.Ballot, current.AcceptedBallot);
            Assert.Equal(newAccept.Value, current.AcceptedValue);
        }
        // The third replica misses every restarted request, but subsequently promises the
        // newer ballot without accepting its value. It still owns the exact old accepted pair.
        Assert.True((await third.PrepareAsync("durable", new(newAccept.Ballot))).IsPromised);
        var lagging = Assert.Single((await third.ListKeysAsync()).Entries);
        Assert.Equal(oldAccept.Ballot, lagging.AcceptedBallot);
        Assert.Equal(oldAccept.Value, lagging.AcceptedValue);
        Assert.True(lagging.PromisedBallot > oldAccept.Ballot);
        foreach (var peer in new[] { first, second, third })
        {
            var retainsOldPair = ReferenceEquals(peer, third);
            var before = Assert.Single((await peer.ListKeysAsync()).Entries);
            Assert.True(before.PromisedBallot > oldPrepare.Ballot);
            var highestBallot = Ballot.Max(before.PromisedBallot, before.AcceptedBallot);
            var prepare = await peer.PrepareAsync("durable", oldPrepare);
            Assert.False(prepare.IsPromised);
            Assert.Equal(highestBallot, prepare.ConflictingBallot);
            Assert.Equal(before.AcceptedValue, prepare.AcceptedValue);
            Assert.Equal(before.AcceptedBallot, prepare.AcceptedBallot);
            Assert.Equal(before, Assert.Single((await peer.ListKeysAsync()).Entries));

            // A duplicate prepare at the current promise is allowed, but is also read-only.
            var currentPrepare = await peer.PrepareAsync("durable", new(before.PromisedBallot));
            Assert.True(currentPrepare.IsPromised);
            Assert.Equal(Ballot.Zero, currentPrepare.ConflictingBallot);
            Assert.Equal(before.AcceptedBallot, currentPrepare.AcceptedBallot);
            Assert.Equal(before.AcceptedValue, currentPrepare.AcceptedValue);
            Assert.Equal(before, Assert.Single((await peer.ListKeysAsync()).Entries));

            // An exact ballot/value retry can ACK on the lagging replica despite a higher
            // promise. Neither that ACK nor an updated replica's rejection may change state.
            var accept = await peer.AcceptAsync("durable", oldAccept);
            Assert.Equal(retainsOldPair, accept.IsAccepted);
            Assert.Equal(retainsOldPair ? before.PromisedBallot : Ballot.Zero, accept.PromisedBallot);
            Assert.Equal(retainsOldPair ? Ballot.Zero : highestBallot, accept.ConflictingBallot);
            Assert.Equal(before, Assert.Single((await peer.ListKeysAsync()).Entries));

            // Same ballot with different bytes is not an exact retry, even on the lagging
            // replica. A rejected request cannot smuggle in a newer piggyback promise.
            var wrongValue = oldAccept with
            {
                Value = newAccept.Value,
                NextBallotToPrepare = highestBallot.NextRound(8)
            };
            Assert.NotEqual(oldAccept.Value, wrongValue.Value);
            var rejected = await peer.AcceptAsync("durable", wrongValue);
            Assert.False(rejected.IsAccepted);
            Assert.Equal(Ballot.Zero, rejected.PromisedBallot);
            Assert.Equal(highestBallot, rejected.ConflictingBallot);
            Assert.Equal(before, Assert.Single((await peer.ListKeysAsync()).Entries));
        }
        partitioned.IsPartitioned = false;
        // Use a fresh proposer engine and include the old replica again: every read quorum
        // still intersects the two updated replicas, so the delayed retries cannot undo [2].
        await using var reader = new ReplicatedRegisterService(
            [new("c", partitioned), new("a", first), new("b", second)], reopenedBallots, options);
        Assert.Equal(updated.Current, await reader.ReadAsync("durable").AsTask().WaitAsync(TestDatabase.Timeout));
        Assert.Equal<string>(["durable"], (await reader.ListKeysAsync()).Keys);
    }

    private sealed class PartitionedAcceptor(IKeyedAcceptor inner) : IKeyedAcceptor
    {
        public bool IsPartitioned { get; set; } = true;

        public ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default) =>
            IsPartitioned
                ? ValueTask.FromException<AcceptorReadResult>(new IOException("Replica is partitioned."))
                : inner.ReadAsync(key, known, cancellationToken);

        public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default) =>
            IsPartitioned
                ? ValueTask.FromException<AcceptorPrepareResult<ByteString?>>(new IOException("Replica is partitioned."))
                : inner.PrepareAsync(key, request, cancellationToken);

        public ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default) =>
            IsPartitioned
                ? ValueTask.FromException<AcceptorAcceptResult>(new IOException("Replica is partitioned."))
                : inner.AcceptAsync(key, request, cancellationToken);

        public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            IsPartitioned
                ? ValueTask.FromException<KeyPage>(new IOException("Replica is partitioned."))
                : inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken);
    }

    private sealed class RecordingAcceptor(IKeyedAcceptor inner) : IKeyedAcceptor
    {
        public PrepareRequest? Prepare;
        public AcceptRequest<ByteString>? Accept;
        public ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(key, known, cancellationToken);
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
