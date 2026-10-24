using FastCASPaxos.Messages;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class RegisterContractTests
{
    [Theory]
    [InlineData("classic")]
    [InlineData("fast")]
    [InlineData("leader")]
    public async Task ReadAndCompareExchange_PreserveAbsenceEmptyAndFreshOpaqueVersions(string mode)
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var client = cluster.CreateRegister(1, mode);
        var missing = await client.ReadAsync("").AsTask().WaitAsync(Watchdog);
        Assert.False(missing.IsPresent);
        Assert.Null(missing.Value);
        Assert.Null(missing.Version);
        var create = await client.CompareExchangeAsync("", null, default).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, create.Status);
        Assert.Equal(ByteString.Empty, create.Current!.Value);
        Assert.Equal(8, create.Current.Version!.Value.Length);
        Assert.Null(create.Error);
        Assert.Equal(create.Current, await client.ReadAsync(""));
        var mismatch = await client.CompareExchangeAsync("", null, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
        Assert.Equal(create.Current, mismatch.Current);
        Assert.Null(mismatch.Error);
        var sameBytes = await client.CompareExchangeAsync("", create.Current.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, sameBytes.Status);
        Assert.Equal(ByteString.Empty, sameBytes.Current!.Value);
        Assert.NotEqual(create.Current.Version, sameBytes.Current.Version);
        Assert.Null(sameBytes.Error);
        var stale = await client.CompareExchangeAsync("", create.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(sameBytes.Current, stale.Current);
        await Assert.ThrowsAsync<ArgumentException>(() => client.CompareExchangeAsync("", ByteString.Empty, Payload).AsTask());
        Assert.Equal(sameBytes.Current, await client.ReadAsync(""));
        var absentMismatch = await client.CompareExchangeAsync("missing", create.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, absentMismatch.Status);
        Assert.Equal(new RegisterValue(null, null), absentMismatch.Current);
    }

    [Theory]
    [InlineData("classic", false)]
    [InlineData("fast", false)]
    [InlineData("classic", true)]
    [InlineData("fast", true)]
    public async Task ConcurrentIdenticalPayload_ExactlyOneExpectedVersionWins(string mode, bool update)
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var first = cluster.CreateRegister(1, mode);
        var second = cluster.CreateRegister(2, mode);
        ByteString? expected = null;
        ByteString? initialEnvelope = null;
        if (update)
        {
            var initial = await first.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, initial.Status);
            expected = initial.Current!.Version;
            // Acknowledgement proves a quorum, not that peer zero has already committed.
            foreach (var acceptor in cluster.Clients)
            {
                var page = await acceptor.ListKeysAsync();
                if (page.Entries.Count > 0) initialEnvelope = Assert.Single(page.Entries).AcceptedValue;
            }
            Assert.NotNull(initialEnvelope);
        }

        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writes = new HashSet<ByteString>();
        foreach (var endpoint in cluster.Endpoints)
        {
            var inner = endpoint.Accept;
            endpoint.Accept = async (key, request, token) =>
            {
                // Recovery reads retain the initial envelope; only distinct proposed writes gate.
                if (!request.Value.Equals(initialEnvelope))
                {
                    lock (writes)
                    {
                        writes.Add(request.Value);
                        if (writes.Count == 2) both.TrySetResult();
                    }
                    await release.Task.WaitAsync(token);
                }
                return await inner(key, request, token);
            };
        }
        var left = first.CompareExchangeAsync("key", expected, Payload).AsTask();
        var right = second.CompareExchangeAsync("key", expected, Payload).AsTask();
        try
        {
            await both.Task.WaitAsync(Watchdog);
            Assert.False(left.IsCompleted);
            Assert.False(right.IsCompleted);
        }
        finally { release.TrySetResult(); }

        var results = await Task.WhenAll(left, right).WaitAsync(Watchdog);
        var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
        var loser = Assert.Single(results, result => result.Status != CompareExchangeStatus.Applied);
        Assert.Equal(Payload, winner.Current!.Value);
        Assert.NotEqual(expected, winner.Current.Version);
        Assert.Contains(loser.Status, new[] { CompareExchangeStatus.Mismatch, CompareExchangeStatus.Unknown });
        Assert.Equal(winner.Current, loser.Current);
        Assert.Equal(winner.Current, await first.ReadAsync("key"));
        Assert.Equal(winner.Current, await second.ReadAsync("key"));
    }

    [Fact]
    public async Task Restart_PreservesStateAndRejectsDelayedOldRequests()
    {
        using var directory = new TestDirectory();
        RegisterValue original;
        AcceptRequest<ByteString> oldAccept = default;
        await using (var cluster = await SqliteContractCluster.OpenAsync(directory))
        {
            var inner = cluster.Endpoints[0].Accept;
            cluster.Endpoints[0].Accept = (key, request, token) =>
            {
                oldAccept = request;
                return inner(key, request, token);
            };
            var register = cluster.CreateRegister(9);
            var created = await register.CompareExchangeAsync("durable", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            original = created.Current!;
        }

        await using var reopened = await SqliteContractCluster.OpenAsync(directory);
        var client = reopened.CreateRegister(9);
        Assert.Equal(original, await client.ReadAsync("durable").AsTask().WaitAsync(Watchdog));
        var updated = await client.CompareExchangeAsync("durable", original.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.Equal(ByteString.Empty, updated.Current!.Value);
        Assert.NotEqual(original.Version, updated.Current.Version);
        // Acknowledgement guarantees a majority, not that every peer has caught up. An exact
        // retry on a lagging peer may succeed read-only, even after a higher promise.
        var newerCopies = 0;
        foreach (var acceptor in reopened.Clients)
        {
            var before = await acceptor.PrepareAsync("durable", new(new(oldAccept.Ballot.Round + 100, 9)));
            Assert.True(before.IsPromised);
            var delayed = await acceptor.AcceptAsync("durable", oldAccept);
            if (before.AcceptedBallot > oldAccept.Ballot)
            {
                newerCopies++;
                Assert.False(delayed.IsAccepted);
                Assert.True(delayed.ConflictingBallot > oldAccept.Ballot);
            }
            else if (delayed.IsAccepted)
            {
                Assert.Equal(oldAccept.Ballot, before.AcceptedBallot);
                Assert.Equal(oldAccept.Value, before.AcceptedValue);
                Assert.Equal(new FastCASPaxos.Model.Ballot(oldAccept.Ballot.Round + 100, 9), delayed.PromisedBallot);
            }
            var oldPrepare = await acceptor.PrepareAsync("durable", new(oldAccept.Ballot));
            Assert.False(oldPrepare.IsPromised);
            Assert.Equal(before.AcceptedBallot, oldPrepare.AcceptedBallot);
            Assert.Equal(before.AcceptedValue, oldPrepare.AcceptedValue);
        }
        Assert.InRange(newerCopies, 2, 3);
        Assert.Equal(updated.Current, await client.ReadAsync("durable"));
        Assert.Equal(new[] { "durable" }, (await client.ListKeysAsync()).Keys);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task OneUnavailablePeer_QuorumStillReadsWritesAndLists(int offlinePeer)
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var client = cluster.CreateRegister(1, offlinePeer: offlinePeer);
        Assert.Equal(new RegisterValue(null, null), await client.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        var created = await client.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(Payload, created.Current!.Value);
        Assert.Equal(created.Current, await client.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        var page = await client.ListKeysAsync().AsTask().WaitAsync(Watchdog);
        Assert.Equal(new[] { "key" }, page.Keys);
        Assert.Null(page.ContinuationKey);
        Assert.Equal(0, cluster.Endpoints[offlinePeer].Calls);
    }

    [Fact]
    public async Task Listing_PreservesOrdinalShapesAndExclusivePagination()
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var client = cluster.CreateRegister(1, offlinePeer: 2);
        Assert.Empty((await client.ListKeysAsync(limit: 1)).Keys);
        // Literal expected order from lower-layer tests, restricted to well-formed Unicode.
        string[] expected = ["", "\0", "\0a", "%_", "%_a", "A", "a", "a\0", "a\0x", "\U0001f600", "\uffff"];
        foreach (var key in expected.Reverse())
            Assert.Equal(CompareExchangeStatus.Applied,
                (await client.CompareExchangeAsync(key, null, ByteString.Empty).AsTask().WaitAsync(Watchdog)).Status);
        Assert.False((await client.ReadAsync("logical-absence")).IsPresent);
        foreach (var limit in new[] { 1, 3, expected.Length, expected.Length + 1, int.MaxValue })
        {
            var all = new List<string>();
            string? cursor = null;
            for (var pageIndex = 0; pageIndex <= expected.Length; pageIndex++)
            {
                var page = await client.ListKeysAsync(afterKey: cursor, limit: limit).AsTask().WaitAsync(Watchdog);
                all.AddRange(page.Keys);
                if (page.ContinuationKey is null) break;
                Assert.Equal(limit, page.Keys.Count);
                Assert.Equal(page.Keys[^1], page.ContinuationKey);
                Assert.NotEqual(cursor, page.ContinuationKey);
                cursor = page.ContinuationKey;
            }
            Assert.Equal(expected, all);
        }
        var first = await client.ListKeysAsync(limit: 1);
        Assert.Equal("", Assert.Single(first.Keys));
        Assert.Equal("", first.ContinuationKey);
        var afterEmpty = await client.ListKeysAsync(afterKey: "", limit: 1);
        Assert.Equal("\0", Assert.Single(afterEmpty.Keys));
        Assert.Equal("\0", afterEmpty.ContinuationKey);
        Assert.Equal(new[] { "%_", "%_a" }, (await client.ListKeysAsync("%_")).Keys);
        Assert.Equal(new[] { "a\0", "a\0x" }, (await client.ListKeysAsync("a\0")).Keys);
        Assert.Equal(new[] { "\U0001f600" }, (await client.ListKeysAsync("\U0001f600")).Keys);
        Assert.Equal(new[] { "A" }, (await client.ListKeysAsync("A")).Keys);
        Assert.Equal(new[] { "%_a" }, (await client.ListKeysAsync("%_", "%_Z")).Keys);
        Assert.Empty((await client.ListKeysAsync("a", "b")).Keys);
        Assert.Empty((await client.ListKeysAsync("missing")).Keys);
        var pastEnd = await client.ListKeysAsync(afterKey: "\uffff", limit: 1);
        Assert.Empty(pastEnd.Keys);
        Assert.Null(pastEnd.ContinuationKey);
        Assert.Null((await client.ListKeysAsync(limit: expected.Length)).ContinuationKey);
    }

    [Fact]
    public async Task LocalStorage_RetainsFullUtf16CapacityWhileGrpcRejectsMalformedText()
    {
        using var directory = new TestDirectory();
        await using var sqlite = await SqliteAcceptorServer.OpenAsync(directory.Database("acceptor"));
        var key = "\ud800";
        Assert.True((await sqlite.AcceptAsync(key, new(new(2, 1), Payload, new()))).IsAccepted);
        var entry = Assert.Single((await sqlite.ListKeysAsync()).Entries);
        Assert.Equal(key, entry.Key);
        Assert.Equal(Payload, entry.AcceptedValue);
        var invoker = new StubCallInvoker((_, _) => throw new InvalidOperationException("Must not dispatch."));
        var client = new GrpcKeyedAcceptor(invoker);
        await Assert.ThrowsAsync<ArgumentException>(() => client.PrepareAsync(key, new(new(3, 1))).AsTask());
        Assert.Empty(invoker.Calls);
    }
}
