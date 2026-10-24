using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class RegisterListingTests
{
    private static async Task Seed(ControlledPeer peer, string key, int round = 1, bool absent = false)
    {
        Assert.True((await peer.Inner.AcceptAsync(key,
            new(new Ballot(round, 9), absent ? Absent : Envelope(Version(9, round), Payload), Ballot.Zero))).IsAccepted);
    }

    [Fact]
    public async Task Listing_ResolvesDistributedCandidatesAndExcludesLogicalAbsence()
    {
        var peers = Peers();
        // Each committed key is stored by a different majority.
        await Seed(peers[0], "alpha");
        await Seed(peers[2], "alpha");
        await Seed(peers[1], "beta");
        await Seed(peers[2], "beta");
        await Seed(peers[0], "gamma");
        await Seed(peers[1], "gamma");
        await Seed(peers[0], "partial", 2); // Only one accepted copy; listing must resolve it.
        foreach (var peer in peers)
        {
            peer.Read = (key, known, token) => key == "partial"
                ? ValueTask.FromException<AcceptorReadResult>(new IOException("inconclusive partial probe"))
                : peer.Inner.ReadAsync(key, known, token);
            await Seed(peer, "logical-absence", absent: true);
            await peer.Inner.PrepareAsync("promise-only", new(new Ballot(1, 1)));
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options());
        Assert.False((await service.ReadAsync("read-absence")).IsPresent);
        var page = await service.ListKeysAsync().AsTask().WaitAsync(Watchdog);
        Assert.Equal(new[] { "alpha", "beta", "gamma", "partial" }, page.Keys);
        Assert.Null(page.ContinuationKey);
        Assert.All(peers, peer => Assert.Equal(Envelope(Version(9, 2), Payload), peer.Inner.Snapshot("partial")!.AcceptedValue));
        Assert.All(peers, peer => Assert.Equal(Absent, peer.Inner.Snapshot("logical-absence")!.AcceptedValue));
        Assert.All(peers, peer => Assert.Null(peer.Inner.Snapshot("read-absence")));
        Assert.All(peers, peer => Assert.DoesNotContain(peer.Accepts, item => item.Key == "promise-only"));
    }

    [Fact]
    public async Task Listing_PartialCandidateOutsideReadQuorumIsNotBlindlyListed()
    {
        var peers = Peers();
        await Seed(peers[2], "uncommitted");
        // Inventory sees the minority record on peer 2; resolution uses peers 0+1, which can
        // legally observe absence. Listing raw minority inventory without a read quorum is incorrect.
        peers[0].List = (_, _, _, _) => ValueTask.FromException<KeyPage>(new IOException("inventory unavailable"));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options());
        var page = await service.ListKeysAsync().AsTask().WaitAsync(Watchdog);
        Assert.Empty(page.Keys);
        Assert.Null(page.ContinuationKey);
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        Assert.All(peers.Take(2), peer => Assert.Null(peer.Inner.Snapshot("uncommitted")));
        Assert.Equal(Envelope(Version(9, 1), Payload), peers[2].Inner.Snapshot("uncommitted")!.AcceptedValue);
        Assert.False((await service.ReadAsync("uncommitted")).IsPresent);
    }

    [Fact]
    public async Task Listing_UsesCoverageFrontierBeforeReturningPage()
    {
        var peers = Peers();
        foreach (var key in new[] { "a", "z" })
        {
            await Seed(peers[0], key);
            await Seed(peers[2], key);
        }
        foreach (var key in new[] { "b", "c", "d", "e" })
        {
            await Seed(peers[1], key);
            await Seed(peers[2], key);
        }
        peers[2].List = (_, _, _, _) => ValueTask.FromException<KeyPage>(new IOException("offline inventory"));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options());
        var first = await service.ListKeysAsync(limit: 2);
        Assert.Equal(new[] { "a", "b" }, first.Keys); // Not a,z from the first complete local page.
        Assert.Equal("b", first.ContinuationKey);
        var second = await service.ListKeysAsync(afterKey: first.ContinuationKey, limit: 2);
        Assert.Equal(new[] { "c", "d" }, second.Keys);
        Assert.Equal("d", second.ContinuationKey);
        var final = await service.ListKeysAsync(afterKey: second.ContinuationKey, limit: 2);
        Assert.Equal(new[] { "e", "z" }, final.Keys);
        Assert.Null(final.ContinuationKey);
    }

    [Fact]
    public async Task Listing_OrdinalEdgeKeysPrefixAndExclusivePages()
    {
        var peers = Peers();
        // Deliberately specified, not sorted by the implementation's comparison routine.
        // NUL and isolated UTF16 units are data, never theory display names.
        string[] expected = ["", "\0", "\0a", "%_", "%_a", "A", "a", "a\0", "a\0x",
            "\ud800", "\ud800x", "\ud83d\ude00", "\udc00", "\uffff"];
        foreach (var key in expected.Reverse())
            foreach (var peer in peers) await Seed(peer, key);
        // Absence before/between/after real entries must neither consume limit nor hide later keys.
        foreach (var key in new[] { "\0\0", "B", "zz", "\uffffz" })
            foreach (var peer in peers) await Seed(peer, key, absent: true);
        peers[2].GoOffline();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options());
        var all = new List<string>();
        string? cursor = null;
        for (var pageNumber = 0; pageNumber <= expected.Length; pageNumber++)
        {
            var page = await service.ListKeysAsync(afterKey: cursor, limit: 3);
            Assert.InRange(page.Keys.Count, 0, 3);
            all.AddRange(page.Keys);
            if (page.ContinuationKey is null) break;
            Assert.Equal(3, page.Keys.Count);
            Assert.Equal(page.Keys[^1], page.ContinuationKey);
            Assert.NotEqual(cursor, page.ContinuationKey);
            cursor = page.ContinuationKey;
        }
        Assert.Equal(expected, all);
        var first = await service.ListKeysAsync(limit: 1);
        Assert.Equal("", Assert.Single(first.Keys));
        Assert.Equal("", first.ContinuationKey);
        Assert.Equal(new[] { "%_", "%_a" }, (await service.ListKeysAsync("%_")).Keys);
        Assert.Equal(new[] { "a\0", "a\0x" }, (await service.ListKeysAsync("a\0")).Keys);
        Assert.Equal(new[] { "\ud800", "\ud800x" }, (await service.ListKeysAsync("\ud800")).Keys);
        Assert.Equal(new[] { "\ud83d\ude00" }, (await service.ListKeysAsync("\ud83d\ude00")).Keys);
        Assert.Equal(new[] { "A" }, (await service.ListKeysAsync("A")).Keys);
        Assert.Equal(new[] { "%_a" }, (await service.ListKeysAsync("%_", "%_Z")).Keys);
        Assert.Empty((await service.ListKeysAsync("a", "b")).Keys);
        Assert.Empty((await service.ListKeysAsync("missing")).Keys);
        var pastEnd = await service.ListKeysAsync(afterKey: "\uffff", limit: 1);
        Assert.Empty(pastEnd.Keys);
        Assert.Null(pastEnd.ContinuationKey);
        Assert.Equal(expected, (await service.ListKeysAsync(limit: int.MaxValue)).Keys);
        Assert.Null((await service.ListKeysAsync(limit: expected.Length)).ContinuationKey);
    }

    [Fact]
    public async Task Listing_EmptyAndSingletonHaveNoContinuation()
    {
        var peers = Peers();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options());
        var empty = await service.ListKeysAsync(limit: 1);
        Assert.Empty(empty.Keys);
        Assert.Null(empty.ContinuationKey);
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("only", null, ByteString.Empty)).Status);
        var singleton = await service.ListKeysAsync(limit: 1);
        Assert.Equal("only", Assert.Single(singleton.Keys));
        Assert.Null(singleton.ContinuationKey);
    }

    [Fact]
    public async Task Listing_RequiresQuorumAndHonorsCancellation()
    {
        var peers = Peers();
        var clock = new ManualTime();
        using var gate = new Gate();
        for (var i = 1; i < peers.Length; i++)
            peers[i].List = async (_, _, _, _) => { await gate.WaitAsync(); throw new IOException("offline"); };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options(clock: clock));
        var listing = service.ListKeysAsync().AsTask();
        await gate.Arrived;
        Assert.False(listing.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<RegisterUnavailableException>(() => listing.WaitAsync(Watchdog));
        using var cancellation = new CancellationTokenSource();
        var canceled = service.ListKeysAsync(cancellationToken: cancellation.Token).AsTask();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.WaitAsync(Watchdog));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        gate.Release();
        foreach (var peer in peers) peer.List = null;
        Assert.Empty((await service.ListKeysAsync()).Keys);
    }

    [Fact]
    public async Task Listing_ResolutionCannotReturnInventoryWithoutAProtocolQuorum()
    {
        var peers = Peers();
        RequireRecovery(peers);
        foreach (var peer in peers) await Seed(peer, "candidate");
        var clock = new ManualTime();
        using var gate = new Gate();
        for (var i = 1; i < peers.Length; i++)
            peers[i].Prepare = async (_, _, _) => { await gate.WaitAsync(); throw new IOException("offline protocol"); };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options(clock: clock));
        var listing = service.ListKeysAsync().AsTask();
        await gate.Arrived;
        clock.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<RegisterUnavailableException>(() => listing.WaitAsync(Watchdog));
    }

    [Fact]
    public async Task Listing_StableCandidatesUseReadQuorumWithoutAllocatingOrMutating()
    {
        var peers = Peers();
        foreach (var peer in peers)
        {
            await Seed(peer, "a");
            await Seed(peer, "b");
            await Seed(peer, "absent", absent: true);
        }
        var before = peers.SelectMany(peer => new[] { "a", "absent", "b" }.Select(peer.Inner.Snapshot)).ToArray();
        var ballots = new TestBallots(3);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        for (var iteration = 0; iteration < 2; iteration++)
        {
            var page = await service.ListKeysAsync().AsTask().WaitAsync(Watchdog);
            Assert.Equal(new[] { "a", "b" }, page.Keys);
            Assert.Null(page.ContinuationKey);
        }

        Assert.Empty(ballots.Reserved);
        Assert.Equal(before, peers.SelectMany(peer => new[] { "a", "absent", "b" }.Select(peer.Inner.Snapshot)));
        Assert.All(peers, peer =>
        {
            Assert.Equal(new[] { "a", "absent", "b", "a", "absent", "b" }, peer.Reads.Select(read => read.Key));
            Assert.All(peer.Reads, read => Assert.Null(read.Known));
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
    }
}
