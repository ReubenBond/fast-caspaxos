using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class RegisterTests
{
    [Theory]
    [InlineData("classic", 1, 2, 1)]
    [InlineData("fast", 0, 1, 0)]
    [InlineData("leader", 1, 3, 1)]
    [InlineData("adaptive", 0, 1, 0)]
    public async Task IdleSeparatedWritesReusePreparedBallotWithoutVersionReservations(
        string mode, int firstPrepares, int firstReservations, int warmReservations)
    {
        var peers = Peers();
        var ballots = new TestBallots(7);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options(mode));
        Assert.Empty(ballots.Reserved);
        var created = await service.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), created.Current);
        Assert.Equal(firstReservations, ballots.Reserved.Count);
        Assert.All(peers, peer =>
        {
            Assert.Equal(firstPrepares, peer.Prepares.Count);
            Assert.Single(peer.Accepts);
            Assert.Empty(peer.Reads);
        });
        var initial = InspectWrite(peers[0].Accepts.Single().Request.Value);
        Assert.Equal((1UL, 7, 1, 1L, Payload), initial);
        var current = created.Current!;

        // Every call has returned (including releasing its key) before the next is admitted.
        for (ulong revision = 2; revision <= 4; revision++)
        {
            var before = peers.Select(peer => (peer.Prepares.Count, peer.Accepts.Count)).ToArray();
            var previousAccept = peers[0].Accepts.Last().Request;
            var reservations = ballots.Reserved.Count;
            var result = await service.CompareExchangeAsync("key", current.Version, Payload);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new RegisterValue(Payload, Revision(revision)), result.Current);
            Assert.Equal(warmReservations, ballots.Reserved.Count - reservations);
            for (var index = 0; index < peers.Length; index++)
            {
                var peer = peers[index];
                Assert.Equal(mode == "classic" ? 1 : 0, peer.Prepares.Count - before[index].Item1);
                Assert.Equal(1, peer.Accepts.Count - before[index].Item2);
                var accept = peer.Accepts.Last().Request;
                Assert.Equal((revision, 7, initial.Incarnation, (long)revision, Payload), InspectWrite(accept.Value));
                Assert.True(accept.Ballot > previousAccept.Ballot);
                if (mode != "classic") Assert.Equal(previousAccept.NextBallotToPrepare, accept.Ballot);
            }
            current = result.Current!;
        }

        // Revision belongs to the register, whereas invocation identity belongs to the service.
        var other = await service.CompareExchangeAsync("other", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, other.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(1)), other.Current);
        Assert.Equal((1UL, 7, initial.Incarnation, 5L, ByteString.Empty),
            InspectWrite(peers[0].Accepts.Last().Request.Value));
        var final = await service.CompareExchangeAsync("key", current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, final.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(5)), final.Current);
        Assert.Equal((5UL, 7, initial.Incarnation, 6L, Payload),
            InspectWrite(peers[0].Accepts.Last().Request.Value));
    }

    [Fact]
    public async Task DefaultServiceUsesAdaptiveFastPath()
    {
        Assert.Equal(ProposerModeStrategy.Adaptive, new RegisterServiceOptions().Mode.Strategy);
        var peers = Peers();
        var ballots = new TestBallots(4);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots);
        var create = await service.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, create.Status);
        var update = await service.CompareExchangeAsync("key", create.Current!.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, update.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(2)), update.Current);
        Assert.Equal(new Ballot(1, 4), Assert.Single(ballots.Reserved));
        Assert.All(peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Equal(new[] { Ballot.InitialFast(), new Ballot(2, 0) },
                peer.Accepts.Select(item => item.Request.Ballot));
        });
    }

    [Fact]
    public async Task FirstMismatchReservesOnlyOneIncarnationAndDoesNotAdvanceRevision()
    {
        var peers = Peers();
        var ballots = new TestBallots(5);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options("fast"));
        Assert.Equal(new RegisterValue(null, null), await service.ReadAsync("key"));
        Assert.Empty(ballots.Reserved);
        var mismatch = await service.CompareExchangeAsync("key", Revision(1), Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
        Assert.Equal(new RegisterValue(null, null), mismatch.Current);
        Assert.Equal(new Ballot(1, 5), Assert.Single(ballots.Reserved));

        var create = await service.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, create.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), create.Current);
        Assert.Equal(new Ballot(1, 5), Assert.Single(ballots.Reserved));
        Assert.All(peers, peer =>
            Assert.Equal((1UL, 5, 1, 2L, Payload), InspectWrite(peer.Accepts.Last().Request.Value)));
    }

    [Theory]
    [InlineData("fast")]
    [InlineData("leader")]
    [InlineData("adaptive")]
    public async Task IdleCacheEvictsLeastRecentlyUsedKeyAndRecoversRevision(string mode)
    {
        var peers = Peers();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(7), Options(mode, capacity: 2));
        var a = await service.CompareExchangeAsync("a", null, Payload);
        var b = await service.CompareExchangeAsync("b", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, a.Status);
        Assert.Equal(CompareExchangeStatus.Applied, b.Status);
        Assert.Equal(a.Current, await service.ReadAsync("a")); // Touch a, so b is the LRU.
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("c", null, Payload)).Status);

        var before = peers.Select(peer => (peer.Prepares.Count, peer.Accepts.Count)).ToArray();
        var warm = await service.CompareExchangeAsync("a", a.Current!.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, warm.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(2)), warm.Current);
        for (var index = 0; index < peers.Length; index++)
        {
            Assert.Equal(before[index].Item1, peers[index].Prepares.Count);
            Assert.Equal(before[index].Item2 + 1, peers[index].Accepts.Count);
        }

        var promises = peers.Select(peer => peer.Inner.Snapshot("b")!.PromisedBallot).ToArray();
        before = peers.Select(peer => (peer.Prepares.Count, peer.Accepts.Count)).ToArray();
        var recovered = await service.CompareExchangeAsync("b", b.Current!.Version, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, recovered.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(2)), recovered.Current);
        Assert.NotNull(recovered.Current);
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            Assert.True(peer.Prepares.Count > before[index].Item1);
            Assert.Equal(before[index].Item2 + 1, peer.Accepts.Count);
            Assert.Equal("b", peer.Prepares.Last().Key);
            Assert.True(peer.Accepts.Last().Request.Ballot >= promises[index]);
            Assert.Equal(recovered.Current.Version!.Value, Revision(InspectWrite(peer.Accepts.Last().Request.Value).Revision));
        }
    }

    [Theory]
    [InlineData("fast")]
    [InlineData("adaptive")]
    public async Task IdlePreparedWriteRecoversHigherOwnedBallotWithoutOverwritingCompetitor(string mode)
    {
        var peers = Peers();
        var ballots = new TestBallots(1);
        await using var first = new ReplicatedRegisterService(Routes(peers), ballots, Options(mode));
        await using var other = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options());
        var initial = await first.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, initial.Status);
        var competing = await other.CompareExchangeAsync("key", initial.Current!.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, competing.Status);
        var competitor = peers[0].Inner.Snapshot("key")!;

        var stale = await first.CompareExchangeAsync("key", initial.Current.Version, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), stale.Current);
        Assert.Null(stale.Error);
        Assert.All(peers, peer =>
        {
            var recovery = peer.Prepares.Last().Request.Ballot;
            Assert.Equal(1, recovery.Proposer);
            Assert.True(recovery > competitor.PromisedBallot);
            Assert.Equal(competitor.AcceptedValue, peer.Inner.Snapshot("key")!.AcceptedValue);
        });

        var prepares = peers.Select(peer => peer.Prepares.Count).ToArray();
        var accepts = peers.Select(peer => peer.Accepts.Count).ToArray();
        var reservations = ballots.Reserved.Count;
        var next = await first.CompareExchangeAsync("key", competing.Current!.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, next.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(3)), next.Current);
        Assert.Equal(mode == "adaptive" ? 1 : 0, ballots.Reserved.Count - reservations);
        for (var index = 0; index < peers.Length; index++)
        {
            // Recovery completes at a classic majority: adaptive confirms its next owned
            // ballot with those two votes, but fast needs three echoed fast promises.
            Assert.Equal(prepares[index] + (mode == "fast" ? 1 : 0), peers[index].Prepares.Count);
            Assert.Equal(accepts[index] + 1, peers[index].Accepts.Count);
        }
    }

    [Fact]
    public async Task LegacyReadThenWritePreservesBytesAndAdvancesRevision()
    {
        var peers = Peers();
        var version = Version(9, int.MaxValue);
        var legacy = Envelope(version, Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new Ballot(3, 9), legacy, Ballot.Zero))).IsAccepted);
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());
        Assert.Equal(new RegisterValue(Payload, version), await service.ReadAsync("key"));
        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer => Assert.Equal(legacy, peer.Inner.Snapshot("key")!.AcceptedValue));

        var updated = await service.CompareExchangeAsync("key", version, ByteString.Empty).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        const ulong nextRevision = (9UL << 32) + 0x80000000UL;
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(nextRevision)), updated.Current);
        Assert.All(peers, peer =>
        {
            var write = InspectWrite(peer.Inner.Snapshot("key")!.AcceptedValue);
            Assert.Equal(nextRevision, write.Revision);
            Assert.Equal(ByteString.Empty, write.Payload);
            Assert.Equal(1, write.Proposer);
        });
        Assert.Equal(updated.Current, await service.ReadAsync("key"));
    }

    [Theory]
    [InlineData("classic")]
    [InlineData("fast")]
    [InlineData("leader")]
    [InlineData("adaptive")]
    public async Task ReadAndCompareExchangePreserveIdentityAndAdvanceEveryWrite(string mode)
    {
        var peers = Peers();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(7), Options(mode));
        var missing = await service.ReadAsync("key");
        Assert.False(missing.IsPresent);
        Assert.Null(missing.Value);
        Assert.Null(missing.Version);
        Assert.All(peers, peer => Assert.Null(peer.Inner.Snapshot("key")));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));

        var create = await service.CompareExchangeAsync("key", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, create.Status);
        Assert.Equal(ByteString.Empty, create.Current!.Value);
        Assert.Equal(8, create.Current.Version!.Value.Length);
        Assert.Equal(1UL, ReadRevision(create.Current.Version.Value));
        var read = await service.ReadAsync("key");
        Assert.Equal(create.Current, read);
        var mismatch = await service.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
        Assert.Equal(create.Current, mismatch.Current);
        Assert.Null(mismatch.Error);

        var update = await service.CompareExchangeAsync("key", create.Current.Version, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, update.Status);
        Assert.NotEqual(create.Current.Version, update.Current!.Version);
        Assert.Equal(2UL, ReadRevision(update.Current.Version!.Value));
        Assert.Equal(ByteString.Empty, update.Current.Value);
        var stale = await service.CompareExchangeAsync("key", create.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(update.Current, stale.Current);
        Assert.Equal(update.Current, await service.ReadAsync("key"));
    }

    [Theory]
    [InlineData("classic", false, false)]
    [InlineData("fast", false, false)]
    [InlineData("classic", true, false)]
    [InlineData("fast", true, false)]
    [InlineData("fast", false, true)]
    [InlineData("fast", true, true)]
    [InlineData("adaptive", false, true)]
    [InlineData("adaptive", true, true)]
    public async Task ConcurrentSamePayloadOneExpectedVersionAppliesExactlyOnce(string mode, bool update, bool sharedProposer)
    {
        var peers = Peers();
        var firstBallots = new TestBallots(1);
        var secondBallots = sharedProposer ? firstBallots : new TestBallots(2);
        await using var first = new ReplicatedRegisterService(Routes(peers), firstBallots, Options(mode));
        await using var second = new ReplicatedRegisterService(Routes(peers), secondBallots, Options(mode));
        ByteString? expected = null;
        if (update)
        {
            var created = await first.CompareExchangeAsync("key", null, Payload);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            expected = created.Current!.Version;
        }

        using var release = new Gate();
        var observed = new HashSet<ByteString>();
        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        foreach (var peer in peers)
        {
            peer.Accept = async (key, request, token) =>
            {
                lock (observed)
                {
                    observed.Add(request.Value);
                    if (observed.Count >= 2) both.TrySetResult();
                }
                await release.WaitAsync();
                return await peer.Inner.AcceptAsync(key, request, token);
            };
        }
        var left = first.CompareExchangeAsync("key", expected, Payload).AsTask();
        var right = second.CompareExchangeAsync("key", expected, Payload).AsTask();
        await both.Task.WaitAsync(Watchdog);
        Assert.False(left.IsCompleted);
        Assert.False(right.IsCompleted);
        ByteString[] candidateBytes;
        lock (observed) candidateBytes = observed.ToArray();
        var candidates = candidateBytes.Select(InspectWrite).ToArray();
        Assert.Equal(2, candidates.Length);
        Assert.All(candidates, candidate =>
        {
            Assert.Equal(update ? 2UL : 1UL, candidate.Revision);
            Assert.Equal(Payload, candidate.Payload);
        });
        if (sharedProposer)
        {
            Assert.All(candidates, candidate => Assert.Equal(1, candidate.Proposer));
            Assert.NotEqual(candidates[0].Incarnation, candidates[1].Incarnation);
        }
        if ((mode == "fast" || mode == "adaptive") && !update)
        {
            // Both payloads are identical but their operation tokens differ at the SAME shared ballot.
            Assert.Equal(2, peers[0].Accepts.Select(item => item.Request.Value).Distinct().Count());
            Assert.All(peers[0].Accepts, item => Assert.Equal(Ballot.InitialFast(), item.Request.Ballot));
        }
        release.Release();
        var results = await Task.WhenAll(left, right).WaitAsync(Watchdog);
        var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
        Assert.Equal(Payload, winner.Current!.Value);
        Assert.NotEqual(expected, winner.Current.Version);
        Assert.Equal(update ? 2UL : 1UL, ReadRevision(winner.Current.Version!.Value));
        Assert.Equal(winner.Current, await first.ReadAsync("key"));
        Assert.Equal(winner.Current, await second.ReadAsync("key"));
        var loser = Assert.Single(results, result => result.Status != CompareExchangeStatus.Applied);
        Assert.Contains(loser.Status, new[] { CompareExchangeStatus.Mismatch, CompareExchangeStatus.Unknown });
        Assert.Equal(winner.Current, loser.Current);
    }

    [Theory]
    [InlineData("fast")]
    [InlineData("leader")]
    public async Task QueuedRequestsSharePiggybackButIdleReadsStillContactQuorum(string mode)
    {
        var peers = Peers();
        using var gate = new Gate();
        foreach (var peer in peers)
        {
            peer.Accept = async (key, request, token) =>
            {
                var result = await peer.Inner.AcceptAsync(key, request, token);
                await gate.WaitAsync();
                return result;
            };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options(mode));
        var create = service.CompareExchangeAsync("key", null, Payload).AsTask();
        await gate.Arrived;
        var read = service.ReadAsync("key").AsTask();
        var mismatch = service.CompareExchangeAsync("key", null, ByteString.Empty).AsTask();
        Assert.False(read.IsCompleted);
        var prepares = peers.Sum(peer => peer.Prepares.Count);
        foreach (var peer in peers) peer.Accept = null;
        gate.Release();
        var created = await create.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(created.Current, await read.WaitAsync(Watchdog));
        Assert.Equal(CompareExchangeStatus.Mismatch, (await mismatch.WaitAsync(Watchdog)).Status);
        Assert.Equal(prepares, peers.Sum(peer => peer.Prepares.Count));

        // The engine is idle and cached, but a read must still obtain fresh quorum observations.
        var before = peers.Sum(peer => peer.Prepares.Count);
        var readsBefore = peers.Sum(peer => peer.Reads.Count);
        Assert.Equal(created.Current, await service.ReadAsync("key"));
        Assert.Equal(before, peers.Sum(peer => peer.Prepares.Count));
        Assert.True(peers.Sum(peer => peer.Reads.Count) > readsBefore);
    }

    [Theory]
    [InlineData("fast")]
    [InlineData("leader")]
    public async Task QueuedReadStalePiggybackRecoversCompetingWrite(string mode)
    {
        var peers = Peers();
        using var gate = new Gate();
        foreach (var peer in peers)
        {
            peer.Accept = async (key, request, token) =>
            {
                var result = await peer.Inner.AcceptAsync(key, request, token);
                await gate.WaitAsync();
                return result;
            };
        }
        await using var first = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(mode));
        await using var other = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options());
        var create = first.CompareExchangeAsync("key", null, Payload).AsTask();
        await gate.Arrived;
        var queued = first.ReadAsync("key").AsTask();
        foreach (var peer in peers) peer.Accept = null;
        var version = new ByteString(peers[0].Inner.Snapshot("key")!.AcceptedValue.ToArray().AsSpan(5, 8));
        var competing = await other.CompareExchangeAsync("key", version, ByteString.Empty).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, competing.Status);
        gate.Release();
        Assert.Equal(CompareExchangeStatus.Applied, (await create.WaitAsync(Watchdog)).Status);
        Assert.Equal(competing.Current, await queued.WaitAsync(Watchdog));
        Assert.Equal(competing.Current, await other.ReadAsync("key"));
    }

    [Fact]
    public async Task SharedFastBallotConcurrentSameBytesCasAppliesExactlyOnce()
    {
        var peers = Peers();
        RequireRecovery(peers); // This test deliberately retains shared fast-round protocol history.
        var version = Version(9, 1);
        var initial = Envelope(version, Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(Ballot.InitialFast(), initial, Ballot.Zero))).IsAccepted);

        using var reads = new Gate();
        using var writes = new Gate();
        var readCount = 0;
        var writeTokens = new HashSet<ByteString>();
        var bothReads = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var bothWrites = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        foreach (var peer in peers)
            peer.Accept = async (key, request, token) =>
            {
                if (request.Value.Equals(initial))
                {
                    var reply = await peer.Inner.AcceptAsync(key, request, token);
                    if (Interlocked.Increment(ref readCount) == 6) bothReads.TrySetResult();
                    await reads.WaitAsync();
                    return reply;
                }
                lock (writeTokens)
                {
                    writeTokens.Add(request.Value);
                    if (writeTokens.Count == 2) bothWrites.TrySetResult();
                }
                await writes.WaitAsync();
                return await peer.Inner.AcceptAsync(key, request, token);
            };
        await using var first = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options("fast"));
        await using var second = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options("fast"));
        var firstRead = first.ReadAsync("key").AsTask();
        var secondRead = second.ReadAsync("key").AsTask();
        await bothReads.Task.WaitAsync(Watchdog);
        // Queue both CAS operations while their engines retain the same fast-round history.
        var left = first.CompareExchangeAsync("key", version, Payload).AsTask();
        var right = second.CompareExchangeAsync("key", version, Payload).AsTask();
        reads.Release();
        await bothWrites.Task.WaitAsync(Watchdog);
        var contenders = peers[0].Accepts.Where(item => !item.Request.Value.Equals(initial)).ToArray();
        Assert.Equal(2, contenders.Length);
        Assert.Equal(contenders[0].Request.Ballot, contenders[1].Request.Ballot);
        Assert.True(contenders[0].Request.Ballot.IsFastRoundBallot);
        Assert.NotEqual(contenders[0].Request.Value, contenders[1].Request.Value);
        writes.Release();
        Assert.Equal(new RegisterValue(Payload, version), await firstRead.WaitAsync(Watchdog));
        Assert.Equal(new RegisterValue(Payload, version), await secondRead.WaitAsync(Watchdog));
        var results = await Task.WhenAll(left, right).WaitAsync(Watchdog);
        var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
        var loser = Assert.Single(results, result => result.Status != CompareExchangeStatus.Applied);
        Assert.Equal(winner.Current, loser.Current);
        Assert.NotEqual(version, winner.Current!.Version);
        Assert.Equal(Payload, winner.Current.Value);
        Assert.Equal(winner.Current, await first.ReadAsync("key").AsTask().WaitAsync(Watchdog));
    }

    [Fact]
    public async Task ReadPartialAcceptRecoversHighestEnvelopeWithoutChangingVersion()
    {
        var peers = Peers();
        RequireRecovery(peers); // Test identity recovery, not the legal matching old-quorum observation.
        var old = Envelope(Version(9, 1), ByteString.Empty);
        var latest = Envelope(Version(9, 2), Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new Ballot(2, 9), old, Ballot.Zero))).IsAccepted);
        Assert.True((await peers[0].Inner.AcceptAsync("key", new(new Ballot(3, 9), latest, Ballot.Zero))).IsAccepted);
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        var read = await service.ReadAsync("key").AsTask().WaitAsync(Watchdog);
        Assert.Equal(new RegisterValue(Payload, Version(9, 2)), read);
        Assert.All(peers, peer => Assert.Equal(latest, peer.Inner.Snapshot("key")!.AcceptedValue));
        var stale = await service.CompareExchangeAsync("key", Version(9, 1), ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(read, stale.Current);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAcceptedBallotWithoutBytesIsInvalidEvenInRejection(bool rejected)
    {
        var peers = Peers();
        RequireRecovery(peers);
        foreach (var peer in peers)
            peer.Prepare = (_, _, _) => ValueTask.FromResult(new AcceptorPrepareResult<ByteString?>(
                new Ballot(2, 9), null, rejected ? new Ballot(3, 9) : Ballot.Zero));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ReadAsync("key").AsTask());
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
    }

    [Theory]
    [InlineData("classic", false)]
    [InlineData("classic", true)]
    [InlineData("fast", false)]
    [InlineData("fast", true)]
    [InlineData("leader", false)]
    [InlineData("leader", true)]
    [InlineData("adaptive", false)]
    [InlineData("adaptive", true)]
    public async Task ReadRepeatedStableOrMissingStateDoesNotAllocateOrMutate(string mode, bool present)
    {
        var peers = Peers();
        var value = Envelope(Version(9, 2), Payload);
        if (present)
            foreach (var peer in peers)
                Assert.True((await peer.Inner.AcceptAsync("key",
                    new(mode == "fast" ? new Ballot(3, 0) : new Ballot(3, 9), value, new Ballot(8, 9)))).IsAccepted);
        var before = peers.Select(peer => peer.Inner.Snapshot("key")).ToArray();
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options(mode));

        for (var iteration = 0; iteration < 3; iteration++)
            Assert.Equal(present ? new RegisterValue(Payload, Version(9, 2)) : new(null, null),
                await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));

        Assert.Empty(ballots.Reserved);
        Assert.Equal(before, peers.Select(peer => peer.Inner.Snapshot("key")));
        Assert.All(peers, peer =>
        {
            Assert.Equal(3, peer.Reads.Count);
            Assert.All(peer.Reads, read => { Assert.Equal("key", read.Key); Assert.Null(read.Known); });
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
    }

    [Fact]
    public async Task ReadClassicMatchingMajorityIgnoresNewerMinorityAndDifferentPromises()
    {
        var peers = Peers();
        var old = Envelope(Version(9, 1), ByteString.Empty);
        var partial = Envelope(Version(9, 2), Payload);
        for (var index = 0; index < peers.Length; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(index == 0 ? new Ballot(4, 9) : new Ballot(2, 9), index == 0 ? partial : old,
                    new Ballot(10 + index, 9)))).IsAccepted);
        var before = peers.Select(peer => peer.Inner.Snapshot("key")).ToArray();
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        Assert.Equal(new RegisterValue(ByteString.Empty, Version(9, 1)), await service.ReadAsync("key"));

        Assert.Equal(before, peers.Select(peer => peer.Inner.Snapshot("key")));
        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer => { Assert.Empty(peer.Prepares); Assert.Empty(peer.Accepts); });
    }

    [Theory]
    [InlineData(3, "different-version")]
    [InlineData(3, "different-payload")]
    [InlineData(4, "different-version")]
    [InlineData(4, "different-payload")]
    [InlineData(3, "different-identity")]
    [InlineData(4, "different-identity")]
    public async Task ReadFastQuorumRequiresFourOfFiveMatchingEnvelopes(int matching, string difference)
    {
        var peers = Peers(5);
        var version = difference == "different-identity" ? Revision(1) : Version(9, 1);
        var chosen = difference == "different-identity" ? EnvelopeV2(1, Payload) : Envelope(version, Payload);
        var other = difference switch
        {
            "different-version" => Envelope(Version(9, 2), Payload),
            "different-identity" => EnvelopeV2(1, Payload, sequence: 2),
            _ => Envelope(version, ByteString.Empty)
        };
        for (var index = 0; index < peers.Length; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(new Ballot(3, 0), index < matching ? chosen : other, Ballot.Zero))).IsAccepted);
        var before = peers.Select(peer => peer.Inner.Snapshot("key")).ToArray();
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        Assert.Equal(new RegisterValue(Payload, version), await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));

        if (matching == 4)
        {
            Assert.Empty(ballots.Reserved);
            Assert.Equal(before, peers.Select(peer => peer.Inner.Snapshot("key")));
            Assert.All(peers, peer => { Assert.Empty(peer.Prepares); Assert.Empty(peer.Accepts); });
        }
        else
        {
            Assert.NotEmpty(ballots.Reserved);
            Assert.All(peers, peer =>
            {
                Assert.NotEmpty(peer.Prepares);
                Assert.NotEmpty(peer.Accepts);
                Assert.Equal(chosen, peer.Inner.Snapshot("key")!.AcceptedValue);
                Assert.True(peer.Inner.Snapshot("key")!.AcceptedBallot > new Ballot(3, 0));
            });
        }
    }

    [Fact]
    public async Task ReadMatchingBytesAtDifferentBallotsRequiresRecovery()
    {
        var peers = Peers();
        var value = Envelope(Version(9, 1), Payload);
        for (var index = 0; index < peers.Length; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(new Ballot(2 + index, 9), value, Ballot.Zero))).IsAccepted);
        var ballots = new TestBallots(1);
        // Keep the recovery ballot above every seeded ballot so all synchronous peers can accept.
        ballots.Reserve(10);
        ballots.Reserved.Clear();
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        Assert.Equal(new RegisterValue(Payload, Version(9, 1)), await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));

        Assert.NotEmpty(ballots.Reserved);
        Assert.All(peers, peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            Assert.NotEmpty(peer.Accepts);
            Assert.Equal(value, peer.Inner.Snapshot("key")!.AcceptedValue);
            Assert.True(peer.Inner.Snapshot("key")!.AcceptedBallot > new Ballot(4, 9));
        });
    }

    [Theory]
    [InlineData("default-union")]
    [InlineData("unchanged-without-known")]
    [InlineData("wrong-key")]
    [InlineData("zero-accepted-ballot")]
    [InlineData("negative-accepted-round")]
    [InlineData("negative-accepted-proposer")]
    [InlineData("default-bytes")]
    public async Task ReadInvalidObservationThrowsWithoutMutation(string scenario)
    {
        var peers = Peers();
        AcceptorReadResult result = scenario switch
        {
            "unchanged-without-known" => new AcceptorUnchanged(),
            "wrong-key" => new KeyedAcceptorState("other", new(2, 1), new(2, 1), Envelope(Version(9, 1), Payload)),
            "zero-accepted-ballot" => new KeyedAcceptorState("key", new(2, 1), default, Envelope(Version(9, 1), Payload)),
            "negative-accepted-round" => new KeyedAcceptorState("key", new(2, 1), new(-1, 1), Envelope(Version(9, 1), Payload)),
            "negative-accepted-proposer" => new KeyedAcceptorState("key", new(2, 1), new(2, -1), Envelope(Version(9, 1), Payload)),
            "default-bytes" => new KeyedAcceptorState("key", new(2, 1), new(2, 1), default),
            _ => default
        };
        foreach (var peer in peers) peer.Read = (_, _, _) => ValueTask.FromResult(result);
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        await Assert.ThrowsAsync<InvalidDataException>(() => service.ReadAsync("key").AsTask());

        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer => { Assert.Empty(peer.Prepares); Assert.Empty(peer.Accepts); });
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("bad-header")]
    [InlineData("short-header")]
    [InlineData("bad-tag")]
    [InlineData("absent-trailing-data")]
    [InlineData("short-version")]
    [InlineData("zero-owner")]
    [InlineData("negative-owner")]
    [InlineData("zero-round")]
    [InlineData("negative-round")]
    [InlineData("v2-short-header")]
    [InlineData("v2-short-identity")]
    [InlineData("v2-absent-trailing-data")]
    [InlineData("v2-zero-revision")]
    [InlineData("v2-zero-proposer")]
    [InlineData("v2-negative-proposer")]
    [InlineData("v2-zero-incarnation")]
    [InlineData("v2-negative-incarnation")]
    [InlineData("v2-zero-sequence")]
    [InlineData("v2-negative-sequence")]
    public async Task ReadMalformedEnvelopeThrowsInvalidData(string kind)
    {
        var malformed = kind switch
        {
            "empty" => ByteString.Empty,
            "bad-header" => new ByteString("BAD!\0"u8),
            "short-header" => new ByteString("FCR1"u8),
            "bad-tag" => new ByteString("FCR1\u0002"u8),
            "absent-trailing-data" => new ByteString("FCR1\0x"u8),
            "short-version" => new ByteString("FCR1\u0001"u8),
            "zero-owner" => Envelope(Version(0, 1), Payload),
            "negative-owner" => Envelope(Version(-1, 1), Payload),
            "zero-round" => Envelope(Version(1, 0), Payload),
            "negative-round" => Envelope(Version(1, -1), Payload),
            "v2-short-header" => new ByteString("FCR2"u8),
            "v2-short-identity" => new ByteString(EnvelopeV2(1, ByteString.Empty).ToArray().AsSpan(0, 28)),
            "v2-absent-trailing-data" => new ByteString("FCR2\0x"u8),
            "v2-zero-revision" => EnvelopeV2(0, Payload),
            "v2-zero-proposer" => EnvelopeV2(1, Payload, proposer: 0),
            "v2-negative-proposer" => EnvelopeV2(1, Payload, proposer: -1),
            "v2-zero-incarnation" => EnvelopeV2(1, Payload, incarnation: 0),
            "v2-negative-incarnation" => EnvelopeV2(1, Payload, incarnation: -1),
            "v2-zero-sequence" => EnvelopeV2(1, Payload, sequence: 0),
            "v2-negative-sequence" => EnvelopeV2(1, Payload, sequence: -1),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var peers = Peers();
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("bad", new(new(10, 9), malformed, Ballot.Zero))).IsAccepted);
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        await Assert.ThrowsAsync<InvalidDataException>(() => service.ReadAsync("bad").AsTask());
        Assert.All(peers, peer => Assert.Equal(malformed, peer.Inner.Snapshot("bad")!.AcceptedValue));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
    }
}
