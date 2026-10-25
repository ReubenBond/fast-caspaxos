using System.Collections.Concurrent;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Server.Diagnostics;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class FastHandoffTests
{
    [Theory]
    [InlineData("fast")]
    [InlineData("leader")]
    public async Task ClassicWriterPublishesNoExpiredLearnedPromise(string sourceMode)
    {
        var peers = Peers();
        await using var source = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(sourceMode));
        await using var classic = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options("classic"));
        await using var receiver = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"));
        var commits = new List<RegisterCommit>();
        source.Committed += classic.Learn;
        classic.Committed += receiver.Learn;
        classic.Committed += commits.Add;
        var first = await source.CompareExchangeAsync("key", null, Payload);

        var second = await classic.CompareExchangeAsync("key", first.Current!.Version, ByteString.Empty);

        Assert.Equal(CompareExchangeStatus.Applied, second.Status);
        Assert.Null(second.Error);
        var commit = Assert.Single(commits);
        Assert.Equal(Ballot.Zero, commit.PreparedBallot);
        Assert.Null(classic.LastNotificationFailure);
        var third = await receiver.CompareExchangeAsync("key", Revision(2), Payload);
        Assert.Equal(CompareExchangeStatus.Applied, third.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(3)), third.Current);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ThrowingCommitSubscriberPreservesAppliedOutcomeCacheAndOtherSubscribers(bool disposedLearner)
    {
        var peers = Peers();
        using var journal = new NodeEventJournal("source");
        await using var source = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(1), Options("fast"), diagnostics: journal);
        await using var receiver = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options("fast"));
        await using var stopped = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"));
        await stopped.DisposeAsync();
        var expectedError = new InvalidOperationException("subscriber failure");
        source.Committed += disposedLearner ? stopped.Learn : _ => throw expectedError;
        source.Committed += receiver.Learn;
        var delivered = new List<RegisterCommit>();
        source.Committed += delivered.Add;
        var cursor = journal.Cursor;

        var first = await source.CompareExchangeAsync("key", null, Payload);

        Assert.Equal(CompareExchangeStatus.Applied, first.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), first.Current);
        Assert.Null(first.Error);
        var failure = Assert.IsType<RegisterNotificationFailure>(source.LastNotificationFailure);
        Assert.Equal(Assert.Single(delivered), failure.Commit);
        if (disposedLearner) Assert.IsType<ObjectDisposedException>(failure.Error);
        else Assert.Same(expectedError, failure.Error);
        using var timeout = new CancellationTokenSource(Watchdog);
        await using (var events = journal.ReadAsync(cursor, Watchdog, timeout.Token).GetAsyncEnumerator())
        {
            Assert.True(await events.MoveNextAsync());
            Assert.Equal("notification", events.Current.Kind);
            var observation = Assert.IsType<NotificationObservation>(events.Current.Data);
            Assert.Equal("commit", observation.Operation);
            Assert.Equal("key", observation.Key!.Text);
            Assert.Equal(failure.Error.GetType().Name, observation.Error);
        }

        var before = Counts(peers);
        var second = await source.CompareExchangeAsync("key", Revision(1), ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, second.Status);
        Assert.Null(second.Error);
        AssertOneAcceptRound(peers, before);
        Assert.Equal(2, delivered.Count);
        before = Counts(peers);
        var third = await receiver.CompareExchangeAsync("key", Revision(2), Payload);
        Assert.Equal(CompareExchangeStatus.Applied, third.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(3)), third.Current);
        AssertOneAcceptRound(peers, before);
    }

    [Theory]
    [InlineData("fast")]
    [InlineData("adaptive")]
    public async Task LearnedCommitsAllowColdAndWarmAlternatingNodesOneAcceptRound(string mode)
    {
        var peers = Peers();
        await using var a = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(mode), peers[0].Inner);
        await using var b = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options(mode), peers[1].Inner);
        await using var c = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options(mode), peers[2].Inner);
        ReplicatedRegisterService[] services = [a, b, c];
        var commits = new List<RegisterCommit>();
        foreach (var source in services)
            source.Committed += commit =>
            {
                commits.Add(commit);
                foreach (var target in services)
                    if (target != source) target.Learn(commit);
            };

        ByteString? version = null;
        int[] order = [0, 2, 1, 0, 2];
        for (var step = 0; step < order.Length; step++)
        {
            var selected = order[step];
            var inspections = peers.Select(peer => peer.Inner.Inspections.Count).ToArray();
            var before = Counts(peers);
            var value = new ByteString([(byte)step]);
            var result = await services[selected].CompareExchangeAsync("key", version, value).AsTask().WaitAsync(Watchdog);

            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new RegisterValue(value, Revision((ulong)step + 1)), result.Current);
            AssertOneAcceptRound(peers, before);
            Assert.Equal(inspections[selected] + 1, peers[selected].Inner.Inspections.Count);
            var commit = commits.Last();
            Assert.Equal(step + 1, commits.Count);
            Assert.Equal("key", commit.Key);
            Assert.Equal(new Ballot(step + 1, 0), commit.AcceptedBallot);
            Assert.Equal(new Ballot(step + 2, 0), commit.PreparedBallot);
            Assert.All(peers, peer => Assert.Equal(commit.Value, peer.Inner.Snapshot("key")!.AcceptedValue));
            version = result.Current!.Version;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RawMinorityLoserCannotSeedHigherSuccessorOrApplyStaleCas(bool minorityAbsent)
    {
        var peers = Peers(5);
        var ballot = new Ballot(minorityAbsent ? 1 : 10, 0);
        var nextBallot = ballot.NextRound(0);
        var chosenRevision = minorityAbsent ? 1UL : 2UL;
        var winner = EnvelopeV2(chosenRevision, Payload);
        var loser = minorityAbsent ? Absent : EnvelopeV2(1, ByteString.Empty, proposer: 8);
        for (var index = 0; index < peers.Length; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(ballot, index == 4 ? loser : winner, nextBallot))).IsAccepted);

        // Four votes choose W at b. The local fifth vote for L is not a certificate.
        // In particular it must not produce L-derived b+1 before recovery establishes W.
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(5), Options("fast"), peers[4].Inner);
        var result = await service.CompareExchangeAsync("key", minorityAbsent ? null : Revision(1), new([99]))
            .AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Mismatch, result.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(chosenRevision)), result.Current);
        Assert.All(peers, peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            Assert.True(peer.Prepares.First().Request.Ballot > nextBallot);
            Assert.All(peer.Accepts, entry => Assert.Equal(winner, entry.Request.Value));
            Assert.Equal(winner, peer.Inner.Snapshot("key")!.AcceptedValue);
        });

        var next = await service.CompareExchangeAsync("key", Revision(chosenRevision), new([100])).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, next.Status);
        Assert.Equal(new RegisterValue(new([100]), Revision(chosenRevision + 1)), next.Current);
    }

    [Fact]
    public async Task PromiseOnlyLocalSnapshotSkipsInitialBallotWithoutInventingCommittedValue()
    {
        var peers = Peers();
        var promised = new Ballot(20, 7);
        Assert.True((await peers[2].Inner.PrepareAsync("key", new(promised))).IsPromised);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);

        var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
        var observation = Assert.Single(peers[2].Inner.Inspections).State!;
        Assert.Null(observation.AcceptedValue);
        Assert.Equal(Ballot.Zero, observation.AcceptedBallot);
        Assert.Equal(promised, observation.PromisedBallot);
        Assert.All(peers, peer => Assert.True(peer.Prepares.First().Request.Ballot > promised));
    }

    [Fact]
    public async Task HigherLocalPromiseInvalidatesLearnedPreparedBallot()
    {
        var peers = Peers();
        var value = EnvelopeV2(1, Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new(5, 0), value, new(6, 0)))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);
        service.Learn(new("key", new(5, 0), value, new(6, 0)));
        Assert.True((await peers[2].Inner.PrepareAsync("key", new(new Ballot(10, 2)))).IsPromised);

        var result = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
        Assert.All(peers, peer =>
        {
            Assert.True(peer.Prepares.First().Request.Ballot > new Ballot(10, 2));
            Assert.DoesNotContain(peer.Accepts, entry => entry.Request.Ballot == new Ballot(6, 0));
        });
    }

    [Theory]
    [InlineData("classic")]
    [InlineData("leader")]
    public async Task LearnedHandoffDoesNotReuseAnotherProposersOwnedBallot(string mode)
    {
        var peers = Peers();
        await using var a = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(mode), peers[0].Inner);
        await using var c = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options(mode), peers[2].Inner);
        a.Committed += c.Learn;
        var created = await a.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        var before = Counts(peers);

        var updated = await c.CompareExchangeAsync("key", created.Current!.Version, ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), updated.Current);
        for (var index = 0; index < peers.Length; index++)
        {
            Assert.True(peers[index].Prepares.Count > before[index].Prepares);
            Assert.Equal(3, peers[index].Prepares.Last().Request.Ballot.Proposer);
            Assert.Equal(3, peers[index].Accepts.Last().Request.Ballot.Proposer);
        }
    }

    [Theory]
    [InlineData(3, true)]
    [InlineData(2, false)]
    public async Task QuorumReadCertifiesNextFastBallotOnlyWithEnoughMatchingPromises(int matchingPromises, bool canSkipPrepare)
    {
        var peers = Peers();
        var value = EnvelopeV2(1, Payload);
        for (var index = 0; index < peers.Length; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(new(5, 0), value, index < matchingPromises ? new(6, 0) : Ballot.Zero))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);

        Assert.Equal(new RegisterValue(Payload, Revision(1)), await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        var before = Counts(peers);
        var result = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
        if (canSkipPrepare) AssertOneAcceptRound(peers, before);
        else Assert.All(peers, peer => Assert.NotEmpty(peer.Prepares));
    }

    [Fact]
    public async Task DuplicateAndOlderLearnedCommitsCannotRollBackLatestCertificate()
    {
        var peers = Peers();
        var old = new RegisterCommit("key", new(5, 0), EnvelopeV2(1, ByteString.Empty), new(6, 0));
        var latest = new RegisterCommit("key", new(8, 0), EnvelopeV2(2, Payload), new(9, 0));
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key",
                new(latest.AcceptedBallot, latest.Value, latest.PreparedBallot))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);
        service.Learn(latest);
        service.Learn(old);
        service.Learn(latest);
        var before = Counts(peers);

        var result = await service.CompareExchangeAsync("key", Revision(2), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(3)), result.Current);
        AssertOneAcceptRound(peers, before);
        Assert.All(peers, peer => Assert.Equal(new Ballot(9, 0), peer.Accepts.Last().Request.Ballot));
    }

    [Fact]
    public async Task CommitNotificationDoesNotAdvertiseMerelyRequestedFastPromise()
    {
        var peers = Peers();
        // Classic recovery completes at two votes; that is insufficient to certify
        // a requested shared fast successor in a three-member configuration.
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"));
        var notifications = new ConcurrentQueue<RegisterCommit>();
        service.Committed += notifications.Enqueue;
        using var delayed = new Gate();
        peers[2].Accept = async (key, request, token) =>
        {
            await delayed.WaitAsync();
            return await peers[2].Inner.AcceptAsync(key, request, token);
        };
        foreach (var peer in peers)
            Assert.True((await peer.Inner.PrepareAsync("key", new(new Ballot(10, 1)))).IsPromised);

        var write = service.CompareExchangeAsync("key", null, Payload).AsTask();
        try
        {
            await delayed.Arrived;
            var result = await write.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            var commit = Assert.Single(notifications);
            Assert.True(commit.AcceptedBallot.IsClassicRoundBallot);
            Assert.Equal(Ballot.Zero, commit.PreparedBallot);
        }
        finally
        {
            delayed.Release();
            await write.WaitAsync(Watchdog);
        }
    }

    [Fact]
    public async Task LearnWhileAcceptIsActiveSurvivesCompletionAndIdleCachePressure()
    {
        var peers = Peers();
        var initial = new RegisterCommit("key", new(5, 0), EnvelopeV2(1, Payload), new(6, 0));
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key",
                new(initial.AcceptedBallot, initial.Value, initial.PreparedBallot))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast", capacity: 1), peers[2].Inner);
        service.Learn(initial);
        using var a = new Gate();
        using var b = new Gate();
        using var c = new Gate();
        Gate[] gates = [a, b, c];
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            var gate = gates[index];
            peer.Accept = async (key, request, token) =>
            {
                var result = await peer.Inner.AcceptAsync(key, request, token);
                await gate.WaitAsync();
                return result;
            };
        }

        var active = service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask();
        try
        {
            await Task.WhenAll(gates.Select(gate => gate.Arrived)).WaitAsync(Watchdog);
            // The first operation has already been chosen, but its replies are delayed.
            // A newer operation can legitimately complete elsewhere in this interval.
            var later = new RegisterCommit("key", new(10, 0), EnvelopeV2(3, new([30])), new(11, 0));
            foreach (var peer in peers)
                Assert.True((await peer.Inner.AcceptAsync("key",
                    new(later.AcceptedBallot, later.Value, later.PreparedBallot))).IsAccepted);
            service.Learn(later);
            service.Learn(new("idle-a", new(20, 0), EnvelopeV2(1, Payload), new(21, 0)));
            service.Learn(new("idle-b", new(20, 0), EnvelopeV2(1, Payload), new(21, 0)));
            Assert.False(active.IsCompleted);
        }
        finally
        {
            foreach (var gate in gates) gate.Release();
        }
        var result = await active.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
        foreach (var peer in peers) peer.Accept = null;
        var before = Counts(peers);

        var next = await service.CompareExchangeAsync("key", Revision(3), new([40])).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, next.Status);
        Assert.Equal(new RegisterValue(new([40]), Revision(4)), next.Current);
        AssertOneAcceptRound(peers, before);
        Assert.All(peers, peer => Assert.Equal(new Ballot(11, 0), peer.Accepts.Last().Request.Ballot));
    }

    [Fact]
    public async Task LearnedIdleKeysUseExistingCapacityAndEvictedKeyRecoversSafely()
    {
        var peers = Peers();
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast", capacity: 1), peers[2].Inner);
        foreach (var key in new[] { "old", "recent" })
        {
            var commit = new RegisterCommit(key, new(5, 0), EnvelopeV2(1, Payload), new(6, 0));
            foreach (var peer in peers)
                Assert.True((await peer.Inner.AcceptAsync(key,
                    new(commit.AcceptedBallot, commit.Value, commit.PreparedBallot))).IsAccepted);
            service.Learn(commit);
        }
        var before = Counts(peers);
        var recent = await service.CompareExchangeAsync("recent", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, recent.Status);
        AssertOneAcceptRound(peers, before);

        var old = await service.CompareExchangeAsync("old", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, old.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), old.Current);
        Assert.All(peers, peer => Assert.Contains(peer.Prepares, entry => entry.Key == "old"));
    }

    [Fact]
    public async Task IdentityRecoveryPublishesChosenValueWithoutRevisionIncrement()
    {
        var peers = Peers();
        var value = EnvelopeV2(2, Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new(5, 0), value, new(6, 0)))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);
        var commits = new ConcurrentQueue<RegisterCommit>();
        service.Committed += commits.Enqueue;

        var mismatch = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(2)), mismatch.Current);
        var commit = Assert.Single(commits);
        Assert.Equal(value, commit.Value);
        Assert.True(commit.AcceptedBallot > new Ballot(6, 0));
        Assert.All(peers, peer => Assert.Equal(commit.Value, peer.Accepts.Last().Request.Value));
    }

    [Fact]
    public async Task ConflictingLearnAtSameBallotIsRejectedWithoutReplacingPendingCertificate()
    {
        var peers = Peers();
        var valid = new RegisterCommit("key", new(5, 0), EnvelopeV2(1, Payload), new(6, 0));
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key",
                new(valid.AcceptedBallot, valid.Value, valid.PreparedBallot))).IsAccepted);
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast"), peers[2].Inner);
        service.Learn(valid);

        Assert.Throws<InvalidDataException>(() => service.Learn(valid with { Value = EnvelopeV2(1, ByteString.Empty) }));
        var before = Counts(peers);
        var result = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
        AssertOneAcceptRound(peers, before);
    }

    [Fact]
    public Task SharedAllocatorLearnDoesNotTransferClassicPreparedBallotOwnership() =>
        AssertSharedAllocatorOwnershipAsync(learnThroughRead: false);

    [Fact]
    public Task SharedAllocatorReadDoesNotTransferClassicPreparedBallotOwnership() =>
        AssertSharedAllocatorOwnershipAsync(learnThroughRead: true);

    private static async Task AssertSharedAllocatorOwnershipAsync(bool learnThroughRead)
    {
        var peers = Peers();
        using var ballots = new TestBallots(7);
        await using var owner = new ReplicatedRegisterService(Routes(peers), ballots, Options("leader"), peers[0].Inner);
        await using var other = new ReplicatedRegisterService(Routes(peers), ballots, Options("leader"), peers[2].Inner);
        var commits = new List<RegisterCommit>();
        owner.Committed += commits.Add;
        var created = await owner.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        var first = Assert.Single(commits);
        Assert.False(first.PreparedBallot.IsZero);
        Assert.Equal(ballots.ProposerId, first.PreparedBallot.Proposer);

        // Reobserving a commit must not discard the same engine's own allocated promise.
        if (learnThroughRead)
            Assert.Equal(created.Current, await owner.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        else
            owner.Learn(first);
        var before = Counts(peers);
        var warm = await owner.CompareExchangeAsync("key", created.Current!.Version, ByteString.Empty)
            .AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, warm.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), warm.Current);
        AssertOneAcceptRound(peers, before);
        Assert.All(peers, peer => Assert.Equal(first.PreparedBallot, peer.Accepts.Last().Request.Ballot));

        var latest = commits.Last();
        Assert.False(latest.PreparedBallot.IsZero);
        Assert.Equal(ballots.ProposerId, latest.PreparedBallot.Proposer);
        before = Counts(peers);
        if (learnThroughRead)
            Assert.Equal(warm.Current, await other.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        else
            other.Learn(latest);
        for (var index = 0; index < peers.Length; index++)
        {
            Assert.Equal(before[index].Prepares, peers[index].Prepares.Count);
            Assert.Equal(before[index].Accepts, peers[index].Accepts.Count);
        }

        // Sharing the numeric proposer ID and allocator does not share an engine's
        // reservation. Reusing it here would allow two proposal values at one classic ballot.
        before = Counts(peers);
        var reservations = ballots.Reserved.Count;
        var updated = await other.CompareExchangeAsync("key", warm.Current!.Version, new([30]))
            .AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.Equal(new RegisterValue(new([30]), Revision(3)), updated.Current);
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            Assert.Equal(before[index].Reads, peer.Reads.Count);
            Assert.Equal(before[index].Prepares + 1, peer.Prepares.Count);
            Assert.Equal(before[index].Accepts + 1, peer.Accepts.Count);
            var prepared = peer.Prepares.Last().Request.Ballot;
            Assert.Equal(ballots.ProposerId, prepared.Proposer);
            Assert.True(prepared > latest.PreparedBallot);
            Assert.Contains(prepared, ballots.Reserved.Skip(reservations));
            Assert.Equal(prepared, peer.Accepts.Last().Request.Ballot);
        }
    }

    private static (int Reads, int Prepares, int Accepts)[] Counts(ControlledPeer[] peers) =>
        peers.Select(peer => (peer.Reads.Count, peer.Prepares.Count, peer.Accepts.Count)).ToArray();

    private static void AssertOneAcceptRound(ControlledPeer[] peers, (int Reads, int Prepares, int Accepts)[] before)
    {
        for (var index = 0; index < peers.Length; index++)
        {
            Assert.Equal(before[index].Reads, peers[index].Reads.Count);
            Assert.Equal(before[index].Prepares, peers[index].Prepares.Count);
            Assert.Equal(before[index].Accepts + 1, peers[index].Accepts.Count);
        }
    }
}
