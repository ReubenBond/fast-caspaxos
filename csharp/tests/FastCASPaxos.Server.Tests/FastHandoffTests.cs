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
    public async Task ClassicAcceptedQuorumWithoutFastPromiseQuorumReadsThenPreparesWithoutLocalInspection()
    {
        var peers = Peers();
        var accepted = new Ballot(5, 7);
        var requestedFastPromise = new Ballot(6, 0);
        var committed = EnvelopeV2(1, Payload);
        // Two matching classic accepts establish the value, but two promises do
        // not establish a shared fast successor in a three-member configuration.
        for (var index = 0; index < 2; index++)
            Assert.True((await peers[index].Inner.AcceptAsync("key",
                new(accepted, committed, requestedFastPromise))).IsAccepted);
        Assert.Null(peers[2].Inner.Snapshot("key"));
        // Intentionally omit localAcceptor: a local promise floor would force
        // recovery independently and could conceal an incorrectly certified fast promise.
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"));

        var read = await service.ReadAsync("key").AsTask().WaitAsync(Watchdog);

        Assert.Equal(new RegisterValue(Payload, Revision(1)), read);
        Assert.All(peers, peer =>
        {
            Assert.Single(peer.Reads);
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
            Assert.Empty(peer.Inner.Inspections);
        });
        Assert.Null(peers[2].Inner.Snapshot("key"));
        var before = Counts(peers);

        var write = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, write.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), write.Current);
        Assert.Null(write.Error);
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            Assert.Equal(before[index].Reads, peer.Reads.Count);
            Assert.Equal(before[index].Prepares + 1, peer.Prepares.Count);
            Assert.Equal(before[index].Accepts + 1, peer.Accepts.Count);
            var prepared = peer.Prepares.Last().Request.Ballot;
            Assert.Equal(requestedFastPromise, prepared);
            Assert.Equal(prepared, peer.Accepts.Last().Request.Ballot);
            Assert.Equal(peer.Accepts.Last().Request.Value, peer.Inner.Snapshot("key")!.AcceptedValue);
            Assert.Empty(peer.Inner.Inspections);
        }
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

    public enum InspectionFailure { Io, DerivedIo, Database, Timeout, Busy, Disposed, ProviderCancellation }

    [Theory]
    [InlineData(InspectionFailure.Io)]
    [InlineData(InspectionFailure.DerivedIo)]
    [InlineData(InspectionFailure.Database)]
    [InlineData(InspectionFailure.Timeout)]
    [InlineData(InspectionFailure.Busy)]
    [InlineData(InspectionFailure.Disposed)]
    [InlineData(InspectionFailure.ProviderCancellation)]
    public async Task OrdinaryBallotInspectionFailureUsesHealthyRemoteClassicQuorum(InspectionFailure scenario)
    {
        Exception error = scenario switch
        {
            InspectionFailure.Io => new IOException("local read unavailable"),
            InspectionFailure.DerivedIo => new EndOfStreamException("local stream ended"),
            InspectionFailure.Database => new InspectionDbException(),
            InspectionFailure.Timeout => new TimeoutException(),
            InspectionFailure.Busy => new RegisterBusyException(),
            InspectionFailure.Disposed => new ObjectDisposedException("local inspection"),
            _ => new OperationCanceledException(),
        };
        var peers = Peers();
        peers[0].GoOffline();
        var inspection = new BallotInspector((_, _) => ValueTask.FromException<LocalAcceptorBallots?>(error));
        using var journal = new NodeEventJournal("source");
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"), inspection, journal);
        var cursor = journal.Cursor;
        var previousContext = DiagnosticContext.Current;
        DiagnosticContext.Current = new("source", "inspection-operation", "inspection-call");
        try
        {
            var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
            Assert.Null(result.Error);
            Assert.Same(error, service.LastInspectionFailure);
            Assert.Equal(1, inspection.Calls);
            foreach (var peer in peers)
            {
                var prepare = Assert.Single(peer.Prepares).Request.Ballot;
                Assert.True(prepare.IsClassicRoundBallot);
                Assert.Equal(3, prepare.Proposer);
                Assert.Equal(prepare, Assert.Single(peer.Accepts).Request.Ballot);
            }
            using var timeout = new CancellationTokenSource(Watchdog);
            await using var events = journal.ReadAsync(cursor, Watchdog, timeout.Token).GetAsyncEnumerator();
            Assert.True(await events.MoveNextAsync());
            Assert.Equal("inspection", events.Current.Kind);
            var observed = Assert.IsType<InspectionObservation>(events.Current.Data);
            Assert.Equal("ballots", observed.Operation);
            Assert.Equal("key", observed.Key!.Text);
            Assert.Equal("inspection-operation", observed.OperationId);
            Assert.Equal(error.GetType().Name, observed.Error);
        }
        finally { DiagnosticContext.Current = previousContext; }
    }

    [Fact]
    public async Task LocalBallotInspectionUsesOnlyBallotsAndPreservesPromiseFloor()
    {
        var peers = Peers();
        var floor = new Ballot(20, 7);
        var inspection = new BallotInspector((_, _) => ValueTask.FromResult<LocalAcceptorBallots?>(new(floor, Ballot.Zero)));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"), inspection);
        var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
        Assert.Equal(1, inspection.Calls);
        Assert.Null(service.LastInspectionFailure);
        Assert.All(peers, peer =>
        {
            var prepared = Assert.Single(peer.Prepares).Request.Ballot;
            Assert.True(prepared > floor);
            Assert.Equal(new Ballot(21, 0), prepared);
            Assert.Equal(prepared, Assert.Single(peer.Accepts).Request.Ballot);
        });
    }

    public enum CorruptBallots { ProviderError, ZeroPromise, NegativePromise, NegativeProposer, MalformedAcceptedZero, NegativeAccepted }

    [Theory]
    [InlineData(CorruptBallots.ProviderError)]
    [InlineData(CorruptBallots.ZeroPromise)]
    [InlineData(CorruptBallots.NegativePromise)]
    [InlineData(CorruptBallots.NegativeProposer)]
    [InlineData(CorruptBallots.MalformedAcceptedZero)]
    [InlineData(CorruptBallots.NegativeAccepted)]
    public async Task CorruptBallotInspectionFailsWithoutProtocolFallback(CorruptBallots scenario)
    {
        var peers = Peers();
        var inspection = new BallotInspector((_, _) => scenario switch
        {
            CorruptBallots.ProviderError => ValueTask.FromException<LocalAcceptorBallots?>(new InvalidDataException("corrupt local store")),
            CorruptBallots.ZeroPromise => ValueTask.FromResult<LocalAcceptorBallots?>(new(Ballot.Zero, Ballot.Zero)),
            CorruptBallots.NegativePromise => ValueTask.FromResult<LocalAcceptorBallots?>(new(new(-1, 1), Ballot.Zero)),
            CorruptBallots.NegativeProposer => ValueTask.FromResult<LocalAcceptorBallots?>(new(new(1, -1), Ballot.Zero)),
            CorruptBallots.MalformedAcceptedZero => ValueTask.FromResult<LocalAcceptorBallots?>(new(new(1, 1), new(0, 1))),
            _ => ValueTask.FromResult<LocalAcceptorBallots?>(new(new(2, 1), new(-1, 1))),
        });
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"), inspection);
        await Assert.ThrowsAsync<InvalidDataException>(() => service.CompareExchangeAsync("key", null, Payload).AsTask());
        Assert.Null(service.LastInspectionFailure);
        Assert.All(peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
            Assert.Null(peer.Inner.Snapshot("key"));
        });
    }

    [Fact]
    public async Task BallotInspectionAcceptsIndependentAcceptedFloorAboveEarlierPromise()
    {
        var peers = Peers();
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new(5, 0), EnvelopeV2(1, Payload), Ballot.Zero))).IsAccepted);
        var state = await peers[0].Inner.InspectBallotsAsync("key");
        Assert.Equal(Ballot.InitialFast(), state!.PromisedBallot);
        Assert.Equal(new Ballot(5, 0), state.AcceptedBallot);
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"), peers[0].Inner);

        var result = await service.CompareExchangeAsync("key", Revision(1), ByteString.Empty);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
        Assert.Null(service.LastInspectionFailure);
        Assert.All(peers, peer => Assert.True(Assert.Single(peer.Prepares).Request.Ballot > new Ballot(5, 0)));
    }

    [Fact]
    public async Task BallotInspectionBudgetExpiresWithoutAwaitingIgnoringProvider()
    {
        var peers = Peers();
        var clock = new ManualTime();
        var underlying = new TaskCompletionSource<LocalAcceptorBallots?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var inspection = new BallotInspector((_, _) => new(underlying.Task));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast", clock), inspection);
        var writing = service.CompareExchangeAsync("key", null, Payload).AsTask();
        try
        {
            await clock.WaitForDelayAsync(TimeSpan.FromSeconds(1));
            clock.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromTicks(1));
            Assert.False(writing.IsCompleted);
            Assert.All(peers, peer => Assert.Empty(peer.Accepts));
            clock.Advance(TimeSpan.FromTicks(1));
            var result = await writing.WaitAsync(Watchdog);
            Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
            Assert.IsType<TimeoutException>(service.LastInspectionFailure);
            Assert.False(underlying.Task.IsCompleted);
            Assert.All(peers, peer =>
            {
                var prepared = Assert.Single(peer.Prepares).Request.Ballot;
                Assert.True(prepared.IsClassicRoundBallot);
                Assert.Equal(prepared, Assert.Single(peer.Accepts).Request.Ballot);
            });
        }
        finally { underlying.TrySetResult(new(new(99, 7), Ballot.Zero)); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancelledOrExpiredOperationCannotResumeAfterIgnoringBallotInspection(bool deadline)
    {
        var peers = Peers();
        var clock = new ManualTime();
        var underlying = new TaskCompletionSource<LocalAcceptorBallots?>();
        using var cancellation = new CancellationTokenSource();
        var inspection = new BallotInspector((_, _) => new(underlying.Task));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast", clock), inspection);
        var writing = service.CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();
        try
        {
            await clock.WaitForDelayAsync(TimeSpan.FromSeconds(1));
            if (deadline)
            {
                clock.Advance(TimeSpan.FromSeconds(5));
                await Assert.ThrowsAsync<RegisterUnavailableException>(() => writing.WaitAsync(Watchdog));
            }
            else
            {
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writing.WaitAsync(Watchdog));
            }
            Assert.False(underlying.Task.IsCompleted);
            underlying.SetResult(new(new(99, 7), Ballot.Zero));
            Assert.Null(service.LastInspectionFailure);
            Assert.All(peers, peer =>
            {
                Assert.Empty(peer.Prepares);
                Assert.Empty(peer.Accepts);
                Assert.Null(peer.Inner.Snapshot("key"));
            });
        }
        finally { underlying.TrySetResult(null); }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task InspectionFailureRacingCancellationPreservesCancellationContract(bool deadline, bool timeoutFailure)
    {
        var peers = Peers();
        var clock = new ManualTime();
        using var cancellation = new CancellationTokenSource();
        var inspection = new BallotInspector((_, _) =>
        {
            if (deadline) clock.Advance(TimeSpan.FromSeconds(5));
            else cancellation.Cancel();
            return ValueTask.FromException<LocalAcceptorBallots?>(timeoutFailure
                ? new TimeoutException("inspection timeout") : new IOException("inspection failure"));
        });
        await using var service = new ReplicatedRegisterService(
            Routes(peers), new TestBallots(3), Options("fast", clock), inspection);

        var writing = service.CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();

        if (deadline) await Assert.ThrowsAsync<RegisterUnavailableException>(() => writing.WaitAsync(Watchdog));
        else
        {
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writing.WaitAsync(Watchdog));
            Assert.Equal(cancellation.Token, error.CancellationToken);
        }
        Assert.Equal(1, inspection.Calls);
        Assert.Null(service.LastInspectionFailure);
        Assert.All(peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
            Assert.Null(peer.Inner.Snapshot("key"));
        });
    }

    private sealed class BallotInspector(Func<string, CancellationToken, ValueTask<LocalAcceptorBallots?>> read) : ILocalAcceptorInspection
    {
        public int Calls { get; private set; }
        public ValueTask<LocalAcceptorBallots?> InspectBallotsAsync(string key, CancellationToken cancellationToken = default)
        {
            Calls++;
            return read(key, cancellationToken);
        }
        public ValueTask<LocalAcceptorState?> InspectAsync(string key, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Preflight must not fetch accepted payloads.");
        public ValueTask<LocalAcceptorPage> InspectKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Preflight must not enumerate local state.");
    }

    private sealed class InspectionDbException() : System.Data.Common.DbException("local database unavailable");

    public enum UnconfirmedAck { Withheld, Rejected, Failed }

    [Theory]
    [InlineData(UnconfirmedAck.Withheld)]
    [InlineData(UnconfirmedAck.Rejected)]
    [InlineData(UnconfirmedAck.Failed)]
    public async Task CommitAcknowledgementsExcludeDispatchedButUnconfirmedPeers(UnconfirmedAck scenario)
    {
        var peers = Peers();
        using var held = new Gate();
        peers[2].Accept = async (key, request, token) =>
        {
            if (scenario == UnconfirmedAck.Rejected)
                return FastCASPaxos.Protocol.AcceptorAcceptResult.Rejected(new(99, 7));
            if (scenario == UnconfirmedAck.Failed) throw new IOException("reply unavailable");
            var accepted = await peers[2].Inner.AcceptAsync(key, request, token);
            await held.WaitAsync();
            return accepted;
        };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("classic"));
        var commits = new ConcurrentQueue<RegisterCommit>();
        service.Committed += commits.Enqueue;
        try
        {
            var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            var commit = Assert.Single(commits);
            Assert.Equal(["peer-0", "peer-1"], commit.AcknowledgedPeers.ToArray());
            Assert.Single(peers[2].Accepts); // A send is not an acknowledgement.
            foreach (var peer in peers.Take(2))
            {
                Assert.Equal(commit.AcceptedBallot, peer.Accepts.Last().Request.Ballot);
                Assert.Equal(commit.Value, peer.Accepts.Last().Request.Value);
            }
            if (scenario == UnconfirmedAck.Withheld)
            {
                await held.Arrived;
                Assert.Equal(commit.Value, peers[2].Inner.Snapshot("key")!.AcceptedValue);
                Assert.False(held.IsReleased); // Even durable bytes do not imply a received ACK.
            }
        }
        finally { held.Release(); }
    }

    [Fact]
    public async Task CommitAcknowledgementsBelongOnlyToFinalAcceptedBallotAndValue()
    {
        var peers = Peers();
        var winner = EnvelopeV2(1, Payload);
        foreach (var peer in peers.Skip(1))
            Assert.True((await peer.Inner.AcceptAsync("key", new(new(10, 7), winner, new(11, 7)))).IsAccepted);
        using var held = new Gate();
        peers[0].Accept = async (key, request, token) =>
        {
            var result = await peers[0].Inner.AcceptAsync(key, request, token);
            if (request.Ballot.IsClassicRoundBallot) await held.WaitAsync();
            return result;
        };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(3), Options("fast"));
        var commits = new ConcurrentQueue<RegisterCommit>();
        service.Committed += commits.Enqueue;
        try
        {
            var result = await service.CompareExchangeAsync("key", null, ByteString.Empty).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Mismatch, result.Status);
            Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
            var commit = Assert.Single(commits);
            Assert.True(commit.AcceptedBallot.IsClassicRoundBallot);
            Assert.Equal(winner, commit.Value);
            Assert.Equal(Ballot.Zero, commit.PreparedBallot); // Two ACKs cannot certify the requested fast successor.
            Assert.Equal(["peer-1", "peer-2"], commit.AcknowledgedPeers.ToArray());
            Assert.NotEqual(winner, peers[0].Accepts.First().Request.Value);
            Assert.True(peers[0].Accepts.First().Request.Ballot.IsFastRoundBallot);
            await held.Arrived;
            Assert.False(held.IsReleased);
        }
        finally { held.Release(); }
    }

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
