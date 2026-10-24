using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class DurabilityRepairTests
{
    [Fact]
    public async Task RunPassLocalMemberIsNotFirstPrefersLocalFetchWithoutTrustingItsValue()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create(local: 2);
        await cluster.SeedAsync(0, "key", Payload, new(1, 2));
        await cluster.SeedAsync(2, "key", ByteString.Empty, new(1, 1));

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal(1, result.Repaired);
        Assert.Single(cluster.Digests[2].Fetches);
        Assert.Empty(cluster.Digests[0].Fetches);
        Assert.Empty(cluster.Digests[1].Fetches);
        cluster.AssertValue("key", Payload);
    }

    [Fact]
    public async Task RunPassManyUnavailableKeysBoundsDiagnosticsWithoutChainingPriorDeadlines()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        for (var index = 0; index < 6; index++) await cluster.SeedAsync(0, $"key-{index}", Payload);
        cluster.Offline(1);
        cluster.Offline(2);
        var pass = repair.RunPassAsync(departing: true);
        for (var index = 0; index < 6; index++)
        {
            await cluster.Clock.WaitForDelayAsync(RepairTestCluster.UnavailableRetryDelay);
            cluster.Clock.Advance(TimeSpan.FromSeconds(10));
        }
        var result = await pass.WaitAsync(Watchdog);

        Assert.Equal((6L, 0L, 6L), (result.Examined, result.Repaired, result.Pending));
        Assert.InRange(result.Errors.Length, 1, 4); // At most one error per member plus the last key deadline.
        var deadline = Assert.Single(result.Errors.OfType<RegisterUnavailableException>());
        var causes = Assert.IsType<AggregateException>(deadline.InnerException);
        Assert.Equal(2, causes.InnerExceptions.Count);
        Assert.All(causes.InnerExceptions, error => Assert.IsType<IOException>(error));
        Assert.All(cluster.Peers, peer => Assert.Empty(peer.Accepts));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunPassResponsiveLaggerHasHugePromiseJumpsAboveFloorAndRepairsIt(bool acceptRace)
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        var floor = new Ballot(1_000_000, 19);
        if (!acceptRace)
            await cluster.Peers[2].Inner.PrepareAsync("key", new(floor));
        else
            cluster.Peers[2].Accept = async (key, request, token) =>
            {
                await cluster.Peers[2].Inner.PrepareAsync(key, new(floor), token);
                return await cluster.Peers[2].Inner.AcceptAsync(key, request, token);
            };

        var pass = repair.RunPassAsync();
        await cluster.Clock.WaitForDelayAsync(RepairTestCluster.RetryDelay);
        Assert.False(pass.IsCompleted);
        Assert.Null(cluster.Peers[2].Inner.Snapshot("key"));
        cluster.Clock.Advance(RepairTestCluster.RetryDelay);
        var result = await pass.WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        cluster.AssertValue("key", Payload);
        Assert.Equal(2, cluster.Ballots.Reserved.Count);
        Assert.True(cluster.Peers[2].Inner.Snapshot("key")!.AcceptedBallot > floor);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunPassDivergentAndMissingReplicasRecoversNewestValueThroughClassicProposal(bool emptyValue)
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        var winner = emptyValue ? ByteString.Empty : new ByteString([40, 50, 60]);
        await cluster.SeedAsync(0, "key", Payload);
        await cluster.SeedAsync(1, "key", winner, new(1, 2));

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.Empty(result.Errors);
        cluster.AssertValue("key", winner);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            var accept = Assert.Single(peer.Accepts).Request;
            Assert.True(accept.Ballot.IsClassicRoundBallot);
            Assert.Equal(cluster.Ballots.ProposerId, accept.Ballot.Proposer);
            Assert.Equal(winner, accept.Value);
            Assert.True(accept.Ballot > new Ballot(1, 2));
        });
    }

    [Fact]
    public async Task RunPassLocalFastLoserPreservesFastQuorumWinner()
    {
        var cluster = new RepairTestCluster(5);
        var repair = cluster.Create();
        var winner = new ByteString([70, 80]);
        await cluster.SeedAsync(0, "key", Payload, Ballot.InitialFast());
        for (var index = 1; index < 5; index++)
            await cluster.SeedAsync(index, "key", winner, Ballot.InitialFast());

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.Single(cluster.Digests[0].Fetches); // The fetched seed is deliberately the losing value.
        cluster.AssertValue("key", winner);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            Assert.All(peer.Accepts, item =>
            {
                Assert.True(item.Request.Ballot.IsClassicRoundBallot);
                Assert.Equal(winner, item.Request.Value);
            });
        });
    }

    [Fact]
    public async Task RunPassSplitUnchosenFastRoundConvergesThroughRecovery()
    {
        var cluster = new RepairTestCluster(5);
        var repair = cluster.Create();
        var other = new ByteString([99]);
        for (var index = 0; index < 5; index++)
            await cluster.SeedAsync(index, "key", index < 3 ? Payload : other, Ballot.InitialFast());

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal(1, result.Repaired);
        Assert.Equal(0, result.Pending);
        var chosen = cluster.Peers[0].Inner.Snapshot("key")!.AcceptedValue;
        Assert.Contains(chosen, new[] { Payload, other }); // Neither had the required four fast votes.
        cluster.AssertValue("key", chosen);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            Assert.NotEmpty(peer.Accepts);
            Assert.All(peer.Accepts, item => Assert.True(item.Request.Ballot.IsClassicRoundBallot));
        });
    }

    [Fact]
    public async Task RunPassHigherWriteBetweenPrepareAndAcceptPreservesNewerValue()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        using var accepts = new Gate();
        foreach (var peer in cluster.Peers)
            peer.Accept = async (key, request, token) =>
            {
                await accepts.WaitAsync();
                return await peer.Inner.AcceptAsync(key, request, token);
            };

        var pass = repair.RunPassAsync();
        await accepts.Arrived;
        Assert.All(cluster.Peers, peer => Assert.Single(peer.Prepares));
        var higher = new Ballot(50, 19);
        var winner = new ByteString([100, 101]);
        foreach (var peer in cluster.Peers)
        {
            Assert.True((await peer.Inner.PrepareAsync("key", new(higher))).IsPromised);
            Assert.True((await peer.Inner.AcceptAsync("key", new(higher, winner, Ballot.Zero))).IsAccepted);
        }
        accepts.Release();
        await cluster.Clock.WaitForDelayAsync(RepairTestCluster.RetryDelay);
        cluster.AssertValue("key", winner); // Safety before allowing the recovery retry.
        cluster.Clock.Advance(RepairTestCluster.RetryDelay);
        var result = await pass.WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        cluster.AssertValue("key", winner);
        Assert.All(cluster.Peers, peer =>
        {
            var requests = peer.Accepts.ToArray();
            Assert.Equal(2, requests.Length);
            Assert.True(requests[0].Request.Ballot < higher);
            Assert.True(requests[1].Request.Ballot > higher);
            Assert.Equal(winner, requests[1].Request.Value);
        });
    }

    [Theory]
    [InlineData(false, 3)]
    [InlineData(true, 4)]
    public async Task RunPassMatchingReachableCommitQuorumDoesNotFetchOrAllocateBallots(bool fast, int reachable)
    {
        var cluster = new RepairTestCluster(5);
        var repair = cluster.Create();
        for (var index = 0; index < reachable; index++)
            await cluster.SeedAsync(index, "key", Payload, fast ? Ballot.InitialFast() : new Ballot(1, 1));
        for (var index = reachable; index < 5; index++) cluster.Offline(index);

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((1L, 0L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Digests, digest => Assert.Empty(digest.Fetches));
        Assert.All(cluster.Peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
    }

    [Fact]
    public async Task RunPassOnlyClassicMajorityMatchesFastBallotStillRunsRecovery()
    {
        var cluster = new RepairTestCluster(5);
        var repair = cluster.Create();
        for (var index = 0; index < 3; index++)
            await cluster.SeedAsync(index, "key", Payload, Ballot.InitialFast());
        cluster.Offline(3);
        cluster.Offline(4);

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.NotEmpty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers.Take(3), peer =>
        {
            Assert.NotEmpty(peer.Prepares);
            Assert.True(peer.Inner.Snapshot("key")!.AcceptedBallot.IsClassicRoundBallot);
            Assert.Equal(Payload, peer.Inner.Snapshot("key")!.AcceptedValue);
        });
    }

    [Fact]
    public async Task RunPassSkewedOrdinalPagesRepairsUnionIncludingRemoteOnlyKeysOnce()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create(pageSize: 2);
        // Deliberately interleaved inventories: a peer can finish before another peer's frontier.
        string[][] inventories = [["z", "a"], ["é", "A", "b", "c"], ["Ω", "B", "y"]];
        for (var peer = 0; peer < inventories.Length; peer++)
            foreach (var key in inventories[peer])
                await cluster.SeedAsync(peer, key, new(System.Text.Encoding.UTF8.GetBytes(key)));
        string[] expected = ["A", "B", "a", "b", "c", "y", "z", "é", "Ω"];

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((9L, 9L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.Empty(result.Errors);
        foreach (var key in expected)
            cluster.AssertValue(key, new(System.Text.Encoding.UTF8.GetBytes(key)));
        Assert.Equal(expected, cluster.Peers[0].Prepares.Select(call => call.Key));
        Assert.Equal(expected, cluster.Digests.SelectMany(source => source.Fetches).Select(call => call.Key)
            .OrderBy(key => key, StringComparer.Ordinal));
        Assert.All(cluster.Digests, source =>
        {
            var calls = source.Lists.ToArray();
            Assert.True(calls.Length > 1);
            Assert.Null(calls[0].After);
            Assert.All(calls, call => { Assert.Equal("", call.Prefix); Assert.Equal(2, call.Limit); });
            for (var index = 1; index < calls.Length; index++)
                Assert.True(calls[index].After is not null &&
                    (index == 1 || StringComparer.Ordinal.Compare(calls[index - 1].After, calls[index].After) < 0));
        });
    }

    [Fact]
    public async Task RunPassDepartingRepairsOnlyLocalKeysAcrossRemotePages()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create(pageSize: 2);
        await cluster.SeedAsync(0, "z-local", Payload);
        foreach (var key in new[] { "a-remote", "b-remote", "c-remote", "d-remote", "e-remote" })
            await cluster.SeedAsync(1, key, ByteString.Empty);

        var result = await repair.RunPassAsync(departing: true).WaitAsync(Watchdog);

        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        cluster.AssertValue("z-local", Payload);
        Assert.All(cluster.Peers.SelectMany(peer => peer.Prepares), call => Assert.Equal("z-local", call.Key));
        Assert.Null(cluster.Peers[0].Inner.Snapshot("a-remote"));
        Assert.Null(cluster.Peers[2].Inner.Snapshot("a-remote"));
        Assert.Single(cluster.Digests[0].Lists);
    }

    [Fact]
    public async Task RunPassDepartingDoesNotCountLocalVoteAndCompletesWhenRemoteReturns()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        await cluster.SeedAsync(1, "key", Payload);
        cluster.Offline(2);

        var pass = repair.RunPassAsync(departing: true);
        await cluster.Clock.WaitForDelayAsync(RepairTestCluster.UnavailableRetryDelay);
        Assert.Equal(2, repair.TargetReplicas);
        Assert.False(pass.IsCompleted); // Local + one remote is a quorum, but not two remote copies.
        Assert.Equal(Payload, cluster.Peers[1].Inner.Snapshot("key")!.AcceptedValue);
        Assert.Null(cluster.Peers[2].Inner.Snapshot("key"));

        cluster.Online(2);
        cluster.Clock.Advance(RepairTestCluster.UnavailableRetryDelay);
        var result = await pass.WaitAsync(Watchdog);
        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        cluster.AssertValue("key", Payload);
        Assert.NotEmpty(cluster.Peers[2].Accepts);
    }

    [Fact]
    public async Task RunPassDepartingExplicitTargetRequiresFreshRemoteAcknowledgement()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create(target: 1);
        await cluster.SeedAsync(0, "key", Payload);
        await cluster.SeedAsync(1, "key", Payload);
        cluster.Offline(2);

        var result = await repair.RunPassAsync(departing: true).WaitAsync(Watchdog);

        Assert.Equal(1, repair.TargetReplicas);
        Assert.Equal((1L, 1L, 0L), (result.Examined, result.Repaired, result.Pending));
        Assert.NotEmpty(cluster.Ballots.Reserved);
        Assert.Single(cluster.Peers[1].Accepts);
    }

    [Fact]
    public async Task RunPassDepartingMatchingDigestsOnDrainingPeersCannotSatisfyRemoteTarget()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        for (var index = 0; index < 3; index++) await cluster.SeedAsync(index, "key", Payload);
        foreach (var peer in cluster.Peers.Skip(1))
        {
            peer.Prepare = (_, _, _) => ValueTask.FromException<AcceptorPrepareResult<ByteString?>>(
                new RegisterUnavailableException(new InvalidOperationException("draining")));
            peer.Accept = (_, _, _) => ValueTask.FromException<AcceptorAcceptResult>(
                new RegisterUnavailableException(new InvalidOperationException("draining")));
        }

        var pass = repair.RunPassAsync(departing: true);
        await cluster.Clock.WaitForDelayAsync(RepairTestCluster.UnavailableRetryDelay);
        Assert.False(pass.IsCompleted);
        cluster.AssertValue("key", Payload);
        Assert.All(cluster.Peers.Skip(1), peer => Assert.NotEmpty(peer.Prepares));
        cluster.Clock.Advance(TimeSpan.FromSeconds(10));
        var pending = await pass.WaitAsync(Watchdog);
        Assert.Equal((1L, 0L, 1L), (pending.Examined, pending.Repaired, pending.Pending));

        cluster.Online(1);
        cluster.Online(2);
        var recovered = await repair.RunPassAsync(departing: true).WaitAsync(Watchdog);
        Assert.Equal((1L, 1L, 0L), (recovered.Examined, recovered.Repaired, recovered.Pending));
        cluster.AssertValue("key", Payload);
        Assert.All(cluster.Peers.Skip(1), peer => Assert.NotEmpty(peer.Accepts));
    }

    [Fact]
    public async Task RunPassDepartingRemoteTargetUnavailableDeadlineReportsPendingThenRecovers()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        cluster.Offline(2);

        var pass = repair.RunPassAsync(departing: true);
        await cluster.Clock.WaitForDelayAsync(RepairTestCluster.UnavailableRetryDelay);
        cluster.Clock.Advance(TimeSpan.FromSeconds(10));
        var unavailable = await pass.WaitAsync(Watchdog);
        Assert.Equal((1L, 0L, 1L), (unavailable.Examined, unavailable.Repaired, unavailable.Pending));
        Assert.Contains(unavailable.Errors, error => error is RegisterUnavailableException);
        Assert.Equal(Payload, cluster.Peers[0].Inner.Snapshot("key")!.AcceptedValue);
        Assert.Equal(Payload, cluster.Peers[1].Inner.Snapshot("key")!.AcceptedValue);

        cluster.Online(2);
        var recovered = await repair.RunPassAsync(departing: true).WaitAsync(Watchdog);
        Assert.Equal((1L, 1L, 0L), (recovered.Examined, recovered.Repaired, recovered.Pending));
        cluster.AssertValue("key", Payload);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunPassDepartingCancellationDuringAcceptDoesNotWaitForLateTransport(bool durableBeforeCancel)
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        using var blocked = new Gate();
        using var cancellation = new CancellationTokenSource();
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Peers[2].Accept = async (key, request, _) =>
        {
            try
            {
                AcceptorAcceptResult result = default;
                if (durableBeforeCancel) result = await cluster.Peers[2].Inner.AcceptAsync(key, request, CancellationToken.None);
                await blocked.WaitAsync(); // Deliberately ignore the caller's cancellation.
                return durableBeforeCancel ? result : await cluster.Peers[2].Inner.AcceptAsync(key, request, CancellationToken.None);
            }
            finally { returned.TrySetResult(); }
        };

        var pass = repair.RunPassAsync(departing: true, cancellation.Token);
        await blocked.Arrived;
        Assert.Equal(durableBeforeCancel, cluster.Peers[2].Inner.Snapshot("key") is not null);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pass.WaitAsync(Watchdog));
        Assert.False(blocked.IsReleased);
        Assert.Equal(TimeSpan.Zero, cluster.Clock.GetUtcNow() - DateTimeOffset.UnixEpoch);
        Assert.Equal(Payload, cluster.Peers[0].Inner.Snapshot("key")!.AcceptedValue);
        Assert.Equal(Payload, cluster.Peers[1].Inner.Snapshot("key")!.AcceptedValue);
        blocked.Release();
        await returned.Task.WaitAsync(Watchdog);
        cluster.AssertValue("key", Payload); // Cancellation does not roll back already selected accepts.
    }

    [Fact]
    public async Task RunPassCanceledSerializedWaitDoesNotStartAnotherInventory()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        using var blocked = new Gate();
        using var activeCancellation = new CancellationTokenSource();
        using var queuedCancellation = new CancellationTokenSource();
        cluster.Digests[0].List = async (_, _, _, _) =>
        {
            await blocked.WaitAsync();
            return new([], null);
        };
        var active = repair.RunPassAsync(cancellationToken: activeCancellation.Token);
        await blocked.Arrived;
        var queued = repair.RunPassAsync(cancellationToken: queuedCancellation.Token);
        queuedCancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued.WaitAsync(Watchdog));
        Assert.False(active.IsCompleted);
        Assert.All(cluster.Digests, source => Assert.Single(source.Lists));
        activeCancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.WaitAsync(Watchdog));
        Assert.False(blocked.IsReleased);
    }

    [Fact]
    public async Task RunPassAttemptTimeoutCancelsCooperativePeer()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        using var caller = new CancellationTokenSource();
        var reply = new TaskCompletionSource<AcceptorDigestPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<AcceptorDigestPage>? dispatched = null;
        CancellationToken dispatchedToken = default;
        cluster.Digests[2].List = (_, _, _, token) =>
        {
            dispatchedToken = token;
            dispatched = reply.Task.WaitAsync(token);
            return new(dispatched);
        };
        try
        {
            var pass = repair.RunPassAsync(cancellationToken: caller.Token);
            await cluster.Clock.WaitForDelayAsync(TimeSpan.FromSeconds(1));
            cluster.Clock.Advance(TimeSpan.FromSeconds(1));
            var result = await pass.WaitAsync(Watchdog);

            Assert.Equal((0L, 0L, 0L), (result.Examined, result.Repaired, result.Pending));
            Assert.IsType<TimeoutException>(Assert.Single(result.Errors));
            Assert.True(dispatchedToken.IsCancellationRequested);
            Assert.False(caller.IsCancellationRequested);
            Assert.NotNull(dispatched);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatched.WaitAsync(Watchdog));
        }
        finally { caller.Cancel(); }
    }

    [Fact]
    public async Task RunPassHungPeerAcrossPagesAndPassesRetainsAtMostOneCall()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create(pageSize: 2);
        for (var index = 0; index < 7; index++)
        {
            await cluster.SeedAsync(0, $"key-{index}", Payload);
            await cluster.SeedAsync(1, $"key-{index}", Payload);
        }
        using var blocked = new Gate();
        cluster.Digests[2].List = async (_, _, _, _) =>
        {
            await blocked.WaitAsync();
            return new([], null);
        };
        var first = repair.RunPassAsync();
        await blocked.Arrived;
        await cluster.Clock.WaitForDelayAsync(TimeSpan.FromSeconds(1));
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        var initial = await first.WaitAsync(Watchdog);
        Assert.Equal((7L, 0L, 0L), (initial.Examined, initial.Repaired, initial.Pending));
        Assert.Single(initial.Errors); // Bounded diagnostics, not one exception per key/page.

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var repeated = await repair.RunPassAsync().WaitAsync(Watchdog);
            Assert.Equal((7L, 0L, 0L), (repeated.Examined, repeated.Repaired, repeated.Pending));
            Assert.IsType<RegisterBusyException>(Assert.Single(repeated.Errors));
        }
        Assert.Single(cluster.Digests[2].Lists);
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.False(blocked.IsReleased);
    }

    [Fact]
    public async Task RunPassLocalInventoryCorruptionIsNotReportedAsTemporaryUnavailability()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        var corruption = new InvalidDataException("invalid persisted accepted-state digest");
        cluster.Digests[0].List = (_, _, _, _) => ValueTask.FromException<AcceptorDigestPage>(corruption);

        var failure = await Assert.ThrowsAsync<InvalidDataException>(() => repair.RunPassAsync().WaitAsync(Watchdog));

        Assert.Same(corruption, failure);
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers, peer => Assert.Empty(peer.Accepts));
    }

    [Fact]
    public async Task RunPassLocalInventoryFailureThrowsWithoutProposing()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        var failure = new IOException("local inventory failed");
        cluster.Digests[0].List = (_, _, _, _) => ValueTask.FromException<AcceptorDigestPage>(failure);
        await cluster.SeedAsync(1, "remote", Payload);

        var error = await Assert.ThrowsAsync<IOException>(() => repair.RunPassAsync().WaitAsync(Watchdog));

        Assert.Same(failure, error);
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
    }

    [Fact]
    public async Task RunPassLocalFetchFailureThrowsOriginalErrorWithoutProposing()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        var failure = new IOException("local conditional fetch failed");
        cluster.Digests[0].Fetch = (_, _, _) => ValueTask.FromException<AcceptorReadResult>(failure);

        var error = await Assert.ThrowsAsync<IOException>(() => repair.RunPassAsync().WaitAsync(Watchdog));

        Assert.Same(failure, error);
        Assert.Equal("key", Assert.Single(cluster.Digests[0].Fetches).Key);
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
        Assert.Equal(Payload, cluster.Peers[0].Inner.Snapshot("key")!.AcceptedValue);
        Assert.Null(cluster.Peers[1].Inner.Snapshot("key"));
        Assert.Null(cluster.Peers[2].Inner.Snapshot("key"));
    }

    [Fact]
    public async Task RunPassRemoteFetchFailureReportsPendingWithoutProposing()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        // Only the remote owns this key, so local-source preference cannot hide a failed remote fetch.
        await cluster.SeedAsync(1, "key", Payload);
        var failure = new IOException("remote conditional fetch failed");
        cluster.Digests[1].Fetch = (_, _, _) => ValueTask.FromException<AcceptorReadResult>(failure);

        var result = await repair.RunPassAsync().WaitAsync(Watchdog);

        Assert.Equal((1L, 0L, 1L), (result.Examined, result.Repaired, result.Pending));
        Assert.Same(failure, Assert.Single(result.Errors));
        Assert.Equal("key", Assert.Single(cluster.Digests[1].Fetches).Key);
        Assert.Empty(cluster.Digests[0].Fetches);
        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers, peer =>
        {
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
        });
        Assert.Null(cluster.Peers[0].Inner.Snapshot("key"));
        Assert.Equal(Payload, cluster.Peers[1].Inner.Snapshot("key")!.AcceptedValue);
        Assert.Null(cluster.Peers[2].Inner.Snapshot("key"));
    }

    [Fact]
    public async Task RunPassListedAcceptedKeyBecomesMissingThrowsInvariantFailureWithoutProposing()
    {
        var cluster = new RepairTestCluster();
        var repair = cluster.Create();
        await cluster.SeedAsync(0, "key", Payload);
        cluster.Digests[0].Fetch = (_, _, _) =>
            ValueTask.FromResult<AcceptorReadResult>(new AcceptorMissing());

        await Assert.ThrowsAsync<InvalidDataException>(() => repair.RunPassAsync().WaitAsync(Watchdog));

        Assert.Empty(cluster.Ballots.Reserved);
        Assert.All(cluster.Peers, peer => Assert.Empty(peer.Accepts));
        Assert.Null(cluster.Peers[1].Inner.Snapshot("key"));
    }

    [Theory]
    [InlineData(3, 2)]
    [InlineData(5, 3)]
    [InlineData(7, 4)]
    public void ConstructorDefaultTargetIsFixedMembershipMajority(int members, int expected) =>
        Assert.Equal(expected, new RepairTestCluster(members).Create().TargetReplicas);

    [Theory]
    [InlineData("duplicate-peer")]
    [InlineData("missing-local")]
    [InlineData("shared-fast-proposer")]
    [InlineData("negative-target")]
    [InlineData("target-exceeds-membership")]
    [InlineData("zero-page")]
    public void ConstructorInvalidMembershipOrOptionsRejects(string caseId)
    {
        var peers = Peers();
        var routes = peers.Select((peer, index) =>
            new RepairPeer($"peer-{index}", peer, new RepairDigestSource(peer.Inner))).ToArray();
        if (caseId == "duplicate-peer") routes[2] = routes[0];
        var local = caseId == "missing-local" ? "absent" : "peer-0";
        var ballots = new TestBallots(caseId == "shared-fast-proposer" ? 0 : 11);
        var options = new DurabilityRepairOptions
        {
            TargetReplicas = caseId == "negative-target" ? -1 : caseId == "target-exceeds-membership" ? 4 : 0,
            PageSize = caseId == "zero-page" ? 0 : 2
        };

        Assert.ThrowsAny<ArgumentException>(() => new DurabilityRepair(local, [.. routes], ballots, options));
    }
}
