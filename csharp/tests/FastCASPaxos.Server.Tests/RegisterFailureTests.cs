using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class RegisterFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ClassicQuorumDoesNotWaitForMinority(bool offline)
    {
        var peers = Peers();
        using var gate = new Gate();
        if (offline) peers[0].GoOffline();
        else
        {
            peers[0].Prepare = async (key, request, _) =>
            {
                await gate.WaitAsync();
                return await peers[0].Inner.PrepareAsync(key, request, CancellationToken.None);
            };
            peers[0].Accept = async (key, request, _) =>
            {
                await gate.WaitAsync();
                return await peers[0].Inner.AcceptAsync(key, request, CancellationToken.None);
            };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.False(gate.IsReleased);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(Payload, result.Current!.Value);
        Assert.Equal(result.Current, await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));
        Assert.Null(peers[0].Inner.Snapshot("key"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QuorumLossDeadlineBeforeMutationThrowsUnavailable(bool write)
    {
        var peers = Peers();
        RequireRecovery(peers);
        var clock = new ManualTime();
        using var gate = new Gate();
        for (var i = 1; i < peers.Length; i++)
            peers[i].Prepare = async (_, _, _) => { await gate.WaitAsync(); throw new IOException("unavailable"); };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        Task operation = write ? service.CompareExchangeAsync("key", null, Payload).AsTask() : service.ReadAsync("key").AsTask();
        await gate.Arrived;
        Assert.False(operation.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<RegisterUnavailableException>(() => operation.WaitAsync(Watchdog));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
        Assert.All(peers, peer => Assert.Null(peer.Inner.Snapshot("key")));
        gate.Release();
        foreach (var peer in peers) peer.Prepare = null;
        var recovery = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, recovery.Status);
    }

    [Fact]
    public async Task CancellationBeforeMutationThrowsAndReleasesAdmission()
    {
        var peers = Peers();
        using var gate = new Gate();
        foreach (var peer in peers)
            peer.Prepare = async (_, _, _) => { await gate.WaitAsync(); throw new IOException("late"); };
        using var canceled = new CancellationTokenSource();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(capacity: 3));
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.ReadAsync("pre-canceled", canceled.Token).AsTask());
        Assert.All(peers, peer => Assert.Empty(peer.Reads));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        using var runningCancellation = new CancellationTokenSource();
        var running = service.CompareExchangeAsync("key", null, Payload, runningCancellation.Token).AsTask();
        await gate.Arrived;
        using var queuedCancellation = new CancellationTokenSource();
        var queued = service.CompareExchangeAsync("key", null, Payload, queuedCancellation.Token).AsTask();
        queuedCancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued.WaitAsync(Watchdog));
        runningCancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Watchdog));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
        gate.Release();
        foreach (var peer in peers) peer.Prepare = null;
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("key", null, Payload)).Status);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CancellationOrDeadlineAfterMutationReturnsUnknownEvenBeforeDurability(bool deadline, bool durable)
    {
        var peers = Peers();
        var clock = new ManualTime();
        using var cancellation = new CancellationTokenSource();
        using var gate = new Gate();
        foreach (var peer in peers)
        {
            peer.Accept = async (key, request, _) =>
            {
                AcceptorAcceptResult result = default;
                if (durable) result = await peer.Inner.AcceptAsync(key, request, CancellationToken.None);
                await gate.WaitAsync();
                return durable ? result : await peer.Inner.AcceptAsync(key, request, CancellationToken.None);
            };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        var write = service.CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();
        await gate.Arrived;
        Assert.All(peers, peer => Assert.Equal(durable, peer.Inner.Snapshot("key") is not null));
        var sentVersion = new ByteString(peers[0].Accepts.First().Request.Value.ToArray().AsSpan(5, 8));
        if (deadline) clock.Advance(TimeSpan.FromSeconds(5));
        else cancellation.Cancel();
        var uncertain = await write.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, uncertain.Status);
        Assert.Null(uncertain.Current);
        if (deadline) Assert.IsType<RegisterUnavailableException>(uncertain.Error);
        else Assert.IsAssignableFrom<OperationCanceledException>(uncertain.Error);
        // Safety while replies are lost, then restore delivery and establish the observable state.
        gate.Release();
        foreach (var peer in peers) peer.Accept = null;
        var read = await service.ReadAsync("key").AsTask().WaitAsync(Watchdog);
        // If the original requests were not durable before cancellation, a read can seal absence
        // before they arrive. Either outcome is valid, but the caller must never see Applied above.
        if (durable || read.IsPresent) Assert.Equal(new RegisterValue(Payload, sentVersion), read);
        else Assert.Equal(new RegisterValue(null, null), read);
    }

    [Theory]
    [InlineData("classic")]
    [InlineData("fast")]
    public async Task RetryRecoversOwnTokenWithoutApplyingTwice(string mode)
    {
        var peers = Peers();
        foreach (var peer in peers)
        {
            var calls = 0;
            peer.Accept = async (key, request, token) =>
            {
                var result = await peer.Inner.AcceptAsync(key, request, token);
                if (Interlocked.Increment(ref calls) == 1) throw new IOException("reply lost after commit");
                return result;
            };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(mode));
        var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Null(result.Error);
        var sent = peers.SelectMany(peer => peer.Accepts).Select(item => item.Request).ToArray();
        Assert.True(sent.Length >= 6); // Lost replies force a real second accept phase.
        Assert.Single(sent.Select(request => request.Value).Distinct());
        Assert.Equal(Envelope(result.Current!.Version!.Value, Payload), sent[0].Value);
        Assert.Equal(result.Current, await service.ReadAsync("key"));
    }

    [Fact]
    public async Task SupersededUncertainMutationIsUnknownNotMismatch()
    {
        var peers = Peers();
        using var gate = new Gate();
        foreach (var peer in peers)
        {
            peer.Accept = async (key, request, token) =>
            {
                await peer.Inner.AcceptAsync(key, request, token);
                await gate.WaitAsync();
                throw new IOException("original reply lost");
            };
        }
        await using var first = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        await using var later = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options());
        var original = first.CompareExchangeAsync("key", null, Payload).AsTask();
        await gate.Arrived;
        var originalVersion = new ByteString(peers[0].Inner.Snapshot("key")!.AcceptedValue.ToArray().AsSpan(5, 8));
        foreach (var peer in peers) peer.Accept = null;
        var superseding = await later.CompareExchangeAsync("key", originalVersion, ByteString.Empty).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, superseding.Status);
        gate.Release();
        var result = await original.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(superseding.Current, result.Current);
        Assert.NotEqual(originalVersion, result.Current!.Version);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task AllMutationsExplicitlyRejectedReturnsMismatchRatherThanUnknown()
    {
        var peers = Peers();
        var existingVersion = Version(9, 1);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key",
                new(new Ballot(10, 9), Envelope(existingVersion, ByteString.Empty), Ballot.Zero))).IsAccepted);
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options("fast"));
        var result = await service.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Mismatch, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, existingVersion), result.Current);
        Assert.Null(result.Error);
        // Prove this exercised speculative mutation rejection, not a prepare-only mismatch.
        Assert.All(peers, peer =>
        {
            Assert.Equal(Ballot.InitialFast(), peer.Accepts.First().Request.Ballot);
            Assert.NotEqual(Envelope(existingVersion, ByteString.Empty), peer.Accepts.First().Request.Value);
            Assert.Equal(Envelope(existingVersion, ByteString.Empty), peer.Inner.Snapshot("key")!.AcceptedValue);
        });
    }

    [Fact]
    public async Task StorageFailuresAfterDispatchPreserveUncertaintyAndCauseAtDeadline()
    {
        var peers = Peers();
        var clock = new ManualTime();
        foreach (var peer in peers)
            peer.Accept = (_, _, _) => ValueTask.FromException<AcceptorAcceptResult>(new IOException("storage unavailable"));
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        var write = service.CompareExchangeAsync("key", null, Payload).AsTask();
        await clock.WaitForDelayAsync(TimeSpan.FromMilliseconds(2));
        clock.Advance(TimeSpan.FromSeconds(5));
        var result = await write.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        var error = Assert.IsType<RegisterUnavailableException>(result.Error);
        var failures = Assert.IsType<AggregateException>(error.InnerException);
        Assert.Equal(3, failures.InnerExceptions.Count);
        Assert.All(failures.InnerExceptions, exception => Assert.IsType<IOException>(exception));
        Assert.All(peers, peer => Assert.Null(peer.Inner.Snapshot("key")));
        foreach (var peer in peers) peer.Accept = null;
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("key", null, Payload)).Status);
    }

    [Fact]
    public async Task LateDuplicateAcceptRepliesCannotCompleteRetriedAttempt()
    {
        var peers = Peers();
        var clock = new ManualTime();
        using var oldReplies = new Gate();
        using var newReplies = new Gate();
        var returned = peers.Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            var completion = returned[index];
            var calls = 0;
            peer.Accept = async (key, request, token) =>
            {
                var reply = await peer.Inner.AcceptAsync(key, request, token);
                if (Interlocked.Increment(ref calls) == 1)
                {
                    await oldReplies.WaitAsync();
                    completion.TrySetResult();
                }
                else await newReplies.WaitAsync();
                return reply;
            };
        }
        using var cancellation = new CancellationTokenSource();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        var resultTask = service.CompareExchangeAsync("key", null, Payload, cancellation.Token).AsTask();
        await oldReplies.Arrived;
        clock.Advance(TimeSpan.FromSeconds(1));
        await clock.WaitForDelayAsync(TimeSpan.FromMilliseconds(2));
        clock.Advance(TimeSpan.FromMilliseconds(2));
        await newReplies.Arrived;
        Assert.Equal(2, peers[0].Accepts.Count);
        var requests = peers[0].Accepts.ToArray();
        Assert.True(requests[1].Request.Ballot > requests[0].Request.Ballot);
        Assert.Equal(requests[0].Request.Value, requests[1].Request.Value);
        oldReplies.Release(); // Three duplicate old-accept acknowledgments arrive in a new phase.
        await Task.WhenAll(returned.Select(item => item.Task)).WaitAsync(Watchdog);
        Assert.False(resultTask.IsCompleted);
        cancellation.Cancel();
        var result = await resultTask.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        Assert.IsAssignableFrom<OperationCanceledException>(result.Error);
        newReplies.Release();
        foreach (var peer in peers) peer.Accept = null;
        await using var reader = new ReplicatedRegisterService(Routes(peers), new TestBallots(2), Options());
        var current = await reader.ReadAsync("key").AsTask().WaitAsync(Watchdog);
        Assert.Equal(Envelope(current.Version!.Value, Payload), requests[0].Request.Value);
    }

    [Fact]
    public async Task LatePrepareRepliesCannotOverrideNewerRecovery()
    {
        var peers = Peers();
        RequireRecovery(peers);
        var clock = new ManualTime();
        using var oldReplies = new Gate();
        using var newReplies = new Gate();
        var returned = peers.Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            var completion = returned[index];
            var calls = 0;
            peer.Prepare = async (key, request, token) =>
            {
                var reply = await peer.Inner.PrepareAsync(key, request, token);
                if (Interlocked.Increment(ref calls) == 1)
                {
                    await oldReplies.WaitAsync();
                    completion.TrySetResult();
                }
                else await newReplies.WaitAsync();
                return reply;
            };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        var read = service.ReadAsync("key").AsTask();
        await oldReplies.Arrived;
        // Seed the chosen value at the next owned ballot before the retried prepare. Old replies
        // all describe absence, while the new attempt must recover this independently supplied state.
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key",
                new(new Ballot(2, 1), Envelope(Version(9, 1), Payload), Ballot.Zero))).IsAccepted);
        clock.Advance(TimeSpan.FromSeconds(1));
        await clock.WaitForDelayAsync(TimeSpan.FromMilliseconds(2));
        clock.Advance(TimeSpan.FromMilliseconds(2));
        await newReplies.Arrived;
        oldReplies.Release();
        await Task.WhenAll(returned.Select(item => item.Task)).WaitAsync(Watchdog);
        Assert.False(read.IsCompleted);
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
        newReplies.Release();
        Assert.Equal(new RegisterValue(Payload, Version(9, 1)), await read.WaitAsync(Watchdog));
        Assert.All(peers, peer => Assert.Equal(Envelope(Version(9, 1), Payload), peer.Inner.Snapshot("key")!.AcceptedValue));
    }

    [Fact]
    public async Task CapacityIncludesQueuedCallsAndDoesNotSerializeIndependentKeys()
    {
        var peers = Peers();
        RequireRecovery(peers);
        using var gate = new Gate();
        foreach (var peer in peers)
            peer.Prepare = async (key, request, token) =>
            {
                if (key == "blocked") await gate.WaitAsync();
                return await peer.Inner.PrepareAsync(key, request, token);
            };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(capacity: 3));
        var first = service.ReadAsync("blocked").AsTask();
        await gate.Arrived;
        using var cancellation = new CancellationTokenSource();
        var queued = service.ReadAsync("blocked", cancellation.Token).AsTask();
        var independent = await service.CompareExchangeAsync("independent", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, independent.Status);
        Assert.False(first.IsCompleted);
        Assert.False(queued.IsCompleted);
        var third = service.ReadAsync("blocked").AsTask();
        await Assert.ThrowsAsync<RegisterBusyException>(() => service.ReadAsync("overflow").AsTask());
        await Assert.ThrowsAsync<RegisterBusyException>(() => service.CompareExchangeAsync("overflow", null, Payload).AsTask());
        await Assert.ThrowsAsync<RegisterBusyException>(() => service.ListKeysAsync().AsTask());
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued.WaitAsync(Watchdog));
        Assert.Equal(independent.Current, await service.ReadAsync("independent").AsTask().WaitAsync(Watchdog));
        gate.Release();
        Assert.False((await first.WaitAsync(Watchdog)).IsPresent);
        Assert.False((await third.WaitAsync(Watchdog)).IsPresent);
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("overflow", null, Payload)).Status);
    }

    [Fact]
    public async Task DisposeCancelsWorkAndLeavesCollaboratorsOwnedByCaller()
    {
        var peers = Peers();
        RequireRecovery(peers);
        using var gate = new Gate();
        foreach (var peer in peers)
            peer.Prepare = async (_, _, _) => { await gate.WaitAsync(); throw new IOException("late response"); };
        var ballots = new TestBallots(1);
        var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());
        var running = service.ReadAsync("key").AsTask();
        await gate.Arrived;
        var queued = service.ReadAsync("key").AsTask();
        await service.DisposeAsync().AsTask().WaitAsync(Watchdog);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Watchdog));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued.WaitAsync(Watchdog));
        await service.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => service.ReadAsync("new").AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => service.CompareExchangeAsync("new", null, Payload).AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => service.ListKeysAsync().AsTask());
        Assert.False(ballots.Disposed);
        Assert.All(peers, peer => Assert.False(peer.Disposed));
        Assert.True(ballots.Reserve(1).Round > 0);
        gate.Release();
        foreach (var peer in peers) peer.Prepare = null;
        await using var replacement = new ReplicatedRegisterService(Routes(peers), ballots, Options());
        Assert.Equal(CompareExchangeStatus.Applied, (await replacement.CompareExchangeAsync("new", null, Payload)).Status);
    }

    [Fact]
    public async Task DisposeAfterMutationReturnsUnknownWithoutWaitingForTransport()
    {
        var peers = Peers();
        using var gate = new Gate();
        foreach (var peer in peers)
            peer.Accept = async (key, request, token) =>
            {
                var reply = await peer.Inner.AcceptAsync(key, request, token);
                await gate.WaitAsync();
                return reply;
            };
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        var write = service.CompareExchangeAsync("key", null, Payload).AsTask();
        await gate.Arrived;
        await service.DisposeAsync().AsTask().WaitAsync(Watchdog);
        Assert.False(gate.IsReleased);
        var result = await write.WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        Assert.IsAssignableFrom<OperationCanceledException>(result.Error);
        Assert.All(peers, peer => Assert.Equal(Payload.ToArray(), peer.Inner.Snapshot("key")!.AcceptedValue.ToArray()[13..]));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadMinorityFailureOrLateReplyDoesNotDelayMatchingQuorum(bool failure)
    {
        var peers = Peers();
        var value = Envelope(Version(9, 1), Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new Ballot(3, 9), value, Ballot.Zero))).IsAccepted);
        using var late = new Gate();
        peers[0].Read = async (key, known, _) =>
        {
            if (failure) throw new IOException("offline");
            await late.WaitAsync();
            return await peers[0].Inner.ReadAsync(key, known, CancellationToken.None);
        };
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        Assert.Equal(new RegisterValue(Payload, Version(9, 1)), await service.ReadAsync("key").AsTask().WaitAsync(Watchdog));

        Assert.False(late.IsReleased);
        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer =>
        {
            Assert.Single(peer.Reads);
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
            Assert.Equal(new Ballot(3, 9), peer.Inner.Snapshot("key")!.AcceptedBallot);
        });
    }

    [Fact]
    public async Task ReadCanceledProbeDoesNotDispatchRecovery()
    {
        var peers = Peers();
        using var cancellation = new CancellationTokenSource();
        // Every response differs; cancellation arrives with the final observation, just before fallback.
        for (var index = 0; index < peers.Length; index++)
        {
            var response = new KeyedAcceptorState("key", new(2 + index, 9), new(2 + index, 9),
                Envelope(Version(9, index + 1), Payload));
            var cancel = index == peers.Length - 1;
            peers[index].Read = (_, _, _) =>
            {
                if (cancel) cancellation.Cancel();
                return ValueTask.FromResult<AcceptorReadResult>(response);
            };
        }
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());

        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            service.ReadAsync("key", cancellation.Token).AsTask());

        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer => { Assert.Empty(peer.Prepares); Assert.Empty(peer.Accepts); });
    }

    [Fact]
    public async Task ReadLateObservationCannotCompleteAnotherCall()
    {
        var peers = Peers();
        var old = Envelope(Version(9, 1), ByteString.Empty);
        var current = Envelope(Version(9, 2), Payload);
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new Ballot(2, 9), old, Ballot.Zero))).IsAccepted);
        using var oldReplies = new Gate();
        using var newReplies = new Gate();
        var returned = Enumerable.Range(0, 2).Select(_ =>
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        for (var index = 1; index < peers.Length; index++)
        {
            var peer = peers[index];
            var completion = returned[index - 1];
            var calls = 0;
            peer.Read = async (key, known, _) =>
            {
                var response = await peer.Inner.ReadAsync(key, known, CancellationToken.None);
                if (Interlocked.Increment(ref calls) == 1)
                {
                    await oldReplies.WaitAsync();
                    completion.TrySetResult();
                }
                else await newReplies.WaitAsync();
                return response;
            };
        }
        var ballots = new TestBallots(1);
        await using var service = new ReplicatedRegisterService(Routes(peers), ballots, Options());
        using var cancellation = new CancellationTokenSource();
        var first = service.ReadAsync("key", cancellation.Token).AsTask();
        await oldReplies.Arrived;
        Assert.False(first.IsCompleted); // One peer cannot certify a majority.
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(Watchdog));
        foreach (var peer in peers)
            Assert.True((await peer.Inner.AcceptAsync("key", new(new Ballot(3, 9), current, Ballot.Zero))).IsAccepted);

        var second = service.ReadAsync("key").AsTask();
        await newReplies.Arrived;
        oldReplies.Release();
        await Task.WhenAll(returned.Select(completion => completion.Task)).WaitAsync(Watchdog);
        Assert.False(second.IsCompleted); // Two old replies cannot join this call's single current reply.
        newReplies.Release();
        Assert.Equal(new RegisterValue(Payload, Version(9, 2)), await second.WaitAsync(Watchdog));
        Assert.Empty(ballots.Reserved);
        Assert.All(peers, peer =>
        {
            Assert.Equal(2, peer.Reads.Count);
            Assert.Empty(peer.Prepares);
            Assert.Empty(peer.Accepts);
            Assert.Equal(current, peer.Inner.Snapshot("key")!.AcceptedValue);
        });
    }

    [Fact]
    public async Task ReadProbeAndRecoveryShareOriginalDeadline()
    {
        var peers = Peers();
        var clock = new ManualTime();
        using var probes = new Gate();
        using var recovery = new Gate();
        foreach (var peer in peers)
        {
            peer.Read = async (_, _, _) => { await probes.WaitAsync(); return new AcceptorMissing(); };
            peer.Prepare = async (_, _, _) => { await recovery.WaitAsync(); throw new IOException("late"); };
        }
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options(clock: clock));
        var read = service.ReadAsync("key").AsTask();
        await probes.Arrived;
        clock.Advance(TimeSpan.FromSeconds(1));
        await recovery.Arrived;
        Assert.False(read.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(4));

        await Assert.ThrowsAsync<RegisterUnavailableException>(() => read.WaitAsync(Watchdog));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
    }
}
