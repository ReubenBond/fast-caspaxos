using System.Buffers.Binary;
using FastCASPaxos.Server;
using FastCASPaxos.Testing;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class RegisterSimulationClusterTests
{
    private static ulong ReadRevision(RegisterValue value)
    {
        Assert.Equal(8, value.Version!.Value.Length);
        return BinaryPrimitives.ReadUInt64BigEndian(value.Version.Value.Span);
    }

    [Fact]
    public async Task AbsentCreateUpdateUsesOpaqueVersions()
    {
        await using var cluster = new RegisterSimulationCluster(731);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            Assert.Equal(new RegisterValue(null, null), await client.ReadAsync("key"));
            var created = await client.CompareExchangeAsync("key", null, ByteString.Empty);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            Assert.Equal(ByteString.Empty, created.Current!.Value);
            Assert.True(created.Current.IsPresent);
            Assert.Equal(1UL, ReadRevision(created.Current));
            var original = created.Current.Version!.Value;
            var updated = await client.CompareExchangeAsync("key", new(original.ToArray()), ByteString.Empty);
            Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
            Assert.Equal(ByteString.Empty, updated.Current!.Value);
            Assert.NotEqual(original, updated.Current.Version);
            Assert.Equal(2UL, ReadRevision(updated.Current));
            var mismatch = await client.CompareExchangeAsync("key", original, new([99]));
            Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
            Assert.Equal(updated.Current, mismatch.Current);
            Assert.Equal(updated.Current, await simulation.CreateClient(2).ReadAsync("key"));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HealthyConcurrentWritersHaveOneAppliedAndThirtyOneMismatches(bool existing)
    {
        await using var cluster = new RegisterSimulationCluster(732);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            ByteString? expected = null;
            if (existing)
            {
                var initial = await client.CompareExchangeAsync("race", null, new([100]));
                Assert.Equal(CompareExchangeStatus.Applied, initial.Status);
                expected = initial.Current!.Version;
            }

            // All calls overlap on the scheduler. The shared sticky client targets one real
            // proposer, whose existing host queues requests; the fixture adds no serial lock.
            var writes = Enumerable.Range(0, 32).Select(index =>
                client.CompareExchangeAsync("race", expected, new([existing ? (byte)100 : (byte)index])).AsTask()).ToArray();
            Assert.All(writes, write => Assert.False(write.IsCompleted));
            var results = await Task.WhenAll(writes);
            var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
            Assert.Equal(31, results.Count(result => result.Status == CompareExchangeStatus.Mismatch));
            Assert.DoesNotContain(results, result => result.Status == CompareExchangeStatus.Unknown);
            Assert.All(results, result => Assert.Equal(winner.Current, result.Current));
            var winnerIndex = Array.IndexOf(results, winner);
            Assert.Equal(new ByteString([existing ? (byte)100 : (byte)winnerIndex]), winner.Current!.Value);
            Assert.NotEqual(expected, winner.Current.Version);
            Assert.Equal(existing ? 2UL : 1UL, ReadRevision(winner.Current));
            Assert.Equal(winner.Current, await simulation.CreateClient(3).ReadAsync("race"));
        });
    }

    [Fact]
    public async Task FastModeRecoversWithTwoMembersDown()
    {
        await using var cluster = new RegisterSimulationCluster(733);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            var fast = await client.CompareExchangeAsync("key", null, new([1]));
            Assert.Equal(CompareExchangeStatus.Applied, fast.Status);
            Assert.Contains(simulation.Trace, entry => entry.Contains("commit node=1 fast=True", StringComparison.Ordinal));

            simulation.CrashNode(4);
            simulation.CrashNode(5);
            var recovery = await client.CompareExchangeAsync("key", fast.Current!.Version, new([2]));
            Assert.Equal(CompareExchangeStatus.Applied, recovery.Status);
            Assert.Equal(new ByteString([2]), recovery.Current!.Value);
            Assert.Contains(simulation.Trace, entry => entry.Contains("commit node=1 fast=False", StringComparison.Ordinal));
            simulation.RestartNode(4);
            simulation.RestartNode(5);
            Assert.Equal(recovery.Current, await simulation.CreateClient(5).ReadAsync("key"));
        });
    }

    [Fact]
    public async Task LostReplyIsUnknownEvenThoughTheWriteCommitted()
    {
        await using var cluster = new RegisterSimulationCluster(734);
        cluster.Run(async simulation =>
        {
            // Configure before creating the client: future clients must inherit reply loss.
            simulation.SetReplyLoss(1, true);
            var client = simulation.CreateClient();
            var start = simulation.TimeProvider.GetUtcNow();
            var lost = await client.CompareExchangeAsync("lost", null, new([42]));
            Assert.Equal(CompareExchangeStatus.Unknown, lost.Status);
            Assert.Null(lost.Current);
            Assert.IsType<RegisterUnavailableException>(lost.Error);
            Assert.Equal(start + simulation.OperationTimeout, simulation.TimeProvider.GetUtcNow());
            var committed = await simulation.CreateClient(2).ReadAsync("lost");
            Assert.Equal(new ByteString([42]), committed.Value);
            Assert.True(committed.IsPresent);
            Assert.Contains(simulation.Trace, entry =>
                entry.Contains("kind=ProposeResponse status=Partitioned", StringComparison.Ordinal));
            simulation.SetReplyLoss(1, false);
            var mismatch = await client.CompareExchangeAsync("lost", null, new([99]));
            Assert.Equal(CompareExchangeStatus.Mismatch, mismatch.Status);
            Assert.Equal(committed, mismatch.Current);
        });
    }

    [Fact]
    public async Task SupersededLostReplyIsNotReportedAsMismatch()
    {
        await using var cluster = new RegisterSimulationCluster(735);
        cluster.Run(async simulation =>
        {
            simulation.SetReplyLoss(1, true);
            var lost = simulation.CreateClient().CompareExchangeAsync("lost", null, new([1])).AsTask();
            await simulation.DelayAsync(TimeSpan.FromMilliseconds(50));
            Assert.False(lost.IsCompleted);
            var other = simulation.CreateClient(2);
            var committed = await other.ReadAsync("lost");
            Assert.Equal(new ByteString([1]), committed.Value);
            var replacement = await other.CompareExchangeAsync("lost", committed.Version, new([2]));
            Assert.Equal(CompareExchangeStatus.Applied, replacement.Status);
            var result = await lost;
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.IsType<RegisterUnavailableException>(result.Error);
            simulation.Heal();
            Assert.Equal(replacement.Current, await simulation.CreateClient(1).ReadAsync("lost"));
            var stale = await other.CompareExchangeAsync("lost", committed.Version, new([3]));
            Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
            Assert.Equal(replacement.Current, stale.Current);
        });
    }

    [Fact]
    public async Task DelayedConfirmedReplyRetainsItsVersionAfterSupersession()
    {
        await using var cluster = new RegisterSimulationCluster(736);
        cluster.Run(async simulation =>
        {
            simulation.SetReplyDelay(1, TimeSpan.FromMilliseconds(200));
            var delayed = simulation.CreateClient().CompareExchangeAsync("key", null, new([1])).AsTask();
            await simulation.DelayAsync(TimeSpan.FromMilliseconds(50));
            Assert.False(delayed.IsCompleted);
            var other = simulation.CreateClient(2);
            var before = await other.ReadAsync("key");
            Assert.Equal(new ByteString([1]), before.Value);
            var replacement = await other.CompareExchangeAsync("key", before.Version, new([2]));
            Assert.Equal(CompareExchangeStatus.Applied, replacement.Status);
            Assert.False(delayed.IsCompleted);
            var confirmed = await delayed;
            Assert.Equal(CompareExchangeStatus.Applied, confirmed.Status);
            Assert.Equal(before, confirmed.Current);
            Assert.NotEqual(confirmed.Current!.Version, replacement.Current!.Version);
            Assert.Equal(replacement.Current, await other.ReadAsync("key"));
        });
    }

    [Fact]
    public async Task LateReplyCannotCompleteANewerRequest()
    {
        await using var cluster = new RegisterSimulationCluster(737);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            simulation.SetReplyDelay(1, TimeSpan.FromSeconds(3));
            var timedOut = await client.CompareExchangeAsync("first", null, new([1]));
            Assert.Equal(CompareExchangeStatus.Unknown, timedOut.Status);
            simulation.SetReplyDelay(1, TimeSpan.Zero);
            var next = await client.CompareExchangeAsync("second", null, new([2]));
            Assert.Equal(CompareExchangeStatus.Applied, next.Status);
            await simulation.DelayAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(next.Current, await client.ReadAsync("second"));
            Assert.Equal(new ByteString([1]), (await client.ReadAsync("first")).Value);
            Assert.Contains(simulation.Trace, entry => entry.Contains("reply id=1 ", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task PreCanceledOperationsSendNoRequests()
    {
        await using var cluster = new RegisterSimulationCluster(738);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            var trace = simulation.Trace.ToArray();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.CompareExchangeAsync("key", null, new([1]), cancellation.Token).AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.ReadAsync("key", cancellation.Token).AsTask());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.ListKeysAsync(cancellationToken: cancellation.Token).AsTask());
            Assert.Equal(trace, simulation.Trace.ToArray());
            Assert.Equal(new RegisterValue(null, null), await client.ReadAsync("key"));
        });
    }

    [Fact]
    public async Task CancellationAfterDispatchPreservesUnknown()
    {
        await using var cluster = new RegisterSimulationCluster(739);
        cluster.Run(async simulation =>
        {
            simulation.SetReplyLoss(1, true);
            using var cancellation = new CancellationTokenSource();
            var pending = simulation.CreateClient().CompareExchangeAsync(
                "key", null, new([42]), cancellation.Token).AsTask();
            await simulation.DelayAsync(TimeSpan.FromMilliseconds(50));
            var committed = await simulation.CreateClient(2).ReadAsync("key");
            Assert.Equal(new ByteString([42]), committed.Value);
            cancellation.Cancel();
            var result = await pending;
            Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
            Assert.IsAssignableFrom<OperationCanceledException>(result.Error);
        });
    }

    [Fact]
    public async Task OutageAndAllMemberRestartRetainDurableState()
    {
        await using var cluster = new RegisterSimulationCluster(740);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            var created = await client.CompareExchangeAsync("durable", null, new([10, 20]));
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            for (var node = 1; node <= simulation.NodeCount; node++) simulation.CrashNode(node);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() => client.ReadAsync("durable").AsTask());
            await Assert.ThrowsAsync<RegisterUnavailableException>(() =>
                client.CompareExchangeAsync("not-written", null, new([99])).AsTask());
            for (var node = 1; node <= simulation.NodeCount; node++) simulation.RestartNode(node);
            Assert.Equal(created.Current, await simulation.CreateClient(5).ReadAsync("durable"));
            Assert.Equal(new RegisterValue(null, null), await client.ReadAsync("not-written"));
            var updated = await client.CompareExchangeAsync("durable", created.Current!.Version, new([30]));
            Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
            Assert.Equal(new ByteString([30]), updated.Current!.Value);
            Assert.NotEqual(created.Current.Version, updated.Current.Version);
        });
    }

    [Fact]
    public async Task PartitionAndHealUseBothPhysicalMemberRoles()
    {
        await using var cluster = new RegisterSimulationCluster(741);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            var created = await client.CompareExchangeAsync("key", null, new([1]));
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            for (var peer = 2; peer <= simulation.NodeCount; peer++) simulation.Partition(1, peer);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() => client.ReadAsync("key").AsTask());
            Assert.Contains(simulation.Trace, entry =>
                entry.Contains("kind=PrepareRequest status=Partitioned", StringComparison.Ordinal));
            var beforeMajority = simulation.Trace.Length;
            var majority = await simulation.CreateClient(2).ReadAsync("key");
            Assert.Equal(created.Current, majority);
            Assert.Contains(simulation.Trace.Skip(beforeMajority), entry =>
                entry.Contains("kind=AcceptRequest status=Partitioned", StringComparison.Ordinal));
            simulation.Heal();
            Assert.Equal(created.Current, await client.ReadAsync("key"));
        });
    }

    [Fact]
    public async Task OrdinalKeysAndPagingAreIsolated()
    {
        await using var cluster = new RegisterSimulationCluster(742);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            // Unsafe data is deliberately in the body, never theory display metadata.
            string[] keys = ["", "Key", "key", "key ", "key\0", "kéy", "ke\u0301y"];
            var values = new Dictionary<string, RegisterValue>(StringComparer.Ordinal);
            for (var index = 0; index < keys.Length; index++)
            {
                var result = await client.CompareExchangeAsync(keys[index], null, new([(byte)index]));
                Assert.Equal(CompareExchangeStatus.Applied, result.Status);
                Assert.Equal(1UL, ReadRevision(result.Current!));
                values.Add(keys[index], result.Current!);
            }
            var updated = await client.CompareExchangeAsync("key", values["key"].Version, new([99]));
            Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
            Assert.Equal(2UL, ReadRevision(updated.Current!));
            foreach (var key in keys)
                Assert.Equal(key == "key" ? updated.Current : values[key], await client.ReadAsync(key));

            var first = await client.ListKeysAsync(limit: 2);
            Assert.Equal<string>(["", "Key"], first.Keys);
            Assert.Equal("Key", first.ContinuationKey);
            var second = await client.ListKeysAsync(afterKey: first.ContinuationKey, limit: 2);
            Assert.Equal<string>(["key", "key\0"], second.Keys);
            Assert.Equal("key\0", second.ContinuationKey);
            var prefix = await client.ListKeysAsync("key", "key", 2);
            Assert.Equal<string>(["key\0", "key "], prefix.Keys);
            Assert.Null(prefix.ContinuationKey);
            Assert.Empty((await client.ListKeysAsync("missing")).Keys);
            Assert.Equal(keys.Order(StringComparer.Ordinal),
                (await client.ListKeysAsync(limit: int.MaxValue)).Keys);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.ListKeysAsync(limit: 0).AsTask());
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.ListKeysAsync(limit: -1).AsTask());
        });
    }

    [Fact]
    public async Task SameSeedReplaysProtocolTraceAndVirtualDelays()
    {
        async Task<string[]> Replay(int seed)
        {
            await using var cluster = new RegisterSimulationCluster(seed);
            Assert.Equal(seed, cluster.Seed);
            cluster.Run(async simulation =>
            {
                var start = simulation.TimeProvider.GetUtcNow();
                await simulation.DelayAsync(TimeSpan.FromMilliseconds(7));
                Assert.Equal(start + TimeSpan.FromMilliseconds(7), simulation.TimeProvider.GetUtcNow());
                simulation.SetLinkDelay(1, 2, TimeSpan.FromMilliseconds(10));
                simulation.Partition(1, 5);
                var client = simulation.CreateClient();
                var created = await client.CompareExchangeAsync("key", null, new([3]));
                Assert.Equal(CompareExchangeStatus.Applied, created.Status);
                simulation.Heal();
                simulation.CrashNode(3);
                simulation.RestartNode(3);
                Assert.Equal(created.Current, await simulation.CreateClient(3).ReadAsync("key"));
            });
            return cluster.Trace.ToArray();
        }

        var first = await Replay(743);
        Assert.Equal(first, await Replay(743));
        var changedSeed = await Replay(744);
        Assert.NotEqual(first, changedSeed);
        // Do not count just the seed header as evidence: protocol event timestamps also differ.
        Assert.NotEqual(first.Where(entry => entry.Contains("transport ", StringComparison.Ordinal)),
            changedSeed.Where(entry => entry.Contains("transport ", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task InvalidControlsAndUnscheduledWorkFailExplicitly()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RegisterSimulationCluster(1, 0));
        await using var cluster = new RegisterSimulationCluster(745);
        Assert.Throws<ArgumentOutOfRangeException>(() => cluster.CreateClient(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => cluster.CrashNode(6));
        Assert.Throws<ArgumentOutOfRangeException>(() => cluster.RestartNode(0));
        Assert.Throws<ArgumentException>(() => cluster.Partition(1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => cluster.SetLinkDelay(1, 2, TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => cluster.SetReplyDelay(1, TimeSpan.FromTicks(-1)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => cluster.DelayAsync(TimeSpan.Zero));
        var stalled = Assert.Throws<InvalidOperationException>(() =>
            cluster.Run(_ => new TaskCompletionSource().Task, maxIterations: 10));
        Assert.Contains("seed: 745", stalled.Message);
        Assert.Contains("stalled or escaped", stalled.Message);
        cluster.Run(async simulation =>
        {
            Assert.Throws<InvalidOperationException>(() => simulation.Run(_ => Task.CompletedTask));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => simulation.DelayAsync(TimeSpan.FromTicks(-1)));
            Assert.Equal(new RegisterValue(null, null), await simulation.CreateClient().ReadAsync("key"));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SeparateProposersPreserveGenuineContentionAmbiguity(bool samePayload)
    {
        await using var cluster = new RegisterSimulationCluster(746);
        cluster.Run(async simulation =>
        {
            var writes = Enumerable.Range(1, 5).Select(node =>
                simulation.CreateClient(node).CompareExchangeAsync("race", null,
                    new([samePayload ? (byte)42 : (byte)node])).AsTask()).ToArray();
            Assert.All(writes, write => Assert.False(write.IsCompleted));
            var results = await Task.WhenAll(writes);
            var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
            Assert.Equal(4, results.Count(result => result.Status == CompareExchangeStatus.Unknown));
            Assert.All(results, result =>
            {
                Assert.Null(result.Error);
                Assert.Equal(winner.Current, result.Current);
            });
            Assert.Equal(new ByteString([samePayload ? (byte)42 : (byte)(Array.IndexOf(results, winner) + 1)]),
                winner.Current!.Value);
            Assert.Equal(1UL, ReadRevision(winner.Current));
            Assert.Equal(winner.Current, await simulation.CreateClient().ReadAsync("race"));
        });
    }

    [Fact]
    public async Task ExpiredQueuedWriteCannotApplyAfterHealing()
    {
        await using var cluster = new RegisterSimulationCluster(747);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            for (var node = 2; node <= simulation.NodeCount; node++) simulation.Partition(1, node);
            var blockingRead = client.ReadAsync("key").AsTask();
            // The first request is pending in the real host before enqueueing the write.
            await simulation.DelayAsync(TimeSpan.FromMilliseconds(10));
            var queuedWrite = client.CompareExchangeAsync("key", null, new([99])).AsTask();
            await Assert.ThrowsAsync<RegisterUnavailableException>(() => blockingRead);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() => queuedWrite);
            simulation.Heal();
            // This read is queued behind the expired write; it must not observe a phantom create.
            Assert.Equal(new RegisterValue(null, null), await client.ReadAsync("key"));
            var created = await client.CompareExchangeAsync("key", null, new([42]));
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            Assert.Equal(new ByteString([42]), created.Current!.Value);
        });
    }

    [Fact]
    public async Task LostMismatchReplyIsUnavailableNotAnAmbiguousWrite()
    {
        await using var cluster = new RegisterSimulationCluster(748);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            var created = await client.CompareExchangeAsync("key", null, new([1]));
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            simulation.SetReplyLoss(1, true);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() =>
                client.CompareExchangeAsync("key", null, new([2])).AsTask());
            simulation.Heal();
            Assert.Equal(created.Current, await client.ReadAsync("key"));
        });
    }

    [Fact]
    public async Task LinkDelayAdvancesActualFastQuorumDelivery()
    {
        await using var cluster = new RegisterSimulationCluster(749);
        cluster.Run(async simulation =>
        {
            for (var peer = 3; peer <= 5; peer++)
                simulation.SetLinkDelay(1, peer, TimeSpan.FromMilliseconds(20));
            simulation.SetReplyDelay(1, TimeSpan.Zero);
            var start = simulation.TimeProvider.GetUtcNow();
            var result = await simulation.CreateClient().CompareExchangeAsync("key", null, new([42]));
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new ByteString([42]), result.Current!.Value);
            // Local + node 2 are insufficient for fastQ4. Two delayed members must ACK.
            Assert.InRange(simulation.TimeProvider.GetUtcNow() - start,
                TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(99));
            Assert.Contains(simulation.Trace, entry => entry.Contains("commit node=1 fast=True", StringComparison.Ordinal));
        });
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task VersionShapeIsValidForEverySeedAndMalformedTokensAreRejected(int seed)
    {
        await using var cluster = new RegisterSimulationCluster(seed);
        cluster.Run(async simulation =>
        {
            var client = simulation.CreateClient();
            var result = await client.CompareExchangeAsync("key", null, new([42]));
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.True(RegisterVersion.IsValid(result.Current!.Version!.Value));
            Assert.Equal(1UL, ReadRevision(result.Current));
            foreach (var malformed in new[] { default(ByteString), ByteString.Empty, new ByteString([1]), new ByteString(new byte[8]) })
                await Assert.ThrowsAsync<ArgumentException>(() =>
                    client.CompareExchangeAsync("key", malformed, new([99])).AsTask());
            Assert.Equal(result.Current, await client.ReadAsync("key"));
            await Assert.ThrowsAsync<ArgumentNullException>(() => client.ReadAsync(null!).AsTask());
            await Assert.ThrowsAsync<ArgumentNullException>(() => client.CompareExchangeAsync(null!, null, new([1])).AsTask());
            await Assert.ThrowsAsync<ArgumentException>(() => client.CompareExchangeAsync("key", null, default).AsTask());
            await Assert.ThrowsAsync<ArgumentNullException>(() => client.ListKeysAsync(null!).AsTask());
        });
    }

    [Fact]
    public async Task ContextFreeConsumerWrappersCompleteOnSimulationThreadAndReplayFaultTrace()
    {
        async Task<(string[] Protocol, string[] Consumer)> Replay()
        {
            await using var cluster = new RegisterSimulationCluster(750);
            var consumerTrace = new List<string>();
            cluster.Run(async simulation =>
            {
                var scenarioContext = SynchronizationContext.Current;
                var simulationThread = Environment.CurrentManagedThreadId;
                void Observe(string stage)
                {
                    Assert.Equal(simulationThread, Environment.CurrentManagedThreadId);
                    Assert.Null(SynchronizationContext.Current);
                    Assert.Same(TaskScheduler.Default, TaskScheduler.Current);
                    consumerTrace.Add($"{simulation.TimeProvider.GetTimestamp()} {stage}");
                }

                simulation.SetReplyDelay(1, TimeSpan.FromMilliseconds(25));
                var adapter = new ContextFreeRegisterAdapter(simulation.CreateClient(), Observe);
                var writes = Enumerable.Range(0, 8).Select(index =>
                    adapter.ReadThenWriteAsync("race", new([(byte)index])).AsTask()).ToArray();
                Assert.All(writes, write => Assert.False(write.IsCompleted));
                await simulation.DelayAsync(TimeSpan.FromMilliseconds(10));
                simulation.CrashNode(4);
                simulation.CrashNode(5);
                var results = await Task.WhenAll(writes);
                Assert.Same(scenarioContext, SynchronizationContext.Current);
                Assert.Equal(simulationThread, Environment.CurrentManagedThreadId);
                var winner = Assert.Single(results, result => result.Status == CompareExchangeStatus.Applied);
                Assert.Equal(7, results.Count(result => result.Status == CompareExchangeStatus.Mismatch));
                Assert.Equal(new ByteString([(byte)Array.IndexOf(results, winner)]), winner.Current!.Value);

                simulation.RestartNode(4);
                simulation.RestartNode(5);
                simulation.SetReplyLoss(1, true);
                var lost = adapter.CompareExchangeAsync("lost", null, new([42])).AsTask();
                var delayedRead = adapter.ReadAsync("race").AsTask();
                await simulation.DelayAsync(TimeSpan.FromMilliseconds(50));
                Assert.False(lost.IsCompleted);
                Assert.False(delayedRead.IsCompleted);
                var observer = new ContextFreeRegisterAdapter(simulation.CreateClient(2), Observe);
                Assert.Equal(new ByteString([42]), (await observer.ReadAsync("lost")).Value);
                var lostResult = await lost;
                Assert.Equal(CompareExchangeStatus.Unknown, lostResult.Status);
                Assert.IsType<RegisterUnavailableException>(lostResult.Error);
                await Assert.ThrowsAsync<RegisterUnavailableException>(() => delayedRead);
                simulation.Heal();
                Assert.Equal(winner.Current, await adapter.ReadAsync("race"));
                Assert.Same(scenarioContext, SynchronizationContext.Current);
                Assert.Equal(simulationThread, Environment.CurrentManagedThreadId);
            });
            Assert.Equal(36, consumerTrace.Count);
            return (cluster.Trace.ToArray(), consumerTrace.ToArray());
        }

        var expected = await Replay();
        for (var iteration = 0; iteration < 5; iteration++)
        {
            var replay = await Replay();
            Assert.Equal(expected.Protocol, replay.Protocol);
            Assert.Equal(expected.Consumer, replay.Consumer);
        }
    }

    [Fact]
    public async Task ContextFreeConsumerWrappersPreserveCancellationAndExecutionContext()
    {
        await using var cluster = new RegisterSimulationCluster(751);
        var logicalContext = new AsyncLocal<string?>();
        cluster.Run(async simulation =>
        {
            var scenarioContext = SynchronizationContext.Current;
            var simulationThread = Environment.CurrentManagedThreadId;
            var observed = new List<string>();
            Action<string> Probe(string expectedContext) => stage =>
            {
                Assert.Equal(simulationThread, Environment.CurrentManagedThreadId);
                Assert.Null(SynchronizationContext.Current);
                Assert.Equal(expectedContext, logicalContext.Value);
                observed.Add($"{expectedContext}:{stage}");
            };

            simulation.SetReplyDelay(1, TimeSpan.FromMilliseconds(25));
            using var cancellation = new CancellationTokenSource();
            logicalContext.Value = "canceled-reader";
            var canceled = new ContextFreeRegisterAdapter(simulation.CreateClient(), Probe("canceled-reader"))
                .ReadAsync("key", cancellation.Token).AsTask();
            logicalContext.Value = "inventory-reader";
            var inventory = new ContextFreeRegisterAdapter(simulation.CreateClient(), Probe("inventory-reader"))
                .ListKeysAsync().AsTask();
            logicalContext.Value = "scenario";

            await simulation.DelayAsync(TimeSpan.FromMilliseconds(1));
            Assert.False(canceled.IsCompleted);
            Assert.False(inventory.IsCompleted);
            cancellation.Cancel();
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
            Assert.Equal(cancellation.Token, error.CancellationToken);
            var page = await inventory;
            Assert.Empty(page.Keys);
            Assert.Null(page.ContinuationKey);
            Assert.Equal(["canceled-reader:adapter-read", "inventory-reader:adapter-list"], observed);
            Assert.Equal("scenario", logicalContext.Value);
            Assert.Same(scenarioContext, SynchronizationContext.Current);
            Assert.Equal(simulationThread, Environment.CurrentManagedThreadId);
        });
    }

    // Production-shaped wrappers: each returns an async ValueTask and opts out of
    // context capture. The second layer awaits Task-backed async wrapper completion,
    // not just the fixture's direct ValueTask, before issuing another register call.
    private sealed class ContextFreeRegisterAdapter(IRegisterService register, Action<string> observe)
    {
        public async ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            try
            {
                return await register.ReadAsync(key, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                observe("adapter-read");
            }
        }

        public async ValueTask<RegisterKeyPage> ListKeysAsync()
        {
            var result = await register.ListKeysAsync().ConfigureAwait(false);
            observe("adapter-list");
            return result;
        }

        public async ValueTask<CompareExchangeResult> CompareExchangeAsync(
            string key, ByteString? expected, ByteString value)
        {
            var result = await register.CompareExchangeAsync(key, expected, value).ConfigureAwait(false);
            observe("adapter-write");
            return result;
        }

        public async ValueTask<CompareExchangeResult> ReadThenWriteAsync(string key, ByteString value)
        {
            var previous = await ReadAsync(key).ConfigureAwait(false);
            observe("outer-read");
            var result = await CompareExchangeAsync(key, previous.Version, value).ConfigureAwait(false);
            observe("outer-write");
            return result;
        }
    }
}
