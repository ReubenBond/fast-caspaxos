using FastCASPaxos.Host;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using Grpc.Core;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using static FastCASPaxos.Grpc.Tests.FastHandoffContractTests;

namespace FastCASPaxos.Grpc.Tests;

public sealed class FastHandoffHostTests
{
    [Fact]
    public async Task NodeRuntimeBroadcastEnablesOneRoundAlternatingWritesAfterLearnArrival()
    {
        using var directory = new TestDirectory();
        await using var cluster = await LearningCluster.OpenAsync(directory);
        ByteString? version = null;
        int[] order = [0, 2, 1, 0, 2];
        for (var step = 0; step < order.Length; step++)
        {
            var selected = order[step];
            var cursors = cluster.Nodes.Select(node => node.Diagnostics.Cursor).ToArray();
            var node = cluster.Nodes[selected];
            var value = new ByteString([(byte)step]);

            var result = await node.Registers.CompareExchangeAsync("key", version, value).AsTask().WaitAsync(Watchdog);

            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new RegisterValue(value, Revision((ulong)step + 1)), result.Current);
            var observations = await SinceAsync(node.Diagnostics, cursors[selected]);
            var requests = observations.OfType<ProtocolObservation>()
                .Where(item => item.Phase == "request-initiated" && item.Key?.Text == "key"
                    && item.Operation is "accept" or "prepare" or "read").ToArray();
            Assert.Equal(3, requests.Length);
            Assert.All(requests, request =>
            {
                Assert.Equal("accept", request.Operation);
                Assert.Equal(new Ballot(step + 1, 0), request.Ballot);
            });
            Assert.Equal(3, requests.Select(request => request.PeerId).Distinct().Count());

            // Dissemination is asynchronous and does not add a write-response round.
            // The one-round claim begins after the peer has received the certificate.
            for (var index = 0; index < cluster.Nodes.Count; index++)
                if (index != selected)
                    await WaitForLearnAsync(cluster.Nodes[index].Diagnostics, cursors[index], new(step + 1, 0));
            version = result.Current!.Version;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NodeRuntimeWithoutLearnFastForwardsDurableLocalAcceptedOrPromiseOnlyState(bool promiseOnly)
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.Mode = "fast";
        await using var node = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
        var floor = new Ballot(20, 7);
        if (promiseOnly)
            Assert.True((await node.Acceptor.PrepareAsync("key", new(floor))).IsPromised);
        else
            Assert.True((await node.Acceptor.AcceptAsync("key",
                new(new(19, 0), Envelope(1, Payload), floor))).IsAccepted);
        var before = node.Diagnostics.Cursor;

        var result = await node.Registers.CompareExchangeAsync("key",
            promiseOnly ? null : Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(promiseOnly ? 1UL : 2UL)), result.Current);
        var events = await SinceAsync(node.Diagnostics, before);
        var requests = events.OfType<ProtocolObservation>()
            .Where(item => item.Phase == "request-initiated" && item.Key?.Text == "key").ToArray();
        var prepare = Assert.Single(requests, item => item.Operation == "prepare");
        Assert.True(prepare.Ballot > floor);
        var accept = Assert.Single(requests, item => item.Operation == "accept");
        Assert.Equal(prepare.Ballot, accept.Ballot);
        Assert.DoesNotContain(requests, item => item.Operation == "read");
    }

    [Fact]
    public async Task NodeRuntimeIncomingLearnDoesNotMutateDurableAcceptor()
    {
        using var directory = new TestDirectory();
        var options = DiagnosticHost.Options(directory);
        options.Mode = "fast";
        await using var node = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
        var promised = new Ballot(6, 0);
        Assert.True((await node.Acceptor.PrepareAsync("key", new(promised))).IsPromised);
        var before = await node.Inspection.InspectAsync("key");
        await using var endpoint = await RepairEndpoint.StartAsync(
            node.Acceptor, node.Digests, new CountingRepairScheduler(), learner: node.Learner);
        var client = new GrpcRegisterLearner(endpoint.Channel.CreateCallInvoker());
        var commit = new RegisterCommit("key", new(5, 0), Envelope(1, Payload), promised);
        var cursor = node.Diagnostics.Cursor;

        await client.LearnAsync(commit).AsTask().WaitAsync(Watchdog);

        await WaitForLearnAsync(node.Diagnostics, cursor, commit.AcceptedBallot);
        Assert.Equal(before, await node.Inspection.InspectAsync("key"));
    }

    private static async Task WaitForLearnAsync(NodeEventJournal journal, string cursor, Ballot ballot, string key = "key")
    {
        using var timeout = new CancellationTokenSource(Watchdog);
        await foreach (var entry in journal.ReadAsync(cursor, Watchdog, timeout.Token))
            if (entry.Data is ProtocolObservation
                {
                    Operation: "learn" or "learn-digest", Direction: "inbound", Phase: "response-produced", Outcome: "committed"
                } observation && observation.Key?.Text == key && observation.Ballot == ballot)
                return;
        Assert.Fail("The journal stopped before the expected learn arrival.");
    }

    private static async Task<List<object>> SinceAsync(NodeEventJournal journal, string cursor)
    {
        var end = journal.Cursor;
        var events = new List<object>();
        if (end == cursor) return events;
        using var timeout = new CancellationTokenSource(Watchdog);
        await foreach (var entry in journal.ReadAsync(cursor, Watchdog, timeout.Token))
        {
            Assert.NotEqual("reset", entry.Kind);
            events.Add(entry.Data);
            if (entry.Id == end) return events;
        }
        Assert.Fail("The journal stopped before its captured end cursor.");
        return events;
    }

    [Fact]
    [Trait("Category", "Loopback")]
    public async Task GatedLearnCoalescesLatestWithoutBlockingWritesOtherKeysOrHealthyPeers()
    {
        using var directory = new TestDirectory();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrivals = new ConcurrentQueue<LearningArrival>();
        var held = 0;
        await using var cluster = await LearningCluster.OpenAsync(directory, capacity: 2, beforeLearning: async (slot, message, context) =>
        {
            var arrival = LearningArrival.From(slot, message, context);
            arrivals.Enqueue(arrival);
            if (slot == 1 && arrival.Key == "key" && Interlocked.Increment(ref held) == 1)
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(context.CancellationToken);
            }
        });
        var source = cluster.Nodes[0];
        var cursors = cluster.Nodes.Select(node => node.Diagnostics.Cursor).ToArray();
        var previous = DiagnosticContext.Current;
        try
        {
            ByteString? version = null;
            string? latestOperationId = null;
            for (var step = 1; step <= 4; step++)
            {
                DiagnosticContext.Current = new("a", $"coalesced-{step}", null);
                var result = await source.Registers.CompareExchangeAsync("key", version, new([(byte)step])).AsTask().WaitAsync(Watchdog);
                Assert.Equal(CompareExchangeStatus.Applied, result.Status);
                Assert.Equal(new RegisterValue(new([(byte)step]), Revision((ulong)step)), result.Current);
                latestOperationId = Assert.IsType<OperationObservation>(source.Diagnostics.LastOperation!.Data).OperationId;
                version = result.Current!.Version;
                if (step == 1) await entered.Task.WaitAsync(Watchdog);
            }
            Assert.False(release.Task.IsCompleted);
            await WaitForLearnAsync(cluster.Nodes[2].Diagnostics, cursors[2], new(4, 0));
            DiagnosticContext.Current = new("a", "unrelated-key", null);
            var other = await source.Registers.CompareExchangeAsync("other", null, new([70])).AsTask().WaitAsync(Watchdog);
            Assert.Equal(new RegisterValue(new([70]), Revision(1)), other.Current);
            await WaitForLearnAsync(cluster.Nodes[1].Diagnostics, cursors[1], new(1, 0), "other");
            Assert.False(release.Task.IsCompleted); // Neither unrelated-key nor healthy-peer progress needed the gate.
            Assert.Single(arrivals, item => item.Node == 1 && item.Key == "key");
            release.TrySetResult();
            await WaitForLearnAsync(cluster.Nodes[1].Diagnostics, cursors[1], new(4, 0));
            var delivered = arrivals.Where(item => item.Node == 1 && item.Key == "key").ToArray();
            Assert.Equal([new Ballot(1, 0), new Ballot(4, 0)], delivered.Select(item => item.Ballot));
            Assert.All(delivered, item =>
            {
                Assert.True(item.Compact);
                Assert.Equal("0", item.Epoch);
            });
            Assert.Equal(latestOperationId, delivered[1].OperationId);
            Assert.Equal(new Ballot(5, 0), delivered[1].Prepared);
            var envelope = (await source.Inspection.InspectAsync("key"))!.AcceptedValue!.Value;
            Assert.Equal(new ByteString(SHA256.HashData(envelope.ToArray())), delivered[1].Hash);
            var observations = await SinceAsync(source.Diagnostics, cursors[0]);
            Assert.DoesNotContain(observations.OfType<ProtocolObservation>(),
                item => item.Error?.Contains("capacity", StringComparison.Ordinal) == true);
        }
        finally { release.TrySetResult(); DiagnosticContext.Current = previous; }
    }

    [Fact]
    [Trait("Category", "Loopback")]
    public async Task LearnCapacityCountsActiveKeysAndReportsOnlyTrueOverflow()
    {
        using var directory = new TestDirectory();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrivals = new ConcurrentQueue<LearningArrival>();
        var active = 0;
        var concurrency = new ConcurrentQueue<int>();
        var first = new ConcurrentDictionary<string, bool>();
        await using var cluster = await LearningCluster.OpenAsync(directory, capacity: 2, beforeLearning: async (slot, message, context) =>
        {
            var arrival = LearningArrival.From(slot, message, context);
            arrivals.Enqueue(arrival);
            if (slot != 1 || !first.TryAdd(arrival.Key, true)) return;
            var count = Interlocked.Increment(ref active);
            concurrency.Enqueue(count);
            if (count == 2) entered.TrySetResult();
            try { await release.Task.WaitAsync(context.CancellationToken); }
            finally { Interlocked.Decrement(ref active); }
        });
        var source = cluster.Nodes[0];
        var cursors = cluster.Nodes.Select(node => node.Diagnostics.Cursor).ToArray();
        var previous = DiagnosticContext.Current;
        try
        {
            foreach (var key in new[] { "key", "other" })
            {
                var result = await source.Registers.CompareExchangeAsync(key, null, Payload).AsTask().WaitAsync(Watchdog);
                Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
            }
            await entered.Task.WaitAsync(Watchdog);
            Assert.Equal(2, Volatile.Read(ref active));
            DiagnosticContext.Current = new("a", "actual-overflow", null);
            Assert.Equal(CompareExchangeStatus.Applied,
                (await source.Registers.CompareExchangeAsync("overflow", null, Payload).AsTask().WaitAsync(Watchdog)).Status);
            var overflowOperationId = Assert.IsType<OperationObservation>(source.Diagnostics.LastOperation!.Data).OperationId;
            DiagnosticContext.Current = new("a", "latest-pending", null);
            var latest = await source.Registers.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);
            var latestOperationId = Assert.IsType<OperationObservation>(source.Diagnostics.LastOperation!.Data).OperationId;
            Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), latest.Current);
            var latestState = (await source.Inspection.InspectAsync("key"))!;
            var observations = await SinceAsync(source.Diagnostics, cursors[0]);
            var drop = Assert.Single(observations.OfType<ProtocolObservation>(), item =>
                item.PeerId == "b" && item.Error?.Contains("capacity", StringComparison.Ordinal) == true);
            Assert.Equal("overflow", drop.Key!.Text);
            Assert.Equal(overflowOperationId, drop.OperationId);
            Assert.DoesNotContain(arrivals, item => item.Node == 1 && item.Key == "overflow");
            Assert.Equal(2, concurrency.Max());
            release.TrySetResult();
            await WaitForLearnAsync(cluster.Nodes[1].Diagnostics, cursors[1], latestState.AcceptedBallot);
            var delivered = arrivals.Where(item => item.Node == 1 && item.Key == "key").ToArray();
            Assert.Equal(2, delivered.Length);
            Assert.Equal(latestState.AcceptedBallot, delivered[1].Ballot);
            Assert.Equal(latestOperationId, delivered[1].OperationId);
            Assert.Equal(new ByteString(SHA256.HashData(latestState.AcceptedValue!.Value.ToArray())), delivered[1].Hash);
        }
        finally { release.TrySetResult(); DiagnosticContext.Current = previous; }
    }

    private sealed record LearningArrival(int Node, string Key, Ballot Ballot, Ballot Prepared, ByteString Hash,
        bool Compact, string? Epoch, string? OperationId)
    {
        public static LearningArrival From(int node, object request, ServerCallContext context)
        {
            if (request is Wire.RegisterCommitDigest compact)
                return new(node, compact.Key, new(compact.Accepted.Ballot.Round, compact.Accepted.Ballot.ProposerId),
                    new(compact.PreparedBallot.Round, compact.PreparedBallot.ProposerId), new(compact.Accepted.Sha256.ToByteArray()),
                    true, context.RequestHeaders.GetValue(EpochClientInterceptor.Header), DiagnosticContext.Current?.OperationId);
            var full = (Wire.RegisterCommit)request;
            return new(node, full.Key, new(full.Accepted.Ballot.Round, full.Accepted.Ballot.ProposerId),
                new(full.PreparedBallot.Round, full.PreparedBallot.ProposerId), new(SHA256.HashData(full.Accepted.Value.ToByteArray())),
                false, context.RequestHeaders.GetValue(EpochClientInterceptor.Header), DiagnosticContext.Current?.OperationId);
        }
    }

    [Fact]
    [Trait("Category", "Loopback")]
    public async Task ResetDiscardsOldPendingLearnWithoutRestampingItsEpoch()
    {
        using var directory = new TestDirectory();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arrivals = new ConcurrentQueue<LearningArrival>();
        await using var cluster = await LearningCluster.OpenAsync(directory, capacity: 2, beforeLearning: async (slot, message, context) =>
        {
            var arrival = LearningArrival.From(slot, message, context);
            arrivals.Enqueue(arrival);
            if (slot == 1 && arrival.Epoch == "0")
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(context.CancellationToken);
            }
        });
        try
        {
            var first = await cluster.Nodes[0].Registers.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, first.Status);
            await entered.Task.WaitAsync(Watchdog);
            var second = await cluster.Nodes[0].Registers.CompareExchangeAsync("key", first.Current!.Version, ByteString.Empty).AsTask().WaitAsync(Watchdog);
            Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), second.Current);
            Assert.Single(arrivals, item => item.Node == 1);
            var reset = HostLifecycleTests.NewReset();
            // The source cancels/drains its old workers before the receiver's
            // protocol lease is drained. All state belongs to this temporary fixture.
            foreach (var phase in new[] { "prepare", "clear", "activate" })
                foreach (var node in cluster.Nodes)
                    await node.ApplyResetPhaseAsync(reset, phase).WaitAsync(Watchdog);
            release.TrySetResult();
            foreach (var node in cluster.Nodes)
            {
                Assert.Equal("1", node.Administration.Epoch);
                Assert.Null(await node.Inspection.InspectAsync("key"));
            }
            var cursor = cluster.Nodes[1].Diagnostics.Cursor;
            var fresh = await cluster.Nodes[0].Registers.CompareExchangeAsync("key", null, new([90])).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, fresh.Status);
            Assert.Equal(new RegisterValue(new([90]), new ByteString([.. Revision(1).ToArray(), .. Revision(1).ToArray()])), fresh.Current);
            await WaitForLearnAsync(cluster.Nodes[1].Diagnostics, cursor, new(1, 0));
            var deliveries = arrivals.Where(item => item.Node == 1).ToArray();
            Assert.Equal(2, deliveries.Length);
            Assert.Equal("0", deliveries[0].Epoch);
            Assert.Equal("1", deliveries[1].Epoch);
            Assert.Equal(new Ballot(1, 0), deliveries[1].Ballot);
            var current = (await cluster.Nodes[1].Inspection.InspectAsync("key"))!;
            Assert.Equal(new ByteString(SHA256.HashData(current.AcceptedValue!.Value.ToArray())), deliveries[1].Hash);
        }
        finally { release.TrySetResult(); }
    }

    [Fact]
    [Trait("Category", "Loopback")]
    public async Task CompactDeliveryRequiresExactConsumedAckAndCarriesOnlyConfirmedPromise()
    {
        using var directory = new TestDirectory();
        var arrivals = new ConcurrentQueue<LearningArrival>();
        await using var cluster = await LearningCluster.OpenAsync(directory, capacity: 2, beforeLearning: (slot, message, context) =>
        {
            arrivals.Enqueue(LearningArrival.From(slot, message, context));
            return Task.CompletedTask;
        });
        // Remote floors force recovery from the initial fast attempt. Do not seed
        // the sender's local floor: that would choose a different initial path.
        foreach (var node in cluster.Nodes.Skip(1))
            Assert.True((await node.Acceptor.PrepareAsync("key", new(new(10, 7)))).IsPromised);
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Acceptors[2].Accept = async (key, request, token) =>
        {
            var result = await cluster.Nodes[2].Acceptor.AcceptAsync(key, request, token);
            if (result.IsAccepted && request.Ballot.IsClassicRoundBallot)
            {
                arrived.TrySetResult();
                await release.Task.WaitAsync(token);
            }
            return result;
        };
        var cursors = cluster.Nodes.Select(node => node.Diagnostics.Cursor).ToArray();
        var previous = DiagnosticContext.Current;
        DiagnosticContext.Current = new("a", "ack-eligibility", null);
        try
        {
            var write = await cluster.Nodes[0].Registers.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, write.Status);
            var operationId = Assert.IsType<OperationObservation>(cluster.Nodes[0].Diagnostics.LastOperation!.Data).OperationId;
            await arrived.Task.WaitAsync(Watchdog);
            var state = (await cluster.Nodes[0].Inspection.InspectAsync("key"))!;
            Assert.True(state.AcceptedBallot.IsClassicRoundBallot);
            await WaitForLearnAsync(cluster.Nodes[1].Diagnostics, cursors[1], state.AcceptedBallot);
            await WaitForLearnAsync(cluster.Nodes[2].Diagnostics, cursors[2], state.AcceptedBallot);
            var compact = Assert.Single(arrivals, item => item.Node == 1);
            var full = Assert.Single(arrivals, item => item.Node == 2);
            Assert.True(compact.Compact);
            Assert.False(full.Compact); // c accepted bytes durably, but its ACK was withheld.
            var hash = new ByteString(SHA256.HashData(state.AcceptedValue!.Value.ToArray()));
            foreach (var delivery in new[] { compact, full })
            {
                Assert.Equal(state.AcceptedBallot, delivery.Ballot);
                Assert.Equal(hash, delivery.Hash);
                Assert.Equal(FastCASPaxos.Model.Ballot.Zero, delivery.Prepared);
                Assert.Equal(operationId, delivery.OperationId);
                Assert.Equal("0", delivery.Epoch);
            }
            Assert.False(release.Task.IsCompleted);
        }
        finally { release.TrySetResult(); DiagnosticContext.Current = previous; }
    }

    private sealed class LearningCluster : IAsyncDisposable
    {
        public List<NodeRuntime> Nodes { get; } = [];
        public List<DelegateAcceptor> Acceptors { get; } = [];
        private List<RepairEndpoint> Endpoints { get; } = [];
        private static readonly string[] Ids = ["a", "b", "c"];

        public static async Task<LearningCluster> OpenAsync(TestDirectory directory, int? capacity = null,
            Func<int, object, ServerCallContext, Task>? beforeLearning = null)
        {
            var cluster = new LearningCluster();
            try
            {
                for (var index = 0; index < Ids.Length; index++)
                {
                    var slot = index;
                    var acceptor = new DelegateAcceptor
                    {
                        Read = (key, known, token) => cluster.Nodes[slot].Acceptor.ReadAsync(key, known, token),
                        Prepare = (key, request, token) => cluster.Nodes[slot].Acceptor.PrepareAsync(key, request, token),
                        Accept = (key, request, token) => cluster.Nodes[slot].Acceptor.AcceptAsync(key, request, token),
                        List = (prefix, after, limit, token) => cluster.Nodes[slot].Acceptor.ListKeysAsync(prefix, after, limit, token)
                    };
                    var digests = new DelegateDigests
                    {
                        List = (prefix, after, limit, token) => cluster.Nodes[slot].Digests.ListDigestsAsync(prefix, after, limit, token)
                    };
                    cluster.Acceptors.Add(acceptor);
                    cluster.Endpoints.Add(await RepairEndpoint.StartAsync(acceptor, digests, new CountingRepairScheduler(),
                        learner: new ForwardingLearner(() => cluster.Nodes[slot]),
                        epochNode: () => cluster.Nodes[slot],
                        beforeLearning: beforeLearning is null ? null : (request, context) => beforeLearning(slot, request, context)));
                }
                for (var index = 0; index < Ids.Length; index++)
                {
                    var options = DiagnosticHost.Options(directory, Ids[index], index + 1);
                    options.Mode = "fast";
                    if (capacity is { } limit) options.Capacity = limit;
                    options.PeerCallTimeout = Watchdog;
                    options.Peers = [.. cluster.Endpoints.Select((endpoint, slot) => new PeerOptions
                    {
                        Id = Ids[slot], Endpoint = endpoint.Address,
                        HttpEndpoint = $"http://127.0.0.1:{11 + slot}"
                    })];
                    cluster.Nodes.Add(await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog));
                }
                return cluster;
            }
            catch
            {
                await cluster.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var node in Nodes) await node.DisposeAsync().AsTask().WaitAsync(Watchdog);
            foreach (var endpoint in Endpoints) await endpoint.DisposeAsync();
        }
    }

    private sealed class ForwardingLearner(Func<NodeRuntime> node) : IRegisterLearner, IRegisterDigestLearner
    {
        public void Learn(RegisterCommit commit) => node().Learn(commit);
        public ValueTask<bool> LearnDigestAsync(RegisterCommitDigest commit, CancellationToken cancellationToken = default) =>
            node().LearnDigestAsync(commit, cancellationToken);
    }
}
