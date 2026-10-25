using FastCASPaxos.Host;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
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

    private static async Task WaitForLearnAsync(NodeEventJournal journal, string cursor, Ballot ballot)
    {
        using var timeout = new CancellationTokenSource(Watchdog);
        await foreach (var entry in journal.ReadAsync(cursor, Watchdog, timeout.Token))
            if (entry.Data is ProtocolObservation
                {
                    Operation: "learn", Direction: "inbound", Phase: "response-produced"
                } observation && observation.Key?.Text == "key" && observation.Ballot == ballot)
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

    private sealed class LearningCluster : IAsyncDisposable
    {
        public List<NodeRuntime> Nodes { get; } = [];
        private List<RepairEndpoint> Endpoints { get; } = [];
        private static readonly string[] Ids = ["a", "b", "c"];

        public static async Task<LearningCluster> OpenAsync(TestDirectory directory)
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
                    cluster.Endpoints.Add(await RepairEndpoint.StartAsync(acceptor, digests, new CountingRepairScheduler(),
                        learner: new ForwardingLearner(commit => cluster.Nodes[slot].Learner.Learn(commit))));
                }
                for (var index = 0; index < Ids.Length; index++)
                {
                    var options = DiagnosticHost.Options(directory, Ids[index], index + 1);
                    options.Mode = "fast";
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

    private sealed class ForwardingLearner(Action<RegisterCommit> forward) : IRegisterLearner
    {
        public void Learn(RegisterCommit commit) => forward(commit);
    }
}
