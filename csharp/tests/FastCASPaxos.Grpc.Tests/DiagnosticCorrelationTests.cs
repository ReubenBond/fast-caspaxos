using System.Text;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class DiagnosticCorrelationTests
{
    [Fact]
    public async Task ClientInterceptorCopiesHeadersAndPreservesCallOptionsWithoutAmbientLeakage()
    {
        var original = new Metadata { { "authorization", "test-auth" }, { "custom-bin", new byte[] { 1, 2 } } };
        using var cancellation = new CancellationTokenSource();
        var deadline = DateTime.UtcNow.AddMinutes(1);
        var diagnostic = new DiagnosticContext("中-node", Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N"));
        var stub = StubCallInvoker.Reply(new Wire.PrepareResponse
        {
            Promised = Ballot(3, 1)
        });
        var client = new Wire.Acceptor.AcceptorClient(stub.Intercept(new DiagnosticClientInterceptor()));
        DiagnosticContext.Current = diagnostic;
        try
        {
            using var call = client.PrepareAsync(new Wire.PrepareRequest { Key = "key", Ballot = Ballot() },
                original, deadline, cancellation.Token);
            await call.ResponseAsync;
            var observed = Assert.Single(stub.Calls).Options;
            Assert.Equal(deadline, observed.Deadline);
            Assert.Equal(cancellation.Token, observed.CancellationToken);
            Assert.Equal("test-auth", observed.Headers!.GetValue("authorization"));
            Assert.Equal(new byte[] { 1, 2 }, observed.Headers.GetValueBytes("custom-bin"));
            Assert.Equal(Encoding.UTF8.GetBytes("中-node"), observed.Headers.GetValueBytes("fcp-node-bin"));
            Assert.Equal(diagnostic.OperationId, observed.Headers.GetValue("fcp-operation"));
            Assert.Equal(diagnostic.CallId, observed.Headers.GetValue("fcp-call"));
            Assert.Equal(2, original.Count);
            Assert.Null(original.GetValueBytes("fcp-node-bin"));
            Assert.Same(diagnostic, DiagnosticContext.Current);
        }
        finally { DiagnosticContext.Current = null; }
        using var without = client.PrepareAsync(new Wire.PrepareRequest { Key = "key", Ballot = Ballot() }, original);
        await without.ResponseAsync;
        Assert.Equal(2, stub.Calls.Count);
        Assert.Same(original, stub.Calls.Last().Options.Headers);
        Assert.Null(stub.Calls.Last().Options.Headers!.GetValue("fcp-call"));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ServerInterceptorBoundsUntrustedCorrelationAndRestoresContext(bool invalid, bool fail)
    {
        var previous = new DiagnosticContext("outer", "outer-operation", "outer-call");
        var operation = Guid.NewGuid().ToString("N");
        var call = Guid.NewGuid().ToString("N");
        var headers = new Metadata
        {
            { "fcp-node-bin", invalid ? new byte[1025] : Encoding.UTF8.GetBytes("中") },
            { "fcp-operation", invalid ? "bad-id" : operation },
            { "fcp-call", invalid ? new string('x', 300) : call }
        };
        var context = new TestServerCallContext(new CallOptions(headers));
        var interceptor = new DiagnosticServerInterceptor();
        var expected = new object();
        var error = new IOException("transport fault");
        DiagnosticContext.Current = previous;
        try
        {
            async Task<object> Continuation(object request, ServerCallContext received)
            {
                Assert.Same(expected, request);
                Assert.Same(context, received);
                await Task.Yield();
                Assert.Equal(invalid ? "" : "中", DiagnosticContext.Current!.NodeId);
                Assert.Equal(invalid ? null : operation, DiagnosticContext.Current.OperationId);
                Assert.Equal(invalid ? null : call, DiagnosticContext.Current.CallId);
                if (fail) throw error;
                return request;
            }
            if (fail)
                Assert.Same(error, await Record.ExceptionAsync(() =>
                    interceptor.UnaryServerHandler<object, object>(expected, context, Continuation)));
            else
                Assert.Same(expected, await interceptor.UnaryServerHandler<object, object>(expected, context, Continuation));
            Assert.Same(previous, DiagnosticContext.Current);
        }
        finally { DiagnosticContext.Current = null; }
    }

    [Fact]
    public async Task RealHttp2TwoHostsLinkLocalOutboundInboundAndQuorumOperation()
    {
        using var directory = new TestDirectory();
        await using var remote = await DiagnosticHost.Start(DiagnosticHost.Options(directory, "b", 2));
        var options = DiagnosticHost.Options(directory);
        options.Peers.Add(new() { Id = "b", Endpoint = remote.Address, HttpEndpoint = "http://127.0.0.1:12" });
        await using var local = await DiagnosticHost.Start(options);
        var localCursor = local.Node.Diagnostics.Cursor;
        var remoteCursor = remote.Node.Diagnostics.Cursor;
        // Two members require both votes, so every correlated response is complete before Applied.
        var result = await local.Node.Registers.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(Payload, result.Current!.Value);
        var last = Assert.IsType<NodeEvent>(local.Node.Diagnostics.LastOperation);
        var operation = Assert.IsType<OperationObservation>(last.Data);
        Assert.Equal("applied", operation.Outcome);
        var localEvents = await History(local.Node.Diagnostics, localCursor);
        var remoteEvents = await History(remote.Node.Diagnostics, remoteCursor);
        var outbound = localEvents.Select(e => e.Data).OfType<ProtocolObservation>().Where(p => p.Direction == "outbound").ToArray();
        var inbound = remoteEvents.Select(e => e.Data).OfType<ProtocolObservation>().ToArray();
        Assert.NotEmpty(outbound);
        Assert.Equal(outbound.Length, inbound.Length);
        foreach (var sent in outbound)
        {
            var received = Assert.Single(inbound, p => p.CallId == sent.CallId &&
                p.Phase == (sent.Phase == "request-initiated" ? "request-received" : "response-produced"));
            Assert.Equal("inbound", received.Direction);
            Assert.Equal("a", received.PeerId);
            Assert.Equal("b", sent.PeerId);
            Assert.Equal(operation.OperationId, sent.OperationId);
            Assert.Equal(operation.OperationId, received.OperationId);
            Assert.Equal(sent.Operation, received.Operation);
            Assert.Equal(sent.Key, received.Key);
            Assert.Equal(sent.Ballot, received.Ballot);
            Assert.Equal(sent.NextBallot, received.NextBallot);
            Assert.Equal(sent.Outcome, received.Outcome);
        }
        var localProtocol = localEvents.Select(e => e.Data).OfType<ProtocolObservation>().Where(p => p.Direction == "local").ToArray();
        Assert.NotEmpty(localProtocol);
        Assert.All(localProtocol, p =>
        {
            Assert.Equal(operation.OperationId, p.OperationId);
            Assert.Equal("a", p.PeerId);
        });
        Assert.Contains(localProtocol, p => p.Operation == "prepare" && p.Phase == "response-observed" && p.Outcome == "promised");
        Assert.Contains(localProtocol, p => p.Operation == "accept" && p.Phase == "response-observed" && p.Outcome == "accepted");
        Assert.Contains(remoteEvents, e => e.Kind == "acceptor-state");
        Assert.DoesNotContain(remoteEvents, e => e.Kind == "operation");
        Assert.Null(remote.Node.Diagnostics.LastOperation);
        Assert.Null(DiagnosticContext.Current);
    }

    [Fact]
    public async Task ConcurrentHttp2CallsDoNotCrossContaminateOperationOrCallIds()
    {
        using var directory = new TestDirectory();
        await using var remote = await DiagnosticHost.Start(DiagnosticHost.Options(directory, "b", 2));
        using var channel = GrpcChannel.ForAddress(remote.Address);
        using var local = new NodeEventJournal("a");
        var acceptor = new ObservedAcceptor(new GrpcKeyedAcceptor(
            channel.CreateCallInvoker().Intercept(new DiagnosticClientInterceptor())), local, "outbound", "b");
        var before = remote.Node.Diagnostics.Cursor;
        var operations = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid().ToString("N")).ToArray();
        await Task.WhenAll(operations.Select((id, index) => Invoke(id, index))).WaitAsync(Watchdog);
        var received = (await History(remote.Node.Diagnostics, before)).Select(e => e.Data).OfType<ProtocolObservation>().ToArray();
        Assert.Equal(6, received.Length);
        for (var i = 0; i < operations.Length; i++)
        {
            var pair = received.Where(p => p.OperationId == operations[i]).ToArray();
            Assert.Equal(2, pair.Length);
            Assert.All(pair, p => Assert.Equal("key-" + i, p.Key!.Text));
            Assert.Equal(pair[0].CallId, pair[1].CallId);
        }
        Assert.Equal(3, received.Select(p => p.CallId).Distinct().Count());
        Assert.Null(DiagnosticContext.Current);

        async Task Invoke(string id, int index)
        {
            DiagnosticContext.Current = new("a", id, null);
            try
            {
                Assert.True((await acceptor.PrepareAsync("key-" + index,
                    new(new FastCASPaxos.Model.Ballot(3, index + 1)))).IsPromised);
            }
            finally { DiagnosticContext.Current = null; }
        }
    }

    private static async Task<List<NodeEvent>> History(NodeEventJournal journal, string cursor)
    {
        var end = journal.Cursor;
        var events = new List<NodeEvent>();
        if (end == cursor) return events;
        await using var reader = journal.ReadAsync(cursor, TimeSpan.FromDays(1)).GetAsyncEnumerator();
        while (events.Count < journal.Capacity)
        {
            Assert.True(await reader.MoveNextAsync().AsTask().WaitAsync(Watchdog));
            Assert.NotEqual("reset", reader.Current.Kind);
            events.Add(reader.Current);
            if (reader.Current.Id == end) return events;
        }
        throw new InvalidOperationException("History exceeded the bounded trace.");
    }
}
