using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server.Diagnostics;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;
using static FastCASPaxos.Server.Tests.DiagnosticJournalTests;

namespace FastCASPaxos.Server.Tests;

public sealed class DiagnosticObserverTests
{
    [Theory]
    [InlineData("local", false)]
    [InlineData("local", true)]
    [InlineData("outbound", false)]
    [InlineData("outbound", true)]
    [InlineData("inbound", false)]
    [InlineData("inbound", true)]
    public async Task Acceptor_PrepareAndAcceptPreserveResultsArgumentsAndCorrelation(string direction, bool reject)
    {
        using var journal = new NodeEventJournal("a");
        var before = journal.Cursor;
        var previous = new DiagnosticContext("caller", Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N"));
        DiagnosticContext.Current = previous;
        try
        {
            using var token = new CancellationTokenSource();
            var ballot = new Ballot(7, 3);
            var next = new Ballot(9, 4);
            var prepared = new AcceptorPrepareResult<ByteString?>(new(5, 2), Payload, reject ? next : Ballot.Zero);
            var accepted = reject ? AcceptorAcceptResult.Rejected(next) : AcceptorAcceptResult.Accepted(next);
            var seen = new List<DiagnosticContext>();
            var inner = new ControlledPeer
            {
                Prepare = (key, request, ct) =>
                {
                    Assert.Equal("key", key);
                    Assert.Equal(ballot, request.Ballot);
                    Assert.Equal(token.Token, ct);
                    seen.Add(DiagnosticContext.Current!);
                    return ValueTask.FromResult(prepared);
                },
                Accept = (key, request, ct) =>
                {
                    Assert.Equal("key", key);
                    Assert.Equal(new AcceptRequest<ByteString>(ballot, Payload, next), request);
                    Assert.Equal(token.Token, ct);
                    seen.Add(DiagnosticContext.Current!);
                    return ValueTask.FromResult(accepted);
                }
            };
            var observed = new ObservedAcceptor(inner, journal, direction, "peer-b");
            Assert.Equal(prepared, await observed.PrepareAsync("key", new(ballot), token.Token));
            Assert.Same(previous, DiagnosticContext.Current);
            Assert.Equal(accepted, await observed.AcceptAsync("key", new(ballot, Payload, next), token.Token));
            Assert.Same(previous, DiagnosticContext.Current);
            await using var reader = journal.ReadAsync(before, TimeSpan.FromDays(1)).GetAsyncEnumerator();
            for (var operation = 0; operation < 2; operation++)
            {
                var request = Assert.IsType<ProtocolObservation>((await Next(reader)).Data);
                var response = Assert.IsType<ProtocolObservation>((await Next(reader)).Data);
                Assert.Equal(operation == 0 ? "prepare" : "accept", request.Operation);
                Assert.Equal(request.Operation, response.Operation);
                Assert.Equal(direction, request.Direction);
                Assert.Equal(direction, response.Direction);
                Assert.Equal(direction == "inbound" ? "request-received" : "request-initiated", request.Phase);
                Assert.Equal(direction == "inbound" ? "response-produced" : "response-observed", response.Phase);
                Assert.Equal(direction == "inbound" ? "caller" : "peer-b", response.PeerId);
                Assert.Equal(previous.OperationId, response.OperationId);
                Assert.Equal(seen[operation].CallId, request.CallId);
                Assert.Equal(request.CallId, response.CallId);
                Assert.Equal(new KeyPreview("key", 3, false), response.Key);
                Assert.Equal(ballot, response.Ballot);
                Assert.Equal(operation == 0 ? Ballot.Zero : next, response.NextBallot);
                Assert.Equal(reject ? "rejected" : operation == 0 ? "promised" : "accepted", response.Outcome);
                Assert.Equal(reject ? next : Ballot.Zero, response.ConflictingBallot);
                if (operation == 0)
                {
                    Assert.Equal(new Ballot(5, 2), response.AcceptedBallot);
                    Assert.Equal(PayloadPreview.From(Payload), response.Value);
                    Assert.Equal(reject ? (Ballot?)null : ballot, response.PromisedBallot);
                }
                else
                {
                    Assert.Equal(reject ? Ballot.Zero : next, response.PromisedBallot);
                    Assert.Equal(PayloadPreview.From(Payload), request.Value);
                }
            }
            if (direction == "inbound") Assert.All(seen, c => Assert.Equal(previous.CallId, c.CallId));
            else Assert.NotEqual(seen[0].CallId, seen[1].CallId);
        }
        finally { DiagnosticContext.Current = null; }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Acceptor_FaultAndCancellationPreserveExceptionAndRestoreContext(bool accept, bool cancel)
    {
        using var journal = new NodeEventJournal("a");
        var cursor = journal.Cursor;
        using var token = new CancellationTokenSource();
        token.Cancel();
        Exception error = cancel ? new OperationCanceledException(token.Token) : new IOException("injected");
        var inner = new ControlledPeer
        {
            Prepare = (_, _, ct) =>
            {
                Assert.Equal(token.Token, ct);
                return ValueTask.FromException<AcceptorPrepareResult<ByteString?>>(error);
            },
            Accept = (_, _, ct) =>
            {
                Assert.Equal(token.Token, ct);
                return ValueTask.FromException<AcceptorAcceptResult>(error);
            }
        };
        var observed = new ObservedAcceptor(inner, journal, "outbound", "b");
        var caught = await Record.ExceptionAsync(async () =>
        {
            if (accept) await observed.AcceptAsync("k", new(new(2, 1), Payload, new(3, 1)), token.Token);
            else await observed.PrepareAsync("k", new(new Ballot(2, 1)), token.Token);
        });
        Assert.Same(error, caught);
        Assert.Null(DiagnosticContext.Current);
        await using var reader = journal.ReadAsync(cursor, TimeSpan.FromDays(1)).GetAsyncEnumerator();
        var request = Assert.IsType<ProtocolObservation>((await Next(reader)).Data);
        var fault = Assert.IsType<ProtocolObservation>((await Next(reader)).Data);
        Assert.Equal("fault-observed", fault.Phase);
        Assert.Equal(error.GetType().Name, fault.Error);
        Assert.Equal(request.CallId, fault.CallId);
        Assert.Equal(accept ? "accept" : "prepare", fault.Operation);
        Assert.Equal($"{journal.BootId}:2", journal.Cursor);
    }

    [Fact]
    public async Task Acceptor_DuplicateAcceptanceRemainsSuccessfulAndListingPassesThrough()
    {
        using var journal = new NodeEventJournal("a");
        var inner = new ControlledPeer();
        var observed = new ObservedAcceptor(inner, journal, "local", "a");
        var original = await observed.AcceptAsync("k", new(new(2, 1), Payload, new(3, 1)));
        var retry = await observed.AcceptAsync("k", new(new(2, 1), Payload, new(99, 7)));
        Assert.Equal(original, retry);
        Assert.Equal(new Ballot(3, 1), retry.PromisedBallot);
        var cursor = journal.Cursor;
        var expected = new KeyPage([], "next");
        using var token = new CancellationTokenSource();
        inner.List = (prefix, after, limit, ct) =>
        {
            Assert.Equal("prefix", prefix);
            Assert.Equal("after", after);
            Assert.Equal(7, limit);
            Assert.Equal(token.Token, ct);
            return ValueTask.FromResult(expected);
        };
        Assert.Same(expected, await observed.ListKeysAsync("prefix", "after", 7, token.Token));
        Assert.Equal(cursor, journal.Cursor);
    }

    public enum CurrentKind { None, Absent, PresentEmpty, PresentBytes }

    [Theory]
    [InlineData(CompareExchangeStatus.Applied, CurrentKind.PresentBytes)]
    [InlineData(CompareExchangeStatus.Mismatch, CurrentKind.PresentBytes)]
    [InlineData(CompareExchangeStatus.Unknown, CurrentKind.PresentBytes)]
    [InlineData(CompareExchangeStatus.Unknown, CurrentKind.None)]
    [InlineData(CompareExchangeStatus.Unknown, CurrentKind.Absent)]
    [InlineData(CompareExchangeStatus.Unknown, CurrentKind.PresentEmpty)]
    public async Task Register_CompareExchangePreservesOutcomeCurrentErrorAndOperationContext(
        CompareExchangeStatus status, CurrentKind currentKind)
    {
        using var journal = new NodeEventJournal("a");
        RegisterValue? current = currentKind switch
        {
            CurrentKind.None => null,
            CurrentKind.Absent => new(null, null),
            CurrentKind.PresentEmpty => new(ByteString.Empty, Version(4, 9)),
            _ => new(Payload, Version(4, 9))
        };
        var error = new IOException("uncertain");
        var result = new CompareExchangeResult(status, current, status == CompareExchangeStatus.Unknown ? error : null);
        var inner = new RegisterStub { Result = result };
        var previous = new DiagnosticContext("upstream", "operation", "call");
        DiagnosticContext.Current = previous;
        try
        {
            using var cancellation = new CancellationTokenSource();
            var observed = new ObservedRegisterService(inner, journal);
            Assert.Same(result, await observed.CompareExchangeAsync("key", Version(1, 2), Payload, cancellation.Token));
            Assert.Equal(("key", Version(1, 2), Payload, cancellation.Token), inner.Arguments);
            Assert.Same(previous, DiagnosticContext.Current);
            var operation = LastOperation(journal);
            Assert.Equal(status.ToString().ToLowerInvariant(), operation.Outcome);
            Assert.Equal("compare-exchange", operation.Operation);
            Assert.Equal(inner.Context!.OperationId, operation.OperationId);
            Assert.NotEqual(previous.OperationId, operation.OperationId);
            Assert.Equal("a", inner.Context.NodeId);
            Assert.Null(inner.Context.CallId);
            Assert.Equal(currentKind is CurrentKind.PresentBytes or CurrentKind.PresentEmpty
                ? Convert.ToBase64String(Version(4, 9).ToArray()) : null, operation.Version);
            Assert.Equal(currentKind switch
            {
                CurrentKind.PresentBytes => new PayloadPreview(3, "ChQe", false),
                CurrentKind.PresentEmpty => new PayloadPreview(0, "", false),
                _ => null
            }, operation.Value);
            Assert.Equal(currentKind == CurrentKind.None ? (bool?)null : currentKind != CurrentKind.Absent, operation.IsPresent);
            Assert.Equal(result.Error?.GetType().Name, operation.Error);
            Assert.Equal("starting", journal.Storage.Status);
        }
        finally { DiagnosticContext.Current = null; }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Register_ReadAndListPreserveObjectsAndForwardArguments(bool absent)
    {
        using var journal = new NodeEventJournal("a");
        var inner = new RegisterStub { Current = absent ? new(null, null) : new(Payload, Version(1, 2)) };
        var observed = new ObservedRegisterService(inner, journal);
        using var cancellation = new CancellationTokenSource();
        Assert.Same(inner.Current, await observed.ReadAsync("key", cancellation.Token));
        Assert.Equal(("key", cancellation.Token), inner.ReadArguments);
        Assert.Equal("established", LastOperation(journal).Outcome);
        Assert.Equal("read", LastOperation(journal).Operation);
        Assert.Equal(!absent, LastOperation(journal).IsPresent);
        Assert.Equal(absent ? null : PayloadPreview.From(Payload), LastOperation(journal).Value);
        Assert.Equal(absent ? null : Convert.ToBase64String(Version(1, 2).ToArray()), LastOperation(journal).Version);
        Assert.Equal(new KeyPreview("key", 3, false), LastOperation(journal).Key);
        var readOperation = LastOperation(journal).OperationId;
        Assert.Same(inner.Page, await observed.ListKeysAsync("p", "after", 3, cancellation.Token));
        Assert.Equal(("p", "after", 3, cancellation.Token), inner.ListArguments);
        Assert.Equal("completed", LastOperation(journal).Outcome);
        Assert.Equal("list", LastOperation(journal).Operation);
        Assert.Equal(new KeyPreview("p", 1, false), LastOperation(journal).Key);
        Assert.NotEqual(readOperation, LastOperation(journal).OperationId);
        Assert.Null(LastOperation(journal).Value);
        Assert.Null(LastOperation(journal).Version);
        Assert.Null(LastOperation(journal).IsPresent);
        Assert.Null(DiagnosticContext.Current);
    }

    [Theory]
    [InlineData("read", false)]
    [InlineData("read", true)]
    [InlineData("list", false)]
    [InlineData("list", true)]
    [InlineData("compare-exchange", false)]
    [InlineData("compare-exchange", true)]
    public async Task Register_FaultAndCancellationAreNotConvertedIntoSuccess(string operation, bool cancel)
    {
        using var journal = new NodeEventJournal("a");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Exception error = cancel ? new OperationCanceledException(cancellation.Token) : new IOException("failure");
        var inner = new RegisterStub { Failure = error };
        var observed = new ObservedRegisterService(inner, journal);
        Assert.Same(error, await Record.ExceptionAsync(async () =>
        {
            switch (operation)
            {
                case "read": await observed.ReadAsync("key", cancellation.Token); break;
                case "list": await observed.ListKeysAsync("key", cancellationToken: cancellation.Token); break;
                default: await observed.CompareExchangeAsync("key", null, Payload, cancellation.Token); break;
            }
        }));
        Assert.Equal("fault", LastOperation(journal).Outcome);
        Assert.Equal(operation, LastOperation(journal).Operation);
        Assert.Equal(error.GetType().Name, LastOperation(journal).Error);
        Assert.Equal(cancellation.Token, operation switch
        {
            "read" => inner.ReadArguments.Item2,
            "list" => inner.ListArguments.Item4,
            _ => inner.Arguments.Item4
        });
        Assert.Null(DiagnosticContext.Current);
    }

    private static OperationObservation LastOperation(NodeEventJournal journal) =>
        Assert.IsType<OperationObservation>(Assert.IsType<NodeEvent>(journal.LastOperation).Data);

    private sealed class RegisterStub : IRegisterService
    {
        public RegisterValue Current { get; init; } = new(Payload, Version(1, 2));
        public RegisterKeyPage Page { get; } = new(["p1", "p2"], "p2");
        public CompareExchangeResult Result { get; init; } = new(CompareExchangeStatus.Unknown, null);
        public Exception? Failure { get; init; }
        public DiagnosticContext? Context { get; private set; }
        public (string, ByteString?, ByteString, CancellationToken) Arguments { get; private set; }
        public (string, CancellationToken) ReadArguments { get; private set; }
        public (string, string?, int, CancellationToken) ListArguments { get; private set; }
        public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            ReadArguments = (key, cancellationToken);
            return Failure is null ? ValueTask.FromResult(Current) : ValueTask.FromException<RegisterValue>(Failure);
        }
        public ValueTask<CompareExchangeResult> CompareExchangeAsync(string key, ByteString? expectedVersion,
            ByteString value, CancellationToken cancellationToken = default)
        {
            Arguments = (key, expectedVersion, value, cancellationToken);
            Context = DiagnosticContext.Current;
            return Failure is null ? ValueTask.FromResult(Result) : ValueTask.FromException<CompareExchangeResult>(Failure);
        }
        public ValueTask<RegisterKeyPage> ListKeysAsync(string prefix = "", string? afterKey = null,
            int limit = 100, CancellationToken cancellationToken = default)
        {
            ListArguments = (prefix, afterKey, limit, cancellationToken);
            return Failure is null ? ValueTask.FromResult(Page) : ValueTask.FromException<RegisterKeyPage>(Failure);
        }
    }
}
