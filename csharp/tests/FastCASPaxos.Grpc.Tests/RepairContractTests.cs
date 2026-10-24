using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using ProtoBytes = Google.Protobuf.ByteString;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class RepairContractTests
{
    private static readonly ByteString EmptyHash = new(Convert.FromHexString(
        "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));

    [Fact]
    public async Task DigestWire_RoundTripsOrdinalPagesEmptyCursorAndAcceptedBallot()
    {
        using var directory = new TestDirectory();
        await using var store = await SqliteAcceptorServer.OpenAsync(directory.Database("digests"));
        foreach (var key in new[] { "é", "a", "A", "\0", "" })
            await store.AcceptAsync(key, new(new(3, 0), ByteString.Empty, new(9, 2)));
        await store.PrepareAsync("promise", new(new(10, 2)));
        var client = new GrpcAcceptorDigestSource(new RepairContractInvoker(store));

        var first = await client.ListDigestsAsync(limit: 2);
        Assert.Equal(new[] { "", "\0" }, first.Entries.Select(entry => entry.Key));
        Assert.Equal("\0", first.ContinuationKey);
        Assert.All(first.Entries, entry => Assert.Equal(new AcceptedDigest(new(3, 0), EmptyHash), entry.Digest));
        var second = await client.ListDigestsAsync(afterKey: first.ContinuationKey, limit: 2);
        Assert.Equal(new[] { "A", "a" }, second.Entries.Select(entry => entry.Key));
        Assert.Equal("a", second.ContinuationKey);
        var last = await client.ListDigestsAsync(afterKey: second.ContinuationKey, limit: 2);
        Assert.Equal("é", Assert.Single(last.Entries).Key);
        Assert.Null(last.ContinuationKey);
        Assert.Equal("\0", Assert.Single((await client.ListDigestsAsync(afterKey: "", limit: 1)).Entries).Key);
        Assert.Equal("A", Assert.Single((await client.ListDigestsAsync(prefix: "A")).Entries).Key);
    }

    [Fact]
    public async Task ReadWire_DistinguishesMissingUnchangedAndFullStateIncludingEmptyBytes()
    {
        using var directory = new TestDirectory();
        await using var store = await SqliteAcceptorServer.OpenAsync(directory.Database("fetch"));
        await store.PrepareAsync("promise", new(new(9, 2)));
        await store.AcceptAsync("", new(new(3, 0), ByteString.Empty, new(9, 2)));
        IKeyedAcceptor client = new GrpcKeyedAcceptor(new RepairContractInvoker(store));
        foreach (var key in new[] { "missing", "promise" })
        {
            var missing = await client.ReadAsync(key, null);
            Assert.True(missing is AcceptorMissing);
        }
        var digest = new AcceptedDigest(new(3, 0), EmptyHash);
        var unchanged = await client.ReadAsync("", digest);
        Assert.True(unchanged is AcceptorUnchanged);
        AcceptedDigest?[] different = [null, digest with { AcceptedBallot = new(2, 0) },
            digest with { ValueHash = new(new byte[32]) }];
        foreach (var known in different)
        {
            var fetched = await client.ReadAsync("", known);
            Assert.Equal(new KeyedAcceptorState("", new(9, 2), new(3, 0), ByteString.Empty),
                fetched switch { KeyedAcceptorState state => state, _ => null });
        }
    }

    [Fact]
    public async Task ReadWire_RoutesToKeyedAcceptorRatherThanDigestListingSource()
    {
        var source = new DelegateDigests();
        var known = new AcceptedDigest(new(3, 0), EmptyHash);
        using var cancellation = new CancellationTokenSource();
        var acceptor = new DelegateAcceptor
        {
            Read = (key, digest, token) =>
            {
                Assert.Equal("requested", key);
                Assert.Equal(known, digest);
                Assert.Equal(cancellation.Token, token);
                return ValueTask.FromResult<AcceptorReadResult>(new AcceptorUnchanged());
            }
        };
        IKeyedAcceptor client = new GrpcKeyedAcceptor(new RepairContractInvoker(source, acceptor: acceptor));

        Assert.True(await client.ReadAsync("requested", known, cancellation.Token) is AcceptorUnchanged);

        Assert.Equal(1, acceptor.Calls);
        Assert.Equal(0, source.Calls);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ReadAsync("requested", known, cancellation.Token).AsTask());
        Assert.Equal(1, acceptor.Calls);
        Assert.Equal(0, source.Calls);
    }

    [Theory]
    [InlineData("missing-key")]
    [InlineData("missing-ballot")]
    [InlineData("zero-ballot")]
    [InlineData("negative-proposer")]
    [InlineData("short-hash")]
    [InlineData("long-hash")]
    public async Task ConditionalFetchWire_InvalidRequest_RejectsBeforeSource(string scenario)
    {
        var source = new DelegateDigests();
        var request = new Wire.FetchIfDifferentRequest { Key = "key", Known = WireDigest() };
        switch (scenario)
        {
            case "missing-key": request.ClearKey(); break;
            case "missing-ballot": request.Known.Ballot = null; break;
            case "zero-ballot": request.Known.Ballot = new(); break;
            case "negative-proposer": request.Known.Ballot.ProposerId = -1; break;
            case "short-hash": request.Known.Sha256 = ProtoBytes.CopyFrom(new byte[31]); break;
            case "long-hash": request.Known.Sha256 = ProtoBytes.CopyFrom(new byte[33]); break;
        }
        var raw = new Wire.AcceptorDigests.AcceptorDigestsClient(new RepairContractInvoker(source));

        var failure = await Assert.ThrowsAsync<RpcException>(() => raw.FetchIfDifferentAsync(request).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, failure.StatusCode);
        Assert.Equal(0, source.Calls);
    }

    [Theory]
    [InlineData("missing-outcome")]
    [InlineData("unchanged-without-known")]
    [InlineData("wrong-key")]
    [InlineData("missing-accepted")]
    [InlineData("missing-bytes")]
    [InlineData("zero-ballot")]
    [InlineData("missing-promise")]
    [InlineData("different-but-matching")]
    public async Task ConditionalFetchWire_MalformedResponse_RejectsAsDataLoss(string scenario)
    {
        var response = new Wire.ConditionalAcceptorState { Different = Entry("key", promised: 9, accepted: 3) };
        AcceptedDigest? known = null;
        switch (scenario)
        {
            case "missing-outcome": response.ClearOutcome(); break;
            case "unchanged-without-known": response.Unchanged = new(); break;
            case "wrong-key": response.Different.Key = "other"; break;
            case "missing-accepted": response.Different.Accepted = null; break;
            case "missing-bytes": response.Different.Accepted.ClearValue(); break;
            case "zero-ballot": response.Different.Accepted.Ballot = new(); break;
            case "missing-promise": response.Different.PromisedBallot = null; break;
            case "different-but-matching": known = new(new(3, 1), EmptyHash); break;
        }
        var client = new GrpcKeyedAcceptor(StubCallInvoker.Reply(response));

        var failure = await Assert.ThrowsAsync<RemoteCallException>(() => client.ReadAsync("key", known).AsTask());

        Assert.Equal(StatusCode.DataLoss, failure.StatusCode);
    }

    [Theory]
    [InlineData("missing-key")]
    [InlineData("missing-digest")]
    [InlineData("short-hash")]
    [InlineData("over-limit")]
    [InlineData("wrong-prefix")]
    [InlineData("out-of-order")]
    [InlineData("duplicate")]
    [InlineData("equal-cursor")]
    [InlineData("wrong-continuation")]
    [InlineData("empty-continuation")]
    public async Task DigestWire_MalformedPage_RejectsAsDataLoss(string scenario)
    {
        var response = new Wire.AcceptorDigestPage
        {
            Entries = { new Wire.KeyDigest { Key = "ab", Digest = WireDigest() },
                new Wire.KeyDigest { Key = "ac", Digest = WireDigest() } }
        };
        switch (scenario)
        {
            case "missing-key": response.Entries[0].ClearKey(); break;
            case "missing-digest": response.Entries[0].Digest = null; break;
            case "short-hash": response.Entries[0].Digest.Sha256 = ProtoBytes.Empty; break;
            case "over-limit": response.Entries.Add(new Wire.KeyDigest { Key = "ad", Digest = WireDigest() }); break;
            case "wrong-prefix": response.Entries[1].Key = "b"; break;
            case "out-of-order": response.Entries[0].Key = "ad"; break;
            case "duplicate": response.Entries[1].Key = "ab"; break;
            case "equal-cursor": response.Entries[0].Key = "aa"; break;
            case "wrong-continuation": response.ContinuationKey = "ad"; break;
            case "empty-continuation": response.Entries.Clear(); response.ContinuationKey = ""; break;
        }
        var client = new GrpcAcceptorDigestSource(StubCallInvoker.Reply(response));

        var failure = await Assert.ThrowsAsync<RemoteCallException>(() => client.ListDigestsAsync("a", "aa", 2).AsTask());

        Assert.Equal(StatusCode.DataLoss, failure.StatusCode);
    }

    [Theory]
    [InlineData("unchanged-without-known")]
    [InlineData("different-wrong-key")]
    [InlineData("default-union")]
    [InlineData("zero-accepted-ballot")]
    public async Task ConditionalFetchService_InvalidDomainResult_FailsInsteadOfSerializing(string scenario)
    {
        var state = new KeyedAcceptorState("other", new(9, 2), new(3, 1), ByteString.Empty);
        AcceptorReadResult outcome = scenario switch
        {
            "unchanged-without-known" => new AcceptorUnchanged(),
            "different-wrong-key" => state,
            "zero-accepted-ballot" => state with { Key = "key", AcceptedBallot = default },
            _ => default
        };
        var source = new DelegateDigests { Fetch = (_, _, _) => ValueTask.FromResult(outcome) };
        var raw = new Wire.AcceptorDigests.AcceptorDigestsClient(new RepairContractInvoker(source));

        var failure = await Assert.ThrowsAsync<RpcException>(() =>
            raw.FetchIfDifferentAsync(new() { Key = "key" }).ResponseAsync);

        Assert.Equal(StatusCode.Internal, failure.StatusCode);
        Assert.Equal(1, source.Calls);
    }

    [Theory]
    [InlineData("list")]
    [InlineData("fetch")]
    [InlineData("stage")]
    public async Task RepairClients_ForwardDeadlineAndCancelDispatchedCall(string operation)
    {
        var clock = new DiagnosticClock();
        using var cancellation = new CancellationTokenSource();
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invoker = new StubCallInvoker(async (_, options) =>
        {
            var cancelled = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = options.CancellationToken.Register(() => cancelled.TrySetException(
                new RpcException(new Status(StatusCode.Cancelled, "cancelled transport"))));
            arrived.TrySetResult();
            return await cancelled.Task;
        });
        var source = new GrpcAcceptorDigestSource(invoker, TimeSpan.FromSeconds(7), clock);
        var acceptor = new GrpcKeyedAcceptor(invoker, TimeSpan.FromSeconds(7), clock);
        var scheduler = new GrpcRepairScheduler(invoker, TimeSpan.FromSeconds(7), clock);
        var task = operation switch
        {
            "list" => (Task)source.ListDigestsAsync(cancellationToken: cancellation.Token).AsTask(),
            "fetch" => acceptor.ReadAsync("key", null, cancellation.Token).AsTask(),
            _ => scheduler.StageRepairAsync(cancellation.Token)
        };
        await arrived.Task.WaitAsync(Watchdog);
        var call = Assert.Single(invoker.Calls);
        Assert.Equal(cancellation.Token, call.Options.CancellationToken);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(7).UtcDateTime, call.Options.Deadline);

        cancellation.Cancel();
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task.WaitAsync(Watchdog));
        Assert.Equal(cancellation.Token, failure.CancellationToken);
    }

    [Theory]
    [InlineData("list")]
    [InlineData("fetch")]
    [InlineData("stage")]
    public async Task RepairClients_UnavailablePeer_PreservesTransportFailure(string operation)
    {
        var error = new RpcException(new Status(StatusCode.Unavailable, "offline"));
        var invoker = StubCallInvoker.Fail(error);
        var source = new GrpcAcceptorDigestSource(invoker);
        var acceptor = new GrpcKeyedAcceptor(invoker);
        Task Invoke() => operation switch
        {
            "list" => source.ListDigestsAsync().AsTask(),
            "fetch" => acceptor.ReadAsync("key", null).AsTask(),
            _ => new GrpcRepairScheduler(invoker).StageRepairAsync()
        };

        var failure = await Assert.ThrowsAsync<RemoteCallException>(Invoke);

        Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
        Assert.Same(error, failure.InnerException);
    }

    [Fact]
    public async Task StageWire_InvokesSchedulerAndPreCancellationDoesNotStage()
    {
        var scheduler = new CountingRepairScheduler();
        var client = new GrpcRepairScheduler(new RepairContractInvoker(scheduler: scheduler));
        await client.StageRepairAsync();
        Assert.Equal(1, scheduler.Calls);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.StageRepairAsync(canceled.Token));
        Assert.Equal(1, scheduler.Calls);
        var service = new RepairGrpcService(scheduler, NullLogger<RepairGrpcService>.Instance);
        var failure = await Assert.ThrowsAsync<RpcException>(() =>
            service.Stage(new(), new TestServerCallContext(new(cancellationToken: canceled.Token))));
        Assert.Equal(StatusCode.Cancelled, failure.StatusCode);
        Assert.Equal(1, scheduler.Calls);
    }

    private static Wire.AcceptedDigest WireDigest() =>
        new() { Ballot = Ballot(3), Sha256 = ProtoBytes.CopyFrom(EmptyHash.ToArray()) };
}
