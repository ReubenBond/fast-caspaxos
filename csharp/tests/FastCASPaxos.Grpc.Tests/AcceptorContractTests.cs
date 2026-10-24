using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Core;
using Xunit;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class AcceptorContractTests
{
    [Fact]
    public async Task PrepareAndAccept_MatchCoreOracleAcrossRetriesAndConflicts()
    {
        using var directory = new TestDirectory();
        await using var sqlite = await SqliteAcceptorServer.OpenAsync(directory.Database("acceptor"));
        var client = new GrpcKeyedAcceptor(new ContractCallInvoker(Service(sqlite)));
        var oracle = AcceptorState<ByteString?>.CreateInitial(null);
        var key = "key\0\U0001f600";

        async Task Prepare(int round)
        {
            var request = new PrepareRequest(new(round, 1));
            var expected = AcceptorEngine.Prepare(ref oracle, request);
            Assert.Equal(expected, await client.PrepareAsync(key, request).AsTask().WaitAsync(Watchdog));
        }
        async Task Accept(int round, ByteString value, int nextRound = 0)
        {
            var request = new AcceptRequest<ByteString>(new(round, 1), value,
                nextRound == 0 ? FastCASPaxos.Model.Ballot.Zero : new(nextRound, 1));
            var expected = AcceptorEngine.Accept(ref oracle,
                new AcceptRequest<ByteString?>(request.Ballot, value, request.NextBallotToPrepare));
            Assert.Equal(expected, await client.AcceptAsync(key, request).AsTask().WaitAsync(Watchdog));
        }

        await Prepare(2);
        await Accept(3, default); // Promise 2 is below accepted ballot 3.
        await Accept(3, ByteString.Empty, 9); // Exact retry cannot change the promise.
        await Accept(3, Payload); // Same-ballot different payload must reject.
        await Prepare(5);
        await Accept(3, ByteString.Empty); // Exact retry still succeeds after a higher promise.
        await Accept(4, Payload); // Stale accept rejects.
        await Accept(6, Payload, 7);
        await Prepare(6);
        await Prepare(8);
        var entry = Assert.Single((await client.ListKeysAsync().AsTask().WaitAsync(Watchdog)).Entries);
        Assert.Equal(key, entry.Key);
        Assert.Equal(oracle.PromisedBallot, entry.PromisedBallot);
        Assert.Equal(oracle.AcceptedBallot, entry.AcceptedBallot);
        Assert.Equal(Payload, entry.AcceptedValue);
    }

    [Fact]
    public async Task AcceptWithoutPrepare_PreservesLowerPromiseAndAcceptedInventory()
    {
        using var directory = new TestDirectory();
        var path = directory.Database("acceptor");
        var request = new AcceptRequest<ByteString>(new(3, 1), Payload, FastCASPaxos.Model.Ballot.Zero);
        await using (var sqlite = await SqliteAcceptorServer.OpenAsync(path))
        {
            var client = new GrpcKeyedAcceptor(new ContractCallInvoker(Service(sqlite)));
            var result = await client.AcceptAsync("", request).AsTask().WaitAsync(Watchdog);
            Assert.True(result.IsAccepted);
            Assert.Equal(FastCASPaxos.Model.Ballot.InitialFast(), result.PromisedBallot);
            var entry = Assert.Single((await client.ListKeysAsync()).Entries);
            Assert.Equal(new KeyedAcceptorState("", FastCASPaxos.Model.Ballot.InitialFast(), new(3, 1), Payload), entry);
        }
        await using var reopened = await SqliteAcceptorServer.OpenAsync(path);
        var remote = new GrpcKeyedAcceptor(new ContractCallInvoker(Service(reopened)));
        Assert.Equal(AcceptorAcceptResult.Accepted(FastCASPaxos.Model.Ballot.InitialFast()),
            await remote.AcceptAsync("", request));
        var rejection = await remote.PrepareAsync("", new(new(2, 1)));
        Assert.False(rejection.IsPromised);
        Assert.Equal(new FastCASPaxos.Model.Ballot(3, 1), rejection.ConflictingBallot);
        Assert.Equal(Payload, rejection.AcceptedValue);
    }

    [Fact]
    public async Task Prepare_NeverAcceptedAndAcceptedEmptyRemainDistinct()
    {
        using var directory = new TestDirectory();
        await using var sqlite = await SqliteAcceptorServer.OpenAsync(directory.Database("acceptor"));
        var invoker = new ContractCallInvoker(Service(sqlite));
        var raw = new Wire.Acceptor.AcceptorClient(invoker);
        var missing = await raw.PrepareAsync(new() { Key = "", Ballot = Ballot(1) }).ResponseAsync;
        Assert.Equal(Wire.PrepareResponse.OutcomeOneofCase.Promised, missing.OutcomeCase);
        Assert.Null(missing.Accepted);
        var client = new GrpcKeyedAcceptor(invoker);
        var domain = await client.PrepareAsync("", new(new(1, 1)));
        Assert.True(domain.IsPromised);
        Assert.Null(domain.AcceptedValue);
        Assert.Equal(FastCASPaxos.Model.Ballot.Zero, domain.AcceptedBallot);
        Assert.Empty((await client.ListKeysAsync()).Entries); // Promise-only keys are not inventory.

        Assert.True((await client.AcceptAsync("", new(new(1, 1), default, new()))).IsAccepted);
        var accepted = await raw.PrepareAsync(new() { Key = "", Ballot = Ballot(2) }).ResponseAsync;
        Assert.Equal(Ballot(1), accepted.Accepted.Ballot);
        Assert.True(accepted.Accepted.HasValue);
        Assert.Empty(accepted.Accepted.Value);
        var empty = await client.PrepareAsync("", new(new(2, 1)));
        Assert.Equal(new FastCASPaxos.Model.Ballot(1, 1), empty.AcceptedBallot);
        Assert.Equal(ByteString.Empty, empty.AcceptedValue);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Accept_AbsentAndExplicitZeroNextBallotReachDomainAsZero(bool explicitZero)
    {
        var domain = new DelegateAcceptor
        {
            Accept = (key, request, _) =>
            {
                Assert.Equal("", key);
                Assert.Equal(ByteString.Empty, request.Value);
                Assert.Equal(FastCASPaxos.Model.Ballot.Zero, request.NextBallotToPrepare);
                return ValueTask.FromResult(AcceptorAcceptResult.Accepted(new(1, 1)));
            },
        };
        var request = new Wire.AcceptRequest { Key = "", Ballot = Ballot(1), Value = Google.Protobuf.ByteString.Empty };
        if (explicitZero) request.NextBallot = new();
        var raw = new Wire.Acceptor.AcceptorClient(new ContractCallInvoker(Service(domain)));
        Assert.Equal(Ballot(1), (await raw.AcceptAsync(request).ResponseAsync).Accepted);
        Assert.Equal(1, domain.Calls);
    }

    [Fact]
    public async Task BallotBoundaries_PreserveMaximumRoundAndNonnegativeProposer()
    {
        using var directory = new TestDirectory();
        await using var sqlite = await SqliteAcceptorServer.OpenAsync(directory.Database("acceptor"));
        var client = new GrpcKeyedAcceptor(new ContractCallInvoker(Service(sqlite)));
        var initial = await client.PrepareAsync("key", new(new(1, 0)));
        Assert.True(initial.IsPromised);
        Assert.Equal(FastCASPaxos.Model.Ballot.Zero, initial.AcceptedBallot);
        Assert.Null(initial.AcceptedValue);
        var maximum = new FastCASPaxos.Model.Ballot(int.MaxValue, int.MaxValue);
        var result = await client.AcceptAsync("key", new(maximum, Payload, maximum));
        Assert.True(result.IsAccepted);
        Assert.Equal(maximum, result.PromisedBallot);
        var same = await client.PrepareAsync("key", new(maximum));
        Assert.True(same.IsPromised);
        Assert.Equal(maximum, same.AcceptedBallot);
        Assert.Equal(Payload, same.AcceptedValue);
        var below = await client.PrepareAsync("key", new(new(int.MaxValue, int.MaxValue - 1)));
        Assert.False(below.IsPromised);
        Assert.Equal(maximum, below.ConflictingBallot);
    }
}
