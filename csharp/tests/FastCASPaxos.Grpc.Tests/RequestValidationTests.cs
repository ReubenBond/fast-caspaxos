using FastCASPaxos.Server;
using Grpc.Core;
using Xunit;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class RequestValidationTests
{
    public enum BadRequest
    {
        PrepareMissingKey, PrepareMissingBallot, PrepareZeroRound, PrepareNegativeRound, PrepareNegativeProposer,
        PrepareZeroRoundNonzeroProposer, AcceptMissingKey, AcceptMissingBallot, AcceptMissingValue,
        AcceptZeroRound, AcceptNegativeProposer, AcceptNegativeNextRound, AcceptNegativeNextProposer,
        AcceptZeroNextRoundNonzeroProposer, ReadMissingKey, CasMissingKey, CasMissingValue,
        AcceptorZeroLimit, AcceptorNegativeLimit, RegisterZeroLimit, RegisterNegativeLimit,
    }

    public static IEnumerable<object[]> BadRequests() => Enum.GetValues<BadRequest>().Select(value => new object[] { value });

    [Theory]
    [MemberData(nameof(BadRequests))]
    public async Task MalformedRawRequests_RejectBeforeDomainDispatch(BadRequest scenario)
    {
        var acceptor = new DelegateAcceptor();
        var registers = new DelegateRegister();
        var invoker = new ContractCallInvoker(Service(acceptor), Service(registers));
        var rawAcceptor = new Wire.Acceptor.AcceptorClient(invoker);
        var rawRegister = new Wire.Register.RegisterClient(invoker);
        var prepare = new Wire.PrepareRequest { Key = "key", Ballot = Ballot() };
        var accept = new Wire.AcceptRequest { Key = "key", Ballot = Ballot(), Value = Google.Protobuf.ByteString.Empty };
        var cas = new Wire.CompareExchangeRequest { Key = "key", Value = Google.Protobuf.ByteString.Empty };
        switch (scenario)
        {
            case BadRequest.PrepareMissingKey: prepare.ClearKey(); break;
            case BadRequest.PrepareMissingBallot: prepare.Ballot = null; break;
            case BadRequest.PrepareZeroRound: prepare.Ballot.Round = 0; prepare.Ballot.ProposerId = 0; break;
            case BadRequest.PrepareNegativeRound: prepare.Ballot.Round = -1; break;
            case BadRequest.PrepareNegativeProposer: prepare.Ballot.ProposerId = -1; break;
            case BadRequest.PrepareZeroRoundNonzeroProposer: prepare.Ballot.Round = 0; break;
            case BadRequest.AcceptMissingKey: accept.ClearKey(); break;
            case BadRequest.AcceptMissingBallot: accept.Ballot = null; break;
            case BadRequest.AcceptMissingValue: accept.ClearValue(); break;
            case BadRequest.AcceptZeroRound: accept.Ballot = new(); break;
            case BadRequest.AcceptNegativeProposer: accept.Ballot.ProposerId = -1; break;
            case BadRequest.AcceptNegativeNextRound: accept.NextBallot = Ballot(-1); break;
            case BadRequest.AcceptNegativeNextProposer: accept.NextBallot = Ballot(2, -1); break;
            case BadRequest.AcceptZeroNextRoundNonzeroProposer: accept.NextBallot = Ballot(0, 1); break;
            case BadRequest.CasMissingKey: cas.ClearKey(); break;
            case BadRequest.CasMissingValue: cas.ClearValue(); break;
        }
        async Task Invoke()
        {
            if (scenario <= BadRequest.PrepareZeroRoundNonzeroProposer)
                await rawAcceptor.PrepareAsync(prepare).ResponseAsync;
            else if (scenario <= BadRequest.AcceptZeroNextRoundNonzeroProposer)
                await rawAcceptor.AcceptAsync(accept).ResponseAsync;
            else if (scenario == BadRequest.ReadMissingKey)
                await rawRegister.ReadAsync(new()).ResponseAsync;
            else if (scenario <= BadRequest.CasMissingValue)
                await rawRegister.CompareExchangeAsync(cas).ResponseAsync;
            else if (scenario <= BadRequest.AcceptorNegativeLimit)
                await rawAcceptor.ListKeysAsync(new() { Limit = scenario == BadRequest.AcceptorZeroLimit ? 0 : -1 }).ResponseAsync;
            else
                await rawRegister.ListKeysAsync(new() { Limit = scenario == BadRequest.RegisterZeroLimit ? 0 : -1 }).ResponseAsync;
        }
        var error = await Assert.ThrowsAsync<RpcException>(Invoke);
        Assert.Equal(StatusCode.InvalidArgument, error.StatusCode);
        Assert.Equal("invalid", error.Trailers.GetValue("fastcaspaxos-failure"));
        Assert.Equal(0, acceptor.Calls);
        Assert.Equal(0, registers.Calls);
    }

    public enum Operation { Prepare, Accept, AcceptorList, Read, CompareExchange, RegisterList }

    [Theory]
    [InlineData(Operation.Prepare)]
    [InlineData(Operation.Accept)]
    [InlineData(Operation.AcceptorList)]
    [InlineData(Operation.Read)]
    [InlineData(Operation.CompareExchange)]
    [InlineData(Operation.RegisterList)]
    public async Task MalformedText_IsRejectedBeforeDispatch(Operation operation)
    {
        // Control/surrogate data are deliberately not theory arguments or display names.
        string[] invalid = ["\ud800", "\udc00", "\ud800x", "x\udc00", "\ud800\ud800", "\udc00\ud800"];
        var invoker = StubCallInvoker.Fail(new InvalidOperationException("Must never dispatch."));
        var acceptor = new GrpcKeyedAcceptor(invoker);
        var register = new GrpcRegisterService(invoker);
        foreach (var text in invalid)
        {
            if (operation is Operation.AcceptorList or Operation.RegisterList)
            {
                await Assert.ThrowsAsync<ArgumentException>(() => Invoke(operation, acceptor, register, prefix: text));
                await Assert.ThrowsAsync<ArgumentException>(() => Invoke(operation, acceptor, register, cursor: text));
            }
            else
                await Assert.ThrowsAsync<ArgumentException>(() => Invoke(operation, acceptor, register, key: text));
        }
        Assert.Empty(invoker.Calls);
    }

    [Theory]
    [InlineData(Operation.Prepare)]
    [InlineData(Operation.Accept)]
    [InlineData(Operation.AcceptorList)]
    [InlineData(Operation.Read)]
    [InlineData(Operation.CompareExchange)]
    [InlineData(Operation.RegisterList)]
    public async Task PreCancelledOperations_DoNotDispatch(Operation operation)
    {
        var invoker = StubCallInvoker.Fail(new InvalidOperationException("Must never dispatch."));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Invoke(operation, new(invoker), new(invoker), token: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Empty(invoker.Calls);
    }

    [Fact]
    public async Task InvalidClientArguments_DoNotDispatch()
    {
        var invoker = StubCallInvoker.Fail(new InvalidOperationException("Must never dispatch."));
        var acceptor = new GrpcKeyedAcceptor(invoker);
        var register = new GrpcRegisterService(invoker);
        foreach (var ballot in new FastCASPaxos.Model.Ballot[] { new(), new(-1, 0), new(0, 1), new(1, -1) })
        {
            await Assert.ThrowsAsync<ArgumentException>(() => acceptor.PrepareAsync("key", new(ballot)).AsTask());
            await Assert.ThrowsAsync<ArgumentException>(() => acceptor.AcceptAsync("key", new(ballot, Payload, new())).AsTask());
        }
        foreach (var next in new FastCASPaxos.Model.Ballot[] { new(-1, 0), new(0, 1), new(1, -1) })
            await Assert.ThrowsAsync<ArgumentException>(() => acceptor.AcceptAsync("key", new(new(1, 1), Payload, next)).AsTask());
        foreach (var limit in new[] { 0, -1, int.MinValue })
        {
            await Assert.ThrowsAsync<ArgumentException>(() => acceptor.ListKeysAsync(limit: limit).AsTask());
            await Assert.ThrowsAsync<ArgumentException>(() => register.ListKeysAsync(limit: limit).AsTask());
        }
        await Assert.ThrowsAsync<ArgumentNullException>(() => acceptor.PrepareAsync(null!, new(new(1, 1))).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => register.ReadAsync(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => register.ListKeysAsync(prefix: null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => acceptor.ListKeysAsync(prefix: null!).AsTask());
        Assert.Empty(invoker.Calls);
    }

    [Fact]
    public async Task ExpectedVersionPresence_IsPreservedAndEmptyReachesLowerLayer()
    {
        using var directory = new TestDirectory();
        await using var cluster = await SqliteContractCluster.OpenAsync(directory);
        var inner = cluster.CreateRegister(1);
        var observed = new List<ByteString?>();
        var domain = new DelegateRegister
        {
            CompareExchange = (key, expected, value, token) =>
            {
                observed.Add(expected);
                return inner.CompareExchangeAsync(key, expected, value, token);
            },
        };
        var client = new GrpcRegisterService(new ContractCallInvoker(register: Service(domain)));
        var created = await client.CompareExchangeAsync("key", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Null(Assert.Single(observed));
        await Assert.ThrowsAsync<ArgumentException>(() => client.CompareExchangeAsync("key", ByteString.Empty, Payload).AsTask());
        Assert.Equal(2, observed.Count);
        Assert.Equal(ByteString.Empty, observed[1]);
        Assert.Equal(created.Current, await inner.ReadAsync("key"));
    }

    [Theory]
    [InlineData(Operation.Prepare)]
    [InlineData(Operation.Accept)]
    [InlineData(Operation.AcceptorList)]
    [InlineData(Operation.Read)]
    [InlineData(Operation.CompareExchange)]
    [InlineData(Operation.RegisterList)]
    public async Task Services_ForwardCancellationTokenToEveryDomainOperation(Operation operation)
    {
        using var cancellation = new CancellationTokenSource();
        CancellationToken observed = default;
        var acceptor = new DelegateAcceptor
        {
            Prepare = (_, _, token) =>
            {
                observed = token;
                return ValueTask.FromResult(FastCASPaxos.Protocol.AcceptorPrepareResult.Promised<ByteString?>(new(), null));
            },
            Accept = (_, _, token) =>
            {
                observed = token;
                return ValueTask.FromResult(FastCASPaxos.Protocol.AcceptorAcceptResult.Accepted(new(1, 1)));
            },
            List = (_, _, _, token) => { observed = token; return ValueTask.FromResult(new KeyPage([], null)); },
        };
        var register = new DelegateRegister
        {
            Read = (_, token) => { observed = token; return ValueTask.FromResult(new RegisterValue(null, null)); },
            CompareExchange = (_, _, value, token) =>
            {
                observed = token;
                return ValueTask.FromResult(new CompareExchangeResult(CompareExchangeStatus.Applied,
                    new(value, ContractTestSupport.Version)));
            },
            List = (_, _, _, token) => { observed = token; return ValueTask.FromResult(new RegisterKeyPage([], null)); },
        };
        var invoker = new ContractCallInvoker(Service(acceptor), Service(register));
        await Invoke(operation, new(invoker), new(invoker), token: cancellation.Token);
        Assert.Equal(cancellation.Token, observed);
        Assert.Equal(1, acceptor.Calls + register.Calls);
    }

    internal static async Task Invoke(Operation operation, GrpcKeyedAcceptor acceptor, GrpcRegisterService register,
        string key = "key", string prefix = "", string? cursor = null, CancellationToken token = default)
    {
        switch (operation)
        {
            case Operation.Prepare: await acceptor.PrepareAsync(key, new(new(1, 1)), token); break;
            case Operation.Accept: await acceptor.AcceptAsync(key, new(new(1, 1), Payload, new()), token); break;
            case Operation.AcceptorList: await acceptor.ListKeysAsync(prefix, cursor, 10, token); break;
            case Operation.Read: await register.ReadAsync(key, token); break;
            case Operation.CompareExchange: await register.CompareExchangeAsync(key, null, Payload, token); break;
            case Operation.RegisterList: await register.ListKeysAsync(prefix, cursor, 10, token); break;
            default: throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }
}
