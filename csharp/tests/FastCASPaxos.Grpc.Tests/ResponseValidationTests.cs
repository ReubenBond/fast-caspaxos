using FastCASPaxos.Server;
using Grpc.Core;
using Xunit;
using Wire = FastCASPaxos.Grpc.Wire;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

namespace FastCASPaxos.Grpc.Tests;

public sealed class ResponseValidationTests
{
    public enum BadPrepare
    {
        MissingOutcome, WrongPromise, ZeroPromise, NegativePromiseProposer, AcceptedMissingBallot,
        AcceptedMissingValue, AcceptedZeroBallot, AcceptedNegativeRound, AcceptedNegativeProposer,
        AcceptedAbovePromise, RejectionEqualRequest, RejectionBelowRequest, RejectionBelowAccepted,
    }
    public static IEnumerable<object[]> PrepareCases() => Cases<BadPrepare>();

    [Theory]
    [MemberData(nameof(PrepareCases))]
    public async Task MalformedPrepareResponses_RejectAsDataLoss(BadPrepare scenario)
    {
        var response = new Wire.PrepareResponse { Promised = Ballot(3) };
        switch (scenario)
        {
            case BadPrepare.MissingOutcome: response.ClearOutcome(); break;
            case BadPrepare.WrongPromise: response.Promised = Ballot(4); break;
            case BadPrepare.ZeroPromise: response.Promised = new(); break;
            case BadPrepare.NegativePromiseProposer: response.Promised.ProposerId = -1; break;
            case BadPrepare.RejectionEqualRequest: response.Rejected = Ballot(3); break;
            case BadPrepare.RejectionBelowRequest: response.Rejected = Ballot(2); break;
            default:
                response.Accepted = new() { Ballot = Ballot(2), Value = Google.Protobuf.ByteString.Empty };
                switch (scenario)
                {
                    case BadPrepare.AcceptedMissingBallot: response.Accepted.Ballot = null; break;
                    case BadPrepare.AcceptedMissingValue: response.Accepted.ClearValue(); break;
                    case BadPrepare.AcceptedZeroBallot: response.Accepted.Ballot = new(); break;
                    case BadPrepare.AcceptedNegativeRound: response.Accepted.Ballot.Round = -1; break;
                    case BadPrepare.AcceptedNegativeProposer: response.Accepted.Ballot.ProposerId = -1; break;
                    case BadPrepare.AcceptedAbovePromise: response.Accepted.Ballot = Ballot(4); break;
                    case BadPrepare.RejectionBelowAccepted:
                        response.Rejected = Ballot(4);
                        response.Accepted.Ballot = Ballot(5);
                        break;
                }
                break;
        }
        var client = new GrpcKeyedAcceptor(StubCallInvoker.Reply(response));
        await AssertDataLoss(() => client.PrepareAsync("key", new(new(3, 1))).AsTask());
    }

    public enum BadAccept { MissingOutcome, ZeroAccepted, NegativeAccepted, NegativeProposer, RejectionBelowRequest, ZeroRejected }
    public static IEnumerable<object[]> AcceptCases() => Cases<BadAccept>();

    [Theory]
    [MemberData(nameof(AcceptCases))]
    public async Task MalformedAcceptResponses_RejectAsDataLoss(BadAccept scenario)
    {
        var response = scenario switch
        {
            BadAccept.MissingOutcome => new Wire.AcceptResponse(),
            BadAccept.ZeroAccepted => new() { Accepted = new() },
            BadAccept.NegativeAccepted => new() { Accepted = Ballot(-1) },
            BadAccept.NegativeProposer => new() { Accepted = Ballot(3, -1) },
            BadAccept.RejectionBelowRequest => new() { Rejected = Ballot(2) },
            BadAccept.ZeroRejected => new() { Rejected = new() },
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
        var client = new GrpcKeyedAcceptor(StubCallInvoker.Reply(response));
        await AssertDataLoss(() => client.AcceptAsync("key", new(new(3, 1), Payload, new())).AsTask());
    }

    public enum BadState { MissingState, MissingValue, MissingVersion }
    public static IEnumerable<object[]> StateCases() => Cases<BadState>();

    [Theory]
    [MemberData(nameof(StateCases))]
    public async Task MalformedReadResponses_RejectAsDataLoss(BadState scenario)
    {
        var state = MalformedState(scenario);
        var client = new GrpcRegisterService(StubCallInvoker.Reply(state));
        await AssertDataLoss(() => client.ReadAsync("key").AsTask());
    }

    public enum BadCas
    {
        MissingOutcome, AppliedAbsent, AppliedMissingState, AppliedMissingValue, AppliedMissingVersion,
        AppliedWrongPayload, AppliedSameVersion, MismatchMissingState, MismatchMissingValue,
        MismatchMissingVersion, MismatchSameVersion, MismatchAbsentWhenExpectedAbsent,
        UnknownMissingState, UnknownUnspecifiedError, UnknownUnrecognizedError,
    }
    public static IEnumerable<object[]> CasCases() => Cases<BadCas>();

    [Theory]
    [MemberData(nameof(CasCases))]
    public async Task MalformedCasResponses_RemainUnknown(BadCas scenario)
    {
        ByteString? expected = ContractTestSupport.Version;
        var response = scenario switch
        {
            BadCas.MissingOutcome => new Wire.CompareExchangeResponse(),
            BadCas.AppliedAbsent => new() { Applied = Absent() },
            BadCas.AppliedMissingState => new() { Applied = MalformedState(BadState.MissingState) },
            BadCas.AppliedMissingValue => new() { Applied = MalformedState(BadState.MissingValue) },
            BadCas.AppliedMissingVersion => new() { Applied = MalformedState(BadState.MissingVersion) },
            BadCas.AppliedWrongPayload => new() { Applied = Present([99], [42]) },
            BadCas.AppliedSameVersion => new() { Applied = Present() },
            BadCas.MismatchMissingState => new() { Mismatch = MalformedState(BadState.MissingState) },
            BadCas.MismatchMissingValue => new() { Mismatch = MalformedState(BadState.MissingValue) },
            BadCas.MismatchMissingVersion => new() { Mismatch = MalformedState(BadState.MissingVersion) },
            BadCas.MismatchSameVersion => new() { Mismatch = Present() },
            BadCas.MismatchAbsentWhenExpectedAbsent => new() { Mismatch = Absent() },
            BadCas.UnknownMissingState => new() { Unknown = new() { Current = new() } },
            BadCas.UnknownUnspecifiedError => new() { Unknown = new() { Error = new() } },
            BadCas.UnknownUnrecognizedError => new() { Unknown = new() { Error = new() { Code = (Wire.ErrorCode)999 } } },
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
        if (scenario == BadCas.MismatchAbsentWhenExpectedAbsent) expected = null;
        var client = new GrpcRegisterService(StubCallInvoker.Reply(response));
        var result = await client.CompareExchangeAsync("key", expected, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Null(result.Current);
        Assert.Equal(StatusCode.DataLoss, Assert.IsType<RemoteCallException>(result.Error).StatusCode);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task UnknownOutcome_PreservesOptionalObservationAndDiagnostic(bool current, bool diagnostic)
    {
        var observation = new RegisterValue(ByteString.Empty, ContractTestSupport.Version);
        var domain = new DelegateRegister
        {
            CompareExchange = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult(
                CompareExchangeStatus.Unknown, current ? observation : null,
                diagnostic ? new TimeoutException("bounded deadline") : null)),
        };
        var client = new GrpcRegisterService(new ContractCallInvoker(register: Service(domain)));
        var result = await client.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(current ? observation : null, result.Current);
        if (diagnostic)
        {
            var error = Assert.IsType<RemoteCallException>(result.Error);
            Assert.Equal(StatusCode.DeadlineExceeded, error.StatusCode);
            Assert.Equal("bounded deadline", error.Message);
        }
        else Assert.Null(result.Error);
    }

    [Theory]
    [InlineData(Wire.ErrorCode.Cancelled, StatusCode.Cancelled)]
    [InlineData(Wire.ErrorCode.Deadline, StatusCode.DeadlineExceeded)]
    [InlineData(Wire.ErrorCode.Unavailable, StatusCode.Unavailable)]
    [InlineData(Wire.ErrorCode.Internal, StatusCode.Internal)]
    public async Task UnknownErrorCodes_PreserveRecognizedStatus(Wire.ErrorCode code, StatusCode status)
    {
        Exception cause = code switch
        {
            Wire.ErrorCode.Cancelled => new OperationCanceledException("diagnostic"),
            Wire.ErrorCode.Deadline => new TimeoutException("diagnostic"),
            Wire.ErrorCode.Unavailable => new RemoteCallException(StatusCode.Unavailable, "diagnostic"),
            _ => new InvalidOperationException("diagnostic"),
        };
        var domain = new DelegateRegister
        {
            CompareExchange = (_, _, _, _) => ValueTask.FromResult(new CompareExchangeResult(
                CompareExchangeStatus.Unknown, new(null, null), cause)),
        };
        var invoker = new ContractCallInvoker(register: Service(domain));
        var raw = new Wire.Register.RegisterClient(invoker);
        var reply = await raw.CompareExchangeAsync(new() { Key = "key", Value = Google.Protobuf.ByteString.Empty }).ResponseAsync;
        Assert.Equal(code, reply.Unknown.Error.Code);
        Assert.Equal(Wire.RegisterState.StateOneofCase.Absent, reply.Unknown.Current.StateCase);
        var result = await new GrpcRegisterService(invoker).CompareExchangeAsync("key", null, Payload);
        Assert.Equal(CompareExchangeStatus.Unknown, result.Status);
        Assert.Equal(new RegisterValue(null, null), result.Current);
        var error = Assert.IsType<RemoteCallException>(result.Error);
        Assert.Equal(status, error.StatusCode);
        Assert.Equal("diagnostic", error.Message);
    }

    public enum BadPage
    {
        OverLimit, WrongPrefix, OutOfOrder, Duplicate, EqualCursor, BeforeCursor,
        WrongContinuation, EmptyWithContinuation, MalformedKey, MalformedContinuation,
    }
    public static IEnumerable<object[]> PageCases() => Cases<BadPage>();

    [Theory]
    [MemberData(nameof(PageCases))]
    public async Task MalformedRegisterPages_RejectAsDataLoss(BadPage scenario)
    {
        var (keys, cursor) = Page(scenario);
        var response = new Wire.RegisterKeyPage();
        response.Keys.AddRange(keys);
        if (cursor is not null) response.ContinuationKey = cursor;
        var client = new GrpcRegisterService(StubCallInvoker.Reply(response));
        await AssertDataLoss(() => client.ListKeysAsync("a", "aa", 2).AsTask());
    }

    [Theory]
    [MemberData(nameof(PageCases))]
    public async Task MalformedAcceptorPages_RejectAsDataLoss(BadPage scenario)
    {
        var (keys, cursor) = Page(scenario);
        var response = new Wire.AcceptorKeyPage();
        response.Entries.AddRange(keys.Select(key => Entry(key)));
        if (cursor is not null) response.ContinuationKey = cursor;
        var client = new GrpcKeyedAcceptor(StubCallInvoker.Reply(response));
        await AssertDataLoss(() => client.ListKeysAsync("a", "aa", 2).AsTask());
    }

    public enum BadEntry
    {
        MissingKey, MissingPromise, ZeroPromise, NegativePromiseRound, NegativePromiseProposer,
        MissingAccepted, MissingAcceptedBallot, ZeroAcceptedBallot, MissingAcceptedValue,
    }
    public static IEnumerable<object[]> EntryCases() => Cases<BadEntry>();

    [Theory]
    [MemberData(nameof(EntryCases))]
    public async Task MalformedAcceptorEntry_RejectsAsDataLoss(BadEntry scenario)
    {
        var entry = Entry("ab");
        switch (scenario)
        {
            case BadEntry.MissingKey: entry.ClearKey(); break;
            case BadEntry.MissingPromise: entry.PromisedBallot = null; break;
            case BadEntry.ZeroPromise: entry.PromisedBallot = new(); break;
            case BadEntry.NegativePromiseRound: entry.PromisedBallot.Round = -1; break;
            case BadEntry.NegativePromiseProposer: entry.PromisedBallot.ProposerId = -1; break;
            case BadEntry.MissingAccepted: entry.Accepted = null; break;
            case BadEntry.MissingAcceptedBallot: entry.Accepted.Ballot = null; break;
            case BadEntry.ZeroAcceptedBallot: entry.Accepted.Ballot = new(); break;
            case BadEntry.MissingAcceptedValue: entry.Accepted.ClearValue(); break;
        }
        var response = new Wire.AcceptorKeyPage();
        response.Entries.Add(entry);
        await AssertDataLoss(() => new GrpcKeyedAcceptor(StubCallInvoker.Reply(response)).ListKeysAsync().AsTask());
    }

    [Theory]
    [MemberData(nameof(PageCases))]
    public async Task InvalidDomainPages_AreInternalNotDefinitiveFailures(BadPage scenario)
    {
        var (keys, cursor) = Page(scenario);
        var acceptor = new DelegateAcceptor
        {
            List = (_, _, _, _) => ValueTask.FromResult(new KeyPage(
                keys.Select(key => new KeyedAcceptorState(key, new(3, 1), new(2, 1), Payload)).ToArray(), cursor)),
        };
        var register = new DelegateRegister
        {
            List = (_, _, _, _) => ValueTask.FromResult(new RegisterKeyPage(keys, cursor)),
        };
        var invoker = new ContractCallInvoker(Service(acceptor), Service(register));
        var request = new Wire.ListKeysRequest { Prefix = "a", AfterKey = "aa", Limit = 2 };
        var acceptorError = await Assert.ThrowsAsync<RpcException>(() =>
            new Wire.Acceptor.AcceptorClient(invoker).ListKeysAsync(request).ResponseAsync);
        var registerError = await Assert.ThrowsAsync<RpcException>(() =>
            new Wire.Register.RegisterClient(invoker).ListKeysAsync(request).ResponseAsync);
        foreach (var error in new[] { acceptorError, registerError })
        {
            Assert.Equal(StatusCode.Internal, error.StatusCode);
            Assert.Null(error.Trailers.GetValue("fastcaspaxos-failure"));
        }
    }

    public enum BadDomainState
    {
        AbsentWithValue, PresentWithoutValue, AppliedWithoutCurrent, AppliedAbsent,
        MismatchWithoutCurrent, UnrecognizedStatus, UnacceptedWithBallot, AcceptedWithoutBallot,
    }
    public static IEnumerable<object[]> DomainStateCases() => Cases<BadDomainState>();

    [Theory]
    [MemberData(nameof(DomainStateCases))]
    public async Task InvalidDomainState_IsInternalWithoutDefinitiveFailureMarker(BadDomainState scenario)
    {
        var invalidRegister = scenario switch
        {
            BadDomainState.AbsentWithValue => new RegisterValue(Payload, null),
            _ => new RegisterValue(null, ContractTestSupport.Version),
        };
        var invalidResult = scenario switch
        {
            BadDomainState.AppliedWithoutCurrent => new CompareExchangeResult(CompareExchangeStatus.Applied, null),
            BadDomainState.AppliedAbsent => new(CompareExchangeStatus.Applied, new(null, null)),
            BadDomainState.MismatchWithoutCurrent => new(CompareExchangeStatus.Mismatch, null),
            _ => new((CompareExchangeStatus)999, new(null, null)),
        };
        var register = new DelegateRegister
        {
            Read = (_, _) => ValueTask.FromResult(invalidRegister),
            CompareExchange = (_, _, _, _) => ValueTask.FromResult(invalidResult),
        };
        var acceptor = new DelegateAcceptor
        {
            Prepare = (_, _, _) => ValueTask.FromResult(
                scenario == BadDomainState.UnacceptedWithBallot
                    ? FastCASPaxos.Protocol.AcceptorPrepareResult.Promised<ByteString?>(new(1, 1), null)
                    : FastCASPaxos.Protocol.AcceptorPrepareResult.Promised<ByteString?>(new(), Payload)),
        };
        var invoker = new ContractCallInvoker(Service(acceptor), Service(register));
        async Task Invoke()
        {
            if (scenario <= BadDomainState.PresentWithoutValue)
                await new Wire.Register.RegisterClient(invoker).ReadAsync(new() { Key = "key" }).ResponseAsync;
            else if (scenario <= BadDomainState.UnrecognizedStatus)
                await new Wire.Register.RegisterClient(invoker).CompareExchangeAsync(new()
                {
                    Key = "key",
                    Value = Google.Protobuf.ByteString.Empty,
                }).ResponseAsync;
            else
                await new Wire.Acceptor.AcceptorClient(invoker).PrepareAsync(new() { Key = "key", Ballot = Ballot(3) }).ResponseAsync;
        }
        var error = await Assert.ThrowsAsync<RpcException>(Invoke);
        Assert.Equal(StatusCode.Internal, error.StatusCode);
        Assert.Null(error.Trailers.GetValue("fastcaspaxos-failure"));
    }

    private static Wire.RegisterState MalformedState(BadState scenario)
    {
        var state = Present();
        switch (scenario)
        {
            case BadState.MissingState: state.ClearState(); break;
            case BadState.MissingValue: state.Present.ClearValue(); break;
            case BadState.MissingVersion: state.Present.ClearVersion(); break;
        }
        return state;
    }

    private static (string[] Keys, string? Cursor) Page(BadPage scenario) => scenario switch
    {
        BadPage.OverLimit => (["ab", "ac", "ad"], null),
        BadPage.WrongPrefix => (["ab", "b"], null),
        BadPage.OutOfOrder => (["ac", "ab"], null),
        BadPage.Duplicate => (["ab", "ab"], null),
        BadPage.EqualCursor => (["aa"], null),
        BadPage.BeforeCursor => (["a"], null),
        BadPage.WrongContinuation => (["ab", "ac"], "ab"),
        BadPage.EmptyWithContinuation => ([], "ab"),
        BadPage.MalformedKey => (["a\ud800"], null),
        BadPage.MalformedContinuation => (["ab"], "\ud800"),
        _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
    };

    private static IEnumerable<object[]> Cases<T>() where T : struct, Enum =>
        Enum.GetValues<T>().Select(value => new object[] { value });

    private static async Task AssertDataLoss(Func<Task> action)
    {
        var error = await Assert.ThrowsAsync<RemoteCallException>(action);
        Assert.Equal(StatusCode.DataLoss, error.StatusCode);
        Assert.IsType<ArgumentException>(error.InnerException);
    }
}
