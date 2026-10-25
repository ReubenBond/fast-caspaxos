using System.Buffers.Binary;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using Wire = FastCASPaxos.Grpc.Wire;
using DomainBallot = FastCASPaxos.Model.Ballot;

namespace FastCASPaxos.Grpc.Tests;

public sealed class FastHandoffContractTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CompactLearnerWirePreservesDigestPromiseAndExplicitOutcome(bool learned, bool prepared)
    {
        var receiver = new DigestLearner(learned);
        var client = new GrpcRegisterLearner(new ContractCallInvoker(
            learner: new RegisterLearnerGrpcService(receiver, NullLogger<RegisterLearnerGrpcService>.Instance)));
        var hash = new ByteString(System.Security.Cryptography.SHA256.HashData(Envelope(1, Payload).ToArray()));
        var commit = new RegisterCommitDigest("key\0é", new(new(5, 0), hash), prepared ? new(6, 0) : DomainBallot.Zero);
        Assert.Equal(learned, await client.LearnDigestAsync(commit));
        Assert.Equal(commit, Assert.Single(receiver.Received));
        Assert.Equal(32, receiver.Received[0].Accepted.ValueHash.Length);
    }

    [Fact]
    public async Task CompactLearnerRejectsUnspecifiedWireOutcome()
    {
        var client = new GrpcRegisterLearner(StubCallInvoker.Reply(new Wire.LearnDigestReply()));
        await Assert.ThrowsAsync<InvalidDataException>(() => client.LearnDigestAsync(
            new("key", new(new(5, 0), new ByteString(new byte[32])), new(6, 0))).AsTask());
    }

    public enum InvalidCompact { MissingKey, MissingDigest, MissingBallot, ZeroBallot, ShortHash, LongHash, ExpiredPromise, NegativePromise }

    [Theory]
    [InlineData(InvalidCompact.MissingKey)]
    [InlineData(InvalidCompact.MissingDigest)]
    [InlineData(InvalidCompact.MissingBallot)]
    [InlineData(InvalidCompact.ZeroBallot)]
    [InlineData(InvalidCompact.ShortHash)]
    [InlineData(InvalidCompact.LongHash)]
    [InlineData(InvalidCompact.ExpiredPromise)]
    [InlineData(InvalidCompact.NegativePromise)]
    public async Task CompactLearnerRejectsInvalidWireBeforeDelivery(InvalidCompact scenario)
    {
        var receiver = new DigestLearner(true);
        var wire = new Wire.RegisterLearner.RegisterLearnerClient(new ContractCallInvoker(
            learner: new RegisterLearnerGrpcService(receiver, NullLogger<RegisterLearnerGrpcService>.Instance)));
        var request = new Wire.RegisterCommitDigest
        {
            Key = "key",
            Accepted = new() { Ballot = Ballot(5, 0), Sha256 = Google.Protobuf.ByteString.CopyFrom(new byte[32]) },
            PreparedBallot = Ballot(6, 0),
        };
        switch (scenario)
        {
            case InvalidCompact.MissingKey: request.ClearKey(); break;
            case InvalidCompact.MissingDigest: request.Accepted = null; break;
            case InvalidCompact.MissingBallot: request.Accepted.Ballot = null; break;
            case InvalidCompact.ZeroBallot: request.Accepted.Ballot = new(); break;
            case InvalidCompact.ShortHash: request.Accepted.Sha256 = Google.Protobuf.ByteString.CopyFrom(new byte[31]); break;
            case InvalidCompact.LongHash: request.Accepted.Sha256 = Google.Protobuf.ByteString.CopyFrom(new byte[33]); break;
            case InvalidCompact.ExpiredPromise: request.PreparedBallot = Ballot(5, 0); break;
            case InvalidCompact.NegativePromise: request.PreparedBallot = Ballot(-1, 0); break;
        }
        var error = await Assert.ThrowsAsync<RpcException>(() => wire.LearnDigestAsync(request).ResponseAsync);
        Assert.Equal(StatusCode.InvalidArgument, error.StatusCode);
        Assert.Empty(receiver.Received);
    }

    private sealed class DigestLearner(bool result) : IRegisterLearner, IRegisterDigestLearner
    {
        public List<RegisterCommitDigest> Received { get; } = [];
        public void Learn(RegisterCommit commit) => throw new InvalidOperationException("Expected compact delivery.");
        public ValueTask<bool> LearnDigestAsync(RegisterCommitDigest commit, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Received.Add(commit);
            return ValueTask.FromResult(result);
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task LearnerProtobufRoundTripPreservesKeyBytesAndOptionalPreparedBallot(bool emptyValue, bool prepared)
    {
        var received = new RecordingLearner();
        var service = new RegisterLearnerGrpcService(received, NullLogger<RegisterLearnerGrpcService>.Instance);
        var client = new GrpcRegisterLearner(new ContractCallInvoker(learner: service));
        var commit = new RegisterCommit("key\0\U0001F600", new(int.MaxValue - 1, int.MaxValue),
            emptyValue ? ByteString.Empty : new([0, 10, 255]),
            prepared ? new(int.MaxValue, 0) : DomainBallot.Zero);

        await client.LearnAsync(commit).AsTask().WaitAsync(Watchdog);

        Assert.Equal(commit, Assert.Single(received.Commits));
    }

    public enum InvalidCommit { MissingKey, MissingAccepted, MissingBallot, MissingValue, ZeroBallot, NegativePrepared }

    [Theory]
    [InlineData(InvalidCommit.MissingKey)]
    [InlineData(InvalidCommit.MissingAccepted)]
    [InlineData(InvalidCommit.MissingBallot)]
    [InlineData(InvalidCommit.MissingValue)]
    [InlineData(InvalidCommit.ZeroBallot)]
    [InlineData(InvalidCommit.NegativePrepared)]
    public async Task MalformedLearnerWireRequestIsRejectedBeforeDelivery(InvalidCommit invalid)
    {
        var received = new RecordingLearner();
        var service = new RegisterLearnerGrpcService(received, NullLogger<RegisterLearnerGrpcService>.Instance);
        var raw = new Wire.RegisterLearner.RegisterLearnerClient(new ContractCallInvoker(learner: service));
        var request = new Wire.RegisterCommit
        {
            Key = "key",
            Accepted = new() { Ballot = Ballot(5, 0), Value = Google.Protobuf.ByteString.Empty },
            PreparedBallot = Ballot(6, 0)
        };
        switch (invalid)
        {
            case InvalidCommit.MissingKey: request.ClearKey(); break;
            case InvalidCommit.MissingAccepted: request.Accepted = null; break;
            case InvalidCommit.MissingBallot: request.Accepted.Ballot = null; break;
            case InvalidCommit.MissingValue: request.Accepted.ClearValue(); break;
            case InvalidCommit.ZeroBallot: request.Accepted.Ballot = new(); break;
            case InvalidCommit.NegativePrepared: request.PreparedBallot.Round = -1; break;
        }

        var exception = await Assert.ThrowsAsync<RpcException>(() => raw.LearnAsync(request).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, exception.StatusCode);
        Assert.Empty(received.Commits);
    }

    [Fact]
    public async Task CancelledLearnerDeliveryDoesNotReachDomain()
    {
        var received = new RecordingLearner();
        var service = new RegisterLearnerGrpcService(received, NullLogger<RegisterLearnerGrpcService>.Instance);
        var client = new GrpcRegisterLearner(new ContractCallInvoker(learner: service));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.LearnAsync(new("key", new(5, 0), Payload, new(6, 0)), cancelled.Token).AsTask());

        Assert.Empty(received.Commits);
    }

    [Fact]
    public async Task TransportedCertificateEnablesOneRoundWriteWithoutMutatingSqliteOnLearn()
    {
        using var directory = new TestDirectory();
        var stores = new List<SqliteAcceptorServer>();
        var prepares = new int[3];
        var accepts = new int[3];
        var reads = new int[3];
        try
        {
            var commit = new RegisterCommit("key", new(5, 0), Envelope(1, Payload), new(6, 0));
            for (var index = 0; index < 3; index++)
            {
                var store = await SqliteAcceptorServer.OpenAsync(directory.Database($"acceptor-{index}"));
                stores.Add(store);
                Assert.True((await store.AcceptAsync("key",
                    new(commit.AcceptedBallot, commit.Value, commit.PreparedBallot))).IsAccepted);
            }
            var routes = stores.Select((store, index) => new AcceptorPeer($"peer-{index}", new DelegateAcceptor
            {
                Read = (key, known, token) => { Interlocked.Increment(ref reads[index]); return store.ReadAsync(key, known, token); },
                Prepare = (key, request, token) => { Interlocked.Increment(ref prepares[index]); return store.PrepareAsync(key, request, token); },
                Accept = (key, request, token) => { Interlocked.Increment(ref accepts[index]); return store.AcceptAsync(key, request, token); },
                List = store.ListKeysAsync
            })).ToArray();
            using var ballots = new SqliteProposerBallotSource(directory.Database("proposer"), 3);
            await using var registers = new ReplicatedRegisterService([.. routes], ballots,
                new() { Mode = ProposerModePolicy.StaticFast }, stores[2]);
            var service = new RegisterLearnerGrpcService(registers, NullLogger<RegisterLearnerGrpcService>.Instance);
            var client = new GrpcRegisterLearner(new ContractCallInvoker(learner: service));
            var before = await stores[2].InspectAsync("key");

            await client.LearnAsync(commit).AsTask().WaitAsync(Watchdog);

            Assert.Equal(before, await stores[2].InspectAsync("key"));
            Assert.All(reads, count => Assert.Equal(0, count));
            Assert.All(prepares, count => Assert.Equal(0, count));
            Assert.All(accepts, count => Assert.Equal(0, count));
            var result = await registers.CompareExchangeAsync("key", Revision(1), ByteString.Empty).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), result.Current);
            Assert.All(reads, count => Assert.Equal(0, count));
            Assert.All(prepares, count => Assert.Equal(0, count));
            Assert.All(accepts, count => Assert.Equal(1, count));
            foreach (var store in stores)
            {
                var state = (await store.InspectAsync("key"))!;
                Assert.Equal(new DomainBallot(6, 0), state.AcceptedBallot);
                Assert.Equal(new DomainBallot(7, 0), state.PromisedBallot);
            }
        }
        finally
        {
            foreach (var store in stores) await store.DisposeAsync();
        }
    }

    // Independent wire oracle, shared by the handoff transport/host tests only.
    internal static ByteString Envelope(ulong revision, ByteString payload)
    {
        var bytes = new byte[29 + payload.Length];
        "FCR2"u8.CopyTo(bytes);
        bytes[4] = 1;
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(5), revision);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(13), 1);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(17), 1);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(21), (long)revision);
        payload.ToArray().CopyTo(bytes, 29);
        return new(bytes);
    }

    internal static ByteString Revision(ulong revision)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, revision);
        return new(bytes);
    }

    private sealed class RecordingLearner : IRegisterLearner
    {
        public List<RegisterCommit> Commits { get; } = [];
        public void Learn(RegisterCommit commit) => Commits.Add(commit);
    }
}
