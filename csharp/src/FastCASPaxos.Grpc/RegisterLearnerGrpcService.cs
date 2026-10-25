using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

public sealed class RegisterLearnerGrpcService(IRegisterLearner learner, ILogger<RegisterLearnerGrpcService> logger)
    : Wire.RegisterLearner.RegisterLearnerBase
{
    public override Task<Wire.LearnDigestReply> LearnDigest(Wire.RegisterCommitDigest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            if (learner is not IRegisterDigestLearner digestLearner)
                throw new RpcException(new Status(StatusCode.Unimplemented, "This learner requires full committed values."));
            var commit = new RegisterCommitDigest(WireConvert.Key(request.Key, request.HasKey),
                WireConvert.Digest(request.Accepted),
                request.PreparedBallot is null ? Ballot.Zero : WireConvert.Ballot(request.PreparedBallot, allowZero: true));
            commit.Validate();
            var learned = await digestLearner.LearnDigestAsync(commit, context.CancellationToken).ConfigureAwait(false);
            return learned ? new Wire.LearnDigestReply { Learned = new() } : new Wire.LearnDigestReply { Stale = new() };
        }, context, logger);

    public override Task<Wire.Empty> Learn(Wire.RegisterCommit request, ServerCallContext context) =>
        RpcErrors.Serve(() =>
        {
            var key = WireConvert.Key(request.Key, request.HasKey);
            var (ballot, value) = WireConvert.Accepted(request.Accepted);
            WireConvert.Require(value is not null, "A committed value is required.");
            var prepared = request.PreparedBallot is null
                ? Ballot.Zero : WireConvert.Ballot(request.PreparedBallot, allowZero: true);
            context.CancellationToken.ThrowIfCancellationRequested();
            learner.Learn(new(key, ballot, value!.Value, prepared));
            return Task.FromResult(new Wire.Empty());
        }, context, logger);
}
