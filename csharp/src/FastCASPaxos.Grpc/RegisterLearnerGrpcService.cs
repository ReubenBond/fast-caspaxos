using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

public sealed class RegisterLearnerGrpcService(IRegisterLearner learner, ILogger<RegisterLearnerGrpcService> logger)
    : Wire.RegisterLearner.RegisterLearnerBase
{
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
