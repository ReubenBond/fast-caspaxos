using FastCASPaxos.Server;
using Grpc.Core;

namespace FastCASPaxos.Grpc;

/// <summary>Delivers a trusted proposer's quorum conclusion to another node.</summary>
public sealed class GrpcRegisterLearner(CallInvoker callInvoker, TimeSpan? callTimeout = null, TimeProvider? timeProvider = null)
{
    private readonly Wire.RegisterLearner.RegisterLearnerClient _client = new(callInvoker);
    private readonly ClientCalls _calls = new(callTimeout, timeProvider ?? TimeProvider.System);

    public async ValueTask LearnAsync(RegisterCommit commit, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(commit);
        var request = new Wire.RegisterCommit
        {
            Key = WireConvert.Text(commit.Key),
            Accepted = WireConvert.Accepted(commit.AcceptedBallot, commit.Value),
            PreparedBallot = WireConvert.ToWire(commit.PreparedBallot, allowZero: true),
        };
        await _calls.Invoke(options => _client.LearnAsync(request, options), cancellationToken).ConfigureAwait(false);
    }
}
