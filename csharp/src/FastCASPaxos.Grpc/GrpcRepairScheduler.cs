using Grpc.Core;

namespace FastCASPaxos.Grpc;

public sealed class GrpcRepairScheduler
{
    private readonly Wire.Repair.RepairClient _client;
    private readonly ClientCalls _calls;

    public GrpcRepairScheduler(CallInvoker callInvoker, TimeSpan? callTimeout = null, TimeProvider? timeProvider = null)
    {
        _client = new(callInvoker);
        _calls = new(callTimeout, timeProvider ?? TimeProvider.System);
    }

    public async Task StageRepairAsync(CancellationToken cancellationToken = default)
    {
        await _calls.Invoke(o => _client.StageAsync(new(), o), cancellationToken).ConfigureAwait(false);
    }
}
