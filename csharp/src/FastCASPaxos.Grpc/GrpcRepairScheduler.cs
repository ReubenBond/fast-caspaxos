using Grpc.Core;

namespace FastCASPaxos.Grpc;

public sealed class GrpcRepairScheduler(CallInvoker callInvoker, TimeSpan? callTimeout = null, TimeProvider? timeProvider = null)
{
    private readonly Wire.Repair.RepairClient _client = new(callInvoker);
    private readonly ClientCalls _calls = new(callTimeout, timeProvider ?? TimeProvider.System);

    public async Task StageRepairAsync(CancellationToken cancellationToken = default)
    {
        await _calls.Invoke(o => _client.StageAsync(new(), o), cancellationToken).ConfigureAwait(false);
    }
}
