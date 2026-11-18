using Orleans.Consensus.CasPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Consensus.CasPaxos.Grpc;

public sealed class RepairGrpcService(IRepairScheduler scheduler, ILogger<RepairGrpcService> logger)
    : Wire.Repair.RepairBase
{
    public override Task<Wire.Empty> Stage(Wire.Empty request, ServerCallContext context) =>
        RpcErrors.Serve(() =>
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            scheduler.StageRepair();
            return Task.FromResult(new Wire.Empty());
        }, context, logger);
}
