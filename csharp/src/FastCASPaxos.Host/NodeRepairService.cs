namespace FastCASPaxos.Host;

/// <summary>Runs repair while serving and completes the configured shutdown before Kestrel stops.</summary>
public sealed class NodeRepairService(NodeRuntime node, ILogger<NodeRepairService> logger)
    : BackgroundService, IHostedLifecycleService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => node.RunPeriodicRepairAsync(logger, stoppingToken);

    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        try { await node.StopAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (node.ShutdownMode == NodeShutdownMode.Departure) logger.DurabilityWaitCancelled();
            else logger.RestartDrainCancelled();
        }
    }
}
