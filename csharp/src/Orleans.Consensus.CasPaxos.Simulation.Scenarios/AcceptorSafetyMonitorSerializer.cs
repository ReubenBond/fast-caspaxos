using Orleans.Consensus.CasPaxos.Simulation.Invariants;
using System.Text.Json;

namespace Orleans.Consensus.CasPaxos.Simulation.Scenarios;

internal static class AcceptorSafetyMonitorSerializer
{
    public static async Task WriteAsync(
        string path,
        AcceptorSafetyMonitorArtifact artifact,
        CancellationToken cancellationToken)
    {
        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(
            stream,
            artifact,
            SimulationArtifactJsonContext.Default.AcceptorSafetyMonitorArtifact,
            cancellationToken);
    }
}
