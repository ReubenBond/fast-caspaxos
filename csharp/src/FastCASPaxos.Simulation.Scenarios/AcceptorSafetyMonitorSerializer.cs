using FastCASPaxos.Simulation.Invariants;
using System.Text.Json;

namespace FastCASPaxos.Simulation.Scenarios;

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
