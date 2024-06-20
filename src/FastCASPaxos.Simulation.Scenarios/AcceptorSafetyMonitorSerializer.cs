using FastCASPaxos.Simulation.Invariants;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FastCASPaxos.Simulation.Scenarios;

internal static class AcceptorSafetyMonitorSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static Task WriteAsync(
        string path,
        AcceptorSafetyMonitorArtifact artifact,
        CancellationToken cancellationToken) =>
        File.WriteAllTextAsync(
            path,
            JsonSerializer.Serialize(artifact, Options),
            cancellationToken);
}
