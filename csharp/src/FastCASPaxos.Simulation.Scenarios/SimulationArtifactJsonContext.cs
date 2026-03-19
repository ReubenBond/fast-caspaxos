using FastCASPaxos.Simulation.Invariants;
using System.Text.Json.Serialization;

namespace FastCASPaxos.Simulation.Scenarios;

[JsonSourceGenerationOptions(
    WriteIndented = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(AcceptorSafetyMonitorArtifact))]
[JsonSerializable(typeof(PorcupineHistoryArtifact))]
internal sealed partial class SimulationArtifactJsonContext : JsonSerializerContext
{
}
