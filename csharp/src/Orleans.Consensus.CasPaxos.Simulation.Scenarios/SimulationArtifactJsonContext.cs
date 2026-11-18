using Orleans.Consensus.CasPaxos.Simulation.Invariants;
using System.Text.Json.Serialization;

namespace Orleans.Consensus.CasPaxos.Simulation.Scenarios;

[JsonSourceGenerationOptions(
    WriteIndented = true,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(AcceptorSafetyMonitorArtifact))]
[JsonSerializable(typeof(PorcupineHistoryArtifact))]
internal sealed partial class SimulationArtifactJsonContext : JsonSerializerContext
{
}
