using Orleans.Consensus.CasPaxos.Diagnostics;

namespace Orleans.Consensus.CasPaxos.Protocol;

public sealed class ProposerRuntime(ProposerDiagnostics? diagnostics = null)
{
    public ProposerDiagnostics Diagnostics { get; } = diagnostics ?? ProposerDiagnostics.Nop;
}
