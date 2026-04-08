using FastCASPaxos.Diagnostics;

namespace FastCASPaxos.Protocol;

public sealed class ProposerRuntime(ProposerDiagnostics? diagnostics = null)
{
    public ProposerDiagnostics Diagnostics { get; } = diagnostics ?? ProposerDiagnostics.Nop;
}
