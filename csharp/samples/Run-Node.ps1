param(
    [Parameter(Mandatory)][ValidateSet('a', 'b', 'c')][string]$Node,
    [Parameter(Mandatory)][string]$DataDirectory,
    [int]$BasePort = 5001
)
$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot '..\src\FastCASPaxos.Host'
$dll = Join-Path $source 'bin\Release\net10.0\FastCASPaxos.Host.dll'
if (-not (Test-Path $dll)) { throw 'Build csharp\FastCASPaxos.slnx in Release before running the sample.' }
$id = @('a', 'b', 'c').IndexOf($Node) + 1
$data = [IO.Path]::GetFullPath($DataDirectory)
$arguments = @(
    $dll, '--contentRoot', $source,
    "--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:$($BasePort + $id - 1)",
    "--FastCASPaxos:AcceptorId=$Node",
    "--FastCASPaxos:AcceptorDatabase=$(Join-Path $data "acceptor-$Node.db")",
    "--FastCASPaxos:ProposerId=$id",
    "--FastCASPaxos:ProposerDatabase=$(Join-Path $data "proposer-$id.db")"
)
for ($i = 0; $i -lt 3; $i++) {
    $arguments += "--FastCASPaxos:Peers:${i}:Id=$(@('a', 'b', 'c')[$i])"
    $arguments += "--FastCASPaxos:Peers:${i}:Endpoint=http://127.0.0.1:$($BasePort + $i)"
}
& dotnet @arguments
if ($LASTEXITCODE -ne 0) { throw "Node $Node exited with code $LASTEXITCODE." }
