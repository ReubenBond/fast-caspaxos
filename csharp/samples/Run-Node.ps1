param(
    [Parameter(Mandatory)][ValidateSet('a', 'b', 'c')][string]$Node,
    [Parameter(Mandatory)][string]$DataDirectory,
    [ValidateRange(1, 65530)][int]$BasePort = 5001,
    [switch]$PassThru
)
$ErrorActionPreference = 'Stop'
$Node = $Node.ToLowerInvariant()
$source = Join-Path $PSScriptRoot '..\src\FastCASPaxos.Host'
$dll = Join-Path $source 'bin\Release\net10.0\FastCASPaxos.Host.dll'
if (-not (Test-Path $dll)) { throw 'Build csharp\FastCASPaxos.slnx in Release before running the sample.' }
$id = @('a', 'b', 'c').IndexOf($Node) + 1
$data = [IO.Path]::GetFullPath($DataDirectory)
$arguments = @(
    $dll, '--contentRoot', $source,
    "--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:$($BasePort + $id - 1)",
    "--Kestrel:Endpoints:Rest:Url=http://127.0.0.1:$($BasePort + $id + 2)",
    "--FastCASPaxos:AcceptorId=$Node",
    "--FastCASPaxos:AcceptorDatabase=$(Join-Path $data "acceptor-$Node.db")",
    "--FastCASPaxos:ProposerId=$id",
    "--FastCASPaxos:ProposerDatabase=$(Join-Path $data "proposer-$id.db")"
)
for ($i = 0; $i -lt 3; $i++) {
    $arguments += "--FastCASPaxos:Peers:${i}:Id=$(@('a', 'b', 'c')[$i])"
    $arguments += "--FastCASPaxos:Peers:${i}:Endpoint=http://127.0.0.1:$($BasePort + $i)"
    $arguments += "--FastCASPaxos:Peers:${i}:HttpEndpoint=http://127.0.0.1:$($BasePort + 3 + $i)"
}
if ($PassThru) {
    $info = [Diagnostics.ProcessStartInfo]::new('dotnet')
    $info.UseShellExecute = $false
    foreach ($argument in $arguments) { $info.ArgumentList.Add($argument) }
    return [Diagnostics.Process]::Start($info)
}
else {
    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) { throw "Node $Node exited with code $LASTEXITCODE." }
}
