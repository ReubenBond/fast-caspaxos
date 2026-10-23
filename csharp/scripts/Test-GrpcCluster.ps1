param([int]$BasePort = 15431)
$ErrorActionPreference = 'Stop'
$csharp = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$hostDirectory = Join-Path $csharp 'src\FastCASPaxos.Host\bin\Release\net10.0'
$hostDll = Join-Path $hostDirectory 'FastCASPaxos.Host.dll'
$smokeDll = Join-Path $csharp 'samples\FastCASPaxos.Grpc.Smoke\bin\Release\net10.0\FastCASPaxos.Grpc.Smoke.dll'
if (-not (Test-Path $hostDll) -or -not (Test-Path $smokeDll)) {
    throw 'Build the host and smoke projects in Release first.'
}
$data = Join-Path ([IO.Path]::GetTempPath()) "fastcaspaxos-grpc-smoke-$([Guid]::NewGuid().ToString('N'))"
$null = New-Item -ItemType Directory -Path $data
$processes = @{}
$output = @{}
$errors = @{}
$http = [Net.Http.HttpClient]::new()
$http.DefaultRequestVersion = [Version]::new(2, 0)
$http.DefaultVersionPolicy = [Net.Http.HttpVersionPolicy]::RequestVersionExact
$http.Timeout = [TimeSpan]::FromSeconds(1)

function Start-Node([int]$index) {
    $id = @('a', 'b', 'c')[$index]
    $info = [Diagnostics.ProcessStartInfo]::new('dotnet')
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($argument in @(
        $hostDll, '--contentRoot', $hostDirectory,
        "--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:$($BasePort + $index)",
        "--FastCASPaxos:AcceptorId=$id",
        "--FastCASPaxos:AcceptorDatabase=$(Join-Path $data "acceptor-$id.db")",
        "--FastCASPaxos:ProposerId=$($index + 1)",
        "--FastCASPaxos:ProposerDatabase=$(Join-Path $data "proposer-$id.db")"
    )) { $info.ArgumentList.Add($argument) }
    for ($i = 0; $i -lt 3; $i++) {
        $info.ArgumentList.Add("--FastCASPaxos:Peers:${i}:Id=$(@('a', 'b', 'c')[$i])")
        $info.ArgumentList.Add("--FastCASPaxos:Peers:${i}:Endpoint=http://127.0.0.1:$($BasePort + $i)")
    }
    $process = [Diagnostics.Process]::Start($info)
    $processes[$index] = $process
    $output[$index] = $process.StandardOutput.ReadToEndAsync()
    $errors[$index] = $process.StandardError.ReadToEndAsync()
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($process.HasExited) { throw "Node $id exited: $($errors[$index].GetAwaiter().GetResult())" }
        try {
            $response = $http.GetAsync("http://127.0.0.1:$($BasePort + $index)/health/live").GetAwaiter().GetResult()
            try { if ($response.IsSuccessStatusCode) { return } }
            finally { $response.Dispose() }
        }
        catch [Net.Http.HttpRequestException] { }
        catch [Threading.Tasks.TaskCanceledException] { }
        Start-Sleep -Milliseconds 100
    }
    throw "Node $id failed to become ready."
}

function Stop-Node([int]$index) {
    $process = $processes[$index]
    if ($null -eq $process) { return }
    if (-not $process.HasExited) { $process.Kill($true) }
    $process.WaitForExit()
    $process.Dispose()
    $processes.Remove($index)
}

function Invoke-Smoke([string]$mode, [string]$key, [int[]]$nodes) {
    $endpoints = $nodes | ForEach-Object { "http://127.0.0.1:$($BasePort + $_)" }
    & dotnet $smokeDll $mode $key @endpoints
    if ($LASTEXITCODE -ne 0) { throw "Smoke $mode failed with exit code $LASTEXITCODE." }
}

try {
    0..2 | ForEach-Object { Start-Node $_ }
    Invoke-Smoke 'write' 'smoke/initial' @(0, 1, 2)
    Stop-Node 2
    Invoke-Smoke 'write' 'smoke/degraded' @(0, 1)
    0..1 | ForEach-Object { Stop-Node $_ }
    0..2 | ForEach-Object { Start-Node $_ }
    Invoke-Smoke 'verify' 'smoke/initial' @(0, 1, 2)
    Invoke-Smoke 'verify' 'smoke/degraded' @(0, 1, 2)
}
catch {
    foreach ($index in @($processes.Keys)) { Stop-Node $index }
    foreach ($index in $output.Keys) {
        Write-Host "Node $index stdout: $($output[$index].GetAwaiter().GetResult())"
        Write-Host "Node $index stderr: $($errors[$index].GetAwaiter().GetResult())"
    }
    throw
}
finally {
    foreach ($index in @($processes.Keys)) { Stop-Node $index }
    $http.Dispose()
    Remove-Item -LiteralPath $data -Recurse -Force
}
