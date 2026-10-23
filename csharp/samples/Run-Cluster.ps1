param(
    [string]$DataDirectory = (Join-Path $PSScriptRoot '..\src\FastCASPaxos.Host\data\cluster'),
    [ValidateRange(1, 65530)][int]$BasePort = 5001
)
$ErrorActionPreference = 'Stop'
$data = [IO.Path]::GetFullPath($DataDirectory)
$project = Join-Path $PSScriptRoot '..\src\FastCASPaxos.Host\FastCASPaxos.Host.csproj'
$nodes = [Collections.Generic.List[Diagnostics.Process]]::new()
$failed = $false

& dotnet build $project --configuration Release --verbosity minimal
if ($LASTEXITCODE -ne 0) { throw "Host/UI build failed with code $LASTEXITCODE." }

Write-Host "Starting three nodes with durable data in $data"
Write-Host 'Press Ctrl+C once to stop and wait for the hosts to finish graceful shutdown. Durable files are retained.'
try {
    foreach ($node in @('a', 'b', 'c')) {
        $process = & (Join-Path $PSScriptRoot 'Run-Node.ps1') -Node $node -DataDirectory $data -BasePort $BasePort -PassThru
        $nodes.Add($process)
        $index = $nodes.Count - 1
        Write-Host "Node $node (PID $($process.Id)): gRPC http://127.0.0.1:$($BasePort + $index) | REST http://127.0.0.1:$($BasePort + 3 + $index) | admin http://127.0.0.1:$($BasePort + 3 + $index)/admin/"
    }

    while ($true) {
        foreach ($process in $nodes) {
            if ($process.HasExited) { throw "Node PID $($process.Id) exited with code $($process.ExitCode); stopping the cluster." }
        }
        Start-Sleep -Milliseconds 200
    }
}
catch {
    $failed = $true
    throw
}
finally {
    # The host owns graceful drain/token semantics; bound only failure cleanup here.
    $grace = [Diagnostics.Stopwatch]::StartNew()
    $cleanupErrors = [Collections.Generic.List[Exception]]::new()
    if (-not $failed) { [Console]::Error.WriteLine('Waiting for nodes to finish graceful shutdown.') }
    foreach ($process in $nodes) {
        try {
            if (-not $failed) {
                $process.WaitForExit()
                [Console]::Error.WriteLine("Node PID $($process.Id) finished shutdown with exit code $($process.ExitCode).")
                continue
            }
            $remaining = [Math]::Max(0, 10000 - [int]$grace.ElapsedMilliseconds)
            if (-not $process.WaitForExit($remaining)) {
                [Console]::Error.WriteLine("Force-stopping node PID $($process.Id); durable files are retained.")
                $process.Kill($true)
                if (-not $process.WaitForExit(5000)) { throw "Node PID $($process.Id) did not exit after force-stop." }
            }
        }
        catch {
            $cleanupErrors.Add($_.Exception)
            [Console]::Error.WriteLine("Node PID $($process.Id) cleanup failed: $_")
        }
        finally { $process.Dispose() }
    }
    if ($cleanupErrors.Count -gt 0) { throw [AggregateException]::new('Cluster cleanup failed.', $cleanupErrors) }
}
