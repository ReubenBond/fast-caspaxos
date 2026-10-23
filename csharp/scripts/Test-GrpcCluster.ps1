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
$rest = [Net.Http.HttpClient]::new()
$rest.Timeout = [TimeSpan]::FromSeconds(15)

function Start-Node([int]$index) {
    $id = @('a', 'b', 'c')[$index]
    $info = [Diagnostics.ProcessStartInfo]::new('dotnet')
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($argument in @(
        $hostDll, '--contentRoot', $hostDirectory,
        "--Kestrel:Endpoints:Grpc:Url=http://127.0.0.1:$($BasePort + $index)",
        "--Kestrel:Endpoints:Rest:Url=http://127.0.0.1:$($BasePort + 3 + $index)",
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

function Invoke-Rest([int]$node, [string]$method, [string]$path, [byte[]]$body, [string]$etag) {
    $request = [Net.Http.HttpRequestMessage]::new(
        [Net.Http.HttpMethod]::new($method), "http://127.0.0.1:$($BasePort + 3 + $node)$path")
    $request.Version = [Version]::new(1, 1)
    $request.VersionPolicy = [Net.Http.HttpVersionPolicy]::RequestVersionExact
    try {
        if ($method -eq 'PUT') {
            $request.Content = [Net.Http.ByteArrayContent]::new($body)
            $request.Content.Headers.ContentType = [Net.Http.Headers.MediaTypeHeaderValue]::new('application/octet-stream')
            $header = if ($etag -eq '*') { 'If-None-Match' } else { 'If-Match' }
            $request.Headers.Add($header, $etag)
        }
        $response = $rest.SendAsync($request).GetAwaiter().GetResult()
        try {
            if ($response.Version -ne [Version]::new(1, 1)) { throw 'REST must work over ordinary HTTP/1.1.' }
            if (-not $response.Headers.CacheControl.NoStore) { throw 'REST responses must use no-store.' }
            $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
            return [pscustomobject]@{
                Status = [int]$response.StatusCode
                ETag = if ($null -eq $response.Headers.ETag) { $null } else { $response.Headers.ETag.ToString() }
                Bytes = $bytes
                Text = [Text.Encoding]::UTF8.GetString($bytes)
            }
        }
        finally { $response.Dispose() }
    }
    finally { $request.Dispose() }
}

function Write-RestRegister([string]$key, [int[]]$nodes) {
    $path = "/v1/registers/value?key=$([Uri]::EscapeDataString($key))"
    if ((Invoke-Rest $nodes[0] 'GET' $path).Status -ne 404) { throw 'The REST smoke key must start absent.' }
    [byte[]]$bytes = @(0, 255, 13, 10, 128)
    $created = Invoke-Rest $nodes[0] 'PUT' $path $bytes '*'
    if ($created.Status -ne 201 -or -not $created.ETag) { throw "REST create failed: $($created.Text)" }
    $read = Invoke-Rest $nodes[-1] 'GET' $path
    if ($read.Status -ne 200 -or $read.ETag -ne $created.ETag -or
        [Convert]::ToBase64String($read.Bytes) -ne [Convert]::ToBase64String($bytes)) {
        throw 'REST must preserve raw bytes and the logical version across nodes.'
    }
    $updated = Invoke-Rest $nodes[-1] 'PUT' $path ([byte[]]@()) $created.ETag
    if ($updated.Status -ne 204 -or -not $updated.ETag -or $updated.ETag -eq $created.ETag) {
        throw "REST update failed: $($updated.Text)"
    }
    $stale = Invoke-Rest $nodes[0] 'PUT' $path $bytes $created.ETag
    if ($stale.Status -ne 412 -or $stale.ETag -ne $updated.ETag) { throw 'A stale REST precondition must return 412.' }
    Verify-RestRegister $key $updated.ETag $nodes
    Invoke-Smoke 'verify' $key $nodes | Write-Host
    return $updated.ETag
}

function Verify-RestRegister([string]$key, [string]$etag, [int[]]$nodes) {
    $escaped = [Uri]::EscapeDataString($key)
    foreach ($node in $nodes) {
        $read = Invoke-Rest $node 'GET' "/v1/registers/value?key=$escaped"
        if ($read.Status -ne 200 -or $read.ETag -ne $etag -or $read.Bytes.Length -ne 0) {
            throw 'REST must retain a present empty value and its ETag across restart.'
        }
        $page = Invoke-Rest $node 'GET' "/v1/registers?prefix=$escaped&limit=1"
        $json = $page.Text | ConvertFrom-Json
        if ($page.Status -ne 200 -or $json.keys.Count -ne 1 -or $json.keys[0] -cne $key -or $null -ne $json.continuation) {
            throw "REST listing failed: $($page.Text)"
        }
    }
}

try {
    0..2 | ForEach-Object { Start-Node $_ }
    Invoke-Smoke 'write' 'smoke/initial' @(0, 1, 2)
    $restKey = "rest/../% space/$([char]::ConvertFromUtf32(0x1F600))"
    $restVersion = Write-RestRegister $restKey @(0, 1, 2)
    Stop-Node 2
    Invoke-Smoke 'write' 'smoke/degraded' @(0, 1)
    $degradedVersion = Write-RestRegister 'rest/degraded' @(0, 1)
    0..1 | ForEach-Object { Stop-Node $_ }
    0..2 | ForEach-Object { Start-Node $_ }
    Invoke-Smoke 'verify' 'smoke/initial' @(0, 1, 2)
    Invoke-Smoke 'verify' 'smoke/degraded' @(0, 1, 2)
    Verify-RestRegister $restKey $restVersion @(0, 1, 2)
    Verify-RestRegister 'rest/degraded' $degradedVersion @(0, 1, 2)
    Write-Host 'HTTP/1.1 REST and HTTP/2 gRPC smoke, outage, and restart succeeded.'
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
    $rest.Dispose()
    Remove-Item -LiteralPath $data -Recurse -Force
}
