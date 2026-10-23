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
$streams = @{}
$cursors = @{}
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
        $info.ArgumentList.Add("--FastCASPaxos:Peers:${i}:HttpEndpoint=http://127.0.0.1:$($BasePort + 3 + $i)")
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
    Stop-Events $index
    $process = $processes[$index]
    if ($null -eq $process) { return }
    if (-not $process.HasExited) { $process.Kill($true) }
    $process.WaitForExit()
    $process.Dispose()
    $processes.Remove($index)
}

function Read-Event([int]$node) {
    $fields = @{}
    while ($true) {
        $line = $streams[$node].Reader.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(15)).GetAwaiter().GetResult()
        if ($null -eq $line) { throw "Node $node closed its event stream." }
        if ($line.Length -eq 0) {
            if ($fields.ContainsKey('data')) { break }
            continue
        }
        $separator = $line.IndexOf(':')
        if ($separator -gt 0) { $fields[$line.Substring(0, $separator)] = $line.Substring($separator + 1).TrimStart() }
    }
    $event = $fields.data | ConvertFrom-Json
    if ($event.schemaVersion -ne 1 -or $event.nodeId -cne @('a', 'b', 'c')[$node] -or $fields.event -cne $event.kind) {
        throw "Node $node returned an invalid SSE envelope."
    }
    if ($event.kind -ne 'heartbeat') {
        if ($fields.id -cne "$($event.bootId):$($event.sequence)") { throw 'SSE id must identify boot and exact sequence.' }
        $cursors[$node] = $fields.id
    }
    return $event
}

function Start-Events([int]$node, [string]$after) {
    $origin = "http://127.0.0.1:$($BasePort + 3 + $node)"
    $status = Invoke-Rest $node 'GET' '/v1/admin/node'
    $discovery = $status.Text | ConvertFrom-Json
    if ($status.Status -ne 200 -or $discovery.peers.Count -ne 3 -or $discovery.storage.status -ne 'ready') {
        throw "Node $node discovery/storage initialization failed."
    }
    for ($i = 0; $i -lt 3; $i++) {
        if ($discovery.peers[$i].httpEndpoint -cne "http://127.0.0.1:$($BasePort + 3 + $i)" -or
            $discovery.peers[$i].eventsUrl -cne "$($discovery.peers[$i].httpEndpoint)/v1/admin/events") {
            throw 'Discovery must advertise every explicit browser endpoint.'
        }
    }
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Get, "$origin/v1/admin/events")
    if ($after) { $request.Headers.Add('Last-Event-ID', $after) }
    $response = $rest.SendAsync($request, [Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
    if (-not $response.IsSuccessStatusCode -or $response.Content.Headers.ContentType.MediaType -ne 'text/event-stream' -or
        -not $response.Headers.CacheControl.NoStore -or $response.Headers.GetValues('X-Accel-Buffering') -notcontains 'no') {
        $response.Dispose()
        $request.Dispose()
        throw "Node $node SSE response contract failed."
    }
    $streams[$node] = @{
        Request = $request
        Response = $response
        Reader = [IO.StreamReader]::new($response.Content.ReadAsStream())
    }
    $first = Read-Event $node
    $reason = if ($after) { 'restarted' } else { 'initial' }
    if ($first.kind -ne 'reset' -or $first.data.reason -ne $reason) {
        throw "Node $node must establish an explicit $reason reset."
    }
}

function Stop-Events([int]$node) {
    if (-not $streams.ContainsKey($node)) { return }
    $streams[$node].Reader.Dispose()
    $streams[$node].Response.Dispose()
    $streams[$node].Request.Dispose()
    $streams.Remove($node)
}

function Verify-Events([string]$key, [int[]]$nodes) {
    foreach ($node in $nodes) {
        $seen = @{}
        for ($count = 0; $count -lt 1000 -and $seen.Count -lt 3; $count++) {
            $event = Read-Event $node
            if ($event.kind -eq 'reset') { throw 'Smoke observations unexpectedly overran retained history.' }
            if ($event.data.key.text -cne $key) { continue }
            if ($event.kind -eq 'protocol' -and $event.data.direction -eq 'inbound') {
                if (-not $event.data.peerId -or -not $event.data.callId -or -not $event.data.operationId) {
                    throw 'Cross-host protocol observations must carry peer and invocation correlation.'
                }
                $seen.protocol = $true
            }
            if ($event.kind -eq 'acceptor-state') {
                if ($null -eq $event.data.acceptedBallot.round -or $null -eq $event.data.promisedBallot.proposer) {
                    throw 'Local durable state must preserve full ballots.'
                }
                $seen.state = $true
            }
            if ($event.kind -eq 'operation') { $seen.operation = $true }
        }
        if ($seen.Count -ne 3) { throw "Node $node must expose protocol, durable state, and quorum outcomes." }
        $snapshot = Invoke-Rest $node 'GET' "/v1/admin/state?key=$([Uri]::EscapeDataString($key))"
        $state = $snapshot.Text | ConvertFrom-Json
        if ($snapshot.Status -ne 200 -or $state.entry.key -cne $key -or
            -not $state.entry.register.isPresent -or $state.entry.register.value.length -ne 0) {
            throw 'Read-only local inspection must expose the accepted register envelope.'
        }
    }
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
    0..2 | ForEach-Object { Start-Events $_ }
    Invoke-Smoke 'write' 'smoke/initial' @(0, 1, 2)
    Verify-Events 'smoke/initial' @(0, 1, 2)
    $restKey = "rest/../% space/$([char]::ConvertFromUtf32(0x1F600))"
    $restVersion = Write-RestRegister $restKey @(0, 1, 2)
    Stop-Node 2
    Invoke-Smoke 'write' 'smoke/degraded' @(0, 1)
    Verify-Events 'smoke/degraded' @(0, 1)
    $degradedVersion = Write-RestRegister 'rest/degraded' @(0, 1)
    0..1 | ForEach-Object { Stop-Node $_ }
    0..2 | ForEach-Object { Start-Node $_ }
    0..2 | ForEach-Object { Start-Events $_ $cursors[$_] }
    Invoke-Smoke 'verify' 'smoke/initial' @(0, 1, 2)
    Invoke-Smoke 'verify' 'smoke/degraded' @(0, 1, 2)
    Verify-RestRegister $restKey $restVersion @(0, 1, 2)
    Verify-RestRegister 'rest/degraded' $degradedVersion @(0, 1, 2)
    Write-Host 'REST, gRPC, and per-host SSE smoke, outage, discovery, and restart succeeded.'
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
