param(
    [int]$Seed = 42,
    [int]$Rounds = 1,
    [int]$ProposalCount = 1000,
    [string]$Output = "artifacts\benchmark",
    [string[]]$Mode = @("classic", "fast", "leader", "fast+leader"),
    [string[]]$Topology = @("lan", "uscrossregion", "geo3region"),
    [string[]]$Fault = @("none", "suspend:acceptor:3:at:333:until:666"),
    [int[]]$ProposerCount = @(1, 3),
    [switch]$ContinueOnFailure,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Get-ModeParams {
    param([string]$mode)
    switch ($mode) {
        "classic"      { return @("fast=false", "leader=false") }
        "fast"         { return @("fast=true",  "leader=false") }
        "leader"       { return @("fast=false", "leader=true")  }
        "fast+leader"  { return @("fast=true",  "leader=true")  }
        default        { throw "Unknown mode: $mode" }
    }
}

function Get-TopologyParams {
    param([string]$topology)
    switch ($topology) {
        "lan"            { return @("link-latency-profile=lan",            "base-delay-ms=1", "max-jitter-ms=1") }
        "uscrossregion"  { return @("link-latency-profile=uscrossregion",  "base-delay-ms=1", "max-jitter-ms=1") }
        "geo3region"     { return @("link-latency-profile=geo3region",     "base-delay-ms=1", "max-jitter-ms=1") }
        default          { throw "Unknown topology: $topology" }
    }
}

$totalCombinations = $Mode.Count * $Topology.Count * $Fault.Count * $ProposerCount.Count
$completed = 0
$failed = 0
$startTime = Get-Date

Write-Host "Benchmark sweep: $totalCombinations combinations"
Write-Host "  Modes:     $($Mode -join ', ')"
Write-Host "  Topologies: $($Topology -join ', ')"
Write-Host "  Faults:    $($Fault -join ', ')"
Write-Host "  Proposers: $($ProposerCount -join ', ')"
Write-Host "  Proposals: $ProposalCount per run, $Rounds round(s), seed=$Seed"
Write-Host ""

$manifest = @()

foreach ($mode in $Mode) {
    foreach ($topo in $Topology) {
        foreach ($fault in $Fault) {
            foreach ($pCount in $ProposerCount) {
                $faultSlug = if ($fault -eq "none") { "none" } else { ($fault -replace "[:\.]", "-").Substring(0, [Math]::Min(40, $fault.Length)) }
                $outputDir = Join-Path $Output "$topo\$mode\p${pCount}\$faultSlug"

                $modeParams = Get-ModeParams $mode
                $topoParams = Get-TopologyParams $topo

                $arguments = @(
                    "run", "--project", "src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj",
                    "--",
                    "--scenario", "benchmark-workload",
                    "--seed", $Seed,
                    "--rounds", $Rounds,
                    "--output", $outputDir
                )

                foreach ($p in $modeParams)  { $arguments += @("--param", $p) }
                foreach ($p in $topoParams)  { $arguments += @("--param", $p) }
                $arguments += @("--param", "proposer-count=$pCount")
                $arguments += @("--param", "proposal-count=$ProposalCount")

                if ($fault -ne "none") {
                    $arguments += @("--param", "fault-schedule=$fault")
                }

                if ($ContinueOnFailure) {
                    $arguments += "--continue-on-failure"
                }

                $label = "mode=$mode topo=$topo proposers=$pCount fault=$faultSlug"
                $completed++

                if ($DryRun) {
                    Write-Host "[$completed/$totalCombinations] DRY RUN: $label"
                    Write-Host "  dotnet $($arguments -join ' ')"
                    continue
                }

                Write-Host "[$completed/$totalCombinations] Running: $label"
                & dotnet @arguments
                if ($LASTEXITCODE -ne 0) {
                    $failed++
                    Write-Host "  FAILED (exit code $LASTEXITCODE)" -ForegroundColor Red
                    if (-not $ContinueOnFailure) {
                        Write-Host "Stopping due to failure. Use -ContinueOnFailure to keep going."
                        exit 1
                    }
                } else {
                    Write-Host "  PASSED" -ForegroundColor Green
                }

                $manifest += [PSCustomObject]@{
                    Mode = $mode
                    Topology = $topo
                    ProposerCount = $pCount
                    Fault = $fault
                    OutputDir = (Resolve-Path $outputDir -ErrorAction SilentlyContinue) ?? $outputDir
                    Seed = $Seed
                    Rounds = $Rounds
                    ProposalCount = $ProposalCount
                }
            }
        }
    }
}

$elapsed = (Get-Date) - $startTime
Write-Host ""
Write-Host "Benchmark sweep complete: $($completed - $failed) passed, $failed failed ($($elapsed.TotalSeconds.ToString('F1'))s)"
Write-Host "Output: $(Resolve-Path $Output -ErrorAction SilentlyContinue)"

if ($manifest.Count -gt 0 -and -not $DryRun) {
    $manifest | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $Output "manifest.json")
    Write-Host "Manifest: $(Join-Path $Output 'manifest.json')"
}

Write-Host ""
Write-Host "To generate charts, run:"
Write-Host "  uv run scripts\plot_benchmarks.py --input $Output"
