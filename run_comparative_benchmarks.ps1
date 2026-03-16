param(
    [int]$Rounds = 20,
    [int]$BaseSeed = 5000,
    [string]$Output = "artifacts\comparative",
    [string[]]$Scenario = @("all"),
    [int]$ProposalCount = 500,
    [ValidateRange(5, [int]::MaxValue)]
    [int]$AcceptorCount = 5,
    [switch]$ContinueOnFailure,
    [switch]$DryRun,
    [switch]$SkipCharts,
    [switch]$WaitForDebugger
)

$ErrorActionPreference = "Stop"

$projectPath = "src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj"
$configuration = if ($WaitForDebugger) { "Debug" } else { "Release" }

function Get-ModeParams {
    param([string]$mode)
    switch ($mode) {
        "classic"      { return @("fast=false", "leader=false") }
        "fast"         { return @("fast=true",  "leader=false") }
        "leader"       { return @("fast=false", "leader=true")  }
        default        { throw "Unknown mode: $mode" }
    }
}

function Get-TopologyParams {
    param([string]$topology)
    switch ($topology) {
        # LAN: ~1ms RTT, all links equal.
        "lan"            { return @("link-latency-profile=lan", "base-delay-ms=1",  "max-jitter-ms=1") }
        # 3-region US: East US 2 / West US / West US 3 with Azure-backed RTTs of 20-66ms.
        "us3region"      { return @("link-latency-profile=us3region",      "base-delay-ms=1", "max-jitter-ms=1") }
        "uscrossregion"  { return @("link-latency-profile=us3region",      "base-delay-ms=1", "max-jitter-ms=1") }
        # 5-region GEO: East US 2 / France Central / Southeast Asia / Brazil South / Australia Southeast.
        # Cross-region RTTs now span 85-332ms using the Azure P50 medians for that region set.
        "geo5region"     { return @("link-latency-profile=geo5region",     "base-delay-ms=1", "max-jitter-ms=1") }
        "geo3region"     { return @("link-latency-profile=geo5region",     "base-delay-ms=1", "max-jitter-ms=1") }
        default          { throw "Unknown topology: $topology" }
    }
}

function Get-ClusterSizeParams {
    param(
        [string]$ScenarioName,
        [int]$ProposerCount,
        [int]$AcceptorCount
    )

    if ($ProposerCount -gt $AcceptorCount) {
        throw "Scenario '$ScenarioName' requests proposer-count=$ProposerCount with acceptor-count=$AcceptorCount. Comparative benchmarks keep each proposer collocated with a distinct acceptor, so proposer-count must not exceed acceptor-count."
    }

    return @("proposer-count=$ProposerCount", "acceptor-count=$AcceptorCount")
}

function New-Combination {
    param(
        [string[]]$Params,
        [string]$Label,
        [string]$OutputSubpath
    )
    return @{
        Params        = $Params
        Label         = $Label
        OutputSubpath = $OutputSubpath
    }
}

function Resolve-PathString {
    param([string]$Path)

    $resolvedPath = Resolve-Path -Path $Path -ErrorAction SilentlyContinue
    if ($null -ne $resolvedPath) {
        return $resolvedPath.Path
    }

    return $Path
}

# ---------------------------------------------------------------------------
# Define scenario configurations
# ---------------------------------------------------------------------------

$allModes = @("classic", "fast", "leader")

$scenarios = [ordered]@{
    "S1-geo-no-contention" = @{
        Description  = "Geo-distributed, no contention: one client stream originates from Australia Southeast (no proposer). Fast routes to the nearest proposer in Southeast Asia (88 ms), while leader mode routes to France Central (236 ms) via leader-ordinal=2."
        Combinations = @()
    }
    "S2-agreement-consensus" = @{
        Description  = "Agreement workload with leader suspend: five proposers with per-client routing; suspending proposer 1 at proposal 400 forces client failover, showing leader mode's recovery cost vs fast/classic resilience."
        Combinations = @()
    }
    "S3-register-init-burst" = @{
        Description  = "Register initialization: many independent first-writes where fast commit's implicit ballot gives 1 RTT."
        Combinations = @()
    }
    "S4-infrequent-writes" = @{
        Description  = "Infrequent writes: serial random-proposer selection across proposers with learn propagation between commits."
        Combinations = @()
    }
    "S5-high-throughput-stream" = @{
        Description  = "High write rate: single proposer sustained stream, leader amortizes prepare cost."
        Combinations = @()
    }
    "S6-contention-spectrum" = @{
        Description  = "Contention sweep: increasing proposers shows crossover where leader beats fast."
        Combinations = @()
    }
    "S7-leader-gray-failure" = @{
        Description  = "Leader gray failure: 5 proposers, 1 explicit client at proposer 1, proposer 5 is leader; slow-link (500ms) on proposer 5. Leader mode sends through slow leader; classic/fast route locally."
        Combinations = @()
    }
    "S8-leader-crash-failover" = @{
        Description  = "Leader crash: proposer 1 crashes, clients timeout and failover. Shows the unavailability stutter."
        Combinations = @()
    }
}

# --- S1: geo-no-contention ---
# One explicit geo client is placed in Australia Southeast (ordinal 5), a region with no proposer.
# Its nearest proposer is proposer 3 (Southeast Asia, 88 ms RTT). Leader mode designates
# proposer 2 (France Central) via leader-ordinal=2, so it pays a 236 ms client-to-primary hop
# instead of the 88 ms fast route, and commits through a 148 ms quorum versus fast's 228 ms.
# The workload stays contention-free by driving a single client request stream.
foreach ($mode in $allModes) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "geo5region"
    $clusterParams = Get-ClusterSizeParams -ScenarioName "S1-geo-no-contention" -ProposerCount 3 -AcceptorCount $AcceptorCount
    $params = $modeParams + $topoParams + $clusterParams + @(
        "proposal-count=1000",
        "client-ordinal=5",
        "leader-ordinal=2"
    )
    $scenarios["S1-geo-no-contention"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=geo mode=$mode proposers=3 client=5 leader=2" `
        -OutputSubpath "geo\$mode\p3\client-5"
}

# --- S2: agreement-consensus ---
# LAN is the merged single-region baseline. Keep five proposers so each proposer
# can align with a distinct acceptor. Per-client routing assigns each client a
# "current proposer" (leader mode: all start on proposer 1; fast/classic: spread).
# Suspending proposer 1 at proposal 400 forces client failover and shows leader
# mode's single-point-of-failure cost versus fast/classic resilience.
$s2SuspendFault = "suspend:proposer:1:at:400:until:1001"
$s2FaultSlug = "suspend-proposer-1-at-400"
$s2ClusterParams = Get-ClusterSizeParams -ScenarioName "S2-agreement-consensus" -ProposerCount 5 -AcceptorCount $AcceptorCount
foreach ($mode in $allModes) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "lan"
    $params = $modeParams + $topoParams + $s2ClusterParams + @(
        "proposal-count=1000",
        "agreement-mode=true",
        "agreement-concurrent-proposers=true",
        "fault-schedule=$s2SuspendFault"
    )
    $scenarios["S2-agreement-consensus"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=lan mode=$mode proposers=5 agreement=true fault=suspend" `
        -OutputSubpath "lan\$mode\p5\$s2FaultSlug"
}

# --- S3: register-init-burst ---
# LAN is the merged single-region baseline.
foreach ($topo in @("lan")) {
    foreach ($pCount in @(1)) {
        foreach ($mode in $allModes) {
            $modeParams = Get-ModeParams $mode
            $topoParams = Get-TopologyParams $topo
            $clusterParams = Get-ClusterSizeParams -ScenarioName "S3-register-init-burst" -ProposerCount $pCount -AcceptorCount $AcceptorCount
            $params = $modeParams + $topoParams + $clusterParams + @(
                "proposal-count=200",
                "register-init-mode=true"
            )
            $scenarios["S3-register-init-burst"].Combinations += New-Combination `
                -Params $params `
                -Label "topo=$topo mode=$mode proposers=$pCount register-init=true" `
                -OutputSubpath "$topo\$mode\p${pCount}\none"
        }
    }
}

# --- S4: infrequent-writes ---
# Serial random-proposer selection across 3 proposers with collocated clients. 1s write interval
# ensures learn broadcasts propagate the committed ballot to all peers before the
# next proposal. Fast reuses the learned fast ballot (1 RTT); classic cannot (2 RTT);
# leader always goes to P1 who reuses its piggybacked ballot (slightly under 2 RTT
# on average due to remote client forwarding).
foreach ($mode in $allModes) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "lan"
    $clusterParams = Get-ClusterSizeParams -ScenarioName "S4-infrequent-writes" -ProposerCount 3 -AcceptorCount $AcceptorCount
    $params = $modeParams + $topoParams + $clusterParams + @(
        "proposal-count=100",
        "write-interval-ms=1000",
        "serial-random-proposer=true"
    )
    $scenarios["S4-infrequent-writes"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=lan mode=$mode proposers=3 serial-random=true interval=1000ms" `
        -OutputSubpath "lan\$mode\p3\serial-random"
}

# --- S5: high-throughput-stream ---
# LAN is the merged single-region throughput baseline.
foreach ($mode in $allModes) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "lan"
    $clusterParams = Get-ClusterSizeParams -ScenarioName "S5-high-throughput-stream" -ProposerCount 1 -AcceptorCount $AcceptorCount
    $params = $modeParams + $topoParams + $clusterParams + @(
        "proposal-count=2000"
    )
    $scenarios["S5-high-throughput-stream"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=lan mode=$mode proposers=1" `
        -OutputSubpath "lan\$mode\p1\none"
}

# --- S6: contention-spectrum ---
# LAN is the merged single-region baseline for the contention sweep.
# Stop at five proposers so every proposer can still align with a distinct acceptor.
foreach ($pCount in @(1, 2, 3, 5)) {
    foreach ($mode in $allModes) {
        $modeParams = Get-ModeParams $mode
        $topoParams = Get-TopologyParams "lan"
        $clusterParams = Get-ClusterSizeParams -ScenarioName "S6-contention-spectrum" -ProposerCount $pCount -AcceptorCount $AcceptorCount
        $params = $modeParams + $topoParams + $clusterParams + @(
            "proposal-count=$ProposalCount"
        )
        $scenarios["S6-contention-spectrum"].Combinations += New-Combination `
            -Params $params `
            -Label "topo=lan mode=$mode proposers=$pCount" `
            -OutputSubpath "lan\$mode\p${pCount}\none"
    }
}

# --- S7: leader-gray-failure ---
# 5 proposers, 1 explicit client colocated with proposer 1.
# Proposer 5 is the initial leader. slow-link adds 500ms+50ms to proposer 5
# from proposal 200 through the end of the run (no recovery).
# Client timeout is set high (30s) so the client never fails over away from the slow leader.
# Leader mode: client sends to proposer 5 (slow), sustained degradation.
# Classic/fast: client sends to local proposer 1 (unaffected).
$grayFault = "slow-link:proposer:5:at:200:until:600:delay:15:jitter:3"
$grayFaultSlug = "slow-link-proposer-5-at-200-until-600"
foreach ($mode in $allModes) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "lan"
    $clusterParams = Get-ClusterSizeParams -ScenarioName "S7-leader-gray-failure" -ProposerCount 5 -AcceptorCount $AcceptorCount
    $modeExtra = @()
    $params = $modeParams + $topoParams + $clusterParams + @(
        "proposal-count=600",
        "fault-schedule=$grayFault",
        "client-ordinal=1",
        "leader-ordinal=5",
        "client-timeout-ms=30000",
        "proposer-response-timeout-ms=3000"
    ) + $modeExtra
    $scenarios["S7-leader-gray-failure"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=lan mode=$mode proposers=5 client=1 leader=5 fault=slow-link" `
        -OutputSubpath "lan\$mode\p5\$grayFaultSlug"
}

# --- S8: leader-crash-failover ---
# LAN baseline; crash + client failover. Keep timeout modest so failover remains visible on a low-latency link.
# Keep this scenario smaller than the others so the crash/retry window stays representative
# without dominating the full comparative suite runtime.
$crashFault = "crash:proposer:1:at:30:until:60"
$crashFaultSlug = "crash-proposer-1-at-30-until-60"
foreach ($mode in @("fast", "leader")) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "lan"
    $clusterParams = Get-ClusterSizeParams -ScenarioName "S8-leader-crash-failover" -ProposerCount 3 -AcceptorCount $AcceptorCount
    $params = $modeParams + $topoParams + $clusterParams + @(
        "proposal-count=90",
        "fault-schedule=$crashFault",
        "agreement-mode=true"
    )
    $scenarios["S8-leader-crash-failover"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=lan mode=$mode proposers=3 fault=crash" `
        -OutputSubpath "lan\$mode\p3\$crashFaultSlug"
}

# ---------------------------------------------------------------------------
# Resolve which scenarios to run
# ---------------------------------------------------------------------------

$selectedScenarios = if ($Scenario -contains "all") {
    $scenarios.Keys | Sort-Object
} else {
    $resolved = @()
    foreach ($s in $Scenario) {
        $match = $scenarios.Keys | Where-Object { $_ -like "*$s*" }
        if (-not $match) { throw "Unknown scenario: $s" }
        $resolved += $match
    }
    $resolved | Sort-Object -Unique
}

$totalCombinations = ($selectedScenarios | ForEach-Object { $scenarios[$_].Combinations.Count } | Measure-Object -Sum).Sum

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "Fast CASPaxos Comparative Benchmark Suite" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Configuration:          $configuration"
Write-Host "Rounds per combination: $Rounds (for statistical averaging)"
Write-Host "Base seed:              $BaseSeed"
Write-Host "Output:                 $Output"
Write-Host "Scenarios:              $($selectedScenarios -join ', ')"
Write-Host "Acceptors per scenario: $AcceptorCount"
Write-Host "Total combinations:     $totalCombinations"
Write-Host ""

# ---------------------------------------------------------------------------
# Build once up front
# ---------------------------------------------------------------------------

if (-not $DryRun) {
    Write-Host "Building ($configuration)..." -ForegroundColor Yellow
    & dotnet build $projectPath -c $configuration --nologo -v quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
    Write-Host "Build succeeded." -ForegroundColor Green
    Write-Host ""
}

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

$completed = 0
$failed = 0
$startTime = Get-Date
$manifest = @()

foreach ($scenarioName in $selectedScenarios) {
    $scenarioDef = $scenarios[$scenarioName]
    Write-Host ""
    Write-Host "=== $scenarioName ===" -ForegroundColor Cyan
    Write-Host $scenarioDef.Description

    foreach ($combo in $scenarioDef.Combinations) {
        $outputDir = Join-Path $Output "$scenarioName\$($combo.OutputSubpath)"

        $arguments = @(
            "run", "--no-build", "--project", $projectPath, "-c", $configuration,
            "--",
            "--scenario", "benchmark-workload",
            "--seed", $BaseSeed,
            "--rounds", $Rounds,
            "--output", $outputDir
        )

        foreach ($p in $combo.Params) {
            $arguments += @("--param", $p)
        }

        if ($ContinueOnFailure) {
            $arguments += "--continue-on-failure"
        }

        if ($WaitForDebugger) {
            $arguments += "--wait-for-debugger"
        }

        $completed++
        $label = $combo.Label

        if ($DryRun) {
            Write-Host "  [$completed/$totalCombinations] DRY RUN: $label"
            Write-Host "    dotnet $($arguments -join ' ')"
            continue
        }

        Write-Host "  [$completed/$totalCombinations] Running: $label"
        & dotnet @arguments
        if ($LASTEXITCODE -ne 0) {
            $failed++
            Write-Host "    FAILED (exit code $LASTEXITCODE)" -ForegroundColor Red
            if (-not $ContinueOnFailure) {
                Write-Host "Stopping due to failure. Use -ContinueOnFailure to keep going."
                exit 1
            }
        } else {
            Write-Host "    PASSED" -ForegroundColor Green
        }

        $manifest += [PSCustomObject]@{
            Scenario      = $scenarioName
            Label         = $label
            OutputDir     = Resolve-PathString $outputDir
            Params        = ($combo.Params -join "; ")
            Rounds        = $Rounds
            BaseSeed      = $BaseSeed
            AcceptorCount = $AcceptorCount
        }
    }
}

# ---------------------------------------------------------------------------
# Manifest & Summary
# ---------------------------------------------------------------------------

$elapsed = (Get-Date) - $startTime
Write-Host ""
Write-Host "Comparative benchmark complete: $($completed - $failed) passed, $failed failed ($($elapsed.TotalSeconds.ToString('F1'))s)"
Write-Host "Output: $(Resolve-PathString $Output)"

if ($manifest.Count -gt 0 -and -not $DryRun) {
    $manifestPath = Join-Path $Output "comparative-manifest.json"
    $manifest | ConvertTo-Json -Depth 100 | Set-Content $manifestPath
    Write-Host "Manifest: $manifestPath"
}

if (-not $SkipCharts -and -not $DryRun) {
    Write-Host ""
    Write-Host "Generating charts..."
    & uv run scripts\plot_benchmarks.py --input $Output --comparative
}

Write-Host ""
Write-Host "To regenerate charts manually, run:"
Write-Host "  uv run scripts\plot_benchmarks.py --input $Output --comparative"
