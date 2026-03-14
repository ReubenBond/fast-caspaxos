param(
    [int]$Rounds = 20,
    [int]$BaseSeed = 5000,
    [string]$Output = "artifacts\comparative",
    [string[]]$Scenario = @("all"),
    [int]$ProposalCount = 500,
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
        # LAN: ~1ms RTT, all links equal. Both quorums local → difference is purely RTT count.
        "lan"            { return @("link-latency-profile=lan", "base-delay-ms=1",  "max-jitter-ms=1") }
        # Symmetric 40ms: all links ~40ms one-way (~80-90ms RTT). Eliminates quorum-distribution
        # effects — fast saves exactly 1 RTT of ~85ms vs classic's 2 RTTs. This is the clearest
        # topology for demonstrating the 1-vs-2 RTT advantage.
        "symmetric40"    { return @("link-latency-profile=lan", "base-delay-ms=40", "max-jitter-ms=5") }
        # US cross-region: US-East (P1-2, A1-3) + US-West (P3, A4-5), 15ms cross-region.
        # Classic quorum (3/5) fits entirely in US-East → ~1ms. Fast quorum (4/5) needs
        # cross-region → ~30ms. Shows the quorum-size cost of fast mode.
        "uscrossregion"  { return @("link-latency-profile=uscrossregion",  "base-delay-ms=1", "max-jitter-ms=1") }
        # 3-region geo: US-East (P1, A1-2), EU-West (P2, A3-4), AP-SE (P3, A5).
        # Both quorums need EU acceptors → same ~85ms bottleneck. Fast saves 1 RTT = ~85ms.
        "geo3region"     { return @("link-latency-profile=geo3region",     "base-delay-ms=1", "max-jitter-ms=1") }
        default          { throw "Unknown topology: $topology" }
    }
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
    "S1-geo-low-contention" = @{
        Description  = "Geo-distributed, low-contention: 1 proposer, no faults. Fast saves 1 RTT when all links are high-latency."
        Combinations = @()
    }
    "S2-agreement-consensus" = @{
        Description  = "Almost-everywhere agreement: concurrent proposers propose the same value, fast rounds succeed without conflict."
        Combinations = @()
    }
    "S3-register-init-burst" = @{
        Description  = "Register initialization: many independent first-writes where fast commit's implicit ballot gives 1 RTT."
        Combinations = @()
    }
    "S4-infrequent-writes" = @{
        Description  = "Infrequent writes: gaps between proposals prevent leader's prepare amortization."
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
        Description  = "Leader gray failure: slow-link on proposer 1, all leader traffic degrades vs only one fast proposer."
        Combinations = @()
    }
    "S8-leader-crash-failover" = @{
        Description  = "Leader crash: proposer 1 crashes, clients timeout and failover. Shows the unavailability stutter."
        Combinations = @()
    }
}

# --- S1: geo-low-contention ---
# Primary showcase: symmetric40 (all links equal, 1-vs-2 RTT is the only signal).
# Also LAN (small absolute difference) and geo3region (real-world asymmetric).
foreach ($topo in @("symmetric40", "lan", "geo3region")) {
    foreach ($mode in $allModes) {
        $modeParams = Get-ModeParams $mode
        $topoParams = Get-TopologyParams $topo
        $params = $modeParams + $topoParams + @(
            "proposer-count=1",
            "acceptor-count=5",
            "proposal-count=1000"
        )
        $scenarios["S1-geo-low-contention"].Combinations += New-Combination `
            -Params $params `
            -Label "topo=$topo mode=$mode proposers=1" `
            -OutputSubpath "$topo\$mode\p1\none"
    }
}

# --- S2: agreement-consensus ---
# Symmetric40 shows the RTT advantage clearly; agreement=true vs false shows value-agreement effect.
foreach ($pCount in @(3, 5)) {
    foreach ($mode in $allModes) {
        foreach ($agreement in @("true", "false")) {
            $modeParams = Get-ModeParams $mode
            $topoParams = Get-TopologyParams "symmetric40"
            $params = $modeParams + $topoParams + @(
                "proposer-count=$pCount",
                "acceptor-count=5",
                "proposal-count=1000",
                "agreement-mode=$agreement"
            )
            $slug = "agreement-$agreement"
            $scenarios["S2-agreement-consensus"].Combinations += New-Combination `
                -Params $params `
                -Label "topo=symmetric40 mode=$mode proposers=$pCount agreement=$agreement" `
                -OutputSubpath "symmetric40\$mode\p${pCount}\$slug"
        }
    }
}

# --- S3: register-init-burst ---
# Symmetric40 for clear RTT distinction, LAN for baseline.
foreach ($topo in @("symmetric40", "lan")) {
    foreach ($pCount in @(1, 3)) {
        foreach ($mode in $allModes) {
            $modeParams = Get-ModeParams $mode
            $topoParams = Get-TopologyParams $topo
            $params = $modeParams + $topoParams + @(
                "proposer-count=$pCount",
                "acceptor-count=5",
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
# Symmetric40 removes quorum noise; write-interval is the sweep variable.
foreach ($interval in @(0, 10, 50, 200)) {
    foreach ($mode in @("fast", "leader")) {
        $modeParams = Get-ModeParams $mode
        $topoParams = Get-TopologyParams "symmetric40"
        $params = $modeParams + $topoParams + @(
            "proposer-count=3",
            "acceptor-count=5",
            "proposal-count=100",
            "write-interval-ms=$interval"
        )
        $slug = "interval-$interval"
        $scenarios["S4-infrequent-writes"].Combinations += New-Combination `
            -Params $params `
            -Label "topo=symmetric40 mode=$mode proposers=3 interval=${interval}ms" `
            -OutputSubpath "symmetric40\$mode\p3\$slug"
    }
}

# --- S5: high-throughput-stream ---
# LAN shows throughput ceiling; symmetric40 shows RTT-dominated throughput.
foreach ($topo in @("lan", "symmetric40")) {
    foreach ($mode in $allModes) {
        $modeParams = Get-ModeParams $mode
        $topoParams = Get-TopologyParams $topo
        $params = $modeParams + $topoParams + @(
            "proposer-count=1",
            "acceptor-count=5",
            "proposal-count=2000"
        )
        $scenarios["S5-high-throughput-stream"].Combinations += New-Combination `
            -Params $params `
            -Label "topo=$topo mode=$mode proposers=1" `
            -OutputSubpath "$topo\$mode\p1\none"
    }
}

# --- S6: contention-spectrum ---
# Symmetric40 isolates the contention variable (no quorum distribution noise).
foreach ($pCount in @(1, 2, 3, 5, 7)) {
    foreach ($mode in $allModes) {
        $modeParams = Get-ModeParams $mode
        $topoParams = Get-TopologyParams "symmetric40"
        $params = $modeParams + $topoParams + @(
            "proposer-count=$pCount",
            "acceptor-count=5",
            "proposal-count=$ProposalCount"
        )
        $scenarios["S6-contention-spectrum"].Combinations += New-Combination `
            -Params $params `
            -Label "topo=symmetric40 mode=$mode proposers=$pCount" `
            -OutputSubpath "symmetric40\$mode\p${pCount}\none"
    }
}

# --- S7: leader-gray-failure ---
# Symmetric40 baseline; slow-link adds 200ms+50ms to the leader, making the degradation dramatic.
$grayFault = "slow-link:proposer:1:at:200:until:400:delay:200:jitter:50"
$grayFaultSlug = "slow-link-proposer-1-at-200-until-400"
foreach ($mode in @("fast", "leader")) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "symmetric40"
    $params = $modeParams + $topoParams + @(
        "proposer-count=3",
        "acceptor-count=5",
        "proposal-count=600",
        "fault-schedule=$grayFault"
    )
    $scenarios["S7-leader-gray-failure"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=symmetric40 mode=$mode proposers=3 fault=slow-link" `
        -OutputSubpath "symmetric40\$mode\p3\$grayFaultSlug"
}

# --- S8: leader-crash-failover ---
# Symmetric40; crash + client failover. Client timeout at 3x baseline RTT for realistic detection.
# Keep this scenario smaller than the others so the crash/retry window stays representative
# without dominating the full comparative suite runtime.
$crashFault = "crash:proposer:1:at:30:until:60"
$crashFaultSlug = "crash-proposer-1-at-30-until-60"
foreach ($mode in @("fast", "leader")) {
    $modeParams = Get-ModeParams $mode
    $topoParams = Get-TopologyParams "symmetric40"
    $params = $modeParams + $topoParams + @(
        "proposer-count=3",
        "acceptor-count=5",
        "proposal-count=90",
        "fault-schedule=$crashFault",
        "client-timeout-ms=300",
        "agreement-mode=true"
    )
    $scenarios["S8-leader-crash-failover"].Combinations += New-Combination `
        -Params $params `
        -Label "topo=symmetric40 mode=$mode proposers=3 fault=crash" `
        -OutputSubpath "symmetric40\$mode\p3\$crashFaultSlug"
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
