param(
    [int]$Workers = 32
)

$ErrorActionPreference = "Stop"

$sany = "C:\tools\tlaplus\sany.cmd"
$tlc = "C:\tools\tlaplus\tlc.cmd"

$modules = @(
    ".\MCFastCASPaxosSafety.tla",
    ".\MCFastCASPaxosLiveness.tla"
)

$runs = @(
    @{
        Name = "MCFastCASPaxosSafety"
        Config = ".\MCFastCASPaxosSafety.cfg"
        Spec = ".\MCFastCASPaxosSafety.tla"
    },
    @{
        Name = "MCFastCASPaxosLiveness"
        Config = ".\MCFastCASPaxosLiveness.cfg"
        Spec = ".\MCFastCASPaxosLiveness.tla"
    }
)

Push-Location $PSScriptRoot
try {
    $originalJavaToolOptions = $env:JAVA_TOOL_OPTIONS
    if ([string]::IsNullOrWhiteSpace($originalJavaToolOptions)) {
        $env:JAVA_TOOL_OPTIONS = "-XX:+UseParallelGC"
    }
    elseif ($originalJavaToolOptions -notmatch "(?i)(^|\\s)-XX:\\+UseParallelGC(\\s|$)") {
        $env:JAVA_TOOL_OPTIONS = "-XX:+UseParallelGC $originalJavaToolOptions"
    }

    foreach ($module in $modules) {
        Write-Host "=== SANY $module ==="
        & $sany $module
        if ($LASTEXITCODE -ne 0) {
            throw "SANY failed for $module"
        }
    }

    foreach ($run in $runs) {
        Write-Host "=== TLC $($run.Name) ==="
        & $tlc -workers $Workers -checkpoint 0 -config $run.Config $run.Spec
        if ($LASTEXITCODE -ne 0) {
            throw "TLC failed for $($run.Name)"
        }
    }
}
finally {
    $env:JAVA_TOOL_OPTIONS = $originalJavaToolOptions
    Pop-Location
}
