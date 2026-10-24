param(
    [string[]]$Scenario = @("string-corpus", "set-corpus"),
    [int]$Seed = 0,
    [ValidateRange(100, [int]::MaxValue)]
    [int]$Rounds = 100,
    [switch]$KeepArtifacts
)

$ErrorActionPreference = "Stop"
$csharpRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = Split-Path -Parent $csharpRoot
$checkerPath = Join-Path $repoRoot "tools\porcupine-checker"
$outputDirectory = Join-Path $env:TEMP ("fast-caspaxos-porcupine-" + [guid]::NewGuid().ToString("N"))
$pushedRepoRoot = $false
$pushedCheckerPath = $false

if ($Seed -le 0)
{
    $Seed = Get-Random -Minimum 10000 -Maximum ([int]::MaxValue - $Rounds)
}

try
{
    Push-Location $csharpRoot
    $pushedRepoRoot = $true

    Write-Host "Using randomized validation seed $Seed across $Rounds rounds."

    $scenarioArgs = @()
    foreach ($name in $Scenario)
    {
        $scenarioArgs += @("--scenario", $name)
    }

    dotnet run --framework net11.0 --project src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj -- @scenarioArgs --seed $Seed --rounds $Rounds --output $outputDirectory
    if ($LASTEXITCODE -ne 0)
    {
        throw "Simulation runner failed with exit code $LASTEXITCODE."
    }

    Push-Location $checkerPath
    $pushedCheckerPath = $true
    go test .
    if ($LASTEXITCODE -ne 0)
    {
        throw "Go checker tests failed with exit code $LASTEXITCODE."
    }

    go run . -history $outputDirectory
    if ($LASTEXITCODE -ne 0)
    {
        throw "Porcupine checker failed for '$outputDirectory' with exit code $LASTEXITCODE."
    }
}
finally
{
    if ($pushedCheckerPath)
    {
        Pop-Location
    }

    if ($pushedRepoRoot)
    {
        Pop-Location
    }

    if (-not $KeepArtifacts -and (Test-Path $outputDirectory))
    {
        Remove-Item -Recurse -Force $outputDirectory
    }
}
