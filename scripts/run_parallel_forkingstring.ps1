param(
    [int]$Rounds = 100,
    [int]$BaseSeed = 7001,
    [int]$Jobs = [System.Environment]::ProcessorCount,
    [string]$Output = "artifacts\simulation-runner\parallel-forkingstring"
)

$repoRoot = Split-Path $PSScriptRoot -Parent
Set-Location $repoRoot

Write-Host "Launching $Jobs parallel forking-string jobs ($Rounds rounds each, base seed $BaseSeed)..."

$jobs = 1..$Jobs | ForEach-Object {
    $jobIndex = $_
    $seed = $BaseSeed + ($jobIndex - 1) * $Rounds
    $outDir = Join-Path $Output "job-$jobIndex"

    Start-Job -ScriptBlock {
        param($root, $seed, $rounds, $outDir)
        Set-Location $root
        dotnet run --project src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj `
            -- --scenario forking-string-corpus `
               --seed $seed `
               --rounds $rounds `
               --output $outDir
    } -ArgumentList $repoRoot, $seed, $Rounds, $outDir
}

$jobs | Wait-Job | Receive-Job

$failed = $jobs | Where-Object { $_.State -eq 'Failed' }
$jobs | Remove-Job

if ($failed.Count -gt 0) {
    Write-Error "$($failed.Count) job(s) failed."
    exit 1
}

Write-Host "All $Jobs parallel forking-string jobs completed successfully."
