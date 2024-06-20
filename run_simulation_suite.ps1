param(
    [string[]]$Scenario = @("all"),
    [int]$Seed = 7001,
    [int]$Rounds = 1,
    [string]$Output = "artifacts\simulation-runner",
    [switch]$ContinueOnFailure,
    [string[]]$ScenarioParam = @(),
    [switch]$WaitForDebugger
)

$scenarioArgs = @()
foreach ($name in $Scenario)
{
    $scenarioArgs += @("--scenario", $name)
}

$arguments = @(
    "run",
    "--project",
    "src\FastCASPaxos.Simulation.Runner\FastCASPaxos.Simulation.Runner.csproj",
    "--"
) + $scenarioArgs + @(
    "--seed",
    $Seed,
    "--rounds",
    $Rounds,
    "--output",
    $Output
)

foreach ($parameter in $ScenarioParam)
{
    $arguments += @("--param", $parameter)
}

if ($ContinueOnFailure)
{
    $arguments += "--continue-on-failure"
}

if ($WaitForDebugger)
{
    $arguments += "--wait-for-debugger"
}

& dotnet @arguments
