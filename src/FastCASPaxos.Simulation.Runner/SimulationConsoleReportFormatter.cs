using System.Globalization;
using System.Text;
using FastCASPaxos.Simulation.Scenarios;

namespace FastCASPaxos.Simulation.Runner;

public static class SimulationConsoleReportFormatter
{
    private const string SuiteScriptPath = @".\run_simulation_suite.ps1";

    public static string FormatBatchReport(
        SimulationBatchResult batchResult,
        string artifactsPath,
        IReadOnlyDictionary<string, string> scenarioParameters)
    {
        ArgumentNullException.ThrowIfNull(batchResult);
        ArgumentException.ThrowIfNullOrWhiteSpace(artifactsPath);
        ArgumentNullException.ThrowIfNull(scenarioParameters);

        List<string> lines = [];
        var successfulRuns = batchResult.Runs.Where(run => run.Success).ToArray();
        var failedRuns = batchResult.Runs.Where(run => !run.Success).ToArray();

        if (successfulRuns.Length > 0)
        {
            lines.Add(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"PASS aggregate summary for {successfulRuns.Length} successful run(s)."));
            AppendBatchSummary(
                lines,
                new SimulationBatchResult(successfulRuns),
                "successful run aggregate statistics:");
            lines.Add(string.Empty);
        }

        foreach (var run in failedRuns)
        {
            lines.Add(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"FAIL {run.ScenarioName} round={run.Round} seed={run.Seed} :: {run.Summary}"));
            lines.Add(SimulationStatisticsFormatter.FormatRunConsoleSummary(run));
            lines.Add("  rerun this failure in isolation:");
            lines.Add($"    {BuildIsolationCommand(run, scenarioParameters, waitForDebugger: false)}");
            lines.Add("  rerun this failure in isolation and wait for the debugger:");
            lines.Add($"    {BuildIsolationCommand(run, scenarioParameters, waitForDebugger: true)}");
            lines.Add(string.Empty);
        }

        lines.Add(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Completed {batchResult.Runs.Count} run(s): {batchResult.SucceededCount} passed, {batchResult.FailedCount} failed."));
        lines.Add($"Artifacts: {artifactsPath}");
        return string.Join(Environment.NewLine, lines);
    }

    private static void AppendBatchSummary(
        List<string> lines,
        SimulationBatchResult batchResult,
        string heading)
    {
        lines.Add(heading);
        var summaryLines = SimulationStatisticsFormatter
            .FormatBatchConsoleSummary(batchResult)
            .Split(Environment.NewLine, StringSplitOptions.None);
        foreach (var summaryLine in summaryLines.Skip(1))
        {
            lines.Add(summaryLine);
        }
    }

    private static string BuildIsolationCommand(
        SimulationRunResult run,
        IReadOnlyDictionary<string, string> scenarioParameters,
        bool waitForDebugger)
    {
        var builder = new StringBuilder()
            .Append(SuiteScriptPath)
            .Append(" -Scenario ")
            .Append(FormatPowerShellLiteral(run.ScenarioName))
            .Append(" -Seed ")
            .Append(run.Seed.ToString(CultureInfo.InvariantCulture))
            .Append(" -Rounds 1");

        foreach (var scenarioParameter in scenarioParameters.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            builder
                .Append(" -ScenarioParam ")
                .Append(FormatPowerShellLiteral($"{scenarioParameter.Key}={scenarioParameter.Value}"));
        }

        if (waitForDebugger)
        {
            builder.Append(" -WaitForDebugger");
        }

        return builder.ToString();
    }

    private static string FormatPowerShellLiteral(string value) =>
        $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
