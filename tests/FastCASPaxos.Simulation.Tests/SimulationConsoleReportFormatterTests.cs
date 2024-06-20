using FastCASPaxos.Simulation.Runner;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class SimulationConsoleReportFormatterTests
{
    [Fact]
    public void FormatBatchReport_CollapsesSuccessfulRunsIntoSingleAggregateBlock()
    {
        var batchResult = new SimulationBatchResult(
        [
            CreateRun("string-corpus", round: 1, seed: 7001, success: true, summary: "ok"),
            CreateRun("set-corpus", round: 1, seed: 7001, success: true, summary: "ok"),
        ]);

        var report = SimulationConsoleReportFormatter.FormatBatchReport(
            batchResult,
            @"C:\artifacts\simulation-runner",
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));

        Assert.Contains(
            "PASS aggregate summary for 2 successful run(s).",
            report,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "PASS string-corpus round=1 seed=7001 :: ok",
            report,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "PASS set-corpus round=1 seed=7001 :: ok",
            report,
            StringComparison.Ordinal);
        Assert.Contains("successful run aggregate statistics:", report, StringComparison.Ordinal);
        Assert.Contains("  runs.total=2", report, StringComparison.Ordinal);
        Assert.Contains(
            $"{Environment.NewLine}{Environment.NewLine}Completed 2 run(s): 2 passed, 0 failed.",
            report,
            StringComparison.Ordinal);
        Assert.Contains("Completed 2 run(s): 2 passed, 0 failed.", report, StringComparison.Ordinal);
        Assert.Contains(@"Artifacts: C:\artifacts\simulation-runner", report, StringComparison.Ordinal);
    }

    [Fact]
    public void FormatBatchReport_ExpandsFailuresAndIncludesIsolationCommands()
    {
        var batchResult = new SimulationBatchResult(
        [
            CreateRun("string-corpus", round: 1, seed: 7001, success: true, summary: "ok"),
            CreateRun(
                "jittered-high-contention",
                round: 2,
                seed: 7002,
                success: false,
                summary: "failed: boom",
                failureMessage: "boom"),
        ]);
        var scenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["leader"] = "true",
            ["proposer-count"] = "5",
        };

        var report = SimulationConsoleReportFormatter.FormatBatchReport(
            batchResult,
            @"C:\artifacts\simulation-runner",
            scenarioParameters);

        Assert.DoesNotContain(
            "PASS string-corpus round=1 seed=7001 :: ok",
            report,
            StringComparison.Ordinal);
        Assert.Contains(
            "FAIL jittered-high-contention round=2 seed=7002 :: failed: boom",
            report,
            StringComparison.Ordinal);
        Assert.Contains("  rerun this failure in isolation:", report, StringComparison.Ordinal);
        Assert.Contains(
            @".\run_simulation_suite.ps1 -Scenario 'jittered-high-contention' -Seed 7002 -Rounds 1 -ScenarioParam 'leader=true' -ScenarioParam 'proposer-count=5'",
            report,
            StringComparison.Ordinal);
        Assert.Contains(
            @".\run_simulation_suite.ps1 -Scenario 'jittered-high-contention' -Seed 7002 -Rounds 1 -ScenarioParam 'leader=true' -ScenarioParam 'proposer-count=5' -WaitForDebugger",
            report,
            StringComparison.Ordinal);
        Assert.Contains(
            $"{Environment.NewLine}{Environment.NewLine}FAIL jittered-high-contention round=2 seed=7002 :: failed: boom",
            report,
            StringComparison.Ordinal);
        Assert.Contains(
            $"{Environment.NewLine}{Environment.NewLine}Completed 2 run(s): 1 passed, 1 failed.",
            report,
            StringComparison.Ordinal);
        Assert.Contains("Completed 2 run(s): 1 passed, 1 failed.", report, StringComparison.Ordinal);
    }

    private static SimulationRunResult CreateRun(
        string scenarioName,
        int round,
        int seed,
        bool success,
        string summary,
        string? failureMessage = null) =>
        new(
            ScenarioName: scenarioName,
            Round: round,
            Seed: seed,
            Success: success,
            Summary: summary,
            ReproductionHint: $"{scenarioName} (seed: {seed})",
            TraceLines: Array.Empty<string>(),
            LogOutput: string.Empty,
            Statistics: new Dictionary<string, long>(StringComparer.Ordinal),
            ProposalRoundTrips: Array.Empty<SimulationProposalRoundTripSample>(),
            Details: new Dictionary<string, string>(StringComparer.Ordinal),
            AcceptorSafetyMonitor: null,
            PorcupineHistory: null,
            FailureMessage: failureMessage,
            FailureDiagnostics: null);
}
