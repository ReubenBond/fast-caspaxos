using FastCASPaxos.Simulation.Scenarios;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastCASPaxos.Simulation.Tests;

public sealed class SimulationBatchRunnerTests(ITestOutputHelper output)
{
    private const int ValidationRounds = 100;

    private readonly ITestOutputHelper _output = output;

    [Fact]
    public async Task RunAsync_LegacyStringScenario_WritesArtifactsAndCapturesStatistics()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["string-corpus"],
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run =>
            {
                Assert.True(run.Success);
                Assert.NotNull(run.AcceptorSafetyMonitor);
                Assert.NotNull(run.PorcupineHistory);
                Assert.Equal("string", run.PorcupineHistory!.ModelKind);
                Assert.NotEmpty(run.PorcupineHistory.Events);
                Assert.Contains("Hello, World!", run.Summary);
                Assert.Equal("available", run.Details["acceptor-safety-monitor"]);
            });

            var run = batchResult.Runs[0];
            var summary = run.CreateStatisticsSummary();
            Assert.Contains(run.TraceLines, line => line.Contains("|proposal|", StringComparison.Ordinal));
            Assert.True(run.Statistics.TryGetValue("fast_caspaxos.proposer.attempts", out var attempts) && attempts > 0);
            Assert.True(summary.DeliveredMessages > 0);
            Assert.True(summary.PrepareAttempts > 0);
            Assert.True(summary.AcceptAttempts > 0);
            Assert.True(summary.ReadAcceptorRoundTripsRecorded > 0);
            Assert.True(summary.WriteAcceptorRoundTripsRecorded > 0);
            Assert.Equal(
                summary.AcceptorRoundTripsRecorded,
                summary.ReadAcceptorRoundTripsRecorded + summary.WriteAcceptorRoundTripsRecorded);

            var scenarioDirectory = GetScenarioDirectory(
                outputDirectory,
                "string-corpus",
                round: 1,
                seed);
            var batchSummaryPath = Path.Combine(outputDirectory, "batch-summary.txt");
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "summary.txt")));
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "trace.log")));
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "logs.txt")));
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "stats.txt")));
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "acceptor-safety-monitor.json")));
            Assert.True(File.Exists(Path.Combine(scenarioDirectory, "porcupine-history.json")));
            Assert.True(File.Exists(batchSummaryPath));
            var statsText = await File.ReadAllTextAsync(Path.Combine(scenarioDirectory, "stats.txt"));
            Assert.Contains("derived:", statsText, StringComparison.Ordinal);
            Assert.Contains(
                "messages.network_per_successful_proposal",
                statsText,
                StringComparison.Ordinal);
            Assert.Contains(
                "round_trips.acceptor_rtts_per_successful_proposal",
                statsText,
                StringComparison.Ordinal);
            Assert.Contains(
                "round_trips.read_acceptor_rtts_histogram",
                statsText,
                StringComparison.Ordinal);
            Assert.Contains(
                "round_trips.write_acceptor_rtts_histogram",
                statsText,
                StringComparison.Ordinal);

            using var document = JsonDocument.Parse(
                await File.ReadAllTextAsync(Path.Combine(scenarioDirectory, "porcupine-history.json")));
            Assert.Equal("string", document.RootElement.GetProperty("ModelKind").GetString());
            Assert.True(document.RootElement.GetProperty("Events").GetArrayLength() > 0);

            using var safetyMonitorDocument = JsonDocument.Parse(
                await File.ReadAllTextAsync(Path.Combine(scenarioDirectory, "acceptor-safety-monitor.json")));
            Assert.Equal(
                "fast-caspaxos-acceptor-safety-monitor-v1",
                safetyMonitorDocument.RootElement.GetProperty("SchemaVersion").GetString());
            Assert.True(safetyMonitorDocument.RootElement.GetProperty("Commits").GetArrayLength() > 0);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_LegacySetScenario_WritesPorcupineHistoryArtifact()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["set-corpus"],
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run =>
            {
                Assert.True(run.Success);
                Assert.NotNull(run.PorcupineHistory);
                Assert.Equal("set", run.PorcupineHistory!.ModelKind);
            });

            var scenarioDirectory = GetScenarioDirectory(
                outputDirectory,
                "set-corpus",
                round: 1,
                seed);
            using var document = JsonDocument.Parse(
                await File.ReadAllTextAsync(Path.Combine(scenarioDirectory, "porcupine-history.json")));
            Assert.Equal("set", document.RootElement.GetProperty("ModelKind").GetString());
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public void AvailableScenarios_ExposeLegacyAndSimulatorNativeCoverage()
    {
        var names = SimulationBatchRunner.AvailableScenarios
            .Select(scenario => scenario.Name)
            .ToArray();

        Assert.Contains("string-corpus", names);
        Assert.Contains("bounded-chaos-sequence", names);
        Assert.Contains("jittered-high-contention", names);
        Assert.Contains("parameterized-append-sequence", names);
        Assert.Contains("seeded-transient-fault-mix-fast", names);
        Assert.Contains("seeded-transient-fault-mix-fast-leader", names);
    }

    [Fact]
    public async Task RunAsync_MultipleRounds_AggregatesStatisticsAcrossAllRuns()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["string-corpus"],
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            var summary = batchResult.CreateStatisticsSummary();
            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run => Assert.True(run.Success));
            Assert.Equal(ValidationRounds, summary.RunCount);
            Assert.True(summary.ProposalAttempts >= summary.ProposalSuccesses);
            Assert.True(summary.DeliveredMessages > 0);
            Assert.InRange(summary.ProposalSuccessRate, 0d, 1d);

            var batchSummary = await File.ReadAllTextAsync(Path.Combine(outputDirectory, "batch-summary.txt"));
            Assert.Contains("aggregate statistics:", batchSummary, StringComparison.Ordinal);
            Assert.Contains("messages.delivered=", batchSummary, StringComparison.Ordinal);
            Assert.Contains(
                "round_trips.acceptor_rtts_per_successful_proposal",
                batchSummary,
                StringComparison.Ordinal);
            Assert.Contains(
                "round_trips.write_acceptor_rtts_histogram",
                batchSummary,
                StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_MultipleScenariosAndRounds_PreservesSerialOrdering()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");
        var scenarioNames = new[] { "string-corpus", "set-corpus" };
        const int rounds = 3;
        const int seed = 7001;

        try
        {
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = scenarioNames,
                    Seed = seed,
                    Rounds = rounds,
                    OutputDirectory = outputDirectory,
                });

            Assert.All(batchResult.Runs, run => Assert.True(run.Success));
            Assert.Equal(rounds * scenarioNames.Length, batchResult.Runs.Count);

            var expectedRuns = (
                from round in Enumerable.Range(1, rounds)
                let roundSeed = seed + round - 1
                from scenarioName in scenarioNames
                select (ScenarioName: scenarioName, Round: round, Seed: roundSeed))
                .ToArray();

            Assert.Equal(
                expectedRuns.Select(run => run.ScenarioName),
                batchResult.Runs.Select(run => run.ScenarioName));
            Assert.Equal(
                expectedRuns.Select(run => run.Round),
                batchResult.Runs.Select(run => run.Round));
            Assert.Equal(
                expectedRuns.Select(run => run.Seed),
                batchResult.Runs.Select(run => run.Seed));
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_ParameterizedAppendSequence_UsesDefaultSingleProposerHappyPath()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["parameterized-append-sequence"],
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run =>
            {
                var summary = run.CreateStatisticsSummary();
                Assert.True(run.Success);
                Assert.Contains("final=ABCDEFGHIJ", run.Summary, StringComparison.Ordinal);
                Assert.Equal("1", run.Details["parameter.proposer-count"]);
                Assert.Equal("5", run.Details["parameter.acceptor-count"]);
                Assert.Equal("10", run.Details["parameter.value-count"]);
                Assert.Equal("0", run.Details["parameter.conflict-rate"]);
                Assert.Equal("0", run.Details["conflict-steps"]);
                Assert.Equal("ABCDEFGHIJ", run.Details["final-value"]);
                Assert.Equal(10, summary.ProposalAttempts);
                Assert.Equal(10, summary.PrepareAttempts);
                Assert.Equal(10, summary.AcceptAttempts);
                Assert.Equal(200, summary.DeliveredMessages);
                Assert.Equal(0, summary.Conflicts);
                Assert.Equal(2.0d, summary.AcceptorRoundTripsPerSuccessfulProposal, 10);
                Assert.Equal(2, summary.AcceptorRoundTripsMin);
                Assert.Equal(2, summary.AcceptorRoundTripsMax);
                Assert.Equal(0, summary.ReadAcceptorRoundTripsRecorded);
                Assert.Equal(10, summary.WriteAcceptorRoundTripsRecorded);
                Assert.Equal(2.0d, summary.WriteAcceptorRoundTripsPerSuccessfulWrite, 10);
                Assert.Equal(2, summary.WriteAcceptorRoundTripsMin);
                Assert.Equal(2, summary.WriteAcceptorRoundTripsMax);
                Assert.True(summary.AcceptorRoundTripHistogram.TryGetValue(2, out var twoRttCount) && twoRttCount == 10);
                Assert.True(summary.WriteAcceptorRoundTripHistogram.TryGetValue(2, out var writeTwoRttCount) && writeTwoRttCount == 10);
            });
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_ParameterizedAppendSequence_LeaderModeReportsAcceptorRttDistribution()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["parameterized-append-sequence"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "1",
                        ["acceptor-count"] = "5",
                        ["value-count"] = "10",
                        ["leader"] = "true",
                    },
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run =>
            {
                var summary = run.CreateStatisticsSummary();
                Assert.True(run.Success);
                Assert.Equal("true", run.Details["parameter.leader"]);
                Assert.Equal(1, summary.PrepareAttempts);
                Assert.Equal(10, summary.AcceptAttempts);
                Assert.Equal(110, summary.DeliveredMessages);
                Assert.Equal(1.1d, summary.AcceptorRoundTripsPerSuccessfulProposal, 10);
                Assert.Equal(1, summary.AcceptorRoundTripsMin);
                Assert.Equal(2, summary.AcceptorRoundTripsMax);
                Assert.Equal(0, summary.ReadAcceptorRoundTripsRecorded);
                Assert.Equal(10, summary.WriteAcceptorRoundTripsRecorded);
                Assert.Equal(1.1d, summary.WriteAcceptorRoundTripsPerSuccessfulWrite, 10);
                Assert.Equal(1, summary.WriteAcceptorRoundTripsMin);
                Assert.Equal(2, summary.WriteAcceptorRoundTripsMax);
                Assert.True(summary.AcceptorRoundTripHistogram.TryGetValue(1, out var oneRttCount) && oneRttCount == 9);
                Assert.True(summary.AcceptorRoundTripHistogram.TryGetValue(2, out var twoRttCount) && twoRttCount == 1);
                Assert.True(summary.WriteAcceptorRoundTripHistogram.TryGetValue(1, out var writeOneRttCount) && writeOneRttCount == 9);
                Assert.True(summary.WriteAcceptorRoundTripHistogram.TryGetValue(2, out var writeTwoRttCount) && writeTwoRttCount == 1);
            });

            var scenarioDirectory = GetScenarioDirectory(
                outputDirectory,
                "parameterized-append-sequence",
                round: 1,
                seed);
            var statsText = await File.ReadAllTextAsync(Path.Combine(scenarioDirectory, "stats.txt"));
            Assert.Contains("messages.network_per_successful_proposal=11.0000", statsText, StringComparison.Ordinal);
            Assert.Contains("round_trips.acceptor_rtts_per_successful_proposal=1.1000", statsText, StringComparison.Ordinal);
            Assert.Contains("round_trips.acceptor_rtts_histogram=1x9,2x1", statsText, StringComparison.Ordinal);
            Assert.Contains("round_trips.read_acceptor_rtts_histogram=none", statsText, StringComparison.Ordinal);
            Assert.Contains("round_trips.write_acceptor_rtts_per_successful_write=1.1000", statsText, StringComparison.Ordinal);
            Assert.Contains("round_trips.write_acceptor_rtts_histogram=1x9,2x1", statsText, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_ParameterizedAppendSequence_SupportsFastConflictParameters()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["parameterized-append-sequence"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["value-count"] = "4",
                        ["conflict-rate"] = "1",
                        ["conflict-fanout"] = "2",
                        ["fast"] = "true",
                        ["leader"] = "true",
                    },
                    Seed = seed,
                    Rounds = ValidationRounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            AssertValidationRuns(batchResult, seed);
            Assert.All(batchResult.Runs, run =>
            {
                var summary = run.CreateStatisticsSummary();
                Assert.True(run.Success);
                Assert.Equal("3", run.Details["parameter.proposer-count"]);
                Assert.Equal("true", run.Details["parameter.fast"]);
                Assert.Equal("true", run.Details["parameter.leader"]);
                Assert.Equal("4", run.Details["conflict-steps"]);
                Assert.Equal("4", run.Details["final-version"]);
                Assert.True(summary.FastRoundAttempts > 0);
                Assert.True(summary.ProposalAttempts > 4);
                Assert.True(summary.DeliveredMessages > 0);
            });
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_ParameterizedAppendSequence_RejectsConflictRateWithoutMultipleProposers()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        try
        {
            var seed = CreateValidationSeed();
            var error = await Assert.ThrowsAsync<ArgumentException>(
                () => SimulationBatchRunner.RunAsync(
                    new SimulationBatchOptions
                    {
                        ScenarioNames = ["parameterized-append-sequence"],
                        ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                        {
                            ["proposer-count"] = "1",
                            ["conflict-rate"] = "0.5",
                        },
                        Seed = seed,
                        Rounds = ValidationRounds,
                        OutputDirectory = outputDirectory,
                    }));

            Assert.Contains("proposer-count >= 2", error.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_JitteredHighContention_ProducesLongWriteTail()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        const int rounds = 5;
        const int seed = 9001;

        try
        {
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["jittered-high-contention"],
                    Seed = seed,
                    Rounds = rounds,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            Assert.All(batchResult.Runs, run => Assert.True(run.Success));
            Assert.Equal(rounds, batchResult.Runs.Count);
            Assert.Contains(
                batchResult.Runs,
                run => run.CreateStatisticsSummary().WriteAcceptorRoundTripsMax >= 4);
            Assert.True(batchResult.CreateStatisticsSummary().WriteAcceptorRoundTripsMax >= 4);
            Assert.All(
                batchResult.Runs,
                run =>
                {
                    Assert.Equal("5", run.Details["contention-fanout"]);
                    Assert.Equal("true", run.Details["parameter.fast"]);
                    Assert.Equal("true", run.Details["parameter.leader"]);
                    Assert.Equal(
                        "exponential-deterministic-jitter",
                        run.Details["parameter.retry-backoff"]);
                    Assert.Equal(
                        "2",
                        run.Details["parameter.retry-backoff-start-after-failures"]);
                    Assert.True(run.CreateStatisticsSummary().WriteAcceptorRoundTripsRecorded > 0);
                });
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RunAsync_JitteredHighContention_Seed9247_Completes()
    {
        var outputDirectory = Path.Combine(
            Path.GetTempPath(),
            $"fast-caspaxos-runner-{Guid.NewGuid():N}");

        const int seed = 9247;

        try
        {
            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["jittered-high-contention"],
                    Seed = seed,
                    Rounds = 1,
                    OutputDirectory = outputDirectory,
                });

            WriteSummaries(batchResult);
            var run = Assert.Single(batchResult.Runs);
            Assert.True(run.Success);
            Assert.Equal(
                "exponential-deterministic-jitter",
                run.Details["parameter.retry-backoff"]);
            Assert.Equal(
                "2",
                run.Details["parameter.retry-backoff-start-after-failures"]);
            Assert.True(run.CreateStatisticsSummary().WriteAcceptorRoundTripsMax >= 4);
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
            {
                Directory.Delete(outputDirectory, recursive: true);
            }
        }
    }

    private void WriteSummaries(SimulationBatchResult batchResult)
    {
        foreach (var run in batchResult.Runs)
        {
            _output.WriteLine(
                $"{(run.Success ? "PASS" : "FAIL")} {run.ScenarioName} round={run.Round} seed={run.Seed} :: {run.Summary}");
            _output.WriteLine(SimulationStatisticsFormatter.FormatRunConsoleSummary(run));
        }

        _output.WriteLine(SimulationStatisticsFormatter.FormatBatchConsoleSummary(batchResult));
    }

    private int CreateValidationSeed()
    {
        var seed = Random.Shared.Next(10_000, int.MaxValue - ValidationRounds);
        _output.WriteLine($"Using randomized validation seed {seed} for {ValidationRounds} rounds.");
        return seed;
    }

    private static void AssertValidationRuns(SimulationBatchResult batchResult, int baseSeed)
    {
        Assert.Equal(ValidationRounds, batchResult.Runs.Count);
        Assert.Equal(
            Enumerable.Range(1, ValidationRounds),
            batchResult.Runs.Select(run => run.Round));
        Assert.Equal(
            Enumerable.Range(baseSeed, ValidationRounds),
            batchResult.Runs.Select(run => run.Seed));
        Assert.Equal(
            ValidationRounds,
            batchResult.Runs.Select(run => run.Seed).Distinct().Count());
    }

    private static string GetScenarioDirectory(
        string outputDirectory,
        string scenarioName,
        int round,
        int seed) =>
        Path.Combine(
            outputDirectory,
            scenarioName,
            $"round-{round:D3}-seed-{seed}");
}
