using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class BenchmarkWorkloadTests
{
    [Fact]
    public async Task RunAsync_BenchmarkWorkload_ProducesLatencySamples()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-test-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "2",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "10",
                        ["fast"] = "true",
                        ["link-latency-profile"] = "lan",
                    },
                    Seed = 7777,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            Assert.Equal(1, result.SucceededCount);
            Assert.Equal(0, result.FailedCount);

            var run = result.Runs[0];
            Assert.True(run.ProposalLatencySamples.Count >= 10,
                $"Expected at least 10 latency samples, got {run.ProposalLatencySamples.Count}");

            foreach (var sample in run.ProposalLatencySamples)
            {
                Assert.True(sample.LatencyMs >= 0, $"Negative latency: {sample.LatencyMs}");
                Assert.True(sample.CompletedMs >= sample.SentMs, "Completed before sent");
            }
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_WritesLatencyCsv()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-csv-test-{Guid.NewGuid():N}");
        try
        {
            await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "1",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "5",
                        ["fast"] = "false",
                        ["leader"] = "true",
                        ["link-latency-profile"] = "lan",
                    },
                    Seed = 8888,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var csvFiles = Directory.GetFiles(outputDir, "benchmark-latency.csv", SearchOption.AllDirectories);
            Assert.Single(csvFiles);

            var lines = await File.ReadAllLinesAsync(csvFiles[0]);
            Assert.True(lines.Length > 1, "Expected CSV header + data rows");
            Assert.StartsWith(
                "request_id,proposer,kind,sent_ms,completed_ms,latency_ms,round_trips,mode,topology,fault_label,failover_count,proposer_count,acceptor_count,agreement_mode,register_init_mode,write_interval_ms,client_timeout_ms,client_max_retries,proposer_timing_source,proposer_expected_one_way_latency_ms,proposer_expected_rtt_ms,proposer_retry_backoff_policy,proposer_retry_backoff_enabled,proposer_retry_backoff_start_after_failures,proposer_retry_backoff_base_ms,proposer_retry_backoff_max_ms,proposer_retry_backoff_jitter_ratio,proposer_response_timeout_ms",
                lines[0]);

            var fields = lines[1].Split(',');
            Assert.Equal(28, fields.Length);
            Assert.True(double.TryParse(fields[3], out _), "sent_ms should be a number");
            Assert.True(double.TryParse(fields[5], out _), "latency_ms should be a number");
            Assert.Equal("leader", fields[7]);
            Assert.Equal("lan", fields[8]);
            Assert.Equal("1", fields[11]);
            Assert.Equal("5", fields[12]);
            Assert.Equal("false", fields[13]);
            Assert.Equal("false", fields[14]);
            Assert.Equal("latency-aware-derived", fields[18]);
            Assert.Equal("4", fields[20]);
            Assert.Equal("exponential-proportional-jitter", fields[21]);
            Assert.Equal("true", fields[22]);
            Assert.Equal("0", fields[23]);
            Assert.Equal("12", fields[24]);
            Assert.Equal("80", fields[25]);
            Assert.Equal("0.5", fields[26]);
            Assert.Equal("16", fields[27]);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_WithTooFewAcceptors_Fails()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-too-few-acceptors-{Guid.NewGuid():N}");
        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                SimulationBatchRunner.RunAsync(
                    new SimulationBatchOptions
                    {
                        ScenarioNames = ["benchmark-workload"],
                        ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                        {
                            ["proposer-count"] = "1",
                            ["acceptor-count"] = "3",
                            ["proposal-count"] = "5",
                            ["fast"] = "true",
                            ["link-latency-profile"] = "lan",
                        },
                        Seed = 4567,
                        Rounds = 1,
                        OutputDirectory = outputDir,
                        StopOnFailure = false,
                    }));

            Assert.Contains("at least 5 acceptors", exception.Message);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Theory]
    [InlineData("uscrossregion", "us3region")]
    [InlineData("geo3region", "geo5region")]
    public async Task RunAsync_BenchmarkWorkload_LegacyTopologyAliases_MapToRenamedProfiles(
        string configuredTopology,
        string effectiveTopology)
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-legacy-topology-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "1",
                        ["fast"] = "true",
                        ["link-latency-profile"] = configuredTopology,
                    },
                    Seed = 8451,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var run = Assert.Single(result.Runs);
            Assert.True(run.Success);
            Assert.Equal(effectiveTopology, run.Details["parameter.topology"]);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_GeoTopology_ProducesHigherLatency()
    {
        var lanDir = Path.Combine(Path.GetTempPath(), $"benchmark-lan-{Guid.NewGuid():N}");
        var geoDir = Path.Combine(Path.GetTempPath(), $"benchmark-geo-{Guid.NewGuid():N}");
        try
        {
            var lanResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "1",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "10",
                        ["fast"] = "true",
                        ["link-latency-profile"] = "lan",
                    },
                    Seed = 9999,
                    Rounds = 1,
                    OutputDirectory = lanDir,
                    StopOnFailure = true,
                });

            var geoResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "1",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "10",
                        ["fast"] = "true",
                        ["link-latency-profile"] = "geo5region",
                    },
                    Seed = 9999,
                    Rounds = 1,
                    OutputDirectory = geoDir,
                    StopOnFailure = true,
                });

            var lanAvgLatency = lanResult.Runs[0].ProposalLatencySamples.Average(s => s.LatencyMs);
            var geoAvgLatency = geoResult.Runs[0].ProposalLatencySamples.Average(s => s.LatencyMs);

            Assert.True(
                geoAvgLatency > lanAvgLatency * 5,
                $"Expected geo latency ({geoAvgLatency:F1}ms) to be significantly higher than LAN latency ({lanAvgLatency:F1}ms)");
        }
        finally
        {
            if (Directory.Exists(lanDir)) Directory.Delete(lanDir, recursive: true);
            if (Directory.Exists(geoDir)) Directory.Delete(geoDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_ExplicitClientOrdinal_RoutesLocallyUnlessLeaderMode()
    {
        var localDir = Path.Combine(Path.GetTempPath(), $"benchmark-explicit-local-{Guid.NewGuid():N}");
        var leaderDir = Path.Combine(Path.GetTempPath(), $"benchmark-explicit-leader-{Guid.NewGuid():N}");
        try
        {
            var localResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "6",
                        ["fast"] = "true",
                        ["leader"] = "false",
                        ["link-latency-profile"] = "geo5region",
                        ["client-ordinal"] = "2",
                    },
                    Seed = 9101,
                    Rounds = 1,
                    OutputDirectory = localDir,
                    StopOnFailure = true,
                });

            var leaderResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "6",
                        ["fast"] = "false",
                        ["leader"] = "true",
                        ["link-latency-profile"] = "geo5region",
                        ["client-ordinal"] = "2",
                    },
                    Seed = 9101,
                    Rounds = 1,
                    OutputDirectory = leaderDir,
                    StopOnFailure = true,
                });

            var localRun = Assert.Single(localResult.Runs);
            var leaderRun = Assert.Single(leaderResult.Runs);
            Assert.Equal("2", localRun.Details["parameter.client-ordinal"]);
            Assert.Equal("2", leaderRun.Details["parameter.client-ordinal"]);

            var localSendLines = localRun.TraceLines
                .Where(line => line.Contains("|send|", StringComparison.Ordinal))
                .ToArray();
            var leaderSendLines = leaderRun.TraceLines
                .Where(line => line.Contains("|send|", StringComparison.Ordinal))
                .ToArray();

            Assert.Equal(6, localSendLines.Length);
            Assert.Equal(6, leaderSendLines.Length);
            Assert.All(
                localSendLines,
                line => Assert.Contains("|send|proposer-2|", line, StringComparison.Ordinal));
            Assert.All(
                leaderSendLines,
                line => Assert.Contains("|send|proposer-1|", line, StringComparison.Ordinal));

            var localAvgLatency = localRun.ProposalLatencySamples.Average(sample => sample.LatencyMs);
            var leaderAvgLatency = leaderRun.ProposalLatencySamples.Average(sample => sample.LatencyMs);
            Assert.True(
                leaderAvgLatency > localAvgLatency + 20,
                $"Expected leader latency ({leaderAvgLatency:F1}ms) to exceed local fast latency ({localAvgLatency:F1}ms) because the client is collocated with proposer-2, not the primary.");
        }
        finally
        {
            if (Directory.Exists(localDir)) Directory.Delete(localDir, recursive: true);
            if (Directory.Exists(leaderDir)) Directory.Delete(leaderDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_ExplicitClientOrdinal_BroadcastsCommittedValueToPeers()
    {
        const string learnMetricName = "fast_caspaxos.simulation.transport.delivered_learn_committed_value";
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-explicit-learn-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "1",
                        ["fast"] = "true",
                        ["leader"] = "false",
                        ["link-latency-profile"] = "geo5region",
                        ["client-ordinal"] = "2",
                    },
                    Seed = 9102,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var run = Assert.Single(result.Runs);
            Assert.True(run.Success);
            Assert.Contains(run.TraceLines, line => line.Contains("|learn|proposer-1|", StringComparison.Ordinal));
            Assert.Contains(run.TraceLines, line => line.Contains("|learn|proposer-3|", StringComparison.Ordinal));
            Assert.True(
                run.Statistics.TryGetValue(learnMetricName, out var learns) && learns >= 2,
                $"Expected at least two delivered learn messages, but saw {run.Statistics.GetValueOrDefault(learnMetricName, 0)}.");
        }
        finally
        {
            if (Directory.Exists(outputDir)) Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_WithFaultSchedule_Succeeds()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-fault-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "2",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "20",
                        ["fast"] = "false",
                        ["link-latency-profile"] = "lan",
                        ["fault-schedule"] = "suspend:acceptor:3:at:5:until:15",
                    },
                    Seed = 5555,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            Assert.Equal(1, result.SucceededCount);
            Assert.Equal(0, result.FailedCount);
            Assert.True(result.Runs[0].ProposalLatencySamples.Count >= 20);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_WithCrashFailover_Completes()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-crash-failover-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "24",
                        ["fast"] = "true",
                        ["leader"] = "false",
                        ["link-latency-profile"] = "lan",
                        ["base-delay-ms"] = "40",
                        ["max-jitter-ms"] = "5",
                        ["fault-schedule"] = "crash:proposer:1:at:8:until:16",
                        ["client-timeout-ms"] = "300",
                        ["agreement-mode"] = "true",
                    },
                    Seed = 7001,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            Assert.Equal(1, result.SucceededCount);
            Assert.Equal(0, result.FailedCount);
            Assert.Equal("crash-proposer1", result.Runs[0].Details["parameter.fault-label"]);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_WithExplicitProposerTimingOverrides_RecordsEffectiveTiming()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-timing-overrides-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "1",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "5",
                        ["fast"] = "true",
                        ["link-latency-profile"] = "lan",
                        ["base-delay-ms"] = "40",
                        ["max-jitter-ms"] = "5",
                        ["proposer-retry-start-after-failures"] = "1",
                        ["proposer-retry-base-ms"] = "250",
                        ["proposer-retry-max-ms"] = "750",
                        ["proposer-retry-jitter-ratio"] = "0.25",
                        ["proposer-response-timeout-ms"] = "500",
                    },
                    Seed = 9991,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var run = Assert.Single(result.Runs);
            Assert.Equal("explicit-overrides", run.Details["parameter.retry-backoff-source"]);
            Assert.Equal("1", run.Details["parameter.retry-backoff-start-after-failures"]);
            Assert.Equal("250", run.Details["parameter.retry-backoff-base-ms"]);
            Assert.Equal("750", run.Details["parameter.retry-backoff-max-ms"]);
            Assert.Equal("0.25", run.Details["parameter.retry-backoff-jitter-ratio"]);
            Assert.Equal("500", run.Details["parameter.proposer-response-timeout-ms"]);
            Assert.Equal("90", run.Details["parameter.proposer-expected-rtt-ms"]);
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_AgreementMode_UsesGeoAwareClassicClients()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-agreement-classic-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "6",
                        ["fast"] = "false",
                        ["leader"] = "false",
                        ["agreement-mode"] = "true",
                        ["link-latency-profile"] = "geo5region",
                    },
                    Seed = 9031,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var run = Assert.Single(result.Runs);
            Assert.True(run.Success);
            Assert.Equal("everywhere", run.Details["parameter.agreement-model"]);
            Assert.Equal("geo5region", run.Details["parameter.topology"]);

            var sendLines = run.TraceLines
                .Where(line => line.Contains("|send|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(6, sendLines.Length);
            Assert.Equal(3, sendLines.Count(line => line.Contains("|send|proposer-1|", StringComparison.Ordinal)));
            Assert.Equal(1, sendLines.Count(line => line.Contains("|send|proposer-2|", StringComparison.Ordinal)));
            Assert.Equal(2, sendLines.Count(line => line.Contains("|send|proposer-3|", StringComparison.Ordinal)));

            var proposalLines = run.TraceLines
                .Where(line => line.Contains("|proposal|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(6, proposalLines.Length);
            Assert.Equal(5, proposalLines.Count(line => line.Contains("committed=Val(A@1)", StringComparison.Ordinal)));
            Assert.Equal(1, proposalLines.Count(line => line.Contains("committed=Val(AB@2)", StringComparison.Ordinal)));
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_BenchmarkWorkload_AgreementMode_LeaderClientsFailOverAndLearn()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"benchmark-agreement-leader-{Guid.NewGuid():N}");
        try
        {
            var result = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = ["benchmark-workload"],
                    ScenarioParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["proposer-count"] = "3",
                        ["acceptor-count"] = "5",
                        ["proposal-count"] = "6",
                        ["fast"] = "false",
                        ["leader"] = "true",
                        ["agreement-mode"] = "true",
                        ["link-latency-profile"] = "lan",
                        ["client-timeout-ms"] = "50",
                        ["client-max-retries"] = "2",
                        ["fault-schedule"] = "slow-link:proposer:1:at:0:until:6:delay:200:jitter:0",
                    },
                    Seed = 9032,
                    Rounds = 1,
                    OutputDirectory = outputDir,
                    StopOnFailure = true,
                });

            var run = Assert.Single(result.Runs);
            Assert.True(run.Success);
            Assert.Equal("everywhere", run.Details["parameter.agreement-model"]);
            Assert.Contains(run.TraceLines, line => line.Contains("|learn|", StringComparison.Ordinal));

            var sendLines = run.TraceLines
                .Where(line => line.Contains("|send|", StringComparison.Ordinal))
                .ToArray();
            Assert.True(sendLines.Length >= 7, $"Expected at least one failover resend, got {sendLines.Length} send lines.");
            Assert.Equal(2, sendLines.Count(line => line.Contains("|request=100000|", StringComparison.Ordinal)));
            Assert.Contains("|send|proposer-1|", sendLines[0], StringComparison.Ordinal);
            Assert.Contains("|send|proposer-2|", sendLines[1], StringComparison.Ordinal);
            Assert.DoesNotContain(
                sendLines.Skip(2),
                line => line.Contains("|send|proposer-1|", StringComparison.Ordinal));

            var proposalLines = run.TraceLines
                .Where(line => line.Contains("|proposal|", StringComparison.Ordinal))
                .ToArray();
            Assert.Equal(6, proposalLines.Length);
            Assert.Equal(3, proposalLines.Count(line => line.Contains("committed=Val(A@1)", StringComparison.Ordinal)));
            Assert.Equal(3, proposalLines.Count(line => line.Contains("committed=Val(AB@2)", StringComparison.Ordinal)));
        }
        finally
        {
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, recursive: true);
        }
    }
}
