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
                "request_id,proposer,kind,sent_ms,completed_ms,latency_ms,round_trips,mode,topology,fault_label,failover_count",
                lines[0]);

            var fields = lines[1].Split(',');
            Assert.Equal(11, fields.Length);
            Assert.True(double.TryParse(fields[3], out _), "sent_ms should be a number");
            Assert.True(double.TryParse(fields[5], out _), "latency_ms should be a number");
            Assert.Equal("leader", fields[7]);
            Assert.Equal("lan", fields[8]);
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
                        ["link-latency-profile"] = "geo3region",
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
}
