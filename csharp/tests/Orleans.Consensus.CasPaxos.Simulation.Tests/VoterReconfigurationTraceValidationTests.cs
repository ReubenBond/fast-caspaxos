using System.ComponentModel;
using System.Diagnostics;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Testing;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class VoterReconfigurationTraceValidationTests
{
    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "quint", "FastCASPaxos.qnt")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Could not locate repository root containing quint/FastCASPaxos.qnt.");
    }

    private static (IReadOnlyList<ProjectedQuintTrace> Traces, IReadOnlyList<NegativeControlQuintTrace> NegativeControls)
        GenerateTraces()
    {
        var replaceTrace = CaptureReplaceLostVoterTrace();
        var postReconfigFastTrace = CapturePostReconfigFastCommitTrace();
        var chainedTrace = CaptureChainedDoubleReplaceTrace();
        var addRemoveTrace = CaptureAddRemoveTrace();
        var staleFastFencedTrace = CaptureStaleFastFencedTrace();

        // Negative control 1: strip raiseFloor(_, 2) before completeVacuous(2) -> must fail replay.
        var vacuousIndex = Array.FindIndex(
            postReconfigFastTrace.Steps.ToArray(),
            static s => s.StartsWith("completeVacuous(2)", StringComparison.Ordinal));
        Assert.True(vacuousIndex >= 0, "Expected completeVacuous(2) in post-reconfig fast trace.");
        var skipFloorPrefix = postReconfigFastTrace.Steps
            .Take(vacuousIndex)
            .Where(static s => !s.StartsWith("raiseFloor(", StringComparison.Ordinal))
            .ToArray();
        var negSkipFloor = new NegativeControlQuintTrace(
            ModuleName: "TraceNegativeSkipFloorVacuous",
            RunName: "skipFloorBreaksVacuousCompletionTraceTest",
            Description: "Omitting raiseFloor(_, 2) makes completeVacuous(2) illegal in FastCASPaxos.qnt.",
            ConfigsLiteral: postReconfigFastTrace.ConfigsLiteral,
            FastQuorumsLiteral: postReconfigFastTrace.FastQuorumsLiteral,
            ProposerCount: postReconfigFastTrace.ProposerCount,
            MaxRound: postReconfigFastTrace.MaxRound,
            ValueDepth: postReconfigFastTrace.ValueDepth,
            PrefixSteps: [.. skipFloorPrefix],
            FailingStep: "completeVacuous(2)");

        // Negative control 2: hand-off issueRecovered using a quorum missing V(1) -> must fail replay.
        var handoffIndex = Array.FindIndex(
            replaceTrace.Steps.ToArray(),
            static s => s.StartsWith("issueRecovered({ config: 2,", StringComparison.Ordinal));
        Assert.True(handoffIndex > 0, "Expected config 2 hand-off issueRecovered in replace-1 trace.");
        var handoffPrefix = replaceTrace.Steps.Take(handoffIndex).ToArray();
        var validHandoffStep = replaceTrace.Steps[handoffIndex];
        // Replace the valid hand-off quorum Set(...) with Set(4) (only the new voter, missing classic quorum of V(1)).
        var openSet = validHandoffStep.IndexOf("Set(", StringComparison.Ordinal);
        var closeSet = validHandoffStep.IndexOf(')', openSet);
        Assert.True(openSet > 0 && closeSet > openSet);
        var illegalHandoffStep = string.Concat(
            validHandoffStep.AsSpan(0, openSet),
            "Set(4)",
            validHandoffStep.AsSpan(closeSet + 1));
        var negIllegalHandoff = new NegativeControlQuintTrace(
            ModuleName: "TraceNegativeIllegalHandoffQuorum",
            RunName: "handoffMissingOldQuorumFailsTraceTest",
            Description: "Hand-off issueRecovered without classic quorum intersection in V(1) is rejected by FastCASPaxos.qnt.",
            ConfigsLiteral: replaceTrace.ConfigsLiteral,
            FastQuorumsLiteral: replaceTrace.FastQuorumsLiteral,
            ProposerCount: replaceTrace.ProposerCount,
            MaxRound: replaceTrace.MaxRound,
            ValueDepth: replaceTrace.ValueDepth,
            PrefixSteps: [.. handoffPrefix],
            FailingStep: illegalHandoffStep);

        // Negative control 3: new voter joining at c = 2 cannot promise a c = 1 ballot (VR-I11).
        var negPreJoin = new NegativeControlQuintTrace(
            ModuleName: "TraceNegativePreJoinPrepare",
            RunName: "newVoterRefusesPreJoinPrepareTraceTest",
            Description: "New voter 4 (firstConfig = 2) has initial floor (2,0,0) and rejects prepare in c = 1.",
            ConfigsLiteral: replaceTrace.ConfigsLiteral,
            FastQuorumsLiteral: replaceTrace.FastQuorumsLiteral,
            ProposerCount: replaceTrace.ProposerCount,
            MaxRound: replaceTrace.MaxRound,
            ValueDepth: replaceTrace.ValueDepth,
            PrefixSteps: [],
            FailingStep: "prepare(4, { config: 1, round: 1, owner: 1 })");

        // Negative control 4: implicit fast ballot (2, 1, 0) in c = 2 is forbidden (§4.2).
        var negImplicitFast = new NegativeControlQuintTrace(
            ModuleName: "TraceNegativeImplicitFastInNewConfig",
            RunName: "implicitFastInConfigTwoFailsTraceTest",
            Description: "Implicit fast ballot (2, 1, 0) is not prepared in c > 1; issuePrepared must fail.",
            ConfigsLiteral: postReconfigFastTrace.ConfigsLiteral,
            FastQuorumsLiteral: postReconfigFastTrace.FastQuorumsLiteral,
            ProposerCount: postReconfigFastTrace.ProposerCount,
            MaxRound: postReconfigFastTrace.MaxRound,
            ValueDepth: postReconfigFastTrace.ValueDepth,
            PrefixSteps: [.. postReconfigFastTrace.Steps.Take(vacuousIndex + 1)],
            FailingStep: "issuePrepared({ config: 2, round: 1, owner: 0 }, 0, 1, { config: 2, round: 2, owner: 0 })");

        return (
            [replaceTrace, postReconfigFastTrace, chainedTrace, addRemoveTrace, staleFastFencedTrace],
            [negSkipFloor, negIllegalHandoff, negPreJoin, negImplicitFast]);
    }

    private static ProjectedQuintTrace CaptureReplaceLostVoterTrace()
    {
        using var cluster = new SimulatedReconfigurationCluster(seed: 3101).AsDisposableSync();
        var c1 = cluster.Cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.Cluster.RegisterConfiguration(2, "v1", "v2", "v4");

        cluster.Cluster.Run(async sim =>
        {
            var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await client.CompareExchangeAsync("alpha", null, new("one"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            sim.LoseVoter("v3");
            var op = sim.CreateOperator(2);
            var active = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active.Number);

            var w2 = await client.CompareExchangeAsync("alpha", w1.Current!.Version, new("two"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
        });

        return cluster.Cluster.TraceRecorder.ProjectKey("alpha", "TraceReplaceLostVoter", "replaceLostVoterTraceTest");
    }

    private static ProjectedQuintTrace CapturePostReconfigFastCommitTrace()
    {
        using var cluster = new SimulatedReconfigurationCluster(seed: 3102).AsDisposableSync();
        var c1 = cluster.Cluster.RegisterConfiguration(1, "v1", "v2", "v3", "v4");
        var c2 = cluster.Cluster.RegisterConfiguration(2, "v1", "v2", "v3", "v5");

        cluster.Cluster.Run(async sim =>
        {
            var op = sim.CreateOperator(2);
            var active = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active.Number);

            var fastClient = sim.CreateClient(1, 2, ["v1", "v2", "v3", "v5"], ProposerModePolicy.StaticFast);
            var w1 = await fastClient.CompareExchangeAsync("vacuous-fast", null, new("c2-classic"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);
            var w2 = await fastClient.CompareExchangeAsync("vacuous-fast", w1.Current!.Version, new("c2-fast-1rtt"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
        });

        return cluster.Cluster.TraceRecorder.ProjectKey("vacuous-fast", "TracePostReconfigFastCommit", "postReconfigFastCommitTraceTest");
    }

    private static ProjectedQuintTrace CaptureChainedDoubleReplaceTrace()
    {
        using var cluster = new SimulatedReconfigurationCluster(seed: 3103).AsDisposableSync();
        var c1 = cluster.Cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.Cluster.RegisterConfiguration(2, "v1", "v2", "v4");
        var c3 = cluster.Cluster.RegisterConfiguration(3, "v1", "v4", "v5");

        cluster.Cluster.Run(async sim =>
        {
            var c1Client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await c1Client.CompareExchangeAsync("chain-key", null, new("s1"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            var op = sim.CreateOperator(2);
            sim.LoseVoter("v3");
            Assert.Equal(2, (await op.ReconfigureAsync(c1, 1, c2)).Number);

            var w2 = await c1Client.CompareExchangeAsync("chain-key", w1.Current!.Version, new("s2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);

            sim.LoseVoter("v2");
            Assert.Equal(3, (await op.ReconfigureAsync(c2, 2, c3)).Number);

            var w3 = await c1Client.CompareExchangeAsync("chain-key", w2.Current!.Version, new("s3"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w3.Status);
        });

        return cluster.Cluster.TraceRecorder.ProjectKey("chain-key", "TraceChainedDoubleReplace", "chainedDoubleReplaceTraceTest");
    }

    private static ProjectedQuintTrace CaptureAddRemoveTrace()
    {
        using var cluster = new SimulatedReconfigurationCluster(seed: 3104).AsDisposableSync();
        var c1 = cluster.Cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.Cluster.RegisterConfiguration(2, "v1", "v2", "v3", "v4");
        var c3 = cluster.Cluster.RegisterConfiguration(3, "v2", "v3", "v4");

        cluster.Cluster.Run(async sim =>
        {
            var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await client.CompareExchangeAsync("ar-key", null, new("a1"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            var op = sim.CreateOperator(2);
            Assert.Equal(2, (await op.ReconfigureAsync(c1, 1, c2)).Number);

            var w2 = await client.CompareExchangeAsync("ar-key", w1.Current!.Version, new("a2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);

            Assert.Equal(3, (await op.ReconfigureAsync(c2, 2, c3)).Number);
            var w3 = await client.CompareExchangeAsync("ar-key", w2.Current!.Version, new("a3"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w3.Status);
        });

        return cluster.Cluster.TraceRecorder.ProjectKey("ar-key", "TraceAddRemove", "addRemoveTraceTest");
    }

    private static ProjectedQuintTrace CaptureStaleFastFencedTrace()
    {
        using var cluster = new SimulatedReconfigurationCluster(seed: 3105).AsDisposableSync();
        var c1 = cluster.Cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.Cluster.RegisterConfiguration(2, "v1", "v2", "v4");

        cluster.Cluster.Run(async sim =>
        {
            var staleFast = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.StaticFast);
            var w1 = await staleFast.CompareExchangeAsync("fenced-fast", null, new("f1"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            var op = sim.CreateOperator(2);
            Assert.Equal(2, (await op.ReconfigureAsync(c1, 1, c2)).Number);

            // Stale proposer holding prepared (1, 2, 0) gets fenced, refreshes to c = 2, and commits.
            var w2 = await staleFast.CompareExchangeAsync("fenced-fast", w1.Current!.Version, new("f2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
        });

        return cluster.Cluster.TraceRecorder.ProjectKey("fenced-fast", "TraceStaleFastFenced", "staleFastFencedTraceTest");
    }

    [Fact]
    public void ExportedQuintTracesMatchCommittedReplayModuleAndReplayAgainstModel()
    {
        var repoRoot = FindRepoRoot();
        var quintDir = Path.Combine(repoRoot, "quint");
        var replayPath = Path.Combine(quintDir, "FastCASPaxosTraceReplay.qnt");

        var (traces, negativeControls) = GenerateTraces();
        var generated = NormalizeNewlines(QuintTraceRecorder.RenderReplayModuleFile(traces, negativeControls));

        if (string.Equals(Environment.GetEnvironmentVariable("UPDATE_QUINT_TRACES"), "1", StringComparison.Ordinal))
        {
            File.WriteAllText(replayPath, generated);
        }

        Assert.True(File.Exists(replayPath), $"Expected committed Quint trace replay module at '{replayPath}'.");
        var committed = NormalizeNewlines(File.ReadAllText(replayPath));
        Assert.Equal(committed, generated);

        if (!TryLocateQuint(out var quintExe))
        {
            if (string.Equals(Environment.GetEnvironmentVariable("FASTCASPAXOS_REQUIRE_QUINT"), "1", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("FASTCASPAXOS_REQUIRE_QUINT=1 is set, but 'quint' CLI was not found on PATH.");
            }

            return;
        }

        // 1. Typecheck the replay module against the unmodified FastCASPaxos.qnt.
        var typecheck = RunProcess(quintExe, "typecheck FastCASPaxosTraceReplay.qnt", quintDir);
        Assert.True(typecheck.ExitCode == 0, $"quint typecheck failed:\n{typecheck.Output}");

        // 2. Replay every positive trace and every negative-control module, asserting Quint actually ran 1 test.
        foreach (var moduleName in traces.Select(static t => t.ModuleName).Concat(negativeControls.Select(static n => n.ModuleName)))
        {
            var run = RunProcess(quintExe, $"test FastCASPaxosTraceReplay.qnt --main {moduleName} --max-samples 1 --seed 1", quintDir);
            Assert.True(run.ExitCode == 0, $"quint test --main {moduleName} failed:\n{run.Output}");
            Assert.Contains("1 passing", run.Output, StringComparison.Ordinal);
        }

        // 3. Positive proof that an unguarded mutated trace (skipping floor before completeVacuous) FAILS replay.
        var badMutant = negativeControls[0];
        var unguardedContent =
            $"module UnguardedMutant {{\n" +
            $"  import FastCASPaxos(\n" +
            $"    Configs = Map({badMutant.ConfigsLiteral}),\n" +
            $"    FastQuorums = Map({badMutant.FastQuorumsLiteral}),\n" +
            $"    ProposerCount = {badMutant.ProposerCount}, MaxRound = {badMutant.MaxRound}, ValueDepth = {badMutant.ValueDepth}\n" +
            $"  ).* from \"./FastCASPaxos\"\n" +
            $"  run unguardedMutantTest = init\n" +
            string.Join("\n", badMutant.PrefixSteps.Select(static s => $"    .then({s})")) +
            $"\n    .then({badMutant.FailingStep})\n" +
            $"    .expect(safety)\n" +
            $"}}\n";

        var tempMutantPath = Path.Combine(quintDir, $".tmp-mutant-{Guid.NewGuid():N}.qnt");
        try
        {
            File.WriteAllText(tempMutantPath, unguardedContent);
            var mutantResult = RunProcess(
                quintExe,
                $"test \"{Path.GetFileName(tempMutantPath)}\" --main UnguardedMutant --max-samples 1 --seed 1",
                quintDir);
            Assert.NotEqual(0, mutantResult.ExitCode);
        }
        finally
        {
            if (File.Exists(tempMutantPath))
            {
                File.Delete(tempMutantPath);
            }
        }
    }

    private static string NormalizeNewlines(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static bool TryLocateQuint(out string executable)
    {
        var candidates = OperatingSystem.IsWindows()
            ? new[] { "quint.cmd", "quint" }
            : new[] { "quint" };
        foreach (var candidate in candidates)
        {
            try
            {
                var probe = RunProcess(candidate, "--version", Environment.CurrentDirectory);
                if (probe.ExitCode == 0)
                {
                    executable = candidate;
                    return true;
                }
            }
            catch (Exception ex) when (ex is Win32Exception or IOException)
            {
                // Try next candidate.
            }
        }

        executable = string.Empty;
        return false;
    }

    private static (int ExitCode, string Output) RunProcess(string fileName, string arguments, string workingDirectory)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };

        process.Start();
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        return (process.ExitCode, stdout + stderr);
    }
}

file static class SyncClusterExtensions
{
    public static IDisposableClusterScope AsDisposableSync(this SimulatedReconfigurationCluster cluster) =>
        new(cluster);

    public sealed class IDisposableClusterScope(SimulatedReconfigurationCluster cluster) : IDisposable
    {
        public SimulatedReconfigurationCluster Cluster { get; } = cluster;
        public void Dispose() => Cluster.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
