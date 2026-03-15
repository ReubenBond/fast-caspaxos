using System.Globalization;
using FastCASPaxos.Simulation;

namespace FastCASPaxos.Simulation.Scenarios;

public enum LinkLatencyProfile
{
    Lan,
    Us3Region,
    Geo5Region,
}

public sealed record FaultScheduleEntry(
    FaultScheduleAction Action,
    FastCasParticipantRole TargetRole,
    int TargetOrdinal,
    int AtProposal,
    int UntilProposal,
    FastCasParticipantRole SecondaryRole = default,
    int SecondaryOrdinal = 0,
    double SlowLinkDelayMs = 0,
    double SlowLinkJitterMs = 0);

public enum FaultScheduleAction
{
    Suspend,
    Partition,
    Crash,
    SlowLink,
}

internal sealed record BenchmarkWorkloadOptions(
    int ProposerCount,
    int AcceptorCount,
    int ProposalCount,
    bool EnableFastCommit,
    bool EnableDistinguishedLeader,
    TimeSpan BaseDelay,
    TimeSpan MaxJitter,
    LinkLatencyProfile Topology,
    IReadOnlyList<FaultScheduleEntry> FaultSchedule,
    bool AgreementMode,
    bool RegisterInitMode,
    int WriteIntervalMs,
    int ClientTimeoutMs,
    int ClientMaxRetries,
    int? ProposerRetryStartAfterFailuresOverride,
    TimeSpan? ProposerRetryBaseDelayOverride,
    TimeSpan? ProposerRetryMaxDelayOverride,
    double? ProposerRetryJitterRatioOverride,
    TimeSpan? ProposerResponseTimeoutOverride)
{
    public const string ScenarioName = "benchmark-workload";
    public const int MinimumAcceptorCount = 5;

    private static readonly HashSet<string> AllowedParameters =
    [
        "proposer-count",
        "acceptor-count",
        "proposal-count",
        "fast",
        "leader",
        "base-delay-ms",
        "max-jitter-ms",
        "link-latency-profile",
        "fault-schedule",
        "agreement-mode",
        "register-init-mode",
        "write-interval-ms",
        "client-timeout-ms",
        "client-max-retries",
        "proposer-retry-start-after-failures",
        "proposer-retry-base-ms",
        "proposer-retry-max-ms",
        "proposer-retry-jitter-ratio",
        "proposer-response-timeout-ms",
    ];

    public string ModeName =>
        (EnableFastCommit, EnableDistinguishedLeader) switch
        {
            (false, false) => "classic",
            (true, false) => "fast",
            (false, true) => "leader",
            (true, true) => "fast+leader",
        };

    public string FaultLabel =>
        FaultSchedule.Count == 0
            ? "none"
            : string.Join("+", FaultSchedule.Select(f =>
                $"{f.Action.ToString().ToLowerInvariant()}-{f.TargetRole.ToString().ToLowerInvariant()}{f.TargetOrdinal}"));

    public static BenchmarkWorkloadOptions Parse(IReadOnlyDictionary<string, string> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ValidateKnownParameters(parameters);

        var proposerCount = GetPositiveInt(parameters, "proposer-count", defaultValue: 3);
        var acceptorCount = GetPositiveInt(
            parameters,
            "acceptor-count",
            defaultValue: MinimumAcceptorCount);
        if (acceptorCount < MinimumAcceptorCount)
        {
            throw new ArgumentOutOfRangeException(
                "acceptor-count",
                acceptorCount,
                $"benchmark-workload requires at least {MinimumAcceptorCount} acceptors.");
        }
        var proposalCount = GetPositiveInt(parameters, "proposal-count", defaultValue: 1000);
        var enableFastCommit = GetBool(parameters, "fast", defaultValue: false);
        var enableDistinguishedLeader = GetBool(parameters, "leader", defaultValue: false);
        var baseDelayMs = GetDouble(parameters, "base-delay-ms", defaultValue: 1, minValue: 0, maxValue: 10000);
        var maxJitterMs = GetDouble(parameters, "max-jitter-ms", defaultValue: 1, minValue: 0, maxValue: 10000);
        var topology = GetTopology(parameters, "link-latency-profile", defaultValue: LinkLatencyProfile.Lan);
        var faultSchedule = ParseFaultSchedule(
            parameters.TryGetValue("fault-schedule", out var fs) ? fs : string.Empty);
        var agreementMode = GetBool(parameters, "agreement-mode", defaultValue: false);
        var registerInitMode = GetBool(parameters, "register-init-mode", defaultValue: false);
        var writeIntervalMs = GetPositiveIntOrZero(parameters, "write-interval-ms", defaultValue: 0);
        var clientTimeoutMs = GetPositiveIntOrZero(parameters, "client-timeout-ms", defaultValue: 0);
        var clientMaxRetries = GetPositiveIntOrZero(parameters, "client-max-retries", defaultValue: 2);
        var proposerRetryStartAfterFailuresOverride = GetOptionalPositiveIntOrZero(
            parameters,
            "proposer-retry-start-after-failures");
        var proposerRetryBaseDelayOverride = GetOptionalMilliseconds(
            parameters,
            "proposer-retry-base-ms",
            minValue: 0,
            maxValue: 100000);
        var proposerRetryMaxDelayOverride = GetOptionalMilliseconds(
            parameters,
            "proposer-retry-max-ms",
            minValue: 0,
            maxValue: 100000);
        var proposerRetryJitterRatioOverride = GetOptionalDouble(
            parameters,
            "proposer-retry-jitter-ratio",
            minValue: 0,
            maxValue: 1);
        var proposerResponseTimeoutOverride = GetOptionalMilliseconds(
            parameters,
            "proposer-response-timeout-ms",
            minValue: 0,
            maxValue: 100000);

        return new(
            proposerCount,
            acceptorCount,
            proposalCount,
            enableFastCommit,
            enableDistinguishedLeader,
            TimeSpan.FromMilliseconds(baseDelayMs),
            TimeSpan.FromMilliseconds(maxJitterMs),
            topology,
            faultSchedule,
            agreementMode,
            registerInitMode,
            writeIntervalMs,
            clientTimeoutMs,
            clientMaxRetries,
            proposerRetryStartAfterFailuresOverride,
            proposerRetryBaseDelayOverride,
            proposerRetryMaxDelayOverride,
            proposerRetryJitterRatioOverride,
            proposerResponseTimeoutOverride);
    }

    public IReadOnlyList<KeyValuePair<string, string>> DescribeParameters() =>
    [
        new("proposer-count", ProposerCount.ToString(CultureInfo.InvariantCulture)),
        new("acceptor-count", AcceptorCount.ToString(CultureInfo.InvariantCulture)),
        new("proposal-count", ProposalCount.ToString(CultureInfo.InvariantCulture)),
        new("fast", EnableFastCommit.ToString().ToLowerInvariant()),
        new("leader", EnableDistinguishedLeader.ToString().ToLowerInvariant()),
        new("base-delay-ms", BaseDelay.TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture)),
        new("max-jitter-ms", MaxJitter.TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture)),
        new("link-latency-profile", Topology.ToString().ToLowerInvariant()),
        new("fault-schedule", FormatFaultSchedule()),
        new("agreement-mode", AgreementMode.ToString().ToLowerInvariant()),
        new("register-init-mode", RegisterInitMode.ToString().ToLowerInvariant()),
        new("write-interval-ms", WriteIntervalMs.ToString(CultureInfo.InvariantCulture)),
        new("client-timeout-ms", ClientTimeoutMs.ToString(CultureInfo.InvariantCulture)),
        new("client-max-retries", ClientMaxRetries.ToString(CultureInfo.InvariantCulture)),
        new("proposer-retry-start-after-failures", FormatOptionalInt(ProposerRetryStartAfterFailuresOverride)),
        new("proposer-retry-base-ms", FormatOptionalMilliseconds(ProposerRetryBaseDelayOverride)),
        new("proposer-retry-max-ms", FormatOptionalMilliseconds(ProposerRetryMaxDelayOverride)),
        new("proposer-retry-jitter-ratio", FormatOptionalDouble(ProposerRetryJitterRatioOverride)),
        new("proposer-response-timeout-ms", FormatOptionalMilliseconds(ProposerResponseTimeoutOverride)),
        new("mode", ModeName),
        new("topology", Topology.ToString().ToLowerInvariant()),
        new("fault-label", FaultLabel),
    ];

    internal int GetAgreementClientCount() => Math.Max(ProposerCount, GetTopologyRegionCount(Topology));

    public void ApplyProposerTimingOverrides(FastCasSimulationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var hasRetryBackoffOverride =
            ProposerRetryStartAfterFailuresOverride is not null
            || ProposerRetryBaseDelayOverride is not null
            || ProposerRetryMaxDelayOverride is not null
            || ProposerRetryJitterRatioOverride is not null;
        if (hasRetryBackoffOverride)
        {
            var current = options.ProposerRetryBackoff;
            var retryBackoff = new FastCasSimulationProposerRetryBackoffOptions
            {
                Enabled = current.Enabled,
                StartAfterFailures = ProposerRetryStartAfterFailuresOverride ?? current.StartAfterFailures,
                BaseDelay = ProposerRetryBaseDelayOverride ?? current.BaseDelay,
                MaxDelay = ProposerRetryMaxDelayOverride ?? current.MaxDelay,
                JitterRatio = ProposerRetryJitterRatioOverride ?? current.JitterRatio,
            };
            retryBackoff.Validate();
            options.ProposerRetryBackoff = retryBackoff;
        }

        if (ProposerResponseTimeoutOverride is { } timeout)
        {
            options.ProposerResponseTimeout = timeout;
        }
    }

    public void ApplyTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        cluster.ConfigureMessageDelays(BaseDelay, MaxJitter);

        switch (Topology)
        {
            case LinkLatencyProfile.Lan:
                break;

            case LinkLatencyProfile.Us3Region:
                ApplyUs3RegionTopology(cluster);
                break;

            case LinkLatencyProfile.Geo5Region:
                ApplyGeo5RegionTopology(cluster);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Topology), Topology, null);
        }
    }

    /// <summary>
    /// 3-region US topology: East US 2 (region 0), West US (region 1), West US 3 (region 2).
    /// Cross-region RTTs: East US 2↔West US = 66ms, East US 2↔West US 3 = 53ms, West US↔West US 3 = 20ms.
    /// </summary>
    private static void ApplyUs3RegionTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var regions = CreateTopologyRegions(cluster, LinkLatencyProfile.Us3Region);
        ApplyCrossRegionLatency(cluster, regions[0], regions[1], medianRttMs: 66);
        ApplyCrossRegionLatency(cluster, regions[0], regions[2], medianRttMs: 53);
        ApplyCrossRegionLatency(cluster, regions[1], regions[2], medianRttMs: 20);
    }

    /// <summary>
    /// 5-region GEO topology: East US 2, France Central, Southeast Asia, Brazil South, Australia Southeast.
    /// Cross-region RTTs follow the confirmed Azure P50 medians for the chosen region set.
    /// </summary>
    private static void ApplyGeo5RegionTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var regions = CreateTopologyRegions(cluster, LinkLatencyProfile.Geo5Region);
        ApplyCrossRegionLatency(cluster, regions[0], regions[1], medianRttMs: 85);
        ApplyCrossRegionLatency(cluster, regions[0], regions[2], medianRttMs: 228);
        ApplyCrossRegionLatency(cluster, regions[0], regions[3], medianRttMs: 116);
        ApplyCrossRegionLatency(cluster, regions[0], regions[4], medianRttMs: 204);
        ApplyCrossRegionLatency(cluster, regions[1], regions[2], medianRttMs: 148);
        ApplyCrossRegionLatency(cluster, regions[1], regions[3], medianRttMs: 190);
        ApplyCrossRegionLatency(cluster, regions[1], regions[4], medianRttMs: 236);
        ApplyCrossRegionLatency(cluster, regions[2], regions[3], medianRttMs: 332);
        ApplyCrossRegionLatency(cluster, regions[2], regions[4], medianRttMs: 88);
        ApplyCrossRegionLatency(cluster, regions[3], regions[4], medianRttMs: 311);
    }

    private static int GetRegion(int ordinal, int regionCount) =>
        ((ordinal - 1) % regionCount);

    internal static int GetClientRegion(int ordinal, int regionCount) =>
        ordinal <= 0
            ? 0
            : GetRegion(ordinal, regionCount);

    internal static int GetTopologyRegionCount(LinkLatencyProfile topology) =>
        topology switch
        {
            LinkLatencyProfile.Lan => 1,
            LinkLatencyProfile.Us3Region => 3,
            LinkLatencyProfile.Geo5Region => 5,
            _ => throw new ArgumentOutOfRangeException(nameof(topology), topology, null),
        };

    internal static int GetTopologyOrdinalRegion(LinkLatencyProfile topology, int ordinal) =>
        topology switch
        {
            LinkLatencyProfile.Lan => 0,
            LinkLatencyProfile.Us3Region => GetClientRegion(ordinal, 3),
            LinkLatencyProfile.Geo5Region => GetClientRegion(ordinal, 5),
            _ => throw new ArgumentOutOfRangeException(nameof(topology), topology, null),
        };

    internal static TimeSpan GetExpectedOneWayLatency(
        LinkLatencyProfile topology,
        int sourceRegion,
        int targetRegion,
        TimeSpan sameRegionLatency)
    {
        if (topology == LinkLatencyProfile.Lan || sourceRegion == targetRegion)
        {
            return sameRegionLatency;
        }

        var latency = GetCrossRegionLatency(topology, sourceRegion, targetRegion);
        return latency.BaseDelay + latency.MaxJitter;
    }

    private static List<FastCasAddress>[] CreateTopologyRegions<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        LinkLatencyProfile topology)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var regions = new List<FastCasAddress>[GetTopologyRegionCount(topology)];
        for (var index = 0; index < regions.Length; index++)
        {
            regions[index] = [];
        }

        foreach (var client in cluster.ClientAddresses)
        {
            regions[GetTopologyOrdinalRegion(topology, client.Ordinal)].Add(client);
        }

        foreach (var proposer in cluster.ProposerNodes)
        {
            regions[GetTopologyOrdinalRegion(topology, proposer.Host.Address.Ordinal)].Add(proposer.Host.Address);
        }

        foreach (var acceptor in cluster.AcceptorNodes)
        {
            regions[GetTopologyOrdinalRegion(topology, acceptor.Host.Address.Ordinal)].Add(acceptor.Host.Address);
        }

        return regions;
    }

    private static (TimeSpan BaseDelay, TimeSpan MaxJitter) GetCrossRegionLatency(
        LinkLatencyProfile topology,
        int region1,
        int region2)
    {
        var normalizedPair = region1 <= region2 ? (region1, region2) : (region2, region1);
        return topology switch
        {
            LinkLatencyProfile.Us3Region => normalizedPair switch
            {
                (0, 1) => CreateLatencyFromMedianRtt(66),
                (0, 2) => CreateLatencyFromMedianRtt(53),
                (1, 2) => CreateLatencyFromMedianRtt(20),
                _ => throw new ArgumentOutOfRangeException(nameof(region1), $"Unsupported {topology} region pair {normalizedPair}."),
            },
            LinkLatencyProfile.Geo5Region => normalizedPair switch
            {
                (0, 1) => CreateLatencyFromMedianRtt(85),
                (0, 2) => CreateLatencyFromMedianRtt(228),
                (0, 3) => CreateLatencyFromMedianRtt(116),
                (0, 4) => CreateLatencyFromMedianRtt(204),
                (1, 2) => CreateLatencyFromMedianRtt(148),
                (1, 3) => CreateLatencyFromMedianRtt(190),
                (1, 4) => CreateLatencyFromMedianRtt(236),
                (2, 3) => CreateLatencyFromMedianRtt(332),
                (2, 4) => CreateLatencyFromMedianRtt(88),
                (3, 4) => CreateLatencyFromMedianRtt(311),
                _ => throw new ArgumentOutOfRangeException(nameof(region1), $"Unsupported {topology} region pair {normalizedPair}."),
            },
            _ => throw new ArgumentOutOfRangeException(nameof(topology), topology, null),
        };
    }

    private static (TimeSpan BaseDelay, TimeSpan MaxJitter) CreateLatencyFromMedianRtt(int medianRttMs)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(medianRttMs, 0);
        if (medianRttMs == 0)
        {
            return (TimeSpan.Zero, TimeSpan.Zero);
        }

        var jitterMs = medianRttMs > 1 && medianRttMs % 2 == 0 ? 2 : 1;
        var baseDelayMs = (medianRttMs - jitterMs) / 2;
        return (TimeSpan.FromMilliseconds(baseDelayMs), TimeSpan.FromMilliseconds(jitterMs));
    }

    private static void ApplyCrossRegionLatency<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        List<FastCasAddress> region1,
        List<FastCasAddress> region2,
        int medianRttMs)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var latency = CreateLatencyFromMedianRtt(medianRttMs);
        ApplyCrossRegionLatency(cluster, region1, region2, latency.BaseDelay, latency.MaxJitter);
    }

    private static void ApplyCrossRegionLatency<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        List<FastCasAddress> region1,
        List<FastCasAddress> region2,
        TimeSpan baseDelay,
        TimeSpan maxJitter)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        foreach (var a in region1)
        {
            foreach (var b in region2)
            {
                cluster.ConfigureBidirectionalLinkLatency(a, b, baseDelay, maxJitter);
            }
        }
    }

    public void ApplyFaultAtProposal<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        int proposalIndex,
        SimulationRunRecorder recorder)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        foreach (var fault in FaultSchedule)
        {
            if (fault.AtProposal == proposalIndex)
            {
                InjectFault(cluster, fault, recorder);
            }

            if (fault.UntilProposal == proposalIndex)
            {
                RecoverFault(cluster, fault, recorder);
            }
        }
    }

    private static void InjectFault<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FaultScheduleEntry fault,
        SimulationRunRecorder recorder)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var target = MakeAddress(fault.TargetRole, fault.TargetOrdinal);
        switch (fault.Action)
        {
            case FaultScheduleAction.Suspend:
                // Isolate the node from the network so proposers don't wait forever
                // for responses from it, then suspend processing.
                cluster.IsolateNode(target);
                cluster.SuspendNode(target);
                recorder.RecordFault(cluster, $"isolate+suspend {target}");
                break;
            case FaultScheduleAction.Partition:
                var secondary = MakeAddress(fault.SecondaryRole, fault.SecondaryOrdinal);
                cluster.CreateBidirectionalPartition(target, secondary);
                recorder.RecordFault(cluster, $"partition {target} <-> {secondary}");
                break;
            case FaultScheduleAction.Crash:
                cluster.CrashNode(target);
                recorder.RecordFault(cluster, $"crash {target}");
                break;
            case FaultScheduleAction.SlowLink:
                var slowDelay = TimeSpan.FromMilliseconds(fault.SlowLinkDelayMs);
                var slowJitter = TimeSpan.FromMilliseconds(fault.SlowLinkJitterMs);
                foreach (var proposerNode in cluster.ProposerNodes)
                {
                    if (proposerNode.Host.Address != target)
                        cluster.ConfigureBidirectionalLinkLatency(target, proposerNode.Host.Address, slowDelay, slowJitter);
                }
                foreach (var acceptorNode in cluster.AcceptorNodes)
                {
                    if (acceptorNode.Host.Address != target)
                        cluster.ConfigureBidirectionalLinkLatency(target, acceptorNode.Host.Address, slowDelay, slowJitter);
                }
                foreach (var client in cluster.ClientAddresses)
                {
                    cluster.ConfigureBidirectionalLinkLatency(target, client, slowDelay, slowJitter);
                }
                recorder.RecordFault(cluster, $"slow-link {target} delay={fault.SlowLinkDelayMs}ms jitter={fault.SlowLinkJitterMs}ms");
                break;
        }
    }

    private static void RecoverFault<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FaultScheduleEntry fault,
        SimulationRunRecorder recorder)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var target = MakeAddress(fault.TargetRole, fault.TargetOrdinal);
        switch (fault.Action)
        {
            case FaultScheduleAction.Suspend:
                cluster.ResumeNode(target);
                cluster.ReconnectNode(target);
                recorder.RecordFault(cluster, $"resume+reconnect {target}");
                break;
            case FaultScheduleAction.Partition:
                var secondary = MakeAddress(fault.SecondaryRole, fault.SecondaryOrdinal);
                cluster.HealBidirectionalPartition(target, secondary);
                recorder.RecordFault(cluster, $"heal {target} <-> {secondary}");
                break;
            case FaultScheduleAction.Crash:
                cluster.RestartNode(target);
                recorder.RecordFault(cluster, $"restart {target}");
                break;
            case FaultScheduleAction.SlowLink:
                foreach (var proposerNode in cluster.ProposerNodes)
                {
                    if (proposerNode.Host.Address != target)
                        cluster.ClearBidirectionalLinkLatency(target, proposerNode.Host.Address);
                }
                foreach (var acceptorNode in cluster.AcceptorNodes)
                {
                    if (acceptorNode.Host.Address != target)
                        cluster.ClearBidirectionalLinkLatency(target, acceptorNode.Host.Address);
                }
                foreach (var client in cluster.ClientAddresses)
                {
                    cluster.ClearBidirectionalLinkLatency(target, client);
                }
                recorder.RecordFault(cluster, $"heal-slow-link {target}");
                break;
        }
    }

    private static FastCasAddress MakeAddress(FastCasParticipantRole role, int ordinal) =>
        role switch
        {
            FastCasParticipantRole.Proposer => FastCasAddress.Proposer(ordinal),
            FastCasParticipantRole.Acceptor => FastCasAddress.Acceptor(ordinal),
            _ => throw new ArgumentOutOfRangeException(nameof(role), role, null),
        };

    private string FormatFaultSchedule() =>
        FaultSchedule.Count == 0
            ? "none"
            : string.Join(",", FaultSchedule.Select(FormatFaultEntry));

    private static string FormatFaultEntry(FaultScheduleEntry entry)
    {
        var action = entry.Action.ToString().ToLowerInvariant();
        if (entry.Action == FaultScheduleAction.SlowLink)
            action = "slow-link";
        var target = $"{entry.TargetRole.ToString().ToLowerInvariant()}:{entry.TargetOrdinal}";
        var timing = $"at:{entry.AtProposal}:until:{entry.UntilProposal}";

        if (entry.Action == FaultScheduleAction.Partition)
            return $"{action}:{target}:{entry.SecondaryRole.ToString().ToLowerInvariant()}:{entry.SecondaryOrdinal}:{timing}";
        if (entry.Action == FaultScheduleAction.SlowLink)
            return $"{action}:{target}:{timing}:delay:{entry.SlowLinkDelayMs}:jitter:{entry.SlowLinkJitterMs}";
        return $"{action}:{target}:{timing}";
    }

    internal static IReadOnlyList<FaultScheduleEntry> ParseFaultSchedule(string schedule)
    {
        if (string.IsNullOrWhiteSpace(schedule) ||
            string.Equals(schedule, "none", StringComparison.OrdinalIgnoreCase))
        {
            return [];
        }

        var entries = new List<FaultScheduleEntry>();
        foreach (var part in schedule.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            entries.Add(ParseFaultEntry(part));
        }

        return entries;
    }

    private static FaultScheduleEntry ParseFaultEntry(string entry)
    {
        var tokens = entry.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (tokens.Length < 5)
        {
            throw new ArgumentException(
                $"Invalid fault schedule entry '{entry}'. Expected format: 'action:role:ordinal:at:N:until:M'.");
        }

        var action = ParseAction(tokens[0]);
        var role = ParseRole(tokens[1]);
        var ordinal = int.Parse(tokens[2], CultureInfo.InvariantCulture);

        int atIndex;
        var secondaryRole = default(FastCasParticipantRole);
        var secondaryOrdinal = 0;

        if (action == FaultScheduleAction.Partition)
        {
            if (tokens.Length < 7)
            {
                throw new ArgumentException(
                    $"Invalid partition fault entry '{entry}'. Expected: 'partition:role:ordinal:role2:ordinal2:at:N:until:M'.");
            }

            secondaryRole = ParseRole(tokens[3]);
            secondaryOrdinal = int.Parse(tokens[4], CultureInfo.InvariantCulture);
            atIndex = 5;
        }
        else
        {
            atIndex = 3;
        }

        if (!string.Equals(tokens[atIndex], "at", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Expected 'at' keyword in fault entry '{entry}'.");
        }

        var atProposal = int.Parse(tokens[atIndex + 1], CultureInfo.InvariantCulture);

        var untilKeyword = tokens[atIndex + 2];
        var untilProposal = int.Parse(tokens[atIndex + 3], CultureInfo.InvariantCulture);

        if (!string.Equals(untilKeyword, "until", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(untilKeyword, "restart", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"Expected 'until' or 'restart' keyword in fault entry '{entry}'.");
        }

        double slowLinkDelayMs = 0;
        double slowLinkJitterMs = 0;

        if (action == FaultScheduleAction.SlowLink)
        {
            var remaining = atIndex + 4;
            if (remaining < tokens.Length && string.Equals(tokens[remaining], "delay", StringComparison.OrdinalIgnoreCase))
            {
                slowLinkDelayMs = double.Parse(tokens[remaining + 1], CultureInfo.InvariantCulture);
                remaining += 2;
            }
            if (remaining < tokens.Length && string.Equals(tokens[remaining], "jitter", StringComparison.OrdinalIgnoreCase))
            {
                slowLinkJitterMs = double.Parse(tokens[remaining + 1], CultureInfo.InvariantCulture);
            }
        }

        return new FaultScheduleEntry(action, role, ordinal, atProposal, untilProposal, secondaryRole, secondaryOrdinal, slowLinkDelayMs, slowLinkJitterMs);
    }

    private static FaultScheduleAction ParseAction(string token) =>
        token.ToLowerInvariant() switch
        {
            "suspend" => FaultScheduleAction.Suspend,
            "partition" => FaultScheduleAction.Partition,
            "crash" => FaultScheduleAction.Crash,
            "slow-link" or "slowlink" => FaultScheduleAction.SlowLink,
            _ => throw new ArgumentException($"Unknown fault action '{token}'. Expected: suspend, partition, crash, slow-link."),
        };

    private static FastCasParticipantRole ParseRole(string token) =>
        token.ToLowerInvariant() switch
        {
            "proposer" => FastCasParticipantRole.Proposer,
            "acceptor" => FastCasParticipantRole.Acceptor,
            _ => throw new ArgumentException($"Unknown role '{token}'. Expected: proposer, acceptor."),
        };

    private static void ValidateKnownParameters(IReadOnlyDictionary<string, string> parameters)
    {
        var unknown = parameters.Keys
            .Where(key => !AllowedParameters.Contains(key))
            .OrderBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (unknown.Length > 0)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' does not recognize parameter(s): {string.Join(", ", unknown)}.");
        }
    }

    private static int GetPositiveInt(
        IReadOnlyDictionary<string, string> parameters, string name, int defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
            return defaultValue;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be a positive integer.");
        return parsed;
    }

    private static double GetDouble(
        IReadOnlyDictionary<string, string> parameters, string name, double defaultValue, double minValue, double maxValue)
    {
        if (!parameters.TryGetValue(name, out var value))
            return defaultValue;
        if (!double.TryParse(value, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be a floating-point value.");
        if (parsed < minValue || parsed > maxValue)
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' between {minValue} and {maxValue}.");
        return parsed;
    }

    private static bool GetBool(
        IReadOnlyDictionary<string, string> parameters, string name, bool defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
            return defaultValue;
        if (!bool.TryParse(value, out var parsed))
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be 'true' or 'false'.");
        return parsed;
    }

    private static int GetPositiveIntOrZero(
        IReadOnlyDictionary<string, string> parameters, string name, int defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
            return defaultValue;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be a non-negative integer.");
        return parsed;
    }

    private static TEnum GetEnum<TEnum>(
        IReadOnlyDictionary<string, string> parameters, string name, TEnum defaultValue)
        where TEnum : struct, Enum
    {
        if (!parameters.TryGetValue(name, out var value))
            return defaultValue;
        if (!Enum.TryParse<TEnum>(value, ignoreCase: true, out var parsed))
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be one of: {string.Join(", ", Enum.GetNames<TEnum>())}.");
        return parsed;
    }

    private static LinkLatencyProfile GetTopology(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        LinkLatencyProfile defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
        {
            return defaultValue;
        }

        if (string.Equals(value, "uscrossregion", StringComparison.OrdinalIgnoreCase))
        {
            return LinkLatencyProfile.Us3Region;
        }

        if (string.Equals(value, "geo3region", StringComparison.OrdinalIgnoreCase))
        {
            return LinkLatencyProfile.Geo5Region;
        }

        if (!Enum.TryParse<LinkLatencyProfile>(value, ignoreCase: true, out var parsed))
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects '{name}' to be one of: lan, us3region, geo5region (legacy aliases: uscrossregion, geo3region).");
        }

        return parsed;
    }

    private static int? GetOptionalPositiveIntOrZero(
        IReadOnlyDictionary<string, string> parameters,
        string name)
    {
        if (!parameters.TryGetValue(name, out var value))
            return null;
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be a non-negative integer.");
        return parsed;
    }

    private static double? GetOptionalDouble(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        double minValue,
        double maxValue)
    {
        if (!parameters.TryGetValue(name, out var value))
            return null;
        if (!double.TryParse(value, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' to be a floating-point value.");
        if (parsed < minValue || parsed > maxValue)
            throw new ArgumentException($"Scenario '{ScenarioName}' expects '{name}' between {minValue} and {maxValue}.");
        return parsed;
    }

    private static TimeSpan? GetOptionalMilliseconds(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        double minValue,
        double maxValue)
    {
        var value = GetOptionalDouble(parameters, name, minValue, maxValue);
        return value is { } parsed
            ? TimeSpan.FromMilliseconds(parsed)
            : null;
    }

    private static string FormatOptionalInt(int? value) =>
        value?.ToString(CultureInfo.InvariantCulture) ?? "auto";

    private static string FormatOptionalDouble(double? value) =>
        value?.ToString("0.###", CultureInfo.InvariantCulture) ?? "auto";

    private static string FormatOptionalMilliseconds(TimeSpan? value) =>
        value is { } timeSpan
            ? timeSpan.TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture)
            : "auto";
}
