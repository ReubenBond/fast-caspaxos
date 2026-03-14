using System.Globalization;
using FastCASPaxos.Simulation;

namespace FastCASPaxos.Simulation.Scenarios;

public enum LinkLatencyProfile
{
    Lan,
    UsCrossRegion,
    Geo3Region,
}

public sealed record FaultScheduleEntry(
    FaultScheduleAction Action,
    FastCasParticipantRole TargetRole,
    int TargetOrdinal,
    int AtProposal,
    int UntilProposal,
    FastCasParticipantRole SecondaryRole = default,
    int SecondaryOrdinal = 0);

public enum FaultScheduleAction
{
    Suspend,
    Partition,
    Crash,
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
    IReadOnlyList<FaultScheduleEntry> FaultSchedule)
{
    public const string ScenarioName = "benchmark-workload";

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
        var acceptorCount = GetPositiveInt(parameters, "acceptor-count", defaultValue: 5);
        var proposalCount = GetPositiveInt(parameters, "proposal-count", defaultValue: 1000);
        var enableFastCommit = GetBool(parameters, "fast", defaultValue: false);
        var enableDistinguishedLeader = GetBool(parameters, "leader", defaultValue: false);
        var baseDelayMs = GetDouble(parameters, "base-delay-ms", defaultValue: 1, minValue: 0, maxValue: 10000);
        var maxJitterMs = GetDouble(parameters, "max-jitter-ms", defaultValue: 1, minValue: 0, maxValue: 10000);
        var topology = GetEnum<LinkLatencyProfile>(parameters, "link-latency-profile", defaultValue: LinkLatencyProfile.Lan);
        var faultSchedule = ParseFaultSchedule(
            parameters.TryGetValue("fault-schedule", out var fs) ? fs : string.Empty);

        return new(
            proposerCount,
            acceptorCount,
            proposalCount,
            enableFastCommit,
            enableDistinguishedLeader,
            TimeSpan.FromMilliseconds(baseDelayMs),
            TimeSpan.FromMilliseconds(maxJitterMs),
            topology,
            faultSchedule);
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
        new("mode", ModeName),
        new("topology", Topology.ToString().ToLowerInvariant()),
        new("fault-label", FaultLabel),
    ];

    public void ApplyTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        cluster.ConfigureMessageDelays(BaseDelay, MaxJitter);

        switch (Topology)
        {
            case LinkLatencyProfile.Lan:
                break;

            case LinkLatencyProfile.UsCrossRegion:
                ApplyUsCrossRegionTopology(cluster);
                break;

            case LinkLatencyProfile.Geo3Region:
                ApplyGeo3RegionTopology(cluster);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Topology), Topology, null);
        }
    }

    /// <summary>
    /// US-East + US-West: proposers 1-2 and acceptors 1-3 in US-East, proposer 3 and acceptors 4-5 in US-West.
    /// Cross-region links: base=15ms, jitter=5ms (~30ms RTT).
    /// </summary>
    private static void ApplyUsCrossRegionTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var crossDelay = TimeSpan.FromMilliseconds(15);
        var crossJitter = TimeSpan.FromMilliseconds(5);

        var usEast = new List<FastCasAddress>();
        var usWest = new List<FastCasAddress>();

        // Proposers: 1-2 East, 3+ West
        foreach (var p in cluster.ProposerNodes)
        {
            if (p.Host.Address.Ordinal <= 2)
                usEast.Add(p.Host.Address);
            else
                usWest.Add(p.Host.Address);
        }

        // Acceptors: 1-3 East, 4+ West
        foreach (var a in cluster.AcceptorNodes)
        {
            if (a.Host.Address.Ordinal <= 3)
                usEast.Add(a.Host.Address);
            else
                usWest.Add(a.Host.Address);
        }

        // Also include the client address in East
        usEast.Add(cluster.ClientAddress);

        foreach (var east in usEast)
        {
            foreach (var west in usWest)
            {
                cluster.ConfigureBidirectionalLinkLatency(east, west, crossDelay, crossJitter);
            }
        }
    }

    /// <summary>
    /// 3-region: US-East (P1+A1-2), EU-West (P2+A3-4), AP-Southeast (P3+A5).
    /// US↔EU: 40ms+5ms, US↔AP: 80ms+10ms, EU↔AP: 60ms+8ms.
    /// </summary>
    private static void ApplyGeo3RegionTopology<TValue>(FastCasSimulationCluster<TValue> cluster)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        var usNodes = new List<FastCasAddress> { cluster.ClientAddress };
        var euNodes = new List<FastCasAddress>();
        var apNodes = new List<FastCasAddress>();

        foreach (var p in cluster.ProposerNodes)
        {
            var region = GetRegion(p.Host.Address.Ordinal, 3);
            GetRegionList(region, usNodes, euNodes, apNodes).Add(p.Host.Address);
        }

        foreach (var a in cluster.AcceptorNodes)
        {
            var region = GetAcceptorRegion(a.Host.Address.Ordinal);
            GetRegionList(region, usNodes, euNodes, apNodes).Add(a.Host.Address);
        }

        ApplyCrossRegionLatency(cluster, usNodes, euNodes, TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(5));
        ApplyCrossRegionLatency(cluster, usNodes, apNodes, TimeSpan.FromMilliseconds(80), TimeSpan.FromMilliseconds(10));
        ApplyCrossRegionLatency(cluster, euNodes, apNodes, TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(8));
    }

    private static int GetRegion(int ordinal, int regionCount) =>
        ((ordinal - 1) % regionCount);

    private static int GetAcceptorRegion(int ordinal) =>
        ordinal switch
        {
            <= 2 => 0, // US-East
            <= 4 => 1, // EU-West
            _ => 2,    // AP-Southeast
        };

    private static List<FastCasAddress> GetRegionList(
        int region,
        List<FastCasAddress> us,
        List<FastCasAddress> eu,
        List<FastCasAddress> ap) =>
        region switch
        {
            0 => us,
            1 => eu,
            _ => ap,
        };

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
        var target = $"{entry.TargetRole.ToString().ToLowerInvariant()}:{entry.TargetOrdinal}";
        var timing = $"at:{entry.AtProposal}:until:{entry.UntilProposal}";

        return entry.Action == FaultScheduleAction.Partition
            ? $"{action}:{target}:{entry.SecondaryRole.ToString().ToLowerInvariant()}:{entry.SecondaryOrdinal}:{timing}"
            : $"{action}:{target}:{timing}";
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

        return new FaultScheduleEntry(action, role, ordinal, atProposal, untilProposal, secondaryRole, secondaryOrdinal);
    }

    private static FaultScheduleAction ParseAction(string token) =>
        token.ToLowerInvariant() switch
        {
            "suspend" => FaultScheduleAction.Suspend,
            "partition" => FaultScheduleAction.Partition,
            "crash" => FaultScheduleAction.Crash,
            _ => throw new ArgumentException($"Unknown fault action '{token}'. Expected: suspend, partition, crash."),
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
}
