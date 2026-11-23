using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Server;

namespace Orleans.Consensus.CasPaxos.Testing;

public enum QuintTraceEventKind
{
    SetFloor,
    PreparePromised,
    AcceptDispatched,
    AcceptAcknowledged,
    KeyTransferred,
    KeyVacuouslyComplete,
}

/// <summary>
/// A single §11.5 trace event recorded from a simulated execution, mapping directly to
/// <c>FastCASPaxos.qnt</c> actions (<c>raiseFloor</c>, <c>prepare</c>, <c>issuePrepared</c>,
/// <c>issueRecovered</c>, <c>accept</c>, <c>completeTransferred</c>, <c>completeVacuous</c>).
/// </summary>
public sealed record QuintTraceEvent(
    long Sequence,
    long Tick,
    QuintTraceEventKind Kind,
    string? Key,
    string? AcceptorId,
    long Configuration,
    Ballot Ballot,
    Ballot SecondaryBallot,
    ByteString? Value,
    bool IsRecovered,
    ImmutableArray<string> QuorumAcceptorIds);

/// <summary>
/// Records per-key reconfiguration traces from deterministic simulations and projects them into
/// executable Quint replay runs against the unmodified <c>quint/FastCASPaxos.qnt</c> model (spec §11.5).
/// </summary>
public sealed class QuintTraceRecorder
{
    private readonly List<QuintTraceEvent> _events = [];
    private readonly SortedDictionary<long, ImmutableArray<string>> _configurations = [];
    private readonly Dictionary<string, int> _voterIds = new(StringComparer.Ordinal);
    private int _nextVoterId;
    private long _nextSequence;

    public IReadOnlyList<QuintTraceEvent> Events => _events;

    public IReadOnlyDictionary<long, ImmutableArray<string>> Configurations => _configurations;

    public void RegisterConfiguration(long configuration, IReadOnlyCollection<string> voters)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(configuration, 1);
        ArgumentNullException.ThrowIfNull(voters);
        var ordered = voters.OrderBy(static id => id, StringComparer.Ordinal).ToImmutableArray();
        _configurations[configuration] = ordered;
        foreach (var voter in ordered)
        {
            EnsureVoterId(voter);
        }
    }

    public int EnsureVoterId(string voterId)
    {
        if (!_voterIds.TryGetValue(voterId, out var numericId))
        {
            numericId = ++_nextVoterId;
            _voterIds[voterId] = numericId;
        }

        return numericId;
    }

    public void RecordSetFloor(long tick, string acceptorId, long configuration)
    {
        EnsureVoterId(acceptorId);
        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.SetFloor,
            Key: null,
            acceptorId,
            configuration,
            Ballot.Floor(configuration),
            Ballot.Zero,
            Value: null,
            IsRecovered: false,
            QuorumAcceptorIds: []));
    }

    public void RecordPreparePromised(
        long tick,
        string key,
        string acceptorId,
        Ballot ballot,
        Ballot acceptedBallot,
        ByteString? acceptedValue)
    {
        EnsureVoterId(acceptorId);
        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.PreparePromised,
            key,
            acceptorId,
            ballot.Configuration,
            ballot,
            acceptedBallot,
            acceptedValue,
            IsRecovered: false,
            QuorumAcceptorIds: []));
    }

    public void RecordAcceptDispatched(
        long tick,
        string key,
        Ballot ballot,
        ByteString value,
        Ballot nextBallot,
        bool isRecovered,
        ImmutableArray<string> quorumAcceptors)
    {
        foreach (var acceptor in quorumAcceptors)
        {
            EnsureVoterId(acceptor);
        }

        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.AcceptDispatched,
            key,
            AcceptorId: null,
            ballot.Configuration,
            ballot,
            nextBallot,
            value,
            isRecovered,
            quorumAcceptors));
    }

    public void RecordAcceptAcknowledged(
        long tick,
        string key,
        string acceptorId,
        Ballot ballot,
        ByteString value,
        Ballot promisedBallot)
    {
        EnsureVoterId(acceptorId);
        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.AcceptAcknowledged,
            key,
            acceptorId,
            ballot.Configuration,
            ballot,
            promisedBallot,
            value,
            IsRecovered: false,
            QuorumAcceptorIds: []));
    }

    public void RecordKeyTransferred(long tick, string key, long configuration)
    {
        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.KeyTransferred,
            key,
            AcceptorId: null,
            configuration,
            Ballot.Floor(configuration),
            Ballot.Zero,
            Value: null,
            IsRecovered: false,
            QuorumAcceptorIds: []));
    }

    public void RecordKeyVacuouslyComplete(long tick, string key, long configuration)
    {
        _events.Add(new QuintTraceEvent(
            ++_nextSequence,
            tick,
            QuintTraceEventKind.KeyVacuouslyComplete,
            key,
            AcceptorId: null,
            configuration,
            Ballot.Floor(configuration),
            Ballot.Zero,
            Value: null,
            IsRecovered: false,
            QuorumAcceptorIds: []));
    }

    /// <summary>
    /// Projects the multi-key event log onto a single <paramref name="key"/> and produces a
    /// self-contained Quint <c>module</c> importing <c>FastCASPaxos</c> from <c>./FastCASPaxos</c>.
    /// </summary>
    public ProjectedQuintTrace ProjectKey(string key, string moduleName, string runName)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentException.ThrowIfNullOrEmpty(moduleName);
        ArgumentException.ThrowIfNullOrEmpty(runName);

        if (_configurations.Count == 0)
        {
            throw new InvalidOperationException("No voter configurations were registered on the trace recorder.");
        }

        // Map voter string IDs appearing in registered configurations to compact 1..K ints.
        var voterMap = new Dictionary<string, int>(StringComparer.Ordinal);
        var nextVoter = 0;
        foreach (var voters in _configurations.Values)
        {
            foreach (var v in voters)
            {
                if (!voterMap.ContainsKey(v))
                {
                    voterMap[v] = ++nextVoter;
                }
            }
        }

        // Compute firstConfig(a) for each voter.
        var firstConfig = new Dictionary<int, long>();
        foreach (var (cfg, voters) in _configurations)
        {
            foreach (var v in voters)
            {
                var id = voterMap[v];
                if (!firstConfig.TryGetValue(id, out var existing) || cfg < existing)
                {
                    firstConfig[id] = cfg;
                }
            }
        }

        // Map classic proposer IDs appearing for this key to compact 1..P ints.
        var proposerMap = new Dictionary<int, int>();
        var nextProposer = 0;
        int MapProposer(int owner)
        {
            if (owner == 0) return 0;
            if (!proposerMap.TryGetValue(owner, out var mapped))
            {
                mapped = ++nextProposer;
                proposerMap[owner] = mapped;
            }

            return mapped;
        }

        Ballot MapBallot(Ballot b) =>
            b.IsZero ? Ballot.Zero : new Ballot(b.Configuration, b.Round, MapProposer(b.Proposer));

        // Track mirror of FastCASPaxos.qnt state for abstract binary-tree value assignment and exact step projection.
        var valueToAbstract = new Dictionary<ByteString, int>
        {
            [RegisterEncoding.Absent] = 0,
        };
        var usedAbstract = new HashSet<int> { 0 };
        var valueDepth = new Dictionary<int, int> { [0] = 0 };
        var maxDepth = 1;

        int AllocateChildValue(int baseInt, ByteString bytes)
        {
            if (valueToAbstract.TryGetValue(bytes, out var existing))
            {
                return existing;
            }

            var left = 2 * baseInt + 1;
            var right = 2 * baseInt + 2;
            var chosen = !usedAbstract.Contains(left) ? left
                : !usedAbstract.Contains(right) ? right
                : throw new InvalidOperationException(
                    $"Binary value tree exhausted from base {baseInt} (more than 2 distinct competing updates from the same base).");

            valueToAbstract[bytes] = chosen;
            usedAbstract.Add(chosen);
            var depth = valueDepth[baseInt] + 1;
            valueDepth[chosen] = depth;
            if (depth > maxDepth) maxDepth = depth;
            return chosen;
        }

        // Mirror acceptor floor/accepted/promised, prepares, requests, acks, and completion to project accurately.
        var floors = firstConfig.ToDictionary(static kv => kv.Key, static kv => kv.Value);
        var accepted = firstConfig.Keys.ToDictionary(static id => id, static _ => (Ballot: Ballot.Zero, Value: 0));
        var completeConfigs = new HashSet<long> { 1 };
        var issuedRequests = new Dictionary<(Ballot Ballot, int Value), Ballot>();
        var issuedClassicBallots = new HashSet<Ballot>();
        var prepareSnapshots = new Dictionary<(Ballot Ballot, int Acceptor), (Ballot Accepted, int Value)>();
        var preparedBaseByBallot = new Dictionary<Ballot, int> { [Ballot.InitialFast()] = 0 };
        var ackVoters = new Dictionary<(Ballot Ballot, int Value), HashSet<int>>();
        var ackPromiseVoters = new Dictionary<(Ballot Ballot, int Value, Ballot Promised), HashSet<int>>();

        var steps = new List<string>();
        var maxRound = 1;

        int QuorumSize(Ballot b)
        {
            var n = _configurations[b.Configuration].Length;
            return b.Proposer == 0 ? (3 * n + 3) / 4 : n / 2 + 1;
        }

        int ClassicQuorumSize(long cfg) => _configurations[cfg].Length / 2 + 1;

        void TrackRound(Ballot b)
        {
            if (b.Round > maxRound) maxRound = b.Round;
        }

        foreach (var ev in _events)
        {
            if (ev.Key is not null && !string.Equals(ev.Key, key, StringComparison.Ordinal))
            {
                continue;
            }

            switch (ev.Kind)
            {
                case QuintTraceEventKind.SetFloor:
                {
                    if (!voterMap.TryGetValue(ev.AcceptorId!, out var a)) break;
                    var c = ev.Configuration;
                    if (c <= 1 || !completeConfigs.Contains(c - 1) || floors[a] >= c) break;
                    floors[a] = c;
                    steps.Add(string.Create(CultureInfo.InvariantCulture, $"raiseFloor({a}, {c})"));
                    break;
                }

                case QuintTraceEventKind.PreparePromised:
                {
                    var a = voterMap[ev.AcceptorId!];
                    var b = MapBallot(ev.Ballot);
                    TrackRound(b);
                    // Omit straggler prepare responses that arrived after the classic ballot was already issued
                    // or duplicate prepare snapshots for the same (ballot, acceptor).
                    if (issuedClassicBallots.Contains(b) || prepareSnapshots.ContainsKey((b, a))) break;
                    if (b.Configuration > 1 && !completeConfigs.Contains(b.Configuration - 1)) break;

                    prepareSnapshots[(b, a)] = accepted[a];
                    steps.Add(string.Create(CultureInfo.InvariantCulture, $"prepare({a}, {FormatBallot(b)})"));
                    break;
                }

                case QuintTraceEventKind.AcceptDispatched:
                {
                    var b = MapBallot(ev.Ballot);
                    var next = MapBallot(ev.SecondaryBallot);
                    TrackRound(b);
                    TrackRound(next);
                    var valBytes = ev.Value!.Value;

                    if (ev.IsRecovered)
                    {
                        if (issuedClassicBallots.Contains(b)) break;

                        // Select the exact prepared responders recorded for this ballot.
                        var quorumIds = ev.QuorumAcceptorIds
                            .Select(id => voterMap[id])
                            .Where(a => prepareSnapshots.ContainsKey((b, a)))
                            .Distinct()
                            .OrderBy(static a => a)
                            .ToArray();

                        // If complete(b.config), FastCASPaxos.qnt requires |q| == classicQuorum(b.config) within V(b.config).
                        if (completeConfigs.Contains(b.Configuration))
                        {
                            var votersInCfg = _configurations[b.Configuration].Select(id => voterMap[id]).ToHashSet();
                            quorumIds = quorumIds
                                .Where(votersInCfg.Contains)
                                .Take(ClassicQuorumSize(b.Configuration))
                                .ToArray();
                        }

                        var baseInt = ComputeRecoveredBase(b, quorumIds, prepareSnapshots, completeConfigs, _configurations, voterMap);
                        var valInt = ResolveAbstractValue(valBytes, baseInt, valueToAbstract, AllocateChildValue);

                        issuedClassicBallots.Add(b);
                        issuedRequests[(b, valInt)] = next;
                        var qSet = $"Set({string.Join(", ", quorumIds)})";
                        steps.Add(string.Create(
                            CultureInfo.InvariantCulture,
                            $"issueRecovered({FormatBallot(b)}, {qSet}, {baseInt}, {valInt}, {FormatBallot(next)})"));
                    }
                    else
                    {
                        if (!preparedBaseByBallot.TryGetValue(b, out var baseInt))
                        {
                            throw new InvalidOperationException(
                                $"Trace key '{key}': issuePrepared for {b} had no certified prepared base.");
                        }

                        var valInt = ResolveAbstractValue(valBytes, baseInt, valueToAbstract, AllocateChildValue);
                        if (issuedRequests.ContainsKey((b, valInt))) break;

                        if (b.Proposer > 0) issuedClassicBallots.Add(b);
                        issuedRequests[(b, valInt)] = next;
                        steps.Add(string.Create(
                            CultureInfo.InvariantCulture,
                            $"issuePrepared({FormatBallot(b)}, {baseInt}, {valInt}, {FormatBallot(next)})"));
                    }

                    break;
                }

                case QuintTraceEventKind.AcceptAcknowledged:
                {
                    var a = voterMap[ev.AcceptorId!];
                    var b = MapBallot(ev.Ballot);
                    var promised = MapBallot(ev.SecondaryBallot);
                    var valBytes = ev.Value!.Value;
                    if (!valueToAbstract.TryGetValue(valBytes, out var valInt))
                    {
                        throw new InvalidOperationException(
                            $"Trace key '{key}': accept acknowledged for unissued value at {b}.");
                    }

                    if (!issuedRequests.TryGetValue((b, valInt), out var next))
                    {
                        throw new InvalidOperationException(
                            $"Trace key '{key}': accept acknowledged for unissued request ({b}, {valInt}).");
                    }

                    accepted[a] = (b, valInt);
                    steps.Add(string.Create(
                        CultureInfo.InvariantCulture,
                        $"accept({a}, {{ ballot: {FormatBallot(b)}, value: {valInt}, next: {FormatBallot(next)} }})"));

                    // Track quorum commitments and piggybacked preparedBases.
                    var votersInCfg = _configurations[b.Configuration].Select(id => voterMap[id]).ToHashSet();
                    if (votersInCfg.Contains(a))
                    {
                        if (!ackVoters.TryGetValue((b, valInt), out var vset))
                        {
                            vset = [];
                            ackVoters[(b, valInt)] = vset;
                        }

                        vset.Add(a);

                        if (!next.IsZero && promised == next)
                        {
                            if (!ackPromiseVoters.TryGetValue((b, valInt, next), out var pset))
                            {
                                pset = [];
                                ackPromiseVoters[(b, valInt, next)] = pset;
                            }

                            pset.Add(a);
                        }

                        if (!next.IsZero
                            && vset.Count >= QuorumSize(b)
                            && ackPromiseVoters.TryGetValue((b, valInt, next), out var echoed)
                            && echoed.Count >= QuorumSize(next))
                        {
                            preparedBaseByBallot[next] = valInt;
                        }
                    }

                    break;
                }

                case QuintTraceEventKind.KeyTransferred:
                {
                    var c = ev.Configuration;
                    if (completeConfigs.Contains(c)) break;
                    completeConfigs.Add(c);
                    steps.Add(string.Create(CultureInfo.InvariantCulture, $"completeTransferred({c})"));
                    break;
                }

                case QuintTraceEventKind.KeyVacuouslyComplete:
                {
                    var c = ev.Configuration;
                    if (completeConfigs.Contains(c)) break;
                    completeConfigs.Add(c);
                    steps.Add(string.Create(CultureInfo.InvariantCulture, $"completeVacuous({c})"));
                    break;
                }
            }
        }

        // Ensure maxRound stays strictly above any ballot that piggybacked a next ballot.
        var effectiveProposerCount = Math.Max(1, nextProposer);
        var configsLiteral = string.Join(
            ", ",
            _configurations.Select(kv =>
                $"{kv.Key} -> Set({string.Join(", ", kv.Value.Select(id => voterMap[id]).OrderBy(static x => x))})"));
        var fastQuorumsLiteral = string.Join(
            ", ",
            _configurations.Select(kv =>
                $"{kv.Key} -> {(3 * kv.Value.Length + 3) / 4}"));

        return new ProjectedQuintTrace(
            ModuleName: moduleName,
            RunName: runName,
            Key: key,
            ConfigsLiteral: configsLiteral,
            FastQuorumsLiteral: fastQuorumsLiteral,
            ProposerCount: effectiveProposerCount,
            MaxRound: maxRound,
            ValueDepth: maxDepth,
            Steps: [.. steps]);
    }

    private static int ResolveAbstractValue(
        ByteString valBytes,
        int baseInt,
        Dictionary<ByteString, int> valueToAbstract,
        Func<int, ByteString, int> allocateChild)
    {
        if (valueToAbstract.TryGetValue(valBytes, out var existing))
        {
            return existing;
        }

        return allocateChild(baseInt, valBytes);
    }

    private static int ComputeRecoveredBase(
        Ballot ballot,
        int[] quorumIds,
        Dictionary<(Ballot Ballot, int Acceptor), (Ballot Accepted, int Value)> prepareSnapshots,
        HashSet<long> completeConfigs,
        SortedDictionary<long, ImmutableArray<string>> configurations,
        Dictionary<string, int> voterMap)
    {
        var minRelevantCfg = completeConfigs.Contains(ballot.Configuration)
            ? ballot.Configuration
            : ballot.Configuration - 1;

        var highest = Ballot.Zero;
        var responses = new List<(int Acceptor, Ballot Accepted, int Value)>(quorumIds.Length);
        foreach (var a in quorumIds)
        {
            var snap = prepareSnapshots[(ballot, a)];
            responses.Add((a, snap.Accepted, snap.Value));
            if (snap.Accepted > highest)
            {
                highest = snap.Accepted;
            }
        }

        if (highest.IsZero)
        {
            return 0;
        }

        if (highest.Configuration < minRelevantCfg)
        {
            return responses.First(r => r.Accepted == highest).Value;
        }

        var votersInHCfg = configurations[highest.Configuration].Select(id => voterMap[id]).ToHashSet();
        var top = responses.Where(r => r.Accepted == highest && votersInHCfg.Contains(r.Acceptor)).ToArray();
        if (highest.Proposer > 0)
        {
            return top[0].Value;
        }

        return top
            .GroupBy(static r => r.Value)
            .OrderByDescending(static g => g.Count())
            .ThenBy(static g => g.Key)
            .First()
            .Key;
    }

    private static string FormatBallot(Ballot b) =>
        b.IsZero
            ? "zero"
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{{ config: {b.Configuration}, round: {b.Round}, owner: {b.Proposer} }}");

    /// <summary>
    /// Renders a complete Quint replay file containing one or more projected traces plus
    /// negative-control mutated traces verifying protocol violations fail replay.
    /// </summary>
    public static string RenderReplayModuleFile(
        IReadOnlyList<ProjectedQuintTrace> traces,
        IReadOnlyList<NegativeControlQuintTrace> negativeControls)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// Auto-generated from Clockwork deterministic reconfiguration simulation traces (spec §11.5).");
        sb.AppendLine("// Replays concrete per-key C# protocol traces directly against the unmodified FastCASPaxos.qnt model.");
        sb.AppendLine();

        foreach (var trace in traces)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"module {trace.ModuleName} {{");
            sb.AppendLine("  import FastCASPaxos(");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    Configs = Map({trace.ConfigsLiteral}),");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    FastQuorums = Map({trace.FastQuorumsLiteral}),");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    ProposerCount = {trace.ProposerCount}, MaxRound = {trace.MaxRound}, ValueDepth = {trace.ValueDepth}");
            sb.AppendLine("  ).* from \"./FastCASPaxos\"");
            sb.AppendLine();
            sb.AppendLine(CultureInfo.InvariantCulture, $"  run {trace.RunName} = init");
            foreach (var step in trace.Steps)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"    .then({step})");
            }

            sb.AppendLine("    .expect(safety)");
            sb.AppendLine("}");
            sb.AppendLine();
        }

        foreach (var neg in negativeControls)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"// Negative control: {neg.Description}");
            sb.AppendLine(CultureInfo.InvariantCulture, $"module {neg.ModuleName} {{");
            sb.AppendLine("  import FastCASPaxos(");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    Configs = Map({neg.ConfigsLiteral}),");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    FastQuorums = Map({neg.FastQuorumsLiteral}),");
            sb.AppendLine(CultureInfo.InvariantCulture, $"    ProposerCount = {neg.ProposerCount}, MaxRound = {neg.MaxRound}, ValueDepth = {neg.ValueDepth}");
            sb.AppendLine("  ).* from \"./FastCASPaxos\"");
            sb.AppendLine();
            sb.AppendLine(CultureInfo.InvariantCulture, $"  run {neg.RunName} = init");
            foreach (var step in neg.PrefixSteps)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"    .then({step})");
            }

            sb.AppendLine(CultureInfo.InvariantCulture, $"    .then({neg.FailingStep}.fail())");
            sb.AppendLine("}");
            sb.AppendLine();
        }

        return sb.ToString();
    }
}

public sealed record ProjectedQuintTrace(
    string ModuleName,
    string RunName,
    string Key,
    string ConfigsLiteral,
    string FastQuorumsLiteral,
    int ProposerCount,
    int MaxRound,
    int ValueDepth,
    ImmutableArray<string> Steps);

public sealed record NegativeControlQuintTrace(
    string ModuleName,
    string RunName,
    string Description,
    string ConfigsLiteral,
    string FastQuorumsLiteral,
    int ProposerCount,
    int MaxRound,
    int ValueDepth,
    ImmutableArray<string> PrefixSteps,
    string FailingStep);
