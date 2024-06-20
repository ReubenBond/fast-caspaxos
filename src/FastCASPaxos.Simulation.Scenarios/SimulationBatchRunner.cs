using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Clockwork;
using FastCASPaxos.Diagnostics;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Invariants;
using FastCASPaxos.Simulation.Hosts;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation.Scenarios;

public sealed class SimulationBatchOptions
{
    public IReadOnlyList<string> ScenarioNames { get; init; } = ["all"];

    public IReadOnlyDictionary<string, string> ScenarioParameters { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    public int Seed { get; init; }

    public int Rounds { get; init; } = 1;

    public string OutputDirectory { get; init; } =
        Path.Combine(Environment.CurrentDirectory, "artifacts", "simulation-runner");

    public bool StopOnFailure { get; init; } = true;
}

public enum SimulationProposalKind
{
    Read,
    Write,
}

public readonly record struct SimulationProposalRoundTripSample(
    SimulationProposalKind Kind,
    int RoundTrips);

public sealed record SimulationRunResult(
    string ScenarioName,
    int Round,
    int Seed,
    bool Success,
    string Summary,
    string ReproductionHint,
    IReadOnlyList<string> TraceLines,
    string LogOutput,
    IReadOnlyDictionary<string, long> Statistics,
    IReadOnlyList<SimulationProposalRoundTripSample> ProposalRoundTrips,
    IReadOnlyDictionary<string, string> Details,
    AcceptorSafetyMonitorArtifact? AcceptorSafetyMonitor,
    PorcupineHistoryArtifact? PorcupineHistory,
    string? FailureMessage,
    string? FailureDiagnostics)
{
    public SimulationStatisticsSummary CreateStatisticsSummary() =>
        SimulationStatisticsSummary.FromRun(this);
}

public sealed record SimulationBatchResult(IReadOnlyList<SimulationRunResult> Runs)
{
    public int SucceededCount => Runs.Count(run => run.Success);

    public int FailedCount => Runs.Count(run => !run.Success);

    public SimulationStatisticsSummary CreateStatisticsSummary() =>
        SimulationStatisticsSummary.FromBatch(this);
}

public sealed record SimulationScenarioDefinition(
    string Name,
    string Description,
    Func<IReadOnlyDictionary<string, string>, SimulationScenarioExecutor> CreateExecutor)
{
    public SimulationScenarioDefinition(
        string name,
        string description,
        SimulationScenarioExecutor executeAsync)
        : this(
            name,
            description,
            _ => executeAsync)
    {
    }
}

public sealed record SimulationScenarioCompletion(
    string Summary,
    string ReproductionHint);

public delegate ValueTask<SimulationScenarioCompletion> SimulationScenarioExecutor(
    int seed,
    int round,
    SimulationRunRecorder recorder,
    SimulationObservabilitySession observability,
    CancellationToken cancellationToken);

public sealed class SimulationRunRecorder : IFastCasSimulationHistoryObserver
{
    private readonly List<string> _traceLines = [];
    private readonly Dictionary<string, long> _statistics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _details = new(StringComparer.Ordinal);
    private readonly PorcupineHistoryBuilder _porcupineHistory = new();

    public IReadOnlyList<string> TraceLines => _traceLines;

    public IReadOnlyDictionary<string, long> Statistics => _statistics;

    public IReadOnlyDictionary<string, string> Details => _details;

    public void RecordTrace(DateTimeOffset timestamp, string category, string detail) =>
        _traceLines.Add($"{timestamp:O}|{category}|{detail}");

    public void RecordClusterTrace<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        string category,
        string detail)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue> =>
        RecordTrace(cluster.TimeProvider.GetUtcNow(), category, detail);

    public void RecordProposal<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FastCasAddress proposer,
        IOperation<TValue> operation,
        ProposeResponse<TValue> response)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        Increment("runner.proposals");
        RecordClusterTrace(
            cluster,
            "proposal",
            string.Create(
                CultureInfo.InvariantCulture,
                $"{proposer}|{operation}|round={response.Round}|committed={response.CommittedValue}"));
    }

    public void RecordSend<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FastCasAddress proposer,
        IOperation<TValue> operation,
        int requestId)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        Increment("runner.sent-proposals");
        RecordClusterTrace(
            cluster,
            "send",
            string.Create(
                CultureInfo.InvariantCulture,
                $"{proposer}|request={requestId}|{operation}"));
    }

    public void RecordRead<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FastCasAddress proposer,
        TValue value)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        Increment("runner.reads");
        RecordClusterTrace(cluster, "read", $"{proposer}|{value}");
    }

    public void RecordFault<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        string detail)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        Increment("runner.faults");
        RecordClusterTrace(cluster, "fault", detail);
    }

    public void Increment(string name, long delta = 1)
    {
        _statistics.TryGetValue(name, out var current);
        _statistics[name] = current + delta;
    }

    public void MergeStatistics(IReadOnlyDictionary<string, long> statistics)
    {
        foreach (var (name, value) in statistics)
        {
            Increment(name, value);
        }
    }

    public void SetDetail(string name, object? value) =>
        _details[name] = value switch
        {
            bool boolean => boolean ? "true" : "false",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };

    public void OnProposalSent<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        IOperation<TValue> operation)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue> =>
        _porcupineHistory.OnProposalSent(
            timestamp,
            proposer,
            requestId,
            operation);

    public void OnProposalCompleted<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        ProposeResponse<TValue> response)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue> =>
        _porcupineHistory.OnProposalCompleted(
            timestamp,
            proposer,
            requestId,
            response);

    public PorcupineHistoryArtifact? CreatePorcupineHistory(string scenarioName, int round, int seed)
    {
        var history = _porcupineHistory.CreateArtifact(scenarioName, round, seed);
        SetDetail(
            "porcupine-history",
            history is not null
                ? "available"
                : _porcupineHistory.UnsupportedReason ?? (_porcupineHistory.HasEvents ? "incomplete" : "empty"));
        return history;
    }
}

public sealed class SimulationObservabilitySession :
    IDisposable,
    IFastCasSimulationTransportObserver,
    IFastCasSimulationHistoryObserver,
    IFastCasSimulationAcceptorStateObserver
{
    private readonly InMemoryLoggerProvider _loggerProvider = new();
    private readonly BufferingLoggerFactory _loggerFactory;
    private readonly DiagnosticListener _diagnosticListener =
        new("FastCASPaxos.Simulation.Runner");
    private readonly Meter _meter =
        new("FastCASPaxos.Simulation.Runner");
    private readonly MeterListener _meterListener = new();
    private readonly Dictionary<string, long> _measurements = new(StringComparer.Ordinal);
    private readonly Counter<long> _transportAttemptedMessages;
    private readonly Counter<long> _transportDeliveredMessages;
    private readonly Counter<long> _transportDroppedMessages;
    private readonly Counter<long> _transportPartitionedMessages;
    private readonly Counter<long> _localProposalSubmissions;
    private readonly Counter<long> _deliveredPrepareRequests;
    private readonly Counter<long> _deliveredPreparePromises;
    private readonly Counter<long> _deliveredPrepareRejections;
    private readonly Counter<long> _deliveredAcceptRequests;
    private readonly Counter<long> _deliveredAcceptAccepted;
    private readonly Counter<long> _deliveredAcceptRejected;
    private readonly Counter<long> _localProposalCompletions;
    private readonly Dictionary<int, ActiveProposalRoundTrip> _activeProposalRoundTrips = [];
    private readonly List<SimulationProposalRoundTripSample> _completedProposalRoundTrips = [];
    private AcceptorSafetyMonitorArtifact? _acceptorSafetyMonitorArtifact;
    private IFastCasSimulationHistoryObserver? _forwardHistoryObserver;
    private IFastCasSimulationAcceptorStateObserver? _forwardAcceptorStateObserver;
    private bool _disposed;

    public IFastCasSimulationHistoryObserver? HistoryObserver { get; set; }

    public SimulationObservabilitySession()
    {
        _loggerFactory = new BufferingLoggerFactory(_loggerProvider);
        _transportAttemptedMessages = _meter.CreateCounter<long>(SimulationMetricNames.TransportAttemptedMessages, description: "Number of inter-node transport sends attempted");
        _transportDeliveredMessages = _meter.CreateCounter<long>(SimulationMetricNames.TransportDeliveredMessages, description: "Number of inter-node messages delivered to registered endpoints");
        _transportDroppedMessages = _meter.CreateCounter<long>(SimulationMetricNames.TransportDroppedMessages, description: "Number of inter-node messages dropped by the simulated network");
        _transportPartitionedMessages = _meter.CreateCounter<long>(SimulationMetricNames.TransportPartitionedMessages, description: "Number of inter-node messages blocked by network partitions");
        _localProposalSubmissions = _meter.CreateCounter<long>(SimulationMetricNames.LocalProposalSubmissions, description: "Number of collocated proposal submissions handed to proposers");
        _deliveredPrepareRequests = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredPrepareRequests, description: "Number of delivered prepare requests");
        _deliveredPreparePromises = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredPreparePromises, description: "Number of delivered prepare promise responses");
        _deliveredPrepareRejections = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredPrepareRejections, description: "Number of delivered prepare rejection responses");
        _deliveredAcceptRequests = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredAcceptRequests, description: "Number of delivered accept requests");
        _deliveredAcceptAccepted = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredAcceptAccepted, description: "Number of delivered accept accepted responses");
        _deliveredAcceptRejected = _meter.CreateCounter<long>(SimulationMetricNames.DeliveredAcceptRejected, description: "Number of delivered accept rejected responses");
        _localProposalCompletions = _meter.CreateCounter<long>(SimulationMetricNames.LocalProposalCompletions, description: "Number of collocated proposal completions returned to callers");
        _meterListener.InstrumentPublished = OnInstrumentPublished;
        _meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        _meterListener.Start();
    }

    public ILoggerFactory LoggerFactory => _loggerFactory;

    public FastCasSimulationOptions AttachOptions(FastCasSimulationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _forwardHistoryObserver = CombineHistoryObservers(HistoryObserver, options.HistoryObserver);
        _forwardAcceptorStateObserver = options.AcceptorStateObserver;

        return new FastCasSimulationOptions
        {
            EnableDistinguishedLeader = options.EnableDistinguishedLeader,
            EnableFastCommit = options.EnableFastCommit,
            CrashRestartStateModel = options.CrashRestartStateModel,
            LoggerFactory = LoggerFactory,
            CreateProposerDiagnostics = CreateProposerDiagnostics,
            AcceptorStateObserver = this,
            HistoryObserver = this,
            TransportObserver = this,
        };
    }

    public string GetLogOutput() => _loggerProvider.Buffer.FormatAllEntries();

    public IReadOnlyDictionary<string, long> GetMeasurements() =>
        new Dictionary<string, long>(_measurements, StringComparer.Ordinal);

    public IReadOnlyList<SimulationProposalRoundTripSample> GetProposalRoundTrips() =>
        [.. _completedProposalRoundTrips];

    public AcceptorSafetyMonitorArtifact? GetAcceptorSafetyMonitorArtifact() =>
        _acceptorSafetyMonitorArtifact;

    public void OnProposalSent<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        IOperation<TValue> operation)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        // The simulation only keeps one in-flight client request per proposer, so proposer ordinal
        // is enough to associate later phase counters with the request kind.
        _activeProposalRoundTrips[proposer.Ordinal] =
            new ActiveProposalRoundTrip(GetProposalKind(operation), 0);
        _forwardHistoryObserver?.OnProposalSent(timestamp, proposer, requestId, operation);
    }

    public void OnProposalCompleted<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        ProposeResponse<TValue> response)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        _forwardHistoryObserver?.OnProposalCompleted(timestamp, proposer, requestId, response);
    }

    public void OnAcceptorStateObserved<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress updatedAcceptor,
        FastCasAcceptorSafetyMonitor<TValue> monitor)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        _acceptorSafetyMonitorArtifact = monitor.CreateArtifact();
        _forwardAcceptorStateObserver?.OnAcceptorStateObserved(timestamp, updatedAcceptor, monitor);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _meterListener.Dispose();
        _meter.Dispose();
        _loggerFactory.Dispose();
        _diagnosticListener.Dispose();
    }

    public void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status)
    {
        if (IsLocalProposalInteraction(kind))
        {
            if (status == DeliveryStatus.Success)
            {
                GetLocalProposalCounter(kind).Add(1);
            }

            return;
        }

        _transportAttemptedMessages.Add(1);
        switch (status)
        {
            case DeliveryStatus.Success:
                _transportDeliveredMessages.Add(1);
                GetDeliveredNetworkCounter(kind).Add(1);
                break;
            case DeliveryStatus.Dropped:
                _transportDroppedMessages.Add(1);
                break;
            case DeliveryStatus.Partitioned:
                _transportPartitionedMessages.Add(1);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(status), status, null);
        }
    }

    private ProposerDiagnostics CreateProposerDiagnostics(FastCasNodeId nodeId)
    {
        var logger = new NodePrefixedLoggerFactory(LoggerFactory, nodeId.Address.ToString())
            .CreateLogger(typeof(ProposerDiagnostics).FullName!);
        return new ProposerDiagnostics(logger, _diagnosticListener, _meter);
    }

    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        if (ReferenceEquals(instrument.Meter, _meter))
        {
            listener.EnableMeasurementEvents(instrument);
        }
    }

    private void OnMeasurementRecorded(
        Instrument instrument,
        long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        _measurements.TryGetValue(instrument.Name, out var current);
        _measurements[instrument.Name] = current + measurement;

        if (!TryGetProposerId(tags, out var proposerId))
        {
            return;
        }

        var delta = checked((int)measurement);
        switch (instrument.Name)
        {
            case ProposerDiagnostics.AttemptsMetricName:
                var startedProposal = GetActiveProposalRoundTrip(proposerId);
                _activeProposalRoundTrips[proposerId] =
                    new ActiveProposalRoundTrip(startedProposal.Kind, 0);
                break;
            case ProposerDiagnostics.PrepareAttemptsMetricName:
            case ProposerDiagnostics.AcceptAttemptsMetricName:
                var activeProposal = GetActiveProposalRoundTrip(proposerId);
                _activeProposalRoundTrips[proposerId] =
                    new ActiveProposalRoundTrip(activeProposal.Kind, activeProposal.RoundTrips + delta);
                break;
            case ProposerDiagnostics.SuccessesMetricName:
                if (_activeProposalRoundTrips.Remove(proposerId, out var completedProposal))
                {
                    _completedProposalRoundTrips.Add(
                        new SimulationProposalRoundTripSample(
                            completedProposal.Kind,
                            completedProposal.RoundTrips));
                }

                break;
        }
    }

    private Counter<long> GetDeliveredNetworkCounter(FastCasTransportMessageKind kind) =>
        kind switch
        {
            FastCasTransportMessageKind.PrepareRequest => _deliveredPrepareRequests,
            FastCasTransportMessageKind.PreparePromise => _deliveredPreparePromises,
            FastCasTransportMessageKind.PrepareRejection => _deliveredPrepareRejections,
            FastCasTransportMessageKind.AcceptRequest => _deliveredAcceptRequests,
            FastCasTransportMessageKind.AcceptAccepted => _deliveredAcceptAccepted,
            FastCasTransportMessageKind.AcceptRejected => _deliveredAcceptRejected,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

    private Counter<long> GetLocalProposalCounter(FastCasTransportMessageKind kind) =>
        kind switch
        {
            FastCasTransportMessageKind.ProposeRequest => _localProposalSubmissions,
            FastCasTransportMessageKind.ProposeResponse => _localProposalCompletions,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

    private static bool IsLocalProposalInteraction(FastCasTransportMessageKind kind) =>
        kind is FastCasTransportMessageKind.ProposeRequest or FastCasTransportMessageKind.ProposeResponse;

    private static bool TryGetProposerId(
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        out int proposerId)
    {
        foreach (var (key, value) in tags)
        {
            if (!string.Equals(key, "proposer_id", StringComparison.Ordinal))
            {
                continue;
            }

            switch (value)
            {
                case int intValue:
                    proposerId = intValue;
                    return true;
                case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                    proposerId = (int)longValue;
                    return true;
            }
        }

        proposerId = default;
        return false;
    }

    private ActiveProposalRoundTrip GetActiveProposalRoundTrip(int proposerId) =>
        _activeProposalRoundTrips.TryGetValue(proposerId, out var activeProposal)
            ? activeProposal
            : new ActiveProposalRoundTrip(SimulationProposalKind.Write, 0);

    private static SimulationProposalKind GetProposalKind<TValue>(IOperation<TValue> operation)
        where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
    {
        if (UnwrapOperation(operation) is IPorcupineHistoryOperationDescriptor<TValue> descriptor
            && string.Equals(
                descriptor.CreatePorcupineInput().Kind,
                "read",
                StringComparison.Ordinal))
        {
            return SimulationProposalKind.Read;
        }

        return SimulationProposalKind.Write;
    }

    private static IOperation<TValue> UnwrapOperation<TValue>(IOperation<TValue> operation)
    {
        var current = operation;
        while (current is RoutedOperation<TValue, FastCasAddress> routed)
        {
            current = routed.Operation;
        }

        return current;
    }

    private static IFastCasSimulationHistoryObserver? CombineHistoryObservers(
        IFastCasSimulationHistoryObserver? left,
        IFastCasSimulationHistoryObserver? right)
    {
        if (left is null)
        {
            return right;
        }

        if (right is null || ReferenceEquals(left, right))
        {
            return left;
        }

        return new CompositeHistoryObserver(left, right);
    }

    private readonly record struct ActiveProposalRoundTrip(
        SimulationProposalKind Kind,
        int RoundTrips);

    private sealed class CompositeHistoryObserver(
        IFastCasSimulationHistoryObserver left,
        IFastCasSimulationHistoryObserver right) : IFastCasSimulationHistoryObserver
    {
        private readonly IFastCasSimulationHistoryObserver _left = left;
        private readonly IFastCasSimulationHistoryObserver _right = right;

        public void OnProposalSent<TValue>(
            DateTimeOffset timestamp,
            FastCasAddress proposer,
            int requestId,
            IOperation<TValue> operation)
            where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
        {
            _left.OnProposalSent(timestamp, proposer, requestId, operation);
            _right.OnProposalSent(timestamp, proposer, requestId, operation);
        }

        public void OnProposalCompleted<TValue>(
            DateTimeOffset timestamp,
            FastCasAddress proposer,
            int requestId,
            ProposeResponse<TValue> response)
            where TValue : FastCASPaxos.Simulation.Contracts.IVersionedValue<TValue>
        {
            _left.OnProposalCompleted(timestamp, proposer, requestId, response);
            _right.OnProposalCompleted(timestamp, proposer, requestId, response);
        }
    }

    private sealed class BufferingLoggerFactory(InMemoryLoggerProvider provider) : ILoggerFactory
    {
        private readonly InMemoryLoggerProvider _provider = provider;

        public ILogger CreateLogger(string categoryName) => _provider.CreateLogger(categoryName);

        public void AddProvider(ILoggerProvider provider) =>
            throw new NotSupportedException("BufferingLoggerFactory uses a fixed in-memory provider.");

        public void Dispose() =>
            _provider.Dispose();
    }
}

public static class SimulationScenarioCatalog
{
    private static readonly IReadOnlyList<SimulationScenarioDefinition> Definitions =
        CreateDefinitions();

    public static IReadOnlyList<SimulationScenarioDefinition> All => Definitions;

    private static IReadOnlyList<SimulationScenarioDefinition> CreateDefinitions()
    {
        List<SimulationScenarioDefinition> definitions =
        [
            new(
                "string-corpus",
                "Replay the string corpus (Coyote parity) on the deterministic simulator.",
                RunLegacyStringCorpusAsync),
            new(
                "set-corpus",
                "Replay the set corpus (Coyote parity) on the deterministic simulator.",
                RunLegacySetCorpusAsync),
            new(
                "random-string-corpus",
                "Replay the randomized string corpus (Coyote parity) on the deterministic simulator.",
                RunLegacyRandomStringCorpusAsync),
            new(
                "forking-string-corpus",
                "Replay the forking string corpus (Coyote parity) on the deterministic simulator.",
                RunLegacyForkingStringCorpusAsync),
            new(
                "randomized-append-mix",
                "Run a seeded randomized append mix across three proposers.",
                RunRandomizedAppendMixAsync),
            new(
                ParameterizedAppendSequenceOptions.ScenarioName,
                "Run a configurable append sequence with tunable cluster size, contention, and fast/leader options.",
                parameters =>
                {
                    var scenarioOptions = ParameterizedAppendSequenceOptions.Parse(parameters);
                    return (seed, round, recorder, observability, cancellationToken) =>
                    RunParameterizedAppendSequenceAsync(
                        scenarioOptions,
                        seed,
                        round,
                        recorder,
                        observability,
                        cancellationToken);
                }),
            new(
                "network-partition-heal",
                "Block quorum with partitions, then heal and verify progress resumes.",
                RunNetworkPartitionHealAsync),
            new(
                "network-delay-progress",
                "Enable message delays and verify proposals still complete.",
                RunNetworkDelayProgressAsync),
            new(
                "dropped-message-retry",
                "Drop all messages, clear the drop rate, then verify retries succeed safely.",
                RunDroppedMessageRetryAsync),
            new(
                "acceptor-restart",
                "Crash and restart an acceptor while preserving durable state.",
                RunAcceptorRestartAsync),
            new(
                "proposer-restart",
                "Crash and restart a proposer while allowing the cluster to keep progressing.",
                RunProposerRestartAsync),
            new(
                "fast-classic-conflict",
                "Run two concurrent fast-round conflicts and verify convergence.",
                RunFastClassicConflictAsync),
            new(
                "fast-leader-contention",
                "Run three-way fast leader contention and verify subsequent progress.",
                RunFastLeaderContentionAsync),
            new(
                "jittered-high-contention",
                "Run repeated jittered concurrent writes to expose dueling proposer tails.",
                RunJitteredHighContentionAsync),
            new(
                "bounded-chaos-sequence",
                "Inject bounded chaos across partitions, suspends, crashes, and delays.",
                RunBoundedChaosSequenceAsync),
        ];

        foreach (var mode in ProtocolOptionMatrix.GetAllModes())
        {
            var slug = ToSlug(mode.Name);
            definitions.Add(
                new(
                    $"seeded-transient-fault-mix-{slug}",
                    $"Run the seeded transient fault mix in {mode.Name} mode.",
                    (seed, round, recorder, observability, cancellationToken) =>
                        RunSeededTransientFaultMixAsync(
                            mode,
                            seed,
                            round,
                            recorder,
                            observability,
                            cancellationToken)));
        }

        return definitions;
    }

    private static async ValueTask<SimulationScenarioCompletion> RunLegacyStringCorpusAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var operations = CoyoteParityCorpus.BuildLegacyStringOperations();
        var expected = CoyoteParityCorpus.LegacyStringExpectedValue;

        for (var index = 0; index < operations.Count; index++)
        {
            var response = cluster.RunProposal(proposers[index % proposers.Length], operations[index]);
            recorder.RecordProposal(cluster, proposers[index % proposers.Length], operations[index], response);
            Ensure(response.CommittedValue.Version == index + 1, $"Expected committed version {index + 1}.");
            Ensure(
                string.Equals(response.CommittedValue.Value, expected[..(index + 1)], StringComparison.Ordinal),
                $"Expected committed value '{expected[..(index + 1)]}', but found '{response.CommittedValue.Value}'.");
        }

        RecordStringReads(cluster, recorder);
        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={expected}",
            ReproductionHint: cluster.GetReproductionHint("string-corpus"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunLegacySetCorpusAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateSetCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = CoyoteParityCorpus.BuildLegacySetOperations();
        var expectedVersion = 0;
        for (var index = 0; index < operations.Count; index++)
        {
            var response = cluster.RunProposal(proposers[index % proposers.Length], operations[index]);
            recorder.RecordProposal(cluster, proposers[index % proposers.Length], operations[index], response);
            expectedVersion = Math.Max(expectedVersion, response.CommittedValue.Version);
        }

        foreach (var value in RecordSetReads(cluster, recorder))
        {
            Ensure(value.Version == expectedVersion, $"Expected committed version {expectedVersion}.");
            Ensure(value.Value.SetEquals(expected), "Expected final membership to match the legacy set corpus.");
        }

        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final-version={expectedVersion} members=[{string.Join(", ", expected.OrderBy(value => value))}]",
            ReproductionHint: cluster.GetReproductionHint("set-corpus"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunLegacyRandomStringCorpusAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var proposerRandom = new Random(seed + 500);
        var (operations, expected) = CoyoteParityCorpus.BuildLegacyRandomStringOperations();
        for (var index = 0; index < operations.Count; index++)
        {
            var proposer = proposers[proposerRandom.Next(proposers.Length)];
            var response = cluster.RunProposal(proposer, operations[index]);
            recorder.RecordProposal(cluster, proposer, operations[index], response);
            Ensure(response.CommittedValue.Version == index + 1, $"Expected committed version {index + 1}.");
        }

        foreach (var value in RecordStringReads(cluster, recorder))
        {
            Ensure(value.Version == expected.Length, $"Expected committed version {expected.Length}.");
            Ensure(string.Equals(value.Value, expected, StringComparison.Ordinal), "Expected all proposers to read the same random corpus result.");
        }

        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={expected}",
            ReproductionHint: cluster.GetReproductionHint("random-string-corpus"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunLegacyForkingStringCorpusAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var (operations, expected) = CoyoteParityCorpus.BuildLegacyForkingStringOperations();
        for (var index = 0; index < operations.Count; index++)
        {
            var proposer = proposers[index % proposers.Length];
            var response = cluster.RunProposal(proposer, operations[index]);
            recorder.RecordProposal(cluster, proposer, operations[index], response);
        }

        foreach (var value in RecordStringReads(cluster, recorder))
        {
            Ensure(value.Version == expected.Length, $"Expected committed version {expected.Length}.");
            Ensure(string.Equals(value.Value, expected, StringComparison.Ordinal), "Expected a single committed chain after forking inputs.");
        }

        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={expected}",
            ReproductionHint: cluster.GetReproductionHint("forking-string-corpus"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunRandomizedAppendMixAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        const string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var operationRandom = new Random(seed + 100);
        var proposerRandom = new Random(seed + 200);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        List<char> expected = [];
        foreach (var _ in Enumerable.Range(0, 12))
        {
            var next = charset[operationRandom.Next(charset.Length)];
            expected.Add(next);
            var proposer = proposers[proposerRandom.Next(proposers.Length)];
            var response = cluster.RunProposal(proposer, StringScenarioOperations.AppendCharacter(next));
            recorder.RecordProposal(cluster, proposer, StringScenarioOperations.AppendCharacter(next), response);
            Ensure(response.CommittedValue.Version == expected.Count, $"Expected committed version {expected.Count}.");
        }

        var expectedValue = new string([.. expected]);
        foreach (var value in RecordStringReads(cluster, recorder))
        {
            Ensure(value.Version == expected.Count, $"Expected committed version {expected.Count}.");
            Ensure(string.Equals(value.Value, expectedValue, StringComparison.Ordinal), "Expected randomized append mix to converge.");
        }

        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={expectedValue}",
            ReproductionHint: cluster.GetReproductionHint("randomized-append-mix"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunParameterizedAppendSequenceAsync(
        ParameterizedAppendSequenceOptions scenarioOptions,
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        foreach (var parameter in scenarioOptions.DescribeParameters())
        {
            recorder.SetDetail($"parameter.{parameter.Key}", parameter.Value);
        }

        var options = observability.AttachOptions(
            new FastCasSimulationOptions
            {
                EnableFastCommit = scenarioOptions.EnableFastCommit,
                EnableDistinguishedLeader = scenarioOptions.EnableDistinguishedLeader,
            });
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(
            proposerCount: scenarioOptions.ProposerCount,
            acceptorCount: scenarioOptions.AcceptorCount);

        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var conflictRandom = new Random(seed + 600);
        var requestId = 1_000;
        var conflictSteps = 0;
        var lastCommittedValue = default(StringValue);

        for (var version = 1; version <= scenarioOptions.ValueCount; version++)
        {
            if (ShouldInjectConflictStep(scenarioOptions, conflictRandom))
            {
                conflictSteps++;
                var contenders = SelectContendingProposers(
                    proposers,
                    conflictFanout: scenarioOptions.ConflictFanout,
                    logicalStep: version);
                List<(FastCasAddress Proposer, IOperation<StringValue> Operation, int RequestId)> submissions = [];
                for (var contenderIndex = 0; contenderIndex < contenders.Count; contenderIndex++)
                {
                    var proposer = contenders[contenderIndex];
                    var operation = StringScenarioOperations.AppendAtVersion(
                        version,
                        GetAppendSegment(version, contenderIndex));
                    var submissionRequestId = requestId++;
                    _ = cluster.SendProposal(proposer, operation, submissionRequestId);
                    recorder.RecordSend(cluster, proposer, operation, submissionRequestId);
                    submissions.Add((proposer, operation, submissionRequestId));
                }

                Ensure(
                    cluster.RunUntil(
                        () => submissions.All(submission => cluster.TryGetResponse(submission.RequestId, out _)),
                        maxIterations: 5000),
                    $"Expected all conflict proposals for logical step {version} to complete.");

                List<StringValue> responses = [];
                foreach (var submission in submissions)
                {
                    Ensure(
                        cluster.TryGetResponse(submission.RequestId, out var response),
                        $"Expected response for request {submission.RequestId}.");
                    recorder.RecordProposal(cluster, submission.Proposer, submission.Operation, response);
                    responses.Add(response.CommittedValue);
                }

                var committedValues = responses.Distinct().ToArray();
                Ensure(
                    committedValues.Length == 1,
                    $"Expected conflict step {version} to converge on a single committed value.");
                lastCommittedValue = committedValues[0];
            }
            else
            {
                var proposer = proposers[(version - 1) % proposers.Length];
                var operation = StringScenarioOperations.AppendAtVersion(
                    version,
                    GetAppendSegment(version, contenderIndex: 0));
                var response = cluster.RunProposal(proposer, operation, maxIterations: 5000);
                recorder.RecordProposal(cluster, proposer, operation, response);
                lastCommittedValue = response.CommittedValue;
            }

            Ensure(
                lastCommittedValue.Version == version,
                $"Expected committed version {version} after logical step {version}.");
        }

        IReadOnlyList<StringValue> values = proposers.Length == 1
            ? [lastCommittedValue]
            : RecordStringReads(cluster, recorder);
        Ensure(values.Distinct().Count() == 1, "Expected all proposers to converge on a single committed value.");
        var finalValue = values[0];
        Ensure(
            finalValue.Version == scenarioOptions.ValueCount,
            $"Expected final committed version {scenarioOptions.ValueCount}.");

        recorder.SetDetail("logical-steps", scenarioOptions.ValueCount);
        recorder.SetDetail("conflict-steps", conflictSteps);
        recorder.SetDetail("final-value", finalValue.Value);
        recorder.SetDetail("final-version", finalValue.Version);

        cluster.AssertSafetyInvariants();
        return new(
            Summary: string.Create(
                CultureInfo.InvariantCulture,
                $"final={finalValue.Value} logical-steps={scenarioOptions.ValueCount} conflict-steps={conflictSteps}"),
            ReproductionHint: string.Create(
                CultureInfo.InvariantCulture,
                $"{cluster.GetReproductionHint(ParameterizedAppendSequenceOptions.ScenarioName)} params: {FormatParameters(scenarioOptions)}"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunNetworkPartitionHealAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(acceptor));
            recorder.RecordFault(cluster, $"partition {proposer} <-> {FastCasAddress.Acceptor(acceptor)}");
        }

        var blockedOperation = StringScenarioOperations.AppendAtVersion(1, "A");
        var blockedRequestId = cluster.SendProposal(proposer, blockedOperation, requestId: 77);
        recorder.RecordSend(cluster, proposer, blockedOperation, blockedRequestId);
        Ensure(!cluster.RunUntil(() => cluster.TryGetResponse(77, out _), maxIterations: 100), "Expected the partitioned proposal to stall.");

        foreach (var acceptor in new[] { 1, 2, 3 })
        {
            cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(acceptor));
            recorder.RecordFault(cluster, $"heal {proposer} <-> {FastCasAddress.Acceptor(acceptor)}");
        }

        var recoveryOperation = StringScenarioOperations.AppendAtVersion(1, "A");
        var response = cluster.RunProposal(proposer, recoveryOperation);
        recorder.RecordProposal(cluster, proposer, recoveryOperation, response);
        Ensure(string.Equals(response.CommittedValue.Value, "A", StringComparison.Ordinal), "Expected the healed cluster to commit 'A'.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: "final=A",
            ReproductionHint: cluster.GetReproductionHint("network-partition-heal"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunNetworkDelayProgressAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(5));
        recorder.RecordFault(cluster, "delays base=00:00:00.0100000 jitter=00:00:00.0050000");

        var start = cluster.TimeProvider.GetUtcNow();
        var operation = StringScenarioOperations.AppendAtVersion(1, "A");
        var response = cluster.RunProposal(FastCasAddress.Proposer(1), operation);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(1), operation, response);
        Ensure(string.Equals(response.CommittedValue.Value, "A", StringComparison.Ordinal), "Expected delayed proposal to commit 'A'.");
        Ensure(cluster.TimeProvider.GetUtcNow() > start, "Expected simulated time to advance while delays were enabled.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={response.CommittedValue.Value}",
            ReproductionHint: cluster.GetReproductionHint("network-delay-progress"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunDroppedMessageRetryAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        cluster.SetMessageDropRate(1.0);
        recorder.RecordFault(cluster, "drop-rate 100%");

        var droppedOperation = StringScenarioOperations.AppendAtVersion(1, "A");
        var droppedRequestId = cluster.SendProposal(proposer, droppedOperation, requestId: 88);
        recorder.RecordSend(cluster, proposer, droppedOperation, droppedRequestId);
        Ensure(!cluster.RunUntil(() => cluster.TryGetResponse(88, out _), maxIterations: 100), "Expected the fully dropped proposal to stall.");

        cluster.SetMessageDropRate(0);
        recorder.RecordFault(cluster, "drop-rate 0%");

        var recoveryOperation = StringScenarioOperations.AppendAtVersion(1, "A");
        var response = cluster.RunProposal(proposer, recoveryOperation);
        recorder.RecordProposal(cluster, proposer, recoveryOperation, response);
        Ensure(string.Equals(response.CommittedValue.Value, "A", StringComparison.Ordinal), "Expected retry after clearing drop rate to commit 'A'.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: "final=A",
            ReproductionHint: cluster.GetReproductionHint("dropped-message-retry"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunAcceptorRestartAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var first = StringScenarioOperations.AppendAtVersion(1, "A");
        var firstResponse = cluster.RunProposal(FastCasAddress.Proposer(1), first);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(1), first, firstResponse);

        cluster.CrashNode(FastCasAddress.Acceptor(1));
        recorder.RecordFault(cluster, $"crash {FastCasAddress.Acceptor(1)}");
        cluster.RestartNode(FastCasAddress.Acceptor(1));
        recorder.RecordFault(cluster, $"restart {FastCasAddress.Acceptor(1)}");

        var second = StringScenarioOperations.AppendAtVersion(2, "B");
        var secondResponse = cluster.RunProposal(FastCasAddress.Proposer(1), second);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(1), second, secondResponse);

        var acceptor = cluster.AcceptorNodes[0].Host as FastCasAcceptorHost<StringValue>
            ?? throw new InvalidOperationException("Expected acceptor host type.");
        Ensure(string.Equals(secondResponse.CommittedValue.Value, "AB", StringComparison.Ordinal), "Expected acceptor restart scenario to commit 'AB'.");
        Ensure(string.Equals(acceptor.State.AcceptedValue.Value, "AB", StringComparison.Ordinal), "Expected restarted acceptor to preserve durable state.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: "final=AB",
            ReproductionHint: cluster.GetReproductionHint("acceptor-restart"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunProposerRestartAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var first = StringScenarioOperations.AppendAtVersion(1, "A");
        var firstResponse = cluster.RunProposal(FastCasAddress.Proposer(1), first);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(1), first, firstResponse);

        cluster.CrashNode(FastCasAddress.Proposer(1));
        recorder.RecordFault(cluster, $"crash {FastCasAddress.Proposer(1)}");
        cluster.RestartNode(FastCasAddress.Proposer(1));
        recorder.RecordFault(cluster, $"restart {FastCasAddress.Proposer(1)}");

        var second = StringScenarioOperations.AppendAtVersion(2, "B");
        var secondResponse = cluster.RunProposal(FastCasAddress.Proposer(2), second);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(2), second, secondResponse);
        Ensure(string.Equals(secondResponse.CommittedValue.Value, "AB", StringComparison.Ordinal), "Expected proposer restart scenario to commit 'AB'.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: "final=AB",
            ReproductionHint: cluster.GetReproductionHint("proposer-restart"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunFastClassicConflictAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(
            new FastCasSimulationOptions
            {
                EnableFastCommit = true,
                EnableDistinguishedLeader = false,
            });
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var op1 = StringScenarioOperations.AppendAtVersion(1, "A");
        var op2 = StringScenarioOperations.AppendAtVersion(1, "B");
        _ = cluster.SendProposal(FastCasAddress.Proposer(1), op1, requestId: 1);
        recorder.RecordSend(cluster, FastCasAddress.Proposer(1), op1, 1);
        _ = cluster.SendProposal(FastCasAddress.Proposer(2), op2, requestId: 2);
        recorder.RecordSend(cluster, FastCasAddress.Proposer(2), op2, 2);

        Ensure(cluster.RunUntil(() => cluster.ClientResponses.Count >= 2, maxIterations: 5000), "Expected both fast-round conflict proposals to complete.");
        foreach (var proposer in new[] { FastCasAddress.Proposer(1), FastCasAddress.Proposer(2) })
        {
            var read = StringScenarioOperations.Read();
            var response = cluster.RunProposal(proposer, read, maxIterations: 5000);
            recorder.RecordProposal(cluster, proposer, read, response);
        }

        var values = RecordStringReads(cluster, recorder);
        Ensure(values.Distinct().Count() == 1, "Expected both proposers to converge on a single committed value.");
        Ensure(values[0].Version == 1, "Expected the converged value to remain at version 1.");
        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={values[0]}",
            ReproductionHint: cluster.GetReproductionHint("fast-classic-conflict"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunFastLeaderContentionAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(
            new FastCasSimulationOptions
            {
                EnableFastCommit = true,
                EnableDistinguishedLeader = true,
            });
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        var initialOperations = new[]
        {
            (FastCasAddress.Proposer(1), StringScenarioOperations.AppendAtVersion(1, "A"), 1),
            (FastCasAddress.Proposer(2), StringScenarioOperations.AppendAtVersion(1, "B"), 2),
            (FastCasAddress.Proposer(3), StringScenarioOperations.AppendAtVersion(1, "C"), 3),
        };

        foreach (var (proposer, operation, requestId) in initialOperations)
        {
            _ = cluster.SendProposal(proposer, operation, requestId);
            recorder.RecordSend(cluster, proposer, operation, requestId);
        }

        Ensure(cluster.RunUntil(() => initialOperations.All(item => cluster.TryGetResponse(item.Item3, out _)), maxIterations: 5000), "Expected all initial leader contention proposals to complete.");
        var convergedBeforeFollowUp = ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster);
        var initial = convergedBeforeFollowUp.Distinct().Single();
        Ensure(initial.Version == 1, "Expected the initial leader contention result to stay at version 1.");

        var followUp = StringScenarioOperations.AppendAtVersion(2, "D");
        var followUpResponse = cluster.RunProposal(FastCasAddress.Proposer(1), followUp, maxIterations: 5000);
        recorder.RecordProposal(cluster, FastCasAddress.Proposer(1), followUp, followUpResponse);
        Ensure(followUpResponse.CommittedValue.Version == 2, "Expected the follow-up proposal to commit version 2.");
        Ensure(followUpResponse.CommittedValue.Value.EndsWith("D", StringComparison.Ordinal), "Expected the follow-up proposal to append 'D'.");

        var converged = ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster).Distinct().Single();
        Ensure(EqualityComparer<StringValue>.Default.Equals(converged, followUpResponse.CommittedValue), "Expected all proposers to converge on the follow-up committed value.");
        RecordStringReads(cluster, recorder);
        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"final={converged}",
            ReproductionHint: cluster.GetReproductionHint("fast-leader-contention"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunJitteredHighContentionAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        const int proposerCount = 5;
        const int acceptorCount = 5;
        const int logicalSteps = 8;
        var baseDelay = TimeSpan.FromMilliseconds(2);
        var maxJitter = TimeSpan.FromMilliseconds(13);

        var options = observability.AttachOptions(
            new FastCasSimulationOptions
            {
                EnableFastCommit = true,
                EnableDistinguishedLeader = true,
            });
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: proposerCount, acceptorCount: acceptorCount);
        cluster.ConfigureMessageDelays(baseDelay, maxJitter);
        recorder.RecordFault(cluster, $"delays base={baseDelay} jitter={maxJitter}");
        recorder.SetDetail("parameter.retry-backoff", "exponential-deterministic-jitter");
        recorder.SetDetail(
            "parameter.retry-backoff-start-after-failures",
            options.ProposerRetryBackoff.StartAfterFailures);
        recorder.SetDetail(
            "parameter.retry-backoff-base-ms",
            options.ProposerRetryBackoff.BaseDelay.TotalMilliseconds);
        recorder.SetDetail(
            "parameter.retry-backoff-max-ms",
            options.ProposerRetryBackoff.MaxDelay.TotalMilliseconds);

        const string charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        var requestId = 50_000;

        for (var step = 0; step < logicalSteps; step++)
        {
            List<(FastCasAddress Proposer, IOperation<StringValue> Operation, int RequestId)> submissions = [];
            for (var contenderIndex = 0; contenderIndex < proposers.Length; contenderIndex++)
            {
                var proposer = proposers[(step + contenderIndex) % proposers.Length];
                var operation = StringScenarioOperations.AppendCharacter(
                    charset[(step * proposers.Length + contenderIndex) % charset.Length]);
                var submissionRequestId = requestId++;
                _ = cluster.SendProposal(proposer, operation, submissionRequestId);
                recorder.RecordSend(cluster, proposer, operation, submissionRequestId);
                submissions.Add((proposer, operation, submissionRequestId));
            }

            Ensure(
                cluster.RunUntil(
                    () => submissions.All(submission => cluster.TryGetResponse(submission.RequestId, out _)),
                    maxIterations: 25_000),
                $"Expected all jittered contention proposals for logical step {step + 1} to complete.");
            foreach (var submission in submissions)
            {
                Ensure(
                    cluster.TryGetResponse(submission.RequestId, out var response),
                    $"Expected response for request {submission.RequestId}.");
                recorder.RecordProposal(cluster, submission.Proposer, submission.Operation, response);
            }
        }

        var convergedValues = ProtocolOptionMatrix.ReadUntilStringValuesAgree(cluster, maxRounds: 10);
        Ensure(
            convergedValues.Distinct().Count() == 1,
            $"Expected jittered high contention scenario to converge. Values: {string.Join(", ", convergedValues)}");
        var values = RecordStringReads(cluster, recorder);
        Ensure(values.Distinct().Count() == 1, "Expected jittered high contention scenario to converge.");
        var finalValue = values[0];
        Ensure(
            finalValue.Version >= logicalSteps,
            $"Expected jittered high contention scenario to commit at least {logicalSteps} versions, but found {finalValue.Version}.");
        cluster.AssertSafetyInvariants();

        recorder.SetDetail("logical-steps", logicalSteps);
        recorder.SetDetail("contention-fanout", proposers.Length);
        recorder.SetDetail("parameter.proposer-count", proposerCount);
        recorder.SetDetail("parameter.acceptor-count", acceptorCount);
        recorder.SetDetail("parameter.fast", true);
        recorder.SetDetail("parameter.leader", true);
        recorder.SetDetail("parameter.delay-base-ms", baseDelay.TotalMilliseconds);
        recorder.SetDetail("parameter.delay-max-jitter-ms", maxJitter.TotalMilliseconds);
        recorder.SetDetail("writes-sent", logicalSteps * proposers.Length);
        recorder.SetDetail("final-version", finalValue.Version);
        recorder.SetDetail("final-value", finalValue.Value ?? string.Empty);

        return new(
            Summary: string.Create(
                CultureInfo.InvariantCulture,
                $"final-version={finalValue.Version}; fanout={proposers.Length}; delays={baseDelay.TotalMilliseconds:0}+{maxJitter.TotalMilliseconds:0}ms"),
            ReproductionHint: cluster.GetReproductionHint("jittered-high-contention"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunBoundedChaosSequenceAsync(
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(new FastCasSimulationOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var random = new Random(seed + 300);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        for (var version = 1; version <= 8; version++)
        {
            var proposer = proposers[version % proposers.Length];
            switch (random.Next(4))
            {
                case 0:
                    cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                    recorder.RecordFault(cluster, $"partition {proposer} <-> {FastCasAddress.Acceptor(1)}");
                    break;
                case 1:
                    cluster.SuspendNode(FastCasAddress.Acceptor(2));
                    recorder.RecordFault(cluster, $"suspend {FastCasAddress.Acceptor(2)}");
                    break;
                case 2:
                    cluster.CrashNode(FastCasAddress.Acceptor(3));
                    recorder.RecordFault(cluster, $"crash {FastCasAddress.Acceptor(3)}");
                    break;
                default:
                    cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(5));
                    recorder.RecordFault(cluster, "delays base=00:00:00.0050000 jitter=00:00:00.0050000");
                    break;
            }

            var operation = StringScenarioOperations.AppendAtVersion(version, version.ToString(CultureInfo.InvariantCulture));
            var response = cluster.RunProposal(proposer, operation);
            recorder.RecordProposal(cluster, proposer, operation, response);
            Ensure(response.CommittedValue.Version == version, $"Expected chaos scenario to commit version {version}.");

            cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
            recorder.RecordFault(cluster, $"heal {proposer} <-> {FastCasAddress.Acceptor(1)}");
            cluster.ResumeNode(FastCasAddress.Acceptor(2));
            recorder.RecordFault(cluster, $"resume {FastCasAddress.Acceptor(2)}");
            cluster.RestartNode(FastCasAddress.Acceptor(3));
            recorder.RecordFault(cluster, $"restart {FastCasAddress.Acceptor(3)}");
            cluster.ConfigureMessageDelays(TimeSpan.Zero, TimeSpan.Zero);
            recorder.RecordFault(cluster, "delays base=00:00:00 jitter=00:00:00");
        }

        cluster.AssertSafetyInvariants();
        Ensure(cluster.ClientResponses.Max(response => response.Value.CommittedValue.Version) == 8, "Expected bounded chaos sequence to commit through version 8.");
        return new(
            Summary: "final-version=8",
            ReproductionHint: cluster.GetReproductionHint("bounded-chaos-sequence"));
    }

    private static async ValueTask<SimulationScenarioCompletion> RunSeededTransientFaultMixAsync(
        ProtocolModeCase mode,
        int seed,
        int round,
        SimulationRunRecorder recorder,
        SimulationObservabilitySession observability,
        CancellationToken cancellationToken)
    {
        _ = round;
        var options = observability.AttachOptions(mode.CreateOptions());
        await using var cluster = CreateStringCluster(seed, options, cancellationToken);
        cluster.CreateConfiguration(proposerCount: 3, acceptorCount: 5);

        const string charset = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        var operationRandom = new Random(seed + 100);
        var proposerRandom = new Random(seed + 200);
        var faultRandom = new Random(seed + 300);
        var proposers = cluster.ProposerNodes.Select(node => node.Host.Address).ToArray();
        // This scenario models replayable CAS-style appends, so each request carries the version it
        // intends to extend instead of behaving like a blind append against a stale local cache.
        List<char> expected = [];

        foreach (var _ in Enumerable.Range(0, 8))
        {
            var next = charset[operationRandom.Next(charset.Length)];
            expected.Add(next);
            var expectedVersion = expected.Count;

            var proposer = proposers[proposerRandom.Next(proposers.Length)];
            var fault = (FaultKind)faultRandom.Next(3);
            ApplyTransientFault(cluster, proposer, fault, recorder);
            try
            {
                var operation = StringScenarioOperations.AppendAtVersion(expectedVersion, next.ToString());
                var response = cluster.RunProposal(proposer, operation, maxIterations: 5000);
                recorder.RecordProposal(cluster, proposer, operation, response);
                Ensure(response.CommittedValue.Version == expectedVersion, $"Expected committed version {expectedVersion}.");
                Ensure(
                    string.Equals(response.CommittedValue.Value, new string([.. expected]), StringComparison.Ordinal),
                    $"Expected committed value '{new string([.. expected])}'.");
            }
            finally
            {
                ClearTransientFault(cluster, proposer, fault, recorder);
            }
        }

        var expectedValue = new string([.. expected]);
        _ = ProtocolOptionMatrix.EnsureConvergedStringValue(cluster, expected.Count, expectedValue);
        RecordStringReads(cluster, recorder);
        cluster.AssertSafetyInvariants();
        return new(
            Summary: $"mode={mode.Name} final={expectedValue}",
            ReproductionHint: cluster.GetReproductionHint($"seeded-transient-fault-mix-{ToSlug(mode.Name)}"));
    }

    private static void ApplyTransientFault(
        FastCasSimulationCluster<StringValue> cluster,
        FastCasAddress proposer,
        FaultKind fault,
        SimulationRunRecorder recorder)
    {
        switch (fault)
        {
            case FaultKind.None:
                return;
            case FaultKind.Delay:
                cluster.ConfigureMessageDelays(TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(3));
                recorder.RecordFault(cluster, "delays base=00:00:00.0020000 jitter=00:00:00.0030000");
                return;
            case FaultKind.Partition:
                cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                recorder.RecordFault(cluster, $"partition {proposer} <-> {FastCasAddress.Acceptor(1)}");
                return;
            default:
                throw new InvalidOperationException($"Unsupported transient fault '{fault}'.");
        }
    }

    private static void ClearTransientFault(
        FastCasSimulationCluster<StringValue> cluster,
        FastCasAddress proposer,
        FaultKind fault,
        SimulationRunRecorder recorder)
    {
        switch (fault)
        {
            case FaultKind.None:
                return;
            case FaultKind.Delay:
                cluster.ConfigureMessageDelays(TimeSpan.Zero, TimeSpan.Zero);
                recorder.RecordFault(cluster, "delays base=00:00:00 jitter=00:00:00");
                return;
            case FaultKind.Partition:
                cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
                recorder.RecordFault(cluster, $"heal {proposer} <-> {FastCasAddress.Acceptor(1)}");
                return;
            default:
                throw new InvalidOperationException($"Unsupported transient fault '{fault}'.");
        }
    }

    private static FastCasSimulationCluster<StringValue> CreateStringCluster(
        int seed,
        FastCasSimulationOptions options,
        CancellationToken cancellationToken) =>
        new(seed, options, DateTimeOffset.UnixEpoch, cancellationToken);

    private static FastCasSimulationCluster<SetValue> CreateSetCluster(
        int seed,
        FastCasSimulationOptions options,
        CancellationToken cancellationToken) =>
        new(seed, options, DateTimeOffset.UnixEpoch, cancellationToken);

    private static IReadOnlyList<StringValue> RecordStringReads(
        FastCasSimulationCluster<StringValue> cluster,
        SimulationRunRecorder recorder)
    {
        List<StringValue> values = [];
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, StringScenarioOperations.Read());
            var host = proposer.Host as FastCasProposerHost<StringValue>
                ?? throw new InvalidOperationException("Expected proposer host type.");
            values.Add(host.CachedValue);
            recorder.RecordRead(cluster, proposer.Host.Address, host.CachedValue);
        }

        return values;
    }

    private static IReadOnlyList<SetValue> RecordSetReads(
        FastCasSimulationCluster<SetValue> cluster,
        SimulationRunRecorder recorder)
    {
        List<SetValue> values = [];
        foreach (var proposer in cluster.ProposerNodes)
        {
            _ = cluster.RunProposal(proposer.Host.Address, SetScenarioOperations.Read());
            var host = proposer.Host as FastCasProposerHost<SetValue>
                ?? throw new InvalidOperationException("Expected proposer host type.");
            values.Add(host.CachedValue);
            recorder.RecordRead(cluster, proposer.Host.Address, host.CachedValue);
        }

        return values;
    }

    private static bool ShouldInjectConflictStep(
        ParameterizedAppendSequenceOptions options,
        Random conflictRandom) =>
        options.ConflictRate > 0
        && conflictRandom.NextDouble() < options.ConflictRate;

    private static IReadOnlyList<FastCasAddress> SelectContendingProposers(
        IReadOnlyList<FastCasAddress> proposers,
        int conflictFanout,
        int logicalStep)
    {
        List<FastCasAddress> selected = [];
        var start = (logicalStep - 1) % proposers.Count;
        for (var index = 0; index < conflictFanout; index++)
        {
            selected.Add(proposers[(start + index) % proposers.Count]);
        }

        return selected;
    }

    private static string GetAppendSegment(int logicalStep, int contenderIndex)
    {
        var baseSegment = GetSequenceToken(logicalStep);
        return contenderIndex == 0
            ? baseSegment
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{baseSegment}_{contenderIndex + 1}");
    }

    private static string GetSequenceToken(int oneBasedIndex)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(oneBasedIndex, 1);

        Span<char> buffer = stackalloc char[16];
        var cursor = buffer.Length;
        var current = oneBasedIndex;
        while (current > 0)
        {
            current--;
            buffer[--cursor] = (char)('A' + (current % 26));
            current /= 26;
        }

        return new string(buffer[cursor..]);
    }

    private static string FormatParameters(ParameterizedAppendSequenceOptions options) =>
        string.Join(
            ", ",
            options.DescribeParameters()
                .Select(parameter => string.Create(
                    CultureInfo.InvariantCulture,
                    $"{parameter.Key}={parameter.Value}")));

    private static string ToSlug(string value) =>
        value.Replace('+', '-').Replace(' ', '-');

    private static void Ensure(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private enum FaultKind
    {
        None,
        Delay,
        Partition,
    }
}

public static class SimulationBatchRunner
{
    public static IReadOnlyList<SimulationScenarioDefinition> AvailableScenarios =>
        SimulationScenarioCatalog.All;

    public static async Task<SimulationBatchResult> RunAsync(
        SimulationBatchOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.OutputDirectory);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Rounds, 1);

        var outputDirectory = Path.GetFullPath(options.OutputDirectory);
        Directory.CreateDirectory(outputDirectory);

        var selectedScenarios = ResolveScenarios(options.ScenarioNames);
        var scenarioParameters = options.ScenarioParameters;
        var plannedRuns = CreatePlannedRuns(selectedScenarios, options);
        var results = new SimulationRunResult?[plannedRuns.Count];

        CancellationTokenSource? stopOnFailureCancellation = null;
        var executionCancellationToken = cancellationToken;
        if (options.StopOnFailure)
        {
            stopOnFailureCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            executionCancellationToken = stopOnFailureCancellation.Token;
        }

        try
        {
            await Parallel.ForEachAsync(
                plannedRuns,
                executionCancellationToken,
                async (plannedRun, runCancellationToken) =>
                {
                    var result = await RunScenarioAsync(
                        plannedRun.Scenario,
                        scenarioParameters,
                        plannedRun.Seed,
                        plannedRun.Round,
                        outputDirectory,
                        runCancellationToken);
                    results[plannedRun.Index] = result;
                    if (!result.Success && options.StopOnFailure)
                    {
                        stopOnFailureCancellation!.Cancel();
                    }
                });
        }
        catch (OperationCanceledException) when (
            options.StopOnFailure &&
            stopOnFailureCancellation is not null &&
            stopOnFailureCancellation.IsCancellationRequested &&
            !cancellationToken.IsCancellationRequested)
        {
            // Stop-on-failure canceled remaining queued work; finalize completed results below.
        }
        finally
        {
            stopOnFailureCancellation?.Dispose();
        }

        List<SimulationRunResult> completedResults = [];
        foreach (var result in results)
        {
            if (result is not null)
            {
                completedResults.Add(result);
            }
        }

        return await FinalizeBatchAsync(completedResults, outputDirectory, cancellationToken);
    }

    private static async Task<SimulationRunResult> RunScenarioAsync(
        SimulationScenarioDefinition scenario,
        IReadOnlyDictionary<string, string> scenarioParameters,
        int seed,
        int round,
        string outputDirectory,
        CancellationToken cancellationToken)
    {
        var recorder = new SimulationRunRecorder();
        recorder.SetDetail("round", round);
        recorder.SetDetail("seed", seed);
        recorder.SetDetail("scenario", scenario.Name);

        var executeAsync = scenario.CreateExecutor(scenarioParameters);
        using var observability = new SimulationObservabilitySession();
        observability.HistoryObserver = recorder;
        try
        {
            var completion = await executeAsync(
                seed,
                round,
                recorder,
                observability,
                cancellationToken);

            recorder.MergeStatistics(observability.GetMeasurements());
            var porcupineHistory = recorder.CreatePorcupineHistory(scenario.Name, round, seed);
            var acceptorSafetyMonitor = observability.GetAcceptorSafetyMonitorArtifact();
            recorder.SetDetail("acceptor-safety-monitor", acceptorSafetyMonitor is not null ? "available" : "unavailable");
            if (acceptorSafetyMonitor is not null)
            {
                recorder.SetDetail("acceptor-safety-monitor-commits", acceptorSafetyMonitor.Commits.Count);
            }
            var result = new SimulationRunResult(
                scenario.Name,
                round,
                seed,
                Success: true,
                completion.Summary,
                completion.ReproductionHint,
                recorder.TraceLines,
                observability.GetLogOutput(),
                new Dictionary<string, long>(recorder.Statistics, StringComparer.Ordinal),
                new List<SimulationProposalRoundTripSample>(observability.GetProposalRoundTrips()),
                new Dictionary<string, string>(recorder.Details, StringComparer.Ordinal),
                acceptorSafetyMonitor,
                porcupineHistory,
                FailureMessage: null,
                FailureDiagnostics: null);
            await WriteArtifactsAsync(outputDirectory, result, cancellationToken);
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            recorder.MergeStatistics(observability.GetMeasurements());
            var porcupineHistory = recorder.CreatePorcupineHistory(scenario.Name, round, seed);
            var acceptorSafetyMonitor = observability.GetAcceptorSafetyMonitorArtifact();
            recorder.SetDetail("acceptor-safety-monitor", acceptorSafetyMonitor is not null ? "available" : "unavailable");
            if (acceptorSafetyMonitor is not null)
            {
                recorder.SetDetail("acceptor-safety-monitor-commits", acceptorSafetyMonitor.Commits.Count);
            }
            var result = new SimulationRunResult(
                scenario.Name,
                round,
                seed,
                Success: false,
                Summary: $"failed: {ex.Message}",
                ReproductionHint: $"{scenario.Name} (seed: {seed})",
                recorder.TraceLines,
                observability.GetLogOutput(),
                new Dictionary<string, long>(recorder.Statistics, StringComparer.Ordinal),
                new List<SimulationProposalRoundTripSample>(observability.GetProposalRoundTrips()),
                new Dictionary<string, string>(recorder.Details, StringComparer.Ordinal),
                acceptorSafetyMonitor,
                porcupineHistory,
                FailureMessage: ex.Message,
                FailureDiagnostics: ex.ToString());
            await WriteArtifactsAsync(outputDirectory, result, cancellationToken);
            return result;
        }
    }

    private static IReadOnlyList<PlannedSimulationRun> CreatePlannedRuns(
        IReadOnlyList<SimulationScenarioDefinition> selectedScenarios,
        SimulationBatchOptions options)
    {
        var plannedRuns = new List<PlannedSimulationRun>(checked(options.Rounds * selectedScenarios.Count));
        var index = 0;
        for (var round = 1; round <= options.Rounds; round++)
        {
            var seed = checked(options.Seed + round - 1);
            foreach (var scenario in selectedScenarios)
            {
                plannedRuns.Add(new PlannedSimulationRun(index++, scenario, round, seed));
            }
        }

        return plannedRuns;
    }

    private static IReadOnlyList<SimulationScenarioDefinition> ResolveScenarios(IReadOnlyList<string> requestedNames)
    {
        if (requestedNames.Count == 0 || requestedNames.Any(name => string.Equals(name, "all", StringComparison.OrdinalIgnoreCase)))
        {
            return AvailableScenarios;
        }

        var lookup = AvailableScenarios.ToDictionary(
            scenario => scenario.Name,
            scenario => scenario,
            StringComparer.OrdinalIgnoreCase);
        List<SimulationScenarioDefinition> selected = [];
        foreach (var requestedName in requestedNames)
        {
            if (!lookup.TryGetValue(requestedName, out var scenario))
            {
                throw new ArgumentException($"Unknown scenario '{requestedName}'. Use --list to see the available scenarios.");
            }

            selected.Add(scenario);
        }

        return selected;
    }

    private static async Task WriteArtifactsAsync(
        string outputDirectory,
        SimulationRunResult result,
        CancellationToken cancellationToken)
    {
        var scenarioDirectory = Path.Combine(
            outputDirectory,
            SanitizePathSegment(result.ScenarioName),
            $"round-{result.Round:D3}-seed-{result.Seed}");
        Directory.CreateDirectory(scenarioDirectory);

        await File.WriteAllTextAsync(
            Path.Combine(scenarioDirectory, "summary.txt"),
            FormatSummary(result),
            cancellationToken);
        await File.WriteAllLinesAsync(
            Path.Combine(scenarioDirectory, "trace.log"),
            result.TraceLines,
            cancellationToken);
        await File.WriteAllTextAsync(
            Path.Combine(scenarioDirectory, "logs.txt"),
            result.LogOutput,
            cancellationToken);
        await File.WriteAllTextAsync(
            Path.Combine(scenarioDirectory, "stats.txt"),
            SimulationStatisticsFormatter.FormatRunArtifact(result),
            cancellationToken);
        if (result.AcceptorSafetyMonitor is not null)
        {
            await AcceptorSafetyMonitorSerializer.WriteAsync(
                Path.Combine(scenarioDirectory, "acceptor-safety-monitor.json"),
                result.AcceptorSafetyMonitor,
                cancellationToken);
        }
        if (result.PorcupineHistory is not null)
        {
            await PorcupineHistorySerializer.WriteAsync(
                Path.Combine(scenarioDirectory, "porcupine-history.json"),
                result.PorcupineHistory,
                cancellationToken);
        }
    }

    private static async Task<SimulationBatchResult> FinalizeBatchAsync(
        IReadOnlyList<SimulationRunResult> results,
        string outputDirectory,
        CancellationToken cancellationToken)
    {
        var batchResult = new SimulationBatchResult(results);
        await File.WriteAllTextAsync(
            Path.Combine(outputDirectory, "batch-summary.txt"),
            SimulationStatisticsFormatter.FormatBatchArtifact(batchResult),
            cancellationToken);
        return batchResult;
    }

    private static string FormatSummary(SimulationRunResult result)
    {
        var lines = new List<string>
        {
            $"scenario: {result.ScenarioName}",
            $"round: {result.Round}",
            $"seed: {result.Seed}",
            $"success: {result.Success}",
            $"summary: {result.Summary}",
            $"reproduction: {result.ReproductionHint}",
        };

        if (result.AcceptorSafetyMonitor is not null)
        {
            lines.Add($"acceptor-safety-monitor: commits={result.AcceptorSafetyMonitor.Commits.Count}");
        }

        if (!string.IsNullOrWhiteSpace(result.FailureMessage))
        {
            lines.Add($"failure: {result.FailureMessage}");
        }

        if (!string.IsNullOrWhiteSpace(result.FailureDiagnostics))
        {
            lines.Add(string.Empty);
            lines.Add("diagnostics:");
            lines.Add(result.FailureDiagnostics);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private readonly record struct PlannedSimulationRun(
        int Index,
        SimulationScenarioDefinition Scenario,
        int Round,
        int Seed);

    private static string SanitizePathSegment(string value)
    {
        var invalidCharacters = Path.GetInvalidFileNameChars();
        var buffer = value.Select(character => invalidCharacters.Contains(character) ? '_' : character).ToArray();
        return new string(buffer);
    }
}
