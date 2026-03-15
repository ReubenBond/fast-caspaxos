using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Invariants;
using FastCASPaxos.Simulation.Nodes;
using FastCASPaxos.Simulation.Transport;
using Microsoft.Extensions.Logging;
using System.Runtime.ExceptionServices;
using System.Text;

namespace FastCASPaxos.Simulation;

/// <summary>
/// Fast CASPaxos-specific simulation harness built on top of <see cref="SimulationCluster{TNode}"/>.
/// It wires proposer and acceptor hosts into Clockwork's deterministic scheduler, network, and diagnostics.
/// </summary>
public sealed class FastCasSimulationCluster<TValue> : SimulationCluster<FastCasSimulationNode<TValue>>
    where TValue : Contracts.IVersionedValue<TValue>
{
    private readonly Dictionary<int, RoutedProposeResponse<TValue, FastCasAddress>> _clientResponses = [];
    private readonly HashSet<FastCasAddress> _clientAddresses = [];
    private readonly List<FastCasSimulationNode<TValue>> _proposerNodes = [];
    private readonly List<FastCasSimulationNode<TValue>> _acceptorNodes = [];
    private readonly List<string> _faultHistory = [];
    private int _nextClientRequestId;

    public FastCasSimulationCluster(
        int seed,
        FastCasSimulationOptions? options = null,
        DateTimeOffset? startDateTime = null,
        CancellationToken cancellationToken = default)
        : base(seed, startDateTime, cancellationToken)
    {
        Options = options ?? new FastCasSimulationOptions();
        Transport = new FastCasSimulationTransport<TValue>(
            new SimulationNetwork(() => Nodes, CreateDerivedRandom()),
            Options.TransportObserver);
        if (Options.LoggerFactory is { } loggerFactory)
        {
            Transport.Network.SetLogger(loggerFactory.CreateLogger(typeof(SimulationNetwork).FullName!));
        }

        ClientAddress = FastCasAddress.Client();
        RegisterClientEndpoint(ClientAddress);
        AcceptorSafetyMonitor = new FastCasAcceptorSafetyMonitor<TValue>();
        RefreshLatencyAwareProposerTiming();
    }

    public FastCasSimulationOptions Options { get; }

    public FastCasSimulationTransport<TValue> Transport { get; }

    public SimulationNetwork Network => Transport.Network;

    public FastCasAcceptorSafetyMonitor<TValue> AcceptorSafetyMonitor { get; }

    public FastCasAddress ClientAddress { get; }

    public IReadOnlyCollection<FastCasAddress> ClientAddresses => _clientAddresses;

    public IReadOnlyDictionary<int, RoutedProposeResponse<TValue, FastCasAddress>> ClientResponses => _clientResponses;

    public IReadOnlyList<FastCasSimulationNode<TValue>> ProposerNodes => _proposerNodes;

    public IReadOnlyList<FastCasSimulationNode<TValue>> AcceptorNodes => _acceptorNodes;

    public IReadOnlyList<string> FaultHistory => _faultHistory;

    public int AvailableAcceptorCount => _acceptorNodes.Count(node => node.IsRunning && !node.IsSuspended);

    public bool CanFormClassicQuorum() => 2 * AvailableAcceptorCount > _acceptorNodes.Count;

    public bool CanReachClassicQuorum(FastCasAddress proposer)
    {
        var reachableAcceptors = _acceptorNodes.Count(node =>
            node.IsRunning
            && !node.IsSuspended
            && Network.CanDeliver(proposer.NetworkAddress, node.NetworkAddress)
            && Network.CanDeliver(node.NetworkAddress, proposer.NetworkAddress));

        return 2 * reachableAcceptors > _acceptorNodes.Count;
    }

    public FastCasSimulationNode<TValue> AddAcceptor(
        int ordinal,
        IFastCasAcceptorStateStore<TValue>? stateStore = null)
    {
        var nodeId = new FastCasNodeId(FastCasParticipantRole.Acceptor, ordinal);
        var host = new FastCasAcceptorHost<TValue>(nodeId, Transport, stateStore, OnAcceptorStateUpdated, Options);
        var node = new FastCasSimulationNode<TValue>(
            host,
            Clock,
            Guard,
            CreateDerivedRandom(),
            TaskQueue,
            Transport,
            CreateNodeLogger(nodeId));

        RegisterNode(node);
        _acceptorNodes.Add(node);
        return node;
    }

    public FastCasSimulationNode<TValue> AddProposer(
        int ordinal,
        IEnumerable<FastCasAddress>? acceptors = null)
    {
        var resolvedAcceptors = acceptors?.ToList() ?? [.. _acceptorNodes.Select(node => node.Host.Address)];
        if (resolvedAcceptors.Count == 0)
        {
            throw new InvalidOperationException("Add at least one acceptor before creating a proposer, or supply explicit acceptor addresses.");
        }

        var nodeId = new FastCasNodeId(FastCasParticipantRole.Proposer, ordinal);
        var host = new FastCasProposerHost<TValue>(nodeId, resolvedAcceptors, Transport, Options);
        var node = new FastCasSimulationNode<TValue>(
            host,
            Clock,
            Guard,
            CreateDerivedRandom(),
            TaskQueue,
            Transport,
            CreateNodeLogger(nodeId));
        host.BindSimulationContext(node.Context);

        RegisterNode(node);
        _proposerNodes.Add(node);
        RefreshCommittedValueBroadcasters();
        return node;
    }

    /// <summary>
    /// Creates a basic fully connected cluster configuration with the requested proposer and acceptor counts.
    /// </summary>
    public void CreateConfiguration(int proposerCount, int acceptorCount)
    {
        for (var acceptor = 1; acceptor <= acceptorCount; acceptor++)
        {
            AddAcceptor(acceptor);
        }

        for (var proposer = 1; proposer <= proposerCount; proposer++)
        {
            AddProposer(proposer);
        }
    }

    public FastCasAddress RegisterClient(int ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ordinal, 0);
        var address = FastCasAddress.Client(ordinal);
        RegisterClientEndpoint(address);
        return address;
    }

    /// <summary>
    /// Sends a client proposal into the simulated cluster without waiting for completion.
    /// </summary>
    public int SendProposal(
        FastCasAddress proposer,
        IOperation<TValue> operation,
        int? requestId = null,
        FastCasAddress? caller = null)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var routedCaller = (operation as IRoutedOperation<FastCasAddress>)?.Caller;
        if (caller is { } explicitCaller
            && routedCaller is { } existingCaller
            && !EqualityComparer<FastCasAddress>.Default.Equals(explicitCaller, existingCaller))
        {
            throw new InvalidOperationException(
                $"Operation '{operation}' is already routed to caller '{existingCaller}', but SendProposal was asked to use caller '{explicitCaller}'.");
        }

        var effectiveCaller = caller ?? routedCaller ?? ClientAddress;
        var effectiveRequestId = requestId ?? ++_nextClientRequestId;
        EnsureClientEndpointRegistered(effectiveCaller);
        _clientResponses.Remove(effectiveRequestId);
        Options.HistoryObserver?.OnProposalSent(
            TimeProvider.GetUtcNow(),
            proposer,
            effectiveRequestId,
            operation);
        _ = Transport.Send(new FastCasProposeRequestMessage<TValue>(
            effectiveCaller,
            proposer,
            effectiveRequestId,
            operation));

        return effectiveRequestId;
    }

    public Ballot GetProposerBallot(FastCasAddress proposer) => GetProposerHost(proposer).Ballot;

    public Ballot? GetProposerPreparedBallot(FastCasAddress proposer) => GetProposerHost(proposer).PreparedBallot;

    public bool ProposerHasPendingClientRequests(FastCasAddress proposer) =>
        GetProposerHost(proposer).HasPendingClientRequests;

    public DeliveryStatus SendLearnCommittedValue(
        FastCasAddress sourceProposer,
        FastCasAddress proposer,
        Ballot acceptedBallot,
        TValue value,
        Ballot preparedBallot = default)
    {
        if (sourceProposer.Role != FastCasParticipantRole.Proposer)
        {
            throw new ArgumentException($"Address '{sourceProposer}' is not a proposer endpoint.", nameof(sourceProposer));
        }

        if (proposer.Role != FastCasParticipantRole.Proposer)
        {
            throw new ArgumentException($"Address '{proposer}' is not a proposer endpoint.", nameof(proposer));
        }

        var deliveryStatus = Transport.Send(new FastCasLearnCommittedValueMessage<TValue>(
            sourceProposer,
            proposer,
            acceptedBallot,
            value,
            preparedBallot));
        if (deliveryStatus == DeliveryStatus.Success)
        {
            Options.HistoryObserver?.OnLearnCommittedValue(
                TimeProvider.GetUtcNow(),
                sourceProposer,
                proposer,
                acceptedBallot,
                value,
                preparedBallot);
        }

        return deliveryStatus;
    }

    public bool TryGetRoutedResponse(
        int requestId,
        out RoutedProposeResponse<TValue, FastCasAddress> response) =>
        _clientResponses.TryGetValue(requestId, out response);

    public bool TryGetResponse(int requestId, out ProposeResponse<TValue> response)
    {
        if (_clientResponses.TryGetValue(requestId, out var routedResponse))
        {
            response = routedResponse.Response;
            return true;
        }

        response = default;
        return false;
    }

    /// <summary>
    /// Sends a proposal and drives the simulation until the client response arrives or progress stalls.
    /// </summary>
    public ProposeResponse<TValue> RunProposal(
        FastCasAddress proposer,
        IOperation<TValue> operation,
        int maxIterations = 100000)
    {
        var requestId = SendProposal(proposer, operation);
        if (!RunUntil(() => _clientResponses.ContainsKey(requestId), maxIterations))
        {
            throw new InvalidOperationException(
                $"Proposal {requestId} did not complete. quorumAvailable={CanFormClassicQuorum()}, reachableQuorumAvailable={CanReachClassicQuorum(proposer)}. {GetFailureDiagnostics(nameof(RunProposal))}");
        }

        return _clientResponses[requestId].Response;
    }

    public string GetReproductionHint(string scenarioName) => $"{scenarioName} (seed: {Seed})";

    protected override ValueTask DisposeAsyncCore()
    {
        foreach (var node in Nodes)
        {
            node.Unregister();
        }

        foreach (var clientAddress in _clientAddresses)
        {
            _ = Transport.UnregisterEndpoint(clientAddress);
        }

        return ValueTask.CompletedTask;
    }

    private void OnClientMessage(IFastCasTransportMessage<TValue> message)
    {
        switch (message)
        {
            case FastCasProposeResponseMessage<TValue> proposeResponse:
                if (_clientResponses.ContainsKey(proposeResponse.RequestId))
                {
                    break;
                }

                _clientResponses[proposeResponse.RequestId] = new RoutedProposeResponse<TValue, FastCasAddress>(
                    proposeResponse.From,
                    proposeResponse.Payload);
                Options.HistoryObserver?.OnProposalCompleted(
                    TimeProvider.GetUtcNow(),
                    proposeResponse.From,
                    proposeResponse.RequestId,
                    proposeResponse.Payload);
                break;
            default:
                throw new InvalidOperationException($"Unexpected client-directed simulation message type '{message.GetType().Name}'.");
        }
    }

    private void RegisterClientEndpoint(FastCasAddress address)
    {
        if (address.Role != FastCasParticipantRole.Client)
        {
            throw new ArgumentException($"Address '{address}' is not a client endpoint.", nameof(address));
        }

        if (!_clientAddresses.Add(address))
        {
            return;
        }

        Transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TValue>(
            address,
            TaskQueue,
            OnClientMessage));
    }

    private void EnsureClientEndpointRegistered(FastCasAddress address)
    {
        if (address.Role == FastCasParticipantRole.Client)
        {
            RegisterClientEndpoint(address);
        }
    }

    private void RefreshCommittedValueBroadcasters()
    {
        foreach (var proposerNode in _proposerNodes)
        {
            if (proposerNode.Host is FastCasProposerHost<TValue> proposerHost)
            {
                proposerHost.ConfigureCommittedValueBroadcaster(BroadcastCommittedValueToPeerProposers);
            }
        }
    }

    private void BroadcastCommittedValueToPeerProposers(
        FastCasAddress sourceProposer,
        Ballot acceptedBallot,
        TValue value,
        Ballot preparedBallot)
    {
        foreach (var proposerNode in _proposerNodes)
        {
            var targetProposer = proposerNode.Host.Address;
            if (targetProposer == sourceProposer
                || !proposerNode.IsRunning
                || proposerNode.IsSuspended)
            {
                continue;
            }

            _ = SendLearnCommittedValue(
                sourceProposer,
                targetProposer,
                acceptedBallot,
                value,
                preparedBallot);
        }
    }

    private FastCasProposerHost<TValue> GetProposerHost(FastCasAddress proposer)
    {
        var node = GetNode(proposer);
        return node.Host as FastCasProposerHost<TValue>
            ?? throw new InvalidOperationException($"Expected proposer host type for '{proposer}'.");
    }

    /// <summary>
    /// Describes the current simulated cluster state, including reachability, queued work, node-local state, and recent faults.
    /// </summary>
    public string DescribeState()
    {
        var builder = new StringBuilder();
        builder.AppendLine($"time: {TimeProvider.GetUtcNow():O}");
        builder.AppendLine($"responses: {_clientResponses.Count}");
        builder.AppendLine($"network: dropRate={Network.MessageDropRate:P0}, delays={Network.EnableDelays}, baseDelay={Network.BaseMessageDelay}, maxJitter={Network.MaxJitter}");
        var linkLatencies = Network.GetAllLinkLatencies();
        if (linkLatencies.Count > 0)
        {
            builder.AppendLine($"link-latencies: {linkLatencies.Count} override(s)");
            foreach (var ((source, target), latency) in linkLatencies.OrderBy(entry => entry.Key.Source, StringComparer.Ordinal).ThenBy(entry => entry.Key.Target, StringComparer.Ordinal))
            {
                builder.AppendLine($"  {source} -> {target}: base={latency.BaseDelay}, jitter={latency.MaxJitter}");
            }
        }
        builder.AppendLine($"classicQuorumAvailable: {CanFormClassicQuorum()}");
        foreach (var proposer in _proposerNodes)
        {
            builder.AppendLine($"reachableQuorum[{proposer.Host.Address}]={CanReachClassicQuorum(proposer.Host.Address)}");
        }
        builder.AppendLine($"clusterQueue: scheduled={TaskQueue.ScheduledItems.Count}");
        builder.AppendLine("nodes:");

        foreach (var node in Nodes.OrderBy(node => node.NetworkAddress))
        {
            builder.Append($"  - {node.NetworkAddress}: running={node.IsRunning}, suspended={node.IsSuspended}, queued={node.Context.TaskQueue.ScheduledItems.Count}");
            switch (node.Host)
            {
                case FastCasProposerHost<TValue> proposer:
                    builder.Append($", cached={proposer.CachedValue}");
                    break;
                case FastCasAcceptorHost<TValue> acceptor:
                    var state = acceptor.State;
                    builder.Append($", promised={state.PromisedBallot}, accepted={state.AcceptedBallot}, value={state.AcceptedValue}");
                    break;
            }

            builder.AppendLine();
        }

        if (_faultHistory.Count > 0)
        {
            builder.AppendLine("fault-history:");
            foreach (var fault in _faultHistory.TakeLast(10))
            {
                builder.AppendLine($"  - {fault}");
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Returns a reproduction hint plus a cluster snapshot suitable for failure messages.
    /// </summary>
    public string GetFailureDiagnostics(string scenarioName) =>
        $"{GetReproductionHint(scenarioName)}{Environment.NewLine}{DescribeState()}";

    /// <summary>
    /// Verifies the cross-proposer safety invariant against the responses observed so far.
    /// </summary>
    public void AssertSafetyInvariants()
    {
        // Build per-proposer histories from client responses (commit history is no longer
        // tracked on the proposer itself — it's now observable via DiagnosticSource events).
        Dictionary<FastCasAddress, IReadOnlyList<TValue>> commitHistories = [];
        foreach (var responseGroup in _clientResponses.Values.GroupBy(r => r.Proposer))
        {
            var values = new List<TValue>();
            foreach (var response in responseGroup.OrderBy(r => r.Round))
            {
                var v = response.CommittedValue;
                if (v is null || EqualityComparer<TValue>.Default.Equals(v, default!))
                {
                    continue;
                }

                if (values.Count == 0 || !EqualityComparer<TValue>.Default.Equals(values[^1], v))
                {
                    values.Add(v);
                }
            }

            commitHistories[responseGroup.Key] = values;
        }

        FastCasSafetyInvariantChecker.AssertSafety(commitHistories, _clientResponses.Values);
    }

    private void OnAcceptorStateUpdated(FastCasAddress updatedAcceptor)
    {
        var timestamp = TimeProvider.GetUtcNow();
        var snapshots = CreateAcceptorStateSnapshots();

        ExceptionDispatchInfo? failure = null;
        try
        {
            AcceptorSafetyMonitor.Observe(timestamp, updatedAcceptor, snapshots);
        }
        catch (Exception ex)
        {
            failure = ExceptionDispatchInfo.Capture(ex);
        }

        Options.AcceptorStateObserver?.OnAcceptorStateObserved(timestamp, updatedAcceptor, AcceptorSafetyMonitor);
        failure?.Throw();
    }

    private IReadOnlyList<FastCasAcceptorStateSnapshot<TValue>> CreateAcceptorStateSnapshots() =>
        [.. _acceptorNodes
            .OrderBy(node => node.Host.Address.ToString(), StringComparer.Ordinal)
            .Select(node =>
            {
                var host = (FastCasAcceptorHost<TValue>)node.Host;
                var state = host.State;
                return new FastCasAcceptorStateSnapshot<TValue>(
                    Acceptor: host.Address,
                    IsRunning: node.IsRunning,
                    IsSuspended: node.IsSuspended,
                    PromisedBallot: state.PromisedBallot,
                    AcceptedBallot: state.AcceptedBallot,
                    AcceptedValue: state.AcceptedValue);
            })];

    public void CreatePartition(FastCasAddress source, FastCasAddress target)
    {
        Network.CreatePartition(source.NetworkAddress, target.NetworkAddress);
        RecordFault($"partition {source} -> {target}");
    }

    public void CreateBidirectionalPartition(FastCasAddress left, FastCasAddress right)
    {
        Network.CreateBidirectionalPartition(left.NetworkAddress, right.NetworkAddress);
        RecordFault($"partition {left} <-> {right}");
    }

    public void HealPartition(FastCasAddress source, FastCasAddress target)
    {
        Network.HealPartition(source.NetworkAddress, target.NetworkAddress);
        RecordFault($"heal {source} -> {target}");
    }

    public void HealBidirectionalPartition(FastCasAddress left, FastCasAddress right)
    {
        Network.HealBidirectionalPartition(left.NetworkAddress, right.NetworkAddress);
        RecordFault($"heal {left} <-> {right}");
    }

    public void IsolateNode(FastCasAddress address)
    {
        var node = GetNode(address);
        Network.IsolateNode(node.NetworkAddress);
        RecordFault($"isolate {address}");
    }

    public void ReconnectNode(FastCasAddress address)
    {
        var node = GetNode(address);
        Network.ReconnectNode(node.NetworkAddress);
        RecordFault($"reconnect {address}");
    }

    public void SetMessageDropRate(double dropRate)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(dropRate, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(dropRate, 1);

        Network.MessageDropRate = dropRate;
        RecordFault($"drop-rate {dropRate:P0}");
    }

    public void ConfigureMessageDelays(TimeSpan baseDelay, TimeSpan maxJitter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(baseDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxJitter, TimeSpan.Zero);

        Network.EnableDelays = baseDelay > TimeSpan.Zero || maxJitter > TimeSpan.Zero;
        Network.BaseMessageDelay = baseDelay;
        Network.MaxJitter = maxJitter;
        RefreshLatencyAwareProposerTiming();
        RecordFault($"delays base={baseDelay} jitter={maxJitter}");
    }

    public void ConfigureLinkLatency(FastCasAddress source, FastCasAddress target, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        Network.EnableDelays = true;
        Network.SetLinkLatency(source.NetworkAddress, target.NetworkAddress, baseDelay, maxJitter);
        RefreshLatencyAwareProposerTiming();
        RecordFault($"link-latency {source} -> {target} base={baseDelay} jitter={maxJitter}");
    }

    public void ConfigureBidirectionalLinkLatency(FastCasAddress node1, FastCasAddress node2, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        Network.EnableDelays = true;
        Network.SetBidirectionalLinkLatency(node1.NetworkAddress, node2.NetworkAddress, baseDelay, maxJitter);
        RefreshLatencyAwareProposerTiming();
        RecordFault($"link-latency {node1} <-> {node2} base={baseDelay} jitter={maxJitter}");
    }

    public void ClearBidirectionalLinkLatency(FastCasAddress node1, FastCasAddress node2)
    {
        Network.ClearLinkLatency(node1.NetworkAddress, node2.NetworkAddress);
        Network.ClearLinkLatency(node2.NetworkAddress, node1.NetworkAddress);
        RefreshLatencyAwareProposerTiming();
    }

    public void ClearLinkLatencies()
    {
        Network.ClearAllLinkLatencies();
        RefreshLatencyAwareProposerTiming();
        RecordFault("link-latencies cleared");
    }

    private void RefreshLatencyAwareProposerTiming()
    {
        var expectedOneWayLatency = Network.EnableDelays
            ? Network.BaseMessageDelay + Network.MaxJitter
            : TimeSpan.Zero;

        foreach (var latency in Network.GetAllLinkLatencies().Values)
        {
            var candidate = latency.BaseDelay + latency.MaxJitter;
            if (candidate > expectedOneWayLatency)
            {
                expectedOneWayLatency = candidate;
            }
        }

        Options.ApplyLatencyAwareProposerTiming(expectedOneWayLatency);
    }

    public void CrashNode(FastCasAddress address)
    {
        var node = GetNode(address);
        node.Crash();
        RecordFault($"crash {address}");
    }

    public void RestartNode(FastCasAddress address)
    {
        var node = GetNode(address);
        node.Restart();
        RecordFault($"restart {address}");
    }

    public void SuspendNode(FastCasAddress address)
    {
        var node = GetNode(address);
        node.Suspend();
        RecordFault($"suspend {address}");
    }

    public void ResumeNode(FastCasAddress address)
    {
        var node = GetNode(address);
        node.Resume();
        RecordFault($"resume {address}");
    }

    private FastCasSimulationNode<TValue> GetNode(FastCasAddress address)
    {
        var node = Nodes.FirstOrDefault(candidate => candidate.Host.Address == address);
        return node ?? throw new InvalidOperationException($"Unknown FastCASPaxos node '{address}'.");
    }

    private void RecordFault(string description) =>
        _faultHistory.Add($"{TimeProvider.GetUtcNow():O} {description}");

    private ILogger? CreateNodeLogger(FastCasNodeId nodeId)
    {
        if (Options.LoggerFactory is not { } loggerFactory)
        {
            return null;
        }

        var prefixedFactory = new NodePrefixedLoggerFactory(loggerFactory, nodeId.Address.ToString());
        return prefixedFactory.CreateLogger(typeof(FastCasSimulationNode<TValue>).FullName!);
    }
}
