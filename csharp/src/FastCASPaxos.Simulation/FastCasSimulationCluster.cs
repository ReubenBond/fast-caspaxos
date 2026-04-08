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
    /// <summary>
    /// Tiny non-zero delay used for collocated links so the simulation engine
    /// schedules delivery on a future tick rather than executing synchronously
    /// in the current tick (which would alter global event ordering).
    /// </summary>
    private static readonly TimeSpan CollocatedLinkDelay = TimeSpan.FromMicroseconds(1);

    private readonly Dictionary<int, RoutedProposeResponse<TValue, FastCasAddress>> _clientResponses = [];
    private readonly HashSet<FastCasAddress> _clientAddresses = [];
    private readonly Dictionary<FastCasAddress, FastCasAddress> _collocatedProposerClients = [];
    private readonly Dictionary<int, FastCasConsensusRegister<TValue>> _consensusRegisters = [];
    private readonly List<FastCasSimulationNode<TValue>> _proposerNodes = [];
    private readonly List<FastCasSimulationNode<TValue>> _acceptorNodes = [];
    private readonly List<string> _faultHistory = [];
    private int _nextClientRequestId;

    public FastCasSimulationCluster(
        int seed,
        TValue initialValue,
        FastCasSimulationOptions? options = null,
        DateTimeOffset? startDateTime = null,
        CancellationToken cancellationToken = default)
        : base(seed, startDateTime, cancellationToken)
    {
        InitialValue = initialValue;
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
        RefreshExpectedLatency();
    }

    public FastCasSimulationOptions Options { get; }

    public TValue InitialValue { get; }

    public FastCasSimulationTransport<TValue> Transport { get; }

    public SimulationNetwork Network => Transport.Network;

    public FastCasAcceptorSafetyMonitor<TValue> AcceptorSafetyMonitor { get; }

    public FastCasAddress ClientAddress { get; }

    public IReadOnlyCollection<FastCasAddress> ClientAddresses => _clientAddresses;

    public IReadOnlyDictionary<int, RoutedProposeResponse<TValue, FastCasAddress>> ClientResponses => _clientResponses;

    public IReadOnlyList<FastCasSimulationNode<TValue>> ProposerNodes => _proposerNodes;

    public IReadOnlyList<FastCasSimulationNode<TValue>> AcceptorNodes => _acceptorNodes;

    public IReadOnlyDictionary<int, FastCasConsensusRegister<TValue>> ConsensusRegisters => _consensusRegisters;

    public IReadOnlyList<string> FaultHistory => _faultHistory;

    public IReadOnlyDictionary<FastCasAddress, FastCasAddress> CollocatedProposerClients => _collocatedProposerClients;

    public int AvailableAcceptorCount => _acceptorNodes.Count(node => node.IsRunning && !node.IsSuspended);

    public FastCasConsensusRegister<TValue> GetConsensusRegister(int ordinal) =>
        _consensusRegisters.TryGetValue(ordinal, out var register)
            ? register
            : throw new InvalidOperationException($"Unknown FastCASPaxos consensus register ordinal '{ordinal}'.");

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
        var host = new FastCasAcceptorHost<TValue>(nodeId, Transport, InitialValue, stateStore, OnAcceptorStateUpdated, Options);
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
        ApplyCollocatedLinkLatencyFor(node.Host.Address);
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
        var host = new FastCasProposerHost<TValue>(nodeId, resolvedAcceptors, Transport, InitialValue, Options);
        var node = new FastCasSimulationNode<TValue>(
            host,
            Clock,
            Guard,
            CreateDerivedRandom(),
            TaskQueue,
            Transport,
            CreateNodeLogger(nodeId));

        RegisterNode(node);
        _proposerNodes.Add(node);
        ApplyCollocatedLinkLatencyFor(node.Host.Address);
        RegisterCollocatedClient(node.Host.Address);
        RefreshCommittedValueBroadcasters();
        return node;
    }

    public FastCasConsensusRegister<TValue> AddConsensusRegister(
        int ordinal,
        IEnumerable<FastCasAddress>? acceptors = null,
        IFastCasAcceptorStateStore<TValue>? stateStore = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ordinal, 1);
        if (_consensusRegisters.ContainsKey(ordinal))
        {
            throw new InvalidOperationException($"A consensus register for ordinal {ordinal} already exists.");
        }

        var proposerAddress = FastCasAddress.Proposer(ordinal);
        var acceptorAddress = FastCasAddress.Acceptor(ordinal);
        EnsureRoleAddressAvailable(proposerAddress);
        EnsureRoleAddressAvailable(acceptorAddress);

        var resolvedAcceptors = acceptors?.Distinct().ToList() ?? [.. _acceptorNodes.Select(node => node.Host.Address)];
        if (!resolvedAcceptors.Contains(acceptorAddress))
        {
            resolvedAcceptors.Add(acceptorAddress);
        }

        var register = new FastCasConsensusRegister<TValue>(
            ordinal,
            resolvedAcceptors,
            Transport,
            InitialValue,
            stateStore,
            OnAcceptorStateUpdated,
            Options);

        var acceptorNode = new FastCasSimulationNode<TValue>(
            register.AcceptorEndpoint,
            Clock,
            Guard,
            CreateDerivedRandom(),
            TaskQueue,
            Transport,
            CreateNodeLogger(register.AcceptorHost.NodeId));
        var proposerNode = new FastCasSimulationNode<TValue>(
            register.ProposerEndpoint,
            Clock,
            Guard,
            CreateDerivedRandom(),
            TaskQueue,
            Transport,
            CreateNodeLogger(register.ProposerHost.NodeId));

        RegisterNode(acceptorNode);
        _acceptorNodes.Add(acceptorNode);
        RegisterNode(proposerNode);
        _proposerNodes.Add(proposerNode);
        _consensusRegisters.Add(ordinal, register);

        ApplyCollocatedLinkLatencyFor(acceptorNode.Host.Address);
        ApplyCollocatedLinkLatencyFor(proposerNode.Host.Address);
        RegisterCollocatedClient(proposerNode.Host.Address);
        RefreshCommittedValueBroadcasters();
        return register;
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

    public void CreateConsensusRegisterConfiguration(int registerCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(registerCount, 1);
        var acceptorAddresses = Enumerable.Range(1, registerCount).Select(FastCasAddress.Acceptor).ToList();
        for (var ordinal = 1; ordinal <= registerCount; ordinal++)
        {
            AddConsensusRegister(ordinal, acceptorAddresses);
        }
    }

    private void ApplyCollocatedLinkLatencyFor(FastCasAddress address)
    {
        switch (address.Role)
        {
            case FastCasParticipantRole.Proposer:
                if (_acceptorNodes.FirstOrDefault(node => node.Host.Address.Ordinal == address.Ordinal) is { } acceptorNode)
                {
                    ApplyCollocatedLinkLatency(address, acceptorNode.Host.Address);
                }
                if (_collocatedProposerClients.TryGetValue(address, out var clientAddress))
                {
                    ApplyCollocatedClientLinkLatency(clientAddress, address);
                }
                break;
            case FastCasParticipantRole.Acceptor:
                if (_proposerNodes.FirstOrDefault(node => node.Host.Address.Ordinal == address.Ordinal) is { } proposerNode)
                {
                    ApplyCollocatedLinkLatency(proposerNode.Host.Address, address);
                }
                break;
        }
    }

    private void ApplyAllCollocatedLinkLatencies()
    {
        foreach (var proposerNode in _proposerNodes)
        {
            ApplyCollocatedLinkLatencyFor(proposerNode.Host.Address);
        }
    }

    private void ApplyCollocatedLinkLatency(FastCasAddress proposer, FastCasAddress acceptor) =>
        Network.SetBidirectionalLinkLatency(
            proposer.NetworkAddress,
            acceptor.NetworkAddress,
            CollocatedLinkDelay,
            TimeSpan.Zero);

    private void ApplyCollocatedClientLinkLatency(FastCasAddress client, FastCasAddress proposer) =>
        Network.SetBidirectionalLinkLatency(
            client.NetworkAddress,
            proposer.NetworkAddress,
            CollocatedLinkDelay,
            TimeSpan.Zero);

    private void RegisterCollocatedClient(FastCasAddress proposer)
    {
        var client = RegisterClient(proposer.Ordinal);
        _collocatedProposerClients[proposer] = client;
        ApplyCollocatedClientLinkLatency(client, proposer);
    }

    /// <summary>
    /// Returns the collocated client address for the given proposer, or <c>null</c> if none exists.
    /// </summary>
    public FastCasAddress? GetCollocatedClient(FastCasAddress proposer) =>
        _collocatedProposerClients.TryGetValue(proposer, out var client) ? client : null;

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
            if (proposerNode.Host is IFastCasProposerNodeHost<TValue> proposerHost)
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

    private IFastCasProposerNodeHost<TValue> GetProposerHost(FastCasAddress proposer)
    {
        var node = GetNode(proposer);
        return node.Host as IFastCasProposerNodeHost<TValue>
            ?? throw new InvalidOperationException($"Expected proposer host type for '{proposer}'.");
    }

    private void EnsureRoleAddressAvailable(FastCasAddress address)
    {
        if (_proposerNodes.Any(node => node.Host.Address == address)
            || _acceptorNodes.Any(node => node.Host.Address == address))
        {
            throw new InvalidOperationException($"A FastCASPaxos node is already registered for '{address}'.");
        }
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
                case IFastCasProposerNodeHost<TValue> proposer:
                    builder.Append($", cached={proposer.CachedValue}");
                    break;
                case IFastCasAcceptorNodeHost<TValue> acceptor:
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
                var host = node.Host as IFastCasAcceptorNodeHost<TValue>
                    ?? throw new InvalidOperationException($"Expected acceptor host type for '{node.Host.Address}'.");
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
        RefreshExpectedLatency();
        RecordFault($"delays base={baseDelay} jitter={maxJitter}");
    }

    public void ConfigureLinkLatency(FastCasAddress source, FastCasAddress target, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        var latency = NormalizeLinkLatency(source, target, baseDelay, maxJitter);
        Network.EnableDelays = Network.EnableDelays
            || latency.BaseDelay > TimeSpan.Zero
            || latency.MaxJitter > TimeSpan.Zero;
        Network.SetLinkLatency(source.NetworkAddress, target.NetworkAddress, latency.BaseDelay, latency.MaxJitter);
        RefreshExpectedLatency();
        RecordFault($"link-latency {source} -> {target} base={latency.BaseDelay} jitter={latency.MaxJitter}");
    }

    public void ConfigureBidirectionalLinkLatency(FastCasAddress node1, FastCasAddress node2, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        var latency = NormalizeLinkLatency(node1, node2, baseDelay, maxJitter);
        Network.EnableDelays = Network.EnableDelays
            || latency.BaseDelay > TimeSpan.Zero
            || latency.MaxJitter > TimeSpan.Zero;
        Network.SetBidirectionalLinkLatency(node1.NetworkAddress, node2.NetworkAddress, latency.BaseDelay, latency.MaxJitter);
        RefreshExpectedLatency();
        RecordFault($"link-latency {node1} <-> {node2} base={latency.BaseDelay} jitter={latency.MaxJitter}");
    }

    public void ClearBidirectionalLinkLatency(FastCasAddress node1, FastCasAddress node2)
    {
        Network.ClearLinkLatency(node1.NetworkAddress, node2.NetworkAddress);
        Network.ClearLinkLatency(node2.NetworkAddress, node1.NetworkAddress);
        if (TryGetCollocatedPair(node1, node2, out var proposer, out var acceptor))
        {
            ApplyCollocatedLinkLatency(proposer, acceptor);
        }
        if (IsCollocatedClientProposerPair(node1, node2))
        {
            var client = node1.Role == FastCasParticipantRole.Client ? node1 : node2;
            var prop = node1.Role == FastCasParticipantRole.Proposer ? node1 : node2;
            ApplyCollocatedClientLinkLatency(client, prop);
        }
        RefreshExpectedLatency();
    }

    public void ClearLinkLatencies()
    {
        Network.ClearAllLinkLatencies();
        ApplyAllCollocatedLinkLatencies();
        RefreshExpectedLatency();
        RecordFault("link-latencies cleared");
    }

    private void RefreshExpectedLatency()
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

        Options.UpdateExpectedLatency(expectedOneWayLatency);
    }

    private static (TimeSpan BaseDelay, TimeSpan MaxJitter) NormalizeLinkLatency(
        FastCasAddress source,
        FastCasAddress target,
        TimeSpan baseDelay,
        TimeSpan maxJitter) =>
        TryGetCollocatedPair(source, target, out _, out _) || IsCollocatedClientProposerPair(source, target)
            ? (CollocatedLinkDelay, TimeSpan.Zero)
            : (baseDelay, maxJitter);

    private static bool IsCollocatedClientProposerPair(FastCasAddress a, FastCasAddress b) =>
        a.Ordinal == b.Ordinal
        && a.Ordinal > 0
        && ((a.Role == FastCasParticipantRole.Client && b.Role == FastCasParticipantRole.Proposer)
            || (a.Role == FastCasParticipantRole.Proposer && b.Role == FastCasParticipantRole.Client));

    private static bool TryGetCollocatedPair(
        FastCasAddress node1,
        FastCasAddress node2,
        out FastCasAddress proposer,
        out FastCasAddress acceptor)
    {
        if (node1.Ordinal == node2.Ordinal)
        {
            if (node1.Role == FastCasParticipantRole.Proposer && node2.Role == FastCasParticipantRole.Acceptor)
            {
                proposer = node1;
                acceptor = node2;
                return true;
            }

            if (node1.Role == FastCasParticipantRole.Acceptor && node2.Role == FastCasParticipantRole.Proposer)
            {
                proposer = node2;
                acceptor = node1;
                return true;
            }
        }

        proposer = default;
        acceptor = default;
        return false;
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

    /// <summary>
    /// Resets every proposer and acceptor to pristine state, as if the cluster were managing a
    /// brand-new register that has never been written to. The acceptor safety monitor is also
    /// cleared so that subsequent commits are validated independently of earlier history.
    /// </summary>
    public void ReinitializeForNewRegister()
    {
        foreach (var proposerNode in _proposerNodes)
        {
            proposerNode.Restart();
        }

        foreach (var acceptorNode in _acceptorNodes)
        {
            if (acceptorNode.Host is IFastCasAcceptorNodeHost<TValue> acceptorHost)
            {
                acceptorHost.ReinitializeAsFreshRegister();
            }
        }

        AcceptorSafetyMonitor.Reset();
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
