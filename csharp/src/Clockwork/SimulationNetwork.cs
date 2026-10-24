using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Clockwork;

/// <summary>
/// Result of checking whether a message can be delivered.
/// </summary>
public enum DeliveryStatus
{
    /// <summary>Message can be delivered normally.</summary>
    Success,

    /// <summary>Message was randomly dropped (transient failure, should retry).</summary>
    Dropped,

    /// <summary>Message blocked by network partition (persistent failure).</summary>
    Partitioned,
}

/// <summary>
/// Represents the latency parameters for a specific network link between two nodes.
/// </summary>
/// <param name="BaseDelay">The base (minimum) message delay for this link.</param>
/// <param name="MaxJitter">The maximum additional random delay for this link.</param>
public readonly record struct LinkLatency(TimeSpan BaseDelay, TimeSpan MaxJitter);

/// <summary>
/// Simulation network for in-memory transport between nodes.
/// Provides hooks for injecting network faults like delays, partitions, and message loss.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public partial class SimulationNetwork
{
    private readonly Func<ImmutableArray<SimulationNode>> _getNodes;
    private readonly SimulationRandom _random;
    private readonly ConcurrentDictionary<string, HashSet<string>> _partitions = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Source, string Target), LinkLatency> _linkLatencies = [];
    private readonly Lock _lock = new();
    private ILogger _logger;

    /// <summary>
    /// Gets or sets the base message delay for all messages.
    /// </summary>
    public TimeSpan BaseMessageDelay { get; set; } = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Gets or sets the maximum additional random delay for messages.
    /// </summary>
    public TimeSpan MaxJitter { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>
    /// Gets or sets the probability of a message being dropped (0.0 to 1.0).
    /// </summary>
    public double MessageDropRate { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether gets or sets whether to simulate message delays.
    /// </summary>
    public bool EnableDelays { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SimulationNetwork"/> class.
    /// Creates a new simulation network.
    /// </summary>
    /// <param name="getNodes">Function to get the list of nodes in the simulation.</param>
    /// <param name="random">The deterministic random number generator.</param>
    public SimulationNetwork(Func<ImmutableArray<SimulationNode>> getNodes, SimulationRandom random)
    {
        ArgumentNullException.ThrowIfNull(getNodes);
        ArgumentNullException.ThrowIfNull(random);
        _getNodes = getNodes;
        _random = random;
        _logger = NullLogger.Instance;
    }

    /// <summary>
    /// Sets the logger for network operations.
    /// </summary>
    public void SetLogger(ILogger logger) => _logger = logger ?? NullLogger.Instance;

    /// <summary>
    /// Creates a network partition between two nodes (unidirectional).
    /// Messages from sourceAddress will not reach targetAddress.
    /// </summary>
    public void CreatePartition(string sourceAddress, string targetAddress)
    {
        lock (_lock)
        {
            var blocked = _partitions.GetOrAdd(sourceAddress, _ => []);
            blocked.Add(targetAddress);
        }

        OnPartitionCreated(sourceAddress, targetAddress);
    }

    /// <summary>
    /// Creates a bidirectional network partition between two nodes.
    /// </summary>
    public void CreateBidirectionalPartition(string node1, string node2)
    {
        OnBidirectionalPartitionCreating(node1, node2);
        CreatePartition(node1, node2);
        CreatePartition(node2, node1);
    }

    /// <summary>
    /// Removes a network partition between two nodes (unidirectional).
    /// </summary>
    public void HealPartition(string sourceAddress, string targetAddress)
    {
        lock (_lock)
        {
            if (_partitions.TryGetValue(sourceAddress, out var blocked))
            {
                blocked.Remove(targetAddress);
            }
        }

        OnPartitionHealed(sourceAddress, targetAddress);
    }

    /// <summary>
    /// Removes a bidirectional network partition between two nodes.
    /// </summary>
    public void HealBidirectionalPartition(string node1, string node2)
    {
        OnBidirectionalPartitionHealing(node1, node2);
        HealPartition(node1, node2);
        HealPartition(node2, node1);
    }

    /// <summary>
    /// Removes all network partitions.
    /// </summary>
    public void HealAllPartitions()
    {
        int count;
        lock (_lock)
        {
            count = _partitions.Count;
            _partitions.Clear();
        }

        OnAllPartitionsHealed(count);
    }

    /// <summary>
    /// Isolates a node from all other nodes (bidirectional).
    /// </summary>
    public void IsolateNode(string nodeAddress)
    {
        OnNodeIsolating(nodeAddress);
        foreach (var node in _getNodes())
        {
            var addr = node.NetworkAddress;
            if (!string.Equals(addr, nodeAddress, StringComparison.Ordinal))
            {
                CreateBidirectionalPartition(nodeAddress, addr);
            }
        }
    }

    /// <summary>
    /// Removes isolation from a node.
    /// </summary>
    public void ReconnectNode(string nodeAddress)
    {
        OnNodeReconnecting(nodeAddress);
        foreach (var node in _getNodes())
        {
            var addr = node.NetworkAddress;
            if (!string.Equals(addr, nodeAddress, StringComparison.Ordinal))
            {
                HealBidirectionalPartition(nodeAddress, addr);
            }
        }
    }

    /// <summary>
    /// Checks if a node is isolated (has partitions with all other nodes).
    /// </summary>
    public bool IsNodeIsolated(string nodeAddress)
    {
        lock (_lock)
        {
            if (!_partitions.TryGetValue(nodeAddress, out var blocked))
            {
                return false;
            }

            // Check if node is partitioned from all other nodes
            foreach (var node in _getNodes())
            {
                var addr = node.NetworkAddress;
                if (!string.Equals(addr, nodeAddress, StringComparison.Ordinal) && !blocked.Contains(addr))
                {
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>
    /// Checks if a message can be delivered from source to target.
    /// Returns a status indicating success or reason for failure.
    /// </summary>
    public DeliveryStatus CheckDelivery(string sourceAddress, string targetAddress)
    {
        // Self-messages (loopback) are always delivered reliably.
        // In real networks, loopback communication doesn't go through the network.
        if (string.Equals(sourceAddress, targetAddress, StringComparison.Ordinal))
        {
            return DeliveryStatus.Success;
        }

        // Check for network partition first (persistent)
        lock (_lock)
        {
            if (_partitions.TryGetValue(sourceAddress, out var blocked) && blocked.Contains(targetAddress))
            {
                OnMessageBlockedByPartition(sourceAddress, targetAddress);
                return DeliveryStatus.Partitioned;
            }
        }

        // Check for random message drop (transient)
        if (MessageDropRate > 0 && _random.Chance(MessageDropRate))
        {
            OnMessageDroppedRandom(sourceAddress, targetAddress);
            return DeliveryStatus.Dropped;
        }

        return DeliveryStatus.Success;
    }

    /// <summary>
    /// Checks if a message can be delivered from source to target.
    /// This is a convenience method that returns true only if delivery would succeed.
    /// </summary>
    public bool CanDeliver(string sourceAddress, string targetAddress) => CheckDelivery(sourceAddress, targetAddress) == DeliveryStatus.Success;

    /// <summary>
    /// Gets the simulated delay for a message using global defaults.
    /// </summary>
    public TimeSpan GetMessageDelay()
    {
        if (!EnableDelays)
        {
            return TimeSpan.Zero;
        }

        var jitter = _random.NextTimeSpan(MaxJitter);
        return BaseMessageDelay + jitter;
    }

    /// <summary>
    /// Gets the simulated delay for a message between specific nodes.
    /// Uses per-link latency overrides when configured, otherwise falls back to global defaults.
    /// </summary>
    /// <param name="sourceAddress">The sending node's network address.</param>
    /// <param name="targetAddress">The receiving node's network address.</param>
    public TimeSpan GetMessageDelay(string sourceAddress, string targetAddress)
    {
        if (!EnableDelays)
        {
            return TimeSpan.Zero;
        }

        // Loopback is always instant
        if (string.Equals(sourceAddress, targetAddress, StringComparison.Ordinal))
        {
            return TimeSpan.Zero;
        }

        LinkLatency? linkLatency;
        lock (_lock)
        {
            linkLatency = _linkLatencies.TryGetValue((sourceAddress, targetAddress), out var ll) ? ll : null;
        }

        if (linkLatency is { } latency)
        {
            var jitter = latency.MaxJitter > TimeSpan.Zero ? _random.NextTimeSpan(latency.MaxJitter) : TimeSpan.Zero;
            return latency.BaseDelay + jitter;
        }

        return GetMessageDelay();
    }

    /// <summary>
    /// Sets a directional per-link latency override from <paramref name="sourceAddress"/> to <paramref name="targetAddress"/>.
    /// </summary>
    public void SetLinkLatency(string sourceAddress, string targetAddress, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(baseDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxJitter, TimeSpan.Zero);

        lock (_lock)
        {
            _linkLatencies[(sourceAddress, targetAddress)] = new LinkLatency(baseDelay, maxJitter);
        }

        OnLinkLatencySet(sourceAddress, targetAddress, baseDelay, maxJitter);
    }

    /// <summary>
    /// Sets a bidirectional per-link latency override between two nodes.
    /// </summary>
    public void SetBidirectionalLinkLatency(string node1, string node2, TimeSpan baseDelay, TimeSpan maxJitter)
    {
        SetLinkLatency(node1, node2, baseDelay, maxJitter);
        SetLinkLatency(node2, node1, baseDelay, maxJitter);
    }

    /// <summary>
    /// Removes a directional per-link latency override. Messages will revert to global defaults.
    /// </summary>
    /// <returns>True if an override was removed; false if none existed.</returns>
    public bool ClearLinkLatency(string sourceAddress, string targetAddress)
    {
        bool removed;
        lock (_lock)
        {
            removed = _linkLatencies.Remove((sourceAddress, targetAddress));
        }

        if (removed)
        {
            OnLinkLatencyCleared(sourceAddress, targetAddress);
        }

        return removed;
    }

    /// <summary>
    /// Removes all per-link latency overrides.
    /// </summary>
    public void ClearAllLinkLatencies()
    {
        int count;
        lock (_lock)
        {
            count = _linkLatencies.Count;
            _linkLatencies.Clear();
        }

        OnAllLinkLatenciesCleared(count);
    }

    /// <summary>
    /// Gets the per-link latency override for a specific directional link, if one exists.
    /// </summary>
    /// <returns>The <see cref="LinkLatency"/> override, or null if the link uses global defaults.</returns>
    public LinkLatency? GetLinkLatency(string sourceAddress, string targetAddress)
    {
        lock (_lock)
        {
            return _linkLatencies.TryGetValue((sourceAddress, targetAddress), out var latency) ? latency : null;
        }
    }

    /// <summary>
    /// Gets all configured per-link latency overrides.
    /// </summary>
    public IReadOnlyDictionary<(string Source, string Target), LinkLatency> GetAllLinkLatencies()
    {
        lock (_lock)
        {
            return new Dictionary<(string, string), LinkLatency>(_linkLatencies);
        }
    }


    /// <summary>Called when a unidirectional partition is created.</summary>
    protected virtual void OnPartitionCreated(string source, string target) => Log.PartitionCreated(_logger, source, target);

    /// <summary>Called when a bidirectional partition is about to be created.</summary>
    protected virtual void OnBidirectionalPartitionCreating(string node1, string node2) => Log.BidirectionalPartitionCreating(_logger, node1, node2);

    /// <summary>Called when a unidirectional partition is healed.</summary>
    protected virtual void OnPartitionHealed(string source, string target) => Log.PartitionHealed(_logger, source, target);

    /// <summary>Called when a bidirectional partition is about to be healed.</summary>
    protected virtual void OnBidirectionalPartitionHealing(string node1, string node2) => Log.BidirectionalPartitionHealing(_logger, node1, node2);

    /// <summary>Called when all partitions are healed.</summary>
    protected virtual void OnAllPartitionsHealed(int count) => Log.AllPartitionsHealed(_logger, count);

    /// <summary>Called when a node is about to be isolated.</summary>
    protected virtual void OnNodeIsolating(string nodeAddress) => Log.NodeIsolating(_logger, nodeAddress);

    /// <summary>Called when a node is about to be reconnected.</summary>
    protected virtual void OnNodeReconnecting(string nodeAddress) => Log.NodeReconnecting(_logger, nodeAddress);

    /// <summary>Called when a message is blocked by a partition.</summary>
    protected virtual void OnMessageBlockedByPartition(string source, string target) => Log.MessageBlockedByPartition(_logger, source, target);

    /// <summary>Called when a message is randomly dropped.</summary>
    protected virtual void OnMessageDroppedRandom(string source, string target) => Log.MessageDroppedRandom(_logger, source, target);

    /// <summary>Called when a per-link latency override is set.</summary>
    protected virtual void OnLinkLatencySet(string source, string target, TimeSpan baseDelay, TimeSpan maxJitter) => Log.LinkLatencySet(_logger, source, target, baseDelay, maxJitter);

    /// <summary>Called when a per-link latency override is cleared.</summary>
    protected virtual void OnLinkLatencyCleared(string source, string target) => Log.LinkLatencyCleared(_logger, source, target);

    /// <summary>Called when all per-link latency overrides are cleared.</summary>
    protected virtual void OnAllLinkLatenciesCleared(int count) => Log.AllLinkLatenciesCleared(_logger, count);


    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"SimulationNetwork(Partitions={_partitions.Count}, LinkLatencies={_linkLatencies.Count}, DropRate={MessageDropRate:P0})");

    private static partial class Log
    {
        [LoggerMessage(Level = LogLevel.Debug, Message = "Partition created: {Source} -> {Target}")]
        public static partial void PartitionCreated(ILogger logger, string source, string target);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Creating bidirectional partition: {Node1} <-> {Node2}")]
        public static partial void BidirectionalPartitionCreating(ILogger logger, string node1, string node2);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Partition healed: {Source} -> {Target}")]
        public static partial void PartitionHealed(ILogger logger, string source, string target);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Healing bidirectional partition: {Node1} <-> {Node2}")]
        public static partial void BidirectionalPartitionHealing(ILogger logger, string node1, string node2);

        [LoggerMessage(Level = LogLevel.Debug, Message = "All partitions healed: {Count} partitions")]
        public static partial void AllPartitionsHealed(ILogger logger, int count);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Isolating node: {Node}")]
        public static partial void NodeIsolating(ILogger logger, string node);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Reconnecting node: {Node}")]
        public static partial void NodeReconnecting(ILogger logger, string node);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Message blocked by partition: {Source} -> {Target}")]
        public static partial void MessageBlockedByPartition(ILogger logger, string source, string target);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Message randomly dropped: {Source} -> {Target}")]
        public static partial void MessageDroppedRandom(ILogger logger, string source, string target);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Link latency set: {Source} -> {Target} (base={BaseDelay}, jitter={MaxJitter})")]
        public static partial void LinkLatencySet(ILogger logger, string source, string target, TimeSpan baseDelay, TimeSpan maxJitter);

        [LoggerMessage(Level = LogLevel.Debug, Message = "Link latency cleared: {Source} -> {Target}")]
        public static partial void LinkLatencyCleared(ILogger logger, string source, string target);

        [LoggerMessage(Level = LogLevel.Debug, Message = "All link latencies cleared: {Count} overrides")]
        public static partial void AllLinkLatenciesCleared(ILogger logger, int count);
    }
}
