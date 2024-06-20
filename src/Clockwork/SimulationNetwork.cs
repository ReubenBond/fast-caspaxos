using System.Collections.Concurrent;
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
/// Simulation network for in-memory transport between nodes.
/// Provides hooks for injecting network faults like delays, partitions, and message loss.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public class SimulationNetwork
{
    private readonly Func<IReadOnlyList<SimulationNode>> _getNodes;
    private readonly SimulationRandom _random;
    private readonly ConcurrentDictionary<string, HashSet<string>> _partitions = new(StringComparer.Ordinal);
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
    public SimulationNetwork(Func<IReadOnlyList<SimulationNode>> getNodes, SimulationRandom random)
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
    /// Gets the simulated delay for a message.
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

#pragma warning disable CA1848 // Use the LoggerMessage delegates - these are virtual hooks, override for high-performance logging
#pragma warning disable CA1873 // Logging message template - these are virtual hooks, override for high-performance logging

    /// <summary>Called when a unidirectional partition is created.</summary>
    protected virtual void OnPartitionCreated(string source, string target) => _logger.LogDebug("Partition created: {Source} -> {Target}", source, target);

    /// <summary>Called when a bidirectional partition is about to be created.</summary>
    protected virtual void OnBidirectionalPartitionCreating(string node1, string node2) => _logger.LogDebug("Creating bidirectional partition: {Node1} <-> {Node2}", node1, node2);

    /// <summary>Called when a unidirectional partition is healed.</summary>
    protected virtual void OnPartitionHealed(string source, string target) => _logger.LogDebug("Partition healed: {Source} -> {Target}", source, target);

    /// <summary>Called when a bidirectional partition is about to be healed.</summary>
    protected virtual void OnBidirectionalPartitionHealing(string node1, string node2) => _logger.LogDebug("Healing bidirectional partition: {Node1} <-> {Node2}", node1, node2);

    /// <summary>Called when all partitions are healed.</summary>
    protected virtual void OnAllPartitionsHealed(int count) => _logger.LogDebug("All partitions healed: {Count} partitions", count);

    /// <summary>Called when a node is about to be isolated.</summary>
    protected virtual void OnNodeIsolating(string nodeAddress) => _logger.LogDebug("Isolating node: {Node}", nodeAddress);

    /// <summary>Called when a node is about to be reconnected.</summary>
    protected virtual void OnNodeReconnecting(string nodeAddress) => _logger.LogDebug("Reconnecting node: {Node}", nodeAddress);

    /// <summary>Called when a message is blocked by a partition.</summary>
    protected virtual void OnMessageBlockedByPartition(string source, string target) => _logger.LogTrace("Message blocked by partition: {Source} -> {Target}", source, target);

    /// <summary>Called when a message is randomly dropped.</summary>
    protected virtual void OnMessageDroppedRandom(string source, string target) => _logger.LogTrace("Message randomly dropped: {Source} -> {Target}", source, target);

#pragma warning restore CA1873, CA1848

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"SimulationNetwork(Partitions={_partitions.Count}, DropRate={MessageDropRate:P0})");
}
