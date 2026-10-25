using FastCASPaxos.Protocol;
using FastCASPaxos.Server;

namespace FastCASPaxos.Host;

public sealed class PeerOptions
{
    public string Id { get; set; } = "";
    public string Endpoint { get; set; } = "";
    public string HttpEndpoint { get; set; } = "";
}

public enum NodeShutdownMode
{
    Departure,
    Restart,
}

public sealed class NodeOptions
{
    public string AcceptorId { get; set; } = "a";
    public string AcceptorDatabase { get; set; } = "data/acceptor-a.db";
    public int ProposerId { get; set; } = 1;
    public string ProposerDatabase { get; set; } = "data/proposer-1.db";
    public List<PeerOptions> Peers { get; set; } = [];
    public int QueueCapacity { get; set; } = 256;
    public int MaxBatchSize { get; set; } = 64;
    public int Capacity { get; set; } = 256;
    public string Mode { get; set; } = "adaptive";
    public ProposerSettings? ProposerSettings { get; set; }
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromMilliseconds(10);
    public TimeSpan PeerCallTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public int MaxReceiveMessageBytes { get; set; } = 4 * 1024 * 1024;
    public int MaxSendMessageBytes { get; set; } = 4 * 1024 * 1024;
    public int MaxConcurrentConnections { get; set; } = 256;
    public int MaxStreamsPerConnection { get; set; } = 128;
    public long MaxRequestBodyBytes { get; set; } = 4 * 1024 * 1024;
    public List<string> TrustedAdminOrigins { get; set; } = [];
    public int EventHistoryCapacity { get; set; } = 1024;
    public int MaxEventStreams { get; set; } = 64;
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);
    public int TargetReplicationFactor { get; set; }
    public int RepairPageSize { get; set; } = 128;
    public TimeSpan RepairInterval { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan RepairRetryInterval { get; set; } = TimeSpan.FromSeconds(1);
    public NodeShutdownMode ShutdownMode { get; set; } = NodeShutdownMode.Departure;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(AcceptorId);
        ArgumentException.ThrowIfNullOrWhiteSpace(AcceptorDatabase);
        ArgumentException.ThrowIfNullOrWhiteSpace(ProposerDatabase);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ProposerId);
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (string.Equals(Path.GetFullPath(AcceptorDatabase), Path.GetFullPath(ProposerDatabase), comparison))
            throw new ArgumentException("Acceptor and proposer databases must have distinct paths.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(QueueCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxBatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(Capacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxReceiveMessageBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxSendMessageBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxConcurrentConnections);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxStreamsPerConnection);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxRequestBodyBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(EventHistoryCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxEventStreams);
        ValidateDuration(HeartbeatInterval, nameof(HeartbeatInterval));
        ArgumentNullException.ThrowIfNull(TrustedAdminOrigins);
        foreach (var origin in TrustedAdminOrigins) ValidateHttpOrigin(origin);
        ValidateDuration(OperationTimeout, nameof(OperationTimeout));
        ValidateDuration(AttemptTimeout, nameof(AttemptTimeout));
        ValidateDuration(RetryBackoff, nameof(RetryBackoff));
        ValidateDuration(PeerCallTimeout, nameof(PeerCallTimeout));
        ArgumentOutOfRangeException.ThrowIfNegative(TargetReplicationFactor);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(RepairPageSize);
        ValidateDuration(RepairInterval, nameof(RepairInterval));
        ValidateDuration(RepairRetryInterval, nameof(RepairRetryInterval));
        if (!Enum.IsDefined(ShutdownMode)) throw new ArgumentOutOfRangeException(nameof(ShutdownMode));
        ProposerModePolicy.Parse(Mode).Validate();
        ProposerSettings?.CreatePolicy();
        ArgumentNullException.ThrowIfNull(Peers);
        var ids = new HashSet<string>(StringComparer.Ordinal);
        var endpoints = new HashSet<Uri>();
        var httpEndpoints = new HashSet<Uri>();
        foreach (var peer in Peers)
        {
            ArgumentNullException.ThrowIfNull(peer);
            ArgumentException.ThrowIfNullOrWhiteSpace(peer.Id);
            if (!ids.Add(peer.Id)) throw new ArgumentException($"Duplicate acceptor id '{peer.Id}'.");
            if (!Uri.TryCreate(peer.Endpoint, UriKind.Absolute, out var endpoint)
                || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps)
                || endpoint.AbsolutePath != "/" || endpoint.UserInfo.Length != 0
                || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
                throw new ArgumentException($"Peer '{peer.Id}' requires an absolute HTTP(S) origin.");
            if (!endpoints.Add(endpoint)) throw new ArgumentException($"Duplicate peer endpoint '{endpoint}'.");
            var httpEndpoint = ValidateHttpOrigin(peer.HttpEndpoint);
            if (!httpEndpoints.Add(httpEndpoint))
                throw new ArgumentException($"Duplicate advertised HTTP endpoint '{httpEndpoint}'.");
        }
        if (!ids.Contains(AcceptorId))
            throw new ArgumentException("The fixed peer set must contain this node's acceptor id exactly once.");
        if (TargetReplicationFactor > Peers.Count)
            throw new ArgumentOutOfRangeException(nameof(TargetReplicationFactor), "The target must fit the fixed membership.");
    }

    internal static Uri ValidateHttpOrigin(string origin)
    {
        if (!Uri.TryCreate(origin, UriKind.Absolute, out var endpoint)
            || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps)
            || endpoint.AbsolutePath != "/" || endpoint.UserInfo.Length != 0
            || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0
            || endpoint.Host is "*" or "+" or "0.0.0.0" or "[::]")
            throw new ArgumentException("Advertised HTTP endpoints and trusted admin origins require browser-reachable absolute HTTP(S) origins.");
        return endpoint;
    }

    internal RegisterServiceOptions RegisterOptions(TimeProvider timeProvider, ProposerSettings settings) => new()
    {
        Mode = settings.CreatePolicy(),
        Capacity = Capacity,
        OperationTimeout = OperationTimeout,
        AttemptTimeout = AttemptTimeout,
        RetryBackoff = RetryBackoff,
        TimeProvider = timeProvider,
    };

    private static void ValidateDuration(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(name, "Durations must be positive and at most one day.");
    }
}
