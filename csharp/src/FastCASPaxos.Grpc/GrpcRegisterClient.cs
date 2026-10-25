using System.Collections.Immutable;
using FastCASPaxos.Server;
using Grpc.Core;
using Grpc.Net.Client;

namespace FastCASPaxos.Grpc;

/// <summary>Bounds each logical call and its endpoint attempts.</summary>
public sealed class GrpcRegisterClientOptions
{
    public TimeSpan CallTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public int MaxReceiveMessageBytes { get; init; } = 4 * 1024 * 1024;
    public int MaxSendMessageBytes { get; init; } = 4 * 1024 * 1024;
}

/// <summary>
/// Accesses a fixed set of register servers, retaining the last successful endpoint.
/// Reads can fail over; writes retry only server-confirmed pre-mutation unavailability.
/// </summary>
public sealed class GrpcRegisterClient : IRegisterService, IDisposable
{
    private readonly ImmutableArray<CallInvoker> _invokers;
    private readonly List<GrpcChannel> _channels = [];
    private readonly GrpcRegisterClientOptions _options;
    private readonly TimeProvider _timeProvider;
    private int _preferredEndpoint;
    private int _disposed;

    /// <summary>Creates and owns one channel per HTTP(S) origin, preserving its TLS authority.</summary>
    public GrpcRegisterClient(ReadOnlySpan<Uri> endpoints, GrpcRegisterClientOptions? options = null,
        TimeProvider? timeProvider = null) : this(options, timeProvider)
    {
        if (endpoints.IsEmpty) throw new ArgumentException("At least one endpoint is required.", nameof(endpoints));
        var unique = new HashSet<Uri>();
        foreach (var address in endpoints)
        {
            if (address is null || !address.IsAbsoluteUri || address.Scheme is not ("http" or "https")
                || address.AbsolutePath != "/" || address.Query.Length != 0
                || address.Fragment.Length != 0 || address.UserInfo.Length != 0 || !unique.Add(address))
                throw new ArgumentException("Endpoints must be distinct HTTP(S) origins.", nameof(endpoints));
        }

        try
        {
            foreach (var address in endpoints)
            {
                _channels.Add(GrpcChannel.ForAddress(address, new GrpcChannelOptions
                {
                    MaxReceiveMessageSize = _options.MaxReceiveMessageBytes,
                    MaxSendMessageSize = _options.MaxSendMessageBytes,
                    // Each authority has its own SDK connection pool. Application mutation replay
                    // requires the definite-failure contract rather than a gRPC retry policy.
                    MaxRetryAttempts = 1,
                    DisableResolverServiceConfig = true,
                }));
            }
            _invokers = [.. _channels.Select(channel => channel.CreateCallInvoker())];
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>
    /// Uses caller-owned invokers (for example, authenticated channels). Configure them for
    /// transport deadline enforcement and application-controlled mutation retries.
    /// </summary>
    public GrpcRegisterClient(ImmutableArray<CallInvoker> invokers, GrpcRegisterClientOptions? options = null,
        TimeProvider? timeProvider = null) : this(options, timeProvider)
    {
        if (invokers.IsDefault) throw new ArgumentNullException(nameof(invokers));
        _invokers = invokers;
        if (_invokers.Length == 0 || _invokers.Any(invoker => invoker is null))
            throw new ArgumentException("At least one non-null invoker is required.", nameof(invokers));
    }

    private GrpcRegisterClient(GrpcRegisterClientOptions? options, TimeProvider? timeProvider)
    {
        _options = options ?? new();
        _timeProvider = timeProvider ?? TimeProvider.System;
        ValidateTimeout(_options.CallTimeout, nameof(GrpcRegisterClientOptions.CallTimeout));
        ValidateTimeout(_options.AttemptTimeout, nameof(GrpcRegisterClientOptions.AttemptTimeout));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.MaxReceiveMessageBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.MaxSendMessageBytes);
        _invokers = [];
    }

    public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default) =>
        InvokeAsync((client, token) => client.ReadAsync(key, token), readOnly: true, cancellationToken);

    public ValueTask<CompareExchangeResult> CompareExchangeAsync(string key, ByteString? expectedVersion,
        ByteString value, CancellationToken cancellationToken = default) =>
        InvokeAsync((client, token) => client.CompareExchangeAsync(key, expectedVersion, value, token),
            readOnly: false, cancellationToken);

    public ValueTask<RegisterKeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100,
        CancellationToken cancellationToken = default) =>
        InvokeAsync((client, token) => client.ListKeysAsync(prefix, afterKey, limit, token),
            readOnly: true, cancellationToken);

    private async ValueTask<T> InvokeAsync<T>(Func<GrpcRegisterService, CancellationToken, ValueTask<T>> invoke,
        bool readOnly, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        var started = _timeProvider.GetTimestamp();
        var first = (uint)Volatile.Read(ref _preferredEndpoint);
        for (var attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = _options.CallTimeout - _timeProvider.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
                throw new RemoteCallException(StatusCode.DeadlineExceeded, "The register client call budget expired.");
            var timeout = remaining < _options.AttemptTimeout ? remaining : _options.AttemptTimeout;
            var index = (int)((first + (uint)attempt) % (uint)_invokers.Length);
            var client = new GrpcRegisterService(_invokers[index], timeout, _timeProvider);
            try
            {
                var result = await invoke(client, cancellationToken).ConfigureAwait(false);
                // Affinity avoids needless contention between server-side proposers.
                if (result is not CompareExchangeResult { Status: CompareExchangeStatus.Unknown })
                    Volatile.Write(ref _preferredEndpoint, index);
                return result;
            }
            catch (Exception error) when (attempt + 1 < _invokers.Length && CanRetry(error, readOnly))
            {
                // Each endpoint is attempted at most once. Unknown is a result, not a retry signal.
            }
        }
    }

    private static bool CanRetry(Exception error, bool readOnly) =>
        error is RegisterUnavailableException
        || (readOnly && error is RemoteCallException
        { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded });

    private static void ValidateTimeout(TimeSpan timeout, string name)
    {
        if (timeout <= TimeSpan.Zero || timeout > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(name, "Timeouts must be positive and at most one day.");
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        foreach (var channel in _channels) channel.Dispose();
    }
}
