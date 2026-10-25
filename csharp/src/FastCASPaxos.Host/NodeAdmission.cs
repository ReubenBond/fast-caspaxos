using FastCASPaxos.Messages;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;

namespace FastCASPaxos.Host;

internal sealed class NodeAdmission
{
    private readonly object _gate = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _closed;
    private int _active;

    public IDisposable Enter()
    {
        lock (_gate)
        {
            if (_closed) throw new RegisterUnavailableException(new InvalidOperationException("The node is quiesced."));
            _active++;
            return new Lease(this);
        }
    }

    public Task CloseAsync()
    {
        lock (_gate)
        {
            _closed = true;
            if (_active == 0) _drained.TrySetResult();
            return _drained.Task;
        }
    }

    private sealed class Lease(NodeAdmission owner) : IDisposable
    {
        public void Dispose()
        {
            lock (owner._gate)
                if (--owner._active == 0 && owner._closed) owner._drained.TrySetResult();
        }
    }
}

internal sealed class EpochAcceptor(string epoch, IKeyedAcceptor inner, IAcceptorDigestSource digests)
    : IKeyedAcceptor, IAcceptorDigestSource
{
    public string Epoch { get; } = epoch;
    public NodeAdmission Admission { get; } = new();

    public async ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        using var lease = Admission.Enter();
        return await inner.ReadAsync(key, known, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        using var lease = Admission.Enter();
        return await inner.PrepareAsync(key, request, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        using var lease = Admission.Enter();
        return await inner.AcceptAsync(key, request, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        using var lease = Admission.Enter();
        return await inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<AcceptorDigestPage> ListDigestsAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        using var lease = Admission.Enter();
        return await digests.ListDigestsAsync(prefix, afterKey, limit, cancellationToken).ConfigureAwait(false);
    }
}
