using FastCASPaxos.Messages;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;

namespace FastCASPaxos.Host;

/// <summary>Seals external protocol mutations and waits for their durable outcomes before shutdown.</summary>
internal sealed class QuiescingAcceptor(IKeyedAcceptor inner) : IKeyedAcceptor
{
    private readonly Lock _gate = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _closed;
    private int _active;

    public ValueTask<AcceptorReadResult> ReadAsync(
        string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default) =>
        inner.ReadAsync(key, known, cancellationToken);

    public Task QuiesceAsync()
    {
        lock (_gate)
        {
            _closed = true;
            if (_active == 0) _drained.TrySetResult();
            return _drained.Task;
        }
    }

    public async ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        Enter();
        try { return await inner.PrepareAsync(key, request, cancellationToken).ConfigureAwait(false); }
        finally { Exit(); }
    }

    public async ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        Enter();
        try { return await inner.AcceptAsync(key, request, cancellationToken).ConfigureAwait(false); }
        finally { Exit(); }
    }

    public ValueTask<KeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
        inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken);

    private void Enter()
    {
        lock (_gate)
        {
            if (_closed) throw new RegisterUnavailableException(new InvalidOperationException("The acceptor is draining for shutdown."));
            _active++;
        }
    }

    private void Exit()
    {
        lock (_gate)
            if (--_active == 0 && _closed) _drained.TrySetResult();
    }
}
