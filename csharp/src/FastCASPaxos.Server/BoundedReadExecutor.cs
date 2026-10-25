using System.Runtime.ExceptionServices;

namespace FastCASPaxos.Server;

/// <summary>Bounds underlying read-only work until completion while bounding each caller's wait independently.</summary>
public sealed class BoundedReadExecutor
{
    private readonly int _capacity;
    private int _active;

    public BoundedReadExecutor(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _capacity = capacity;
    }

    public async ValueTask<T> RunAsync<T>(Func<CancellationToken, ValueTask<T>> read,
        TimeSpan timeout, TimeProvider clock, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(clock);
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(timeout), "A positive finite timer duration is required.");
        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.Increment(ref _active) > _capacity)
        {
            Interlocked.Decrement(ref _active);
            throw new RegisterBusyException();
        }
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            var result = await RunOwnedAsync(read, cancellation.Token)
                .WaitAsync(timeout, clock, cancellationToken).ConfigureAwait(false);
            if (result.Error is { } error) ExceptionDispatchInfo.Capture(error).Throw();
            return result.Value!;
        }
        finally { cancellation.Cancel(); }
    }

    private async Task<(T? Value, Exception? Error)> RunOwnedAsync<T>(
        Func<CancellationToken, ValueTask<T>> read, CancellationToken cancellationToken)
    {
        try { return (await read(cancellationToken).ConfigureAwait(false), null); }
        catch (Exception error) { return (default, error); } // Retain faults even after the caller's wait ends.
        finally { Interlocked.Decrement(ref _active); }
    }
}
