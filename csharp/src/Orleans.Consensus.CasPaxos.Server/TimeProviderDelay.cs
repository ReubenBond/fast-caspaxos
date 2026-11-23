namespace Orleans.Consensus.CasPaxos.Server;

/// <summary>
/// Delays on a supplied <see cref="TimeProvider"/> without forcing asynchronous ThreadPool
/// continuations when running under a single-threaded deterministic simulation clock.
/// </summary>
internal static class TimeProviderDelay
{
    public static Task DelayAsync(TimeProvider timeProvider, TimeSpan delay, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        if (delay == TimeSpan.Zero)
        {
            return Task.CompletedTask;
        }

        if (ReferenceEquals(timeProvider, TimeProvider.System))
        {
            return Task.Delay(delay, cancellationToken);
        }

        var tcs = new TaskCompletionSource();
        ITimer? timer = null;
        CancellationTokenRegistration registration = default;
        timer = timeProvider.CreateTimer(
            _ =>
            {
                registration.Dispose();
                timer?.Dispose();
                tcs.TrySetResult();
            },
            null,
            delay,
            Timeout.InfiniteTimeSpan);

        if (cancellationToken.CanBeCanceled)
        {
            registration = cancellationToken.Register(() =>
            {
                timer.Dispose();
                tcs.TrySetCanceled(cancellationToken);
            });
        }

        return tcs.Task;
    }
}
