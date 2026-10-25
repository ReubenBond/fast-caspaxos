using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class BoundedReadExecutorTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NonpositiveCapacityIsRejected(int capacity) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new BoundedReadExecutor(capacity));

    [Fact]
    public async Task InvalidReadArgumentsRejectBeforeAdmissionAndFiniteBoundariesSucceed()
    {
        var executor = new BoundedReadExecutor(1);
        var calls = 0;
        ValueTask<int> Read(CancellationToken _) { calls++; return ValueTask.FromResult(42); }
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            executor.RunAsync<int>(null!, TimeSpan.FromSeconds(1), TimeProvider.System).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            executor.RunAsync(Read, TimeSpan.FromSeconds(1), null!).AsTask());
        foreach (var timeout in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1), TimeSpan.FromMilliseconds(uint.MaxValue) })
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => executor.RunAsync(Read, timeout, TimeProvider.System).AsTask());
        Assert.Equal(0, calls);
        Assert.Equal(42, await executor.RunAsync(Read, TimeSpan.FromTicks(1), TimeProvider.System));
        Assert.Equal(42, await executor.RunAsync(Read, TimeSpan.FromMilliseconds(uint.MaxValue - 1), TimeProvider.System));
        Assert.Equal(2, calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancelledOrExpiredWaitRetainsPhysicalSlotUntilIgnoringReadFinishes(bool cancel)
    {
        var executor = new BoundedReadExecutor(1);
        var clock = new ManualTime();
        // Deliberately synchronous completion: SetResult drains RunOwnedAsync's
        // continuation, including its slot release, before the next assertion.
        var provider = new TaskCompletionSource<int>();
        using var cancellation = new CancellationTokenSource();
        CancellationToken providerToken = default;
        var waiting = executor.RunAsync(token =>
        {
            providerToken = token;
            return new ValueTask<int>(provider.Task);
        }, TimeSpan.FromSeconds(1), clock, cancellation.Token).AsTask();
        try
        {
            await clock.WaitForDelayAsync(TimeSpan.FromSeconds(1));
            clock.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromTicks(1));
            Assert.False(waiting.IsCompleted);
            if (cancel)
            {
                cancellation.Cancel();
                Assert.Equal(cancellation.Token,
                    (await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(Watchdog))).CancellationToken);
            }
            else
            {
                clock.Advance(TimeSpan.FromTicks(1));
                await Assert.ThrowsAsync<TimeoutException>(() => waiting.WaitAsync(Watchdog));
            }
            Assert.True(providerToken.IsCancellationRequested);
            Assert.False(provider.Task.IsCompleted);
            var overflowCalls = 0;
            await Assert.ThrowsAsync<RegisterBusyException>(() => executor.RunAsync(_ =>
            {
                overflowCalls++;
                return ValueTask.FromResult(99);
            }, TimeSpan.FromSeconds(1), clock).AsTask());
            Assert.Equal(0, overflowCalls);
            CompleteInline(provider, 42);
            Assert.Equal(73, await executor.RunAsync(_ => ValueTask.FromResult(73), TimeSpan.FromSeconds(1), clock));
        }
        finally { provider.TrySetResult(42); }
    }

    [Fact]
    public async Task SuccessfulAndFaultedReadsReturnCapacityAndPreserveExactResults()
    {
        var executor = new BoundedReadExecutor(1);
        var clock = new ManualTime();
        var error = new InvalidDataException("corrupt provider value");
        Assert.Same(error, await Assert.ThrowsAsync<InvalidDataException>(() =>
            executor.RunAsync<int>(_ => throw error, TimeSpan.FromSeconds(1), clock).AsTask()));
        Assert.Equal(42, await executor.RunAsync(_ => ValueTask.FromResult(42), TimeSpan.FromSeconds(1), clock));
        Assert.Same(error, await Assert.ThrowsAsync<InvalidDataException>(() =>
            executor.RunAsync<int>(_ => ValueTask.FromException<int>(error), TimeSpan.FromSeconds(1), clock).AsTask()));
        Assert.Equal(73, await executor.RunAsync(_ => ValueTask.FromResult(73), TimeSpan.FromSeconds(1), clock));
    }

    [Fact]
    public async Task PreCancelledReadNeverInvokesProviderOrConsumesCapacity()
    {
        var executor = new BoundedReadExecutor(1);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var calls = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => executor.RunAsync(_ =>
        {
            calls++;
            return ValueTask.FromResult(1);
        }, TimeSpan.FromSeconds(1), TimeProvider.System, cancellation.Token).AsTask());
        Assert.Equal(0, calls);
        Assert.Equal(2, await executor.RunAsync(_ => ValueTask.FromResult(2), TimeSpan.FromSeconds(1), TimeProvider.System));
    }

    private static void CompleteInline<T>(TaskCompletionSource<T> completion, T value)
    {
        // xUnit's synchronization context otherwise makes even a default-TCS
        // ConfigureAwait(false) continuation queue instead of running inline.
        var previous = SynchronizationContext.Current;
        try { SynchronizationContext.SetSynchronizationContext(null); completion.SetResult(value); }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
    }
}
