using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace FastCASPaxos.Testing;

/// <summary>
/// Single-consumer, non-pooled boundary between a context-capturing fixture operation
/// and consumer code which may opt out of context capture. This is not a scheduler:
/// the operation and all completions are driven by the existing Clockwork queue.
/// </summary>
internal sealed class SimulationValueTaskSource<T> : IValueTaskSource<T>
{
    private ManualResetValueTaskSourceCore<T> _completion;
    private readonly ValueTaskAwaiter<T> _operation;
    private readonly Action<Action<object?>, object?> _runContextFree;

    public SimulationValueTaskSource(ValueTask<T> operation, Action<Action<object?>, object?> runContextFree)
    {
        _operation = operation.GetAwaiter();
        _runContextFree = runContextFree;
        // Constructed under the fixture context, so this continuation is captured
        // by Clockwork even when the public caller is itself context-free.
        _operation.UnsafeOnCompleted(Complete);
    }

    public ValueTask<T> Task => new(this, _completion.Version);
    public T GetResult(short token) => _completion.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        // Respect context-capturing scenario awaits. For context-free awaits, the
        // callback must also clear the current context while finishing Task-backed
        // async wrappers; otherwise their next false-await is sent to the thread pool.
        if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0
            && SynchronizationContext.Current is not null)
        {
            _completion.OnCompleted(continuation, state, token, flags);
        }
        else
        {
            _completion.OnCompleted(static boxed =>
            {
                var invocation = (ContextFreeInvocation)boxed!;
                invocation.Run(invocation.Continuation, invocation.State);
            }, new ContextFreeInvocation(_runContextFree, continuation, state), token, flags);
        }
    }

    private void Complete()
    {
        T result;
        try
        {
            result = _operation.GetResult();
        }
        catch (Exception error)
        {
            _completion.SetException(error);
            return;
        }

        // RunContinuationsAsynchronously is deliberately false. There is no race
        // with registration: all fixture work runs on the single Clockwork thread.
        _completion.SetResult(result);
    }

    private sealed record ContextFreeInvocation(
        Action<Action<object?>, object?> Run, Action<object?> Continuation, object? State);
}
