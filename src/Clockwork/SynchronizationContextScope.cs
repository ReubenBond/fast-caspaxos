namespace Clockwork;

/// <summary>
/// A disposable scope that restores the previous synchronization context when disposed.
/// </summary>
/// <remarks>
/// This struct is only used as a disposable scope and is not intended for comparison.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1815:Override equals and operator equals on value types", Justification = "Struct is only used as disposable scope, not for comparison")]
public readonly struct SynchronizationContextScope : IDisposable
{
    private readonly SynchronizationContext? _previous;
    private readonly bool _shouldRestore;

    /// <summary>
    /// Gets an empty scope that does nothing when disposed.
    /// </summary>
    public static SynchronizationContextScope Empty => default;

    /// <summary>
    /// Initializes a new instance of the <see cref="SynchronizationContextScope"/> struct.
    /// Creates a new scope that will restore the specified context when disposed.
    /// </summary>
    /// <param name="previous">The synchronization context to restore.</param>
    internal SynchronizationContextScope(SynchronizationContext? previous)
    {
        _previous = previous;
        _shouldRestore = true;
    }

    /// <summary>
    /// Restores the previous synchronization context if this scope should restore.
    /// </summary>
    public void Dispose()
    {
        if (_shouldRestore)
        {
            SynchronizationContext.SetSynchronizationContext(_previous);
        }
    }
}
