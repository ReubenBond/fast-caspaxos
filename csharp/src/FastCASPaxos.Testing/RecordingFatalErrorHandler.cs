using System.Collections.Concurrent;
using FastCASPaxos.Server;

namespace FastCASPaxos.Testing;

/// <summary>
/// Records each reported local storage failure where a host's handler would stop the process. The reporting component
/// answers nothing afterward, so a test observes the failure both here and as the component's failed operation.
/// </summary>
public sealed class RecordingFatalErrorHandler : IFatalErrorHandler
{
    private readonly ConcurrentQueue<LocalStorageFailure> _failures = new();

    /// <summary>The reported failures, in report order.</summary>
    public IReadOnlyCollection<LocalStorageFailure> Failures => _failures;

    /// <inheritdoc/>
    public void OnLocalStorageFailure(string operation, string path, Exception exception) =>
        _failures.Enqueue(new(operation, path, exception));
}

/// <summary>A local storage failure reported to a <see cref="RecordingFatalErrorHandler"/>.</summary>
public sealed record LocalStorageFailure(string Operation, string Path, Exception Exception);
