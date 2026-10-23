namespace FastCASPaxos.Server.Diagnostics;

/// <summary>Reports quorum API outcomes separately from individual acceptor observations.</summary>
public sealed class ObservedRegisterService(IRegisterService inner, NodeEventJournal journal) : IRegisterService
{
    public ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default) =>
        Observe("read", key, () => inner.ReadAsync(key, cancellationToken),
            result => ("established", result, (Exception?)null));

    public ValueTask<CompareExchangeResult> CompareExchangeAsync(
        string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default) =>
        Observe("compare-exchange", key, () => inner.CompareExchangeAsync(key, expectedVersion, value, cancellationToken),
            result => (result.Status.ToString().ToLowerInvariant(), result.Current, result.Error));

    public ValueTask<RegisterKeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100,
        CancellationToken cancellationToken = default) =>
        Observe("list", prefix, () => inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken),
            _ => ("completed", (RegisterValue?)null, (Exception?)null));

    private async ValueTask<T> Observe<T>(string operation, string key, Func<ValueTask<T>> invoke,
        Func<T, (string Outcome, RegisterValue? Current, Exception? Error)> describe)
    {
        var previous = DiagnosticContext.Current;
        var id = Guid.NewGuid().ToString("N");
        DiagnosticContext.Current = new(journal.NodeId, id, null);
        try
        {
            var result = await invoke().ConfigureAwait(false);
            var (outcome, current, error) = describe(result);
            journal.Publish("operation", new OperationObservation(operation, id, KeyPreview.From(key), outcome,
                PayloadPreview.From(current?.Value),
                current?.Version is { } version ? Convert.ToBase64String(version.Span) : null,
                error?.GetType().Name));
            return result;
        }
        catch (Exception error)
        {
            journal.Publish("operation", new OperationObservation(operation, id, KeyPreview.From(key), "fault",
                Error: error.GetType().Name));
            throw;
        }
        finally { DiagnosticContext.Current = previous; }
    }
}
