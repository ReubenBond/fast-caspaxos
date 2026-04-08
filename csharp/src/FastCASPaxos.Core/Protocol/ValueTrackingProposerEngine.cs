namespace FastCASPaxos.Protocol;

/// <summary>
/// Optional proposer base class that recreates the old split-value view on top of the slimmer
/// single-slot <see cref="ProposerEngine{TValue,TRoute}"/>.
/// </summary>
public abstract class ValueTrackingProposerEngine<TValue, TRoute> : ProposerEngine<TValue, TRoute>
    where TRoute : notnull
{
    protected ValueTrackingProposerEngine(
        ProposerRuntime runtime,
        TValue initialValue,
        IEnumerable<TRoute> acceptors,
        ProposerModePolicy modePolicy)
        : base(runtime, initialValue, acceptors, modePolicy)
    {
        CachedValue = initialValue;
        PendingAcceptValue = initialValue;
    }

    public TValue CachedValue { get; private set; }

    public TValue PendingAcceptValue { get; private set; }

    protected override void OnCachedValueChanged(TValue value) => CachedValue = value;

    protected override void OnPendingAcceptValueStarted(TValue value) => PendingAcceptValue = value;

    protected override void OnPendingAcceptValueCommitted(TValue value)
    {
        CachedValue = value;
        PendingAcceptValue = _initialValue;
    }

    protected override void OnPendingAcceptValueCleared(TValue value) => PendingAcceptValue = _initialValue;
}
