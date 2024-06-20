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
        IEnumerable<TRoute> acceptors,
        bool enableDistinguishedLeader,
        bool enableFastCommit)
        : base(runtime, acceptors, enableDistinguishedLeader, enableFastCommit)
    {
    }

    public TValue CachedValue { get; private set; } = default!;

    public TValue PendingAcceptValue { get; private set; } = default!;

    protected override void OnCachedValueChanged(TValue value) => CachedValue = value;

    protected override void OnPendingAcceptValueStarted(TValue value) => PendingAcceptValue = value;

    protected override void OnPendingAcceptValueCommitted(TValue value)
    {
        CachedValue = value;
        PendingAcceptValue = default!;
    }

    protected override void OnPendingAcceptValueCleared(TValue value) => PendingAcceptValue = default!;
}
