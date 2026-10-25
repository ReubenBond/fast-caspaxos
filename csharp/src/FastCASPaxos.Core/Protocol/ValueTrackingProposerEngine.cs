using System.Collections.Immutable;

namespace FastCASPaxos.Protocol;

/// <summary>
/// Optional proposer base class that recreates the old split-value view on top of the slimmer
/// single-slot <see cref="ProposerEngine{TValue,TRoute}"/>.
/// </summary>
public abstract class ValueTrackingProposerEngine<TValue, TRoute>(
    ProposerRuntime runtime,
    TValue initialValue,
    ImmutableArray<TRoute> acceptors,
    ProposerModePolicy modePolicy) : ProposerEngine<TValue, TRoute>(runtime, initialValue, acceptors, modePolicy)
    where TRoute : notnull
{
    public TValue CachedValue { get; private set; } = initialValue;

    public TValue PendingAcceptValue { get; private set; } = initialValue;

    protected override void OnCachedValueChanged(TValue value) => CachedValue = value;

    protected override void OnPendingAcceptValueStarted(TValue value) => PendingAcceptValue = value;

    protected override void OnPendingAcceptValueCommitted(TValue value)
    {
        CachedValue = value;
        PendingAcceptValue = _initialValue;
    }

    protected override void OnPendingAcceptValueCleared(TValue value) => PendingAcceptValue = _initialValue;
}
