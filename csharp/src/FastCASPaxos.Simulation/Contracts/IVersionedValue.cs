namespace FastCASPaxos.Simulation.Contracts;

/// <summary>
/// Testing/simulation interface for values that carry a monotonic version and
/// application-defined successor validation. Used by the safety invariant checker
/// to verify committed-value linearity.
/// </summary>
public interface IVersionedValue<TSelf> where TSelf : IVersionedValue<TSelf>
{
    int Version { get; }

    bool IsValidSuccessorTo(TSelf predecessor);
}

public static class VersionedValueExtensions
{
    public static int GetVersionOrDefault<TSelf>(this IVersionedValue<TSelf>? value)
        where TSelf : IVersionedValue<TSelf>
        => value?.Version ?? 0;
}
