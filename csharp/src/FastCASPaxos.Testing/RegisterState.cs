using System.Collections.Immutable;
using FastCASPaxos.Server;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Testing;

// Application state only. All consensus, recovery and durability decisions belong to the
// existing simulation hosts/engines. Keeping the map immutable also prevents aliasing
// acceptor durable state when a different proposer applies a conditional operation.
internal sealed class RegisterState : IVersionedValue<RegisterState>, IEquatable<RegisterState>
{
    private readonly RegisterState? _predecessor;
    private readonly ImmutableSortedDictionary<string, RegisterValue> _values;

    private RegisterState(ImmutableSortedDictionary<string, RegisterValue> values, RegisterState? predecessor)
    {
        _values = values;
        _predecessor = predecessor;
        // Version zero is the simulation oracle's default(TValue) sentinel (null for
        // this reference type). The concrete empty map is the first application state.
        Version = predecessor is null ? 1 : checked(predecessor.Version + 1);
    }

    public static RegisterState Empty { get; } =
        new(ImmutableSortedDictionary.Create<string, RegisterValue>(StringComparer.Ordinal), null);

    public int Version { get; }
    public IEnumerable<string> Keys => _values.Keys;
    public RegisterValue Get(string key) =>
        _values.TryGetValue(key, out var value) ? value : new(null, null);

    public RegisterState Set(string key, RegisterValue value) => new(_values.SetItem(key, value), this);

    // Observers may skip intervening commits, so verify ancestry rather than requiring +1.
    public bool IsValidSuccessorTo(RegisterState predecessor)
    {
        var ancestor = _predecessor;
        while (ancestor is not null && ancestor.Version > predecessor.Version)
            ancestor = ancestor._predecessor;
        return ancestor is not null && ancestor.Equals(predecessor);
    }

    public bool Equals(RegisterState? other) => ReferenceEquals(this, other)
        || other is not null && Version == other.Version && _values.SequenceEqual(other._values);
    public override bool Equals(object? obj) => obj is RegisterState other && Equals(other);
    public override int GetHashCode() => Version;
    public override string ToString() => $"register-revision-{Version}";
}
