namespace FastCASPaxos.Model;

public interface IVersionedValue<TValue, TSelf> where TSelf : IVersionedValue<TValue, TSelf>
{
    int Version { get; }
    TValue Value { get; }
    bool IsValidSuccessorTo(TSelf predecessor);
    bool Equals(TSelf other);
}

public static class VersionedValueExtensions
{
    public static int GetVersionOrDefault<TValue, TSelf>(this IVersionedValue<TValue, TSelf> value)
        where TSelf : IVersionedValue<TValue, TSelf>
    {
        if (value is null) return 0;
        return value.Version;
    }
}
