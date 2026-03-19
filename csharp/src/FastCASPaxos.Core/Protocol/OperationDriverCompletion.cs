using FastCASPaxos.Model;
using System.Collections.Immutable;

namespace FastCASPaxos.Protocol;

public sealed class OperationDriverCompletion<TValue, TRoute>
    where TRoute : notnull
{
    public IReadOnlyDictionary<TRoute, TValue> FinalValues { get; init; } = ImmutableDictionary<TRoute, TValue>.Empty;

    public TValue LatestValue { get; init; } = default!;

    public bool FinalValuesAgree { get; init; }

    public void EnsureExpectedValue(TValue expectedValue, Func<TValue, string>? printValue = null)
    {
        if (!FinalValues.Any())
        {
            throw new InvalidOperationException("Cannot validate an expected value before any proposer has produced a response.");
        }

        var actualValue = LatestValue;
        if (!EqualityComparer<TValue>.Default.Equals(expectedValue, actualValue))
        {
            var expected = printValue?.Invoke(expectedValue) ?? expectedValue?.ToString();
            var actual = printValue?.Invoke(actualValue) ?? actualValue?.ToString();
            throw new InvalidOperationException($"ERROR: final value \"{actual}\" does not equal expected value \"{expected}\"");
        }
    }
}
