namespace Clockwork;

/// <summary>
/// A seeded random number generator for deterministic simulation testing.
/// Wraps System.Random with a known seed for reproducibility.
/// </summary>
/// <remarks>
/// This class intentionally uses System.Random for reproducibility in deterministic simulation testing.
/// It is NOT intended for security-sensitive operations.
/// </remarks>
/// <remarks>
/// Creates a new simulation random with the specified seed.
/// </remarks>
/// <param name="seed">The seed for reproducible random sequences.</param>
#pragma warning disable CA5394 // Do not use insecure randomness - intentionally deterministic for simulation testing
public sealed class SimulationRandom(int seed) : Random(seed)
{
    /// <summary>
    /// Gets the seed used to initialize this random instance.
    /// </summary>
    public int Seed { get; } = seed;

    /// <summary>
    /// Returns a random TimeSpan between zero and maxValue.
    /// </summary>
    public TimeSpan NextTimeSpan(TimeSpan maxValue)
    {
        var ticks = (long)(NextDouble() * maxValue.Ticks);
        return TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// Returns a random TimeSpan between minValue and maxValue.
    /// </summary>
    public TimeSpan NextTimeSpan(TimeSpan minValue, TimeSpan maxValue)
    {
        var range = maxValue.Ticks - minValue.Ticks;
        var ticks = minValue.Ticks + (long)(NextDouble() * range);
        return TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// Shuffles the elements of the list in place.
    /// </summary>
    public void Shuffle<T>(IList<T> list)
    {
        ArgumentNullException.ThrowIfNull(list);
        var n = list.Count;
        while (n > 1)
        {
            n--;
            var k = Next(n + 1);
            (list[k], list[n]) = (list[n], list[k]);
        }
    }

    /// <summary>
    /// Returns a random element from the list.
    /// </summary>
    public T Choose<T>(IList<T> list)
    {
        ArgumentNullException.ThrowIfNull(list);
        if (list.Count == 0)
            throw new ArgumentException("List cannot be empty", nameof(list));
        return list[Next(list.Count)];
    }

    /// <summary>
    /// Returns true with the specified probability (0.0 to 1.0).
    /// </summary>
    public bool Chance(double probability)
    {
        if (probability <= 0) return false;
        if (probability >= 1) return true;
        return NextDouble() < probability;
    }

    /// <summary>
    /// Creates a new SimulationRandom derived from this one.
    /// Useful for creating independent random streams.
    /// </summary>
    public SimulationRandom Fork() => new(Next());

    /// <summary>
    /// Returns a deterministic GUID based on the random sequence.
    /// </summary>
    public Guid NextGuid()
    {
        var bytes = new byte[16];
        NextBytes(bytes);
        return new Guid(bytes);
    }
}
#pragma warning restore CA5394
