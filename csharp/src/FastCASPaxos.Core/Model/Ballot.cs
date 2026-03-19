namespace FastCASPaxos.Model;

/// <summary>
/// Identifies a protocol epoch as <c>(round, proposer)</c>.
/// A proposer id of <c>0</c> denotes the shared fast-round ballot; non-zero proposers own classic rounds.
/// </summary>
public readonly struct Ballot(int round, int id) : IComparable<Ballot>
{
    /// <summary>
    /// The round / epoch number.
    /// </summary>
    public readonly int Round = round;

    /// <summary>
    /// The unique identifier of the proposer.
    /// A proposer id of 0 represents a fast round ballot.
    /// </summary>
    public readonly int Proposer = id;

    /// <summary>
    /// Creates the initial shared fast-round ballot.
    /// </summary>
    public static Ballot InitialFast() => new(1, 0);

    /// <summary>
    /// Creates the initial classic ballot owned by a specific proposer.
    /// </summary>
    public static Ballot InitialClassic(int proposer)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(proposer, 0);
        return new(1, proposer);
    }

    /// <summary>
    /// Advances to the next round for the supplied proposer.
    /// </summary>
    public Ballot NextRound(int proposer) => new(Round + 1, proposer);

    /// <summary>
    /// The zero ballot used before any prepare or accept has succeeded.
    /// </summary>
    public static Ballot Zero => default;

    /// <summary>
    /// Gets whether this ballot is the zero/uninitialized value.
    /// </summary>
    public bool IsZero => Equals(Zero);

    /// <summary>
    /// Gets whether this ballot is the shared fast-round ballot.
    /// </summary>
    public bool IsFastRoundBallot => Proposer == 0;

    /// <summary>
    /// Gets whether this ballot is a proposer-owned classic ballot.
    /// </summary>
    public bool IsClassicRoundBallot => Proposer != 0;

    /// <inheritdoc />
    public override string ToString() => IsZero ? $"{nameof(Ballot)}(ø)" : $"{nameof(Ballot)}(r{Round}, p{Proposer})";

    public bool Equals(Ballot other) => Round == other.Round && Proposer == other.Proposer;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is Ballot ballot && Equals(ballot);

    /// <inheritdoc />
    public int CompareTo(Ballot other)
    {
        var roundComparison = Round - other.Round;
        if (roundComparison != 0)
        {
            return roundComparison;
        }

        return Proposer - other.Proposer;
    }

    public static Ballot Max(Ballot left, Ballot right) => left >= right ? left : right;

    public static bool operator ==(Ballot left, Ballot right) => left.Equals(right);

    public static bool operator !=(Ballot left, Ballot right) => !left.Equals(right);

    public static bool operator <(Ballot left, Ballot right) => left.CompareTo(right) < 0;

    public static bool operator >(Ballot left, Ballot right) => left.CompareTo(right) > 0;

    public static bool operator <=(Ballot left, Ballot right) => left.CompareTo(right) <= 0;

    public static bool operator >=(Ballot left, Ballot right) => left.CompareTo(right) >= 0;

    public override int GetHashCode() => HashCode.Combine(Round, Proposer);
}
