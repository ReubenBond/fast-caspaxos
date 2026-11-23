namespace Orleans.Consensus.CasPaxos.Model;

/// <summary>
/// Identifies a configuration-qualified protocol ballot as <c>(configuration, round, proposer)</c>.
/// <list type="bullet">
/// <item><c>(0, 0, 0)</c> is the zero ballot.</item>
/// <item><c>(c, 0, 0)</c> (<c>c &gt;= 1</c>) is an acceptor floor ballot fencing configurations <c>&lt; c</c>.</item>
/// <item><c>(c, r, 0)</c> (<c>c &gt;= 1, r &gt;= 1</c>) is a shared fast-round ballot in configuration <c>c</c>.</item>
/// <item><c>(c, r, p)</c> (<c>c &gt;= 1, r &gt;= 1, p &gt;= 1</c>) is a proposer-owned classic ballot in configuration <c>c</c>.</item>
/// </list>
/// </summary>
public readonly struct Ballot(long configuration, int round, int id) : IComparable<Ballot>, IEquatable<Ballot>
{
    /// <summary>
    /// Creates a ballot in bootstrap configuration <c>1</c> (or <see cref="Zero"/> when both <paramref name="round"/>
    /// and <paramref name="id"/> are <c>0</c>).
    /// </summary>
    public Ballot(int round, int id)
        : this(round == 0 && id == 0 ? 0L : 1L, round, id)
    {
    }

    /// <summary>
    /// The 1-based configuration number (<c>0</c> exclusively for <see cref="Zero"/>).
    /// </summary>
    public long Configuration { get; } = configuration;

    /// <summary>
    /// The round / epoch number (<c>0</c> for zero and floor ballots).
    /// </summary>
    public int Round { get; } = round;

    /// <summary>
    /// The unique identifier of the proposer.
    /// A proposer id of 0 represents a fast round ballot (when <see cref="Round"/> &gt; 0) or a floor/zero ballot.
    /// </summary>
    public int Proposer { get; } = id;

    /// <summary>
    /// Creates a floor ballot <c>(configuration, 0, 0)</c> fencing all configurations below <paramref name="configuration"/>.
    /// </summary>
    public static Ballot Floor(long configuration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(configuration, 0);
        return new(configuration, 0, 0);
    }

    /// <summary>
    /// Creates the initial shared fast-round ballot <c>(1, 1, 0)</c> in bootstrap configuration <c>1</c>.
    /// </summary>
    public static Ballot InitialFast() => new(1, 1, 0);

    /// <summary>
    /// Creates the initial classic ballot <c>(1, 1, proposer)</c> in bootstrap configuration <c>1</c>.
    /// </summary>
    public static Ballot InitialClassic(int proposer) => InitialClassic(1, proposer);

    /// <summary>
    /// Creates the initial classic ballot <c>(configuration, 1, proposer)</c> owned by a specific proposer.
    /// </summary>
    public static Ballot InitialClassic(long configuration, int proposer)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(configuration, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(proposer, 0);
        return new(configuration, 1, proposer);
    }

    /// <summary>
    /// Advances to the next round within the same configuration for the supplied proposer.
    /// </summary>
    public Ballot NextRound(int proposer) =>
        new(Configuration == 0 ? 1L : Configuration, checked(Round + 1), proposer);

    /// <summary>
    /// The zero ballot <c>(0, 0, 0)</c> used before any prepare or accept has succeeded.
    /// </summary>
    public static Ballot Zero => default;

    /// <summary>
    /// Gets whether this ballot is the zero/uninitialized value <c>(0, 0, 0)</c>.
    /// </summary>
    public bool IsZero => Configuration == 0 && Round == 0 && Proposer == 0;

    /// <summary>
    /// Gets whether this ballot is an acceptor floor ballot <c>(c &gt;= 1, 0, 0)</c>.
    /// </summary>
    public bool IsFloorBallot => Configuration >= 1 && Round == 0 && Proposer == 0;

    /// <summary>
    /// Gets whether this ballot is a shared fast-round ballot <c>(c &gt;= 1, r &gt;= 1, 0)</c>.
    /// </summary>
    public bool IsFastRoundBallot => Configuration >= 1 && Round >= 1 && Proposer == 0;

    /// <summary>
    /// Gets whether this ballot is a proposer-owned classic ballot <c>(c &gt;= 1, r &gt;= 1, p &gt; 0)</c>.
    /// </summary>
    public bool IsClassicRoundBallot => Configuration >= 1 && Round >= 1 && Proposer > 0;

    /// <inheritdoc />
    public override string ToString() =>
        IsZero ? $"{nameof(Ballot)}(ø)"
        : IsFloorBallot ? $"{nameof(Ballot)}(c{Configuration}, floor)"
        : Configuration == 1 ? $"{nameof(Ballot)}(r{Round}, p{Proposer})"
        : $"{nameof(Ballot)}(c{Configuration}, r{Round}, p{Proposer})";

    public bool Equals(Ballot other) =>
        Configuration == other.Configuration && Round == other.Round && Proposer == other.Proposer;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is Ballot ballot && Equals(ballot);

    /// <inheritdoc />
    public int CompareTo(Ballot other)
    {
        var configComparison = Configuration.CompareTo(other.Configuration);
        if (configComparison != 0)
        {
            return configComparison;
        }

        var roundComparison = Round.CompareTo(other.Round);
        if (roundComparison != 0)
        {
            return roundComparison;
        }

        return Proposer.CompareTo(other.Proposer);
    }

    public static Ballot Max(Ballot left, Ballot right) => left >= right ? left : right;

    public static bool operator ==(Ballot left, Ballot right) => left.Equals(right);

    public static bool operator !=(Ballot left, Ballot right) => !left.Equals(right);

    public static bool operator <(Ballot left, Ballot right) => left.CompareTo(right) < 0;

    public static bool operator >(Ballot left, Ballot right) => left.CompareTo(right) > 0;

    public static bool operator <=(Ballot left, Ballot right) => left.CompareTo(right) <= 0;

    public static bool operator >=(Ballot left, Ballot right) => left.CompareTo(right) >= 0;

    public override int GetHashCode() => HashCode.Combine(Configuration, Round, Proposer);
}
