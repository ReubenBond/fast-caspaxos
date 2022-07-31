using System;

namespace FastCASPaxos.Model;

public readonly struct Ballot : IComparable<Ballot>
{
    /// <summary>
    /// The round number.
    /// </summary>
    public readonly int Round;

    /// <summary>
    /// The unique identifier of the proposer.
    /// </summary>
    public readonly int Proposer;

    public Ballot(int round, int id)
    {
        Round = round;
        Proposer = id;
    }

    public Ballot Successor(int id) => new(Round + 1, id);

    public Ballot AdvanceTo(Ballot other, int id) => new(Math.Max(Round, other.Round), id);

    public static Ballot Zero => default;

    public bool IsZero => Equals(Zero);

    public bool IsFastRoundBallot => Proposer == 0;

    public bool IsClassicRoundBallot => Proposer != 0;

    /// <inheritdoc />
    public override string ToString() => IsZero ? $"{nameof(Ballot)}(ø)" : $"{nameof(Ballot)}({Round}.{Proposer})";

    public bool Equals(Ballot other) => Round == other.Round && Proposer == other.Proposer;

    /// <inheritdoc />
    public override bool Equals(object obj) => obj is Ballot ballot && Equals(ballot);

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

    public static bool operator ==(Ballot left, Ballot right) => left.Equals(right);

    public static bool operator !=(Ballot left, Ballot right) => !left.Equals(right);

    public static bool operator <(Ballot left, Ballot right) => left.CompareTo(right) < 0;

    public static bool operator >(Ballot left, Ballot right) => left.CompareTo(right) > 0;

    public static bool operator <=(Ballot left, Ballot right) => left.CompareTo(right) <= 0;

    public static bool operator >=(Ballot left, Ballot right) => left.CompareTo(right) >= 0;

    public override int GetHashCode() => HashCode.Combine(Round, Proposer);
}
