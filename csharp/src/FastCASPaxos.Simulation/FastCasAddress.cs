using System.Globalization;

namespace FastCASPaxos.Simulation;

public readonly record struct FastCasAddress
{
    public FastCasAddress(FastCasParticipantRole role, int ordinal)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ordinal);
        Role = role;
        Ordinal = ordinal;
    }

    public FastCasParticipantRole Role { get; }

    public int Ordinal { get; }

    public static FastCasAddress Client(int ordinal = 0) => new(FastCasParticipantRole.Client, ordinal);

    public static FastCasAddress Proposer(int ordinal) => new(FastCasParticipantRole.Proposer, ordinal);

    public static FastCasAddress Acceptor(int ordinal) => new(FastCasParticipantRole.Acceptor, ordinal);

    public string NetworkAddress => string.Create(CultureInfo.InvariantCulture, $"{ToSegment(Role)}-{Ordinal}");

    public bool IsNodeAddress => Role is FastCasParticipantRole.Proposer or FastCasParticipantRole.Acceptor;

    public override string ToString() => NetworkAddress;

    private static string ToSegment(FastCasParticipantRole role) => role switch
    {
        FastCasParticipantRole.Client => "client",
        FastCasParticipantRole.Proposer => "proposer",
        FastCasParticipantRole.Acceptor => "acceptor",
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unknown FastCASPaxos participant role."),
    };
}
