namespace FastCASPaxos.Simulation;

public readonly record struct FastCasNodeId
{
    public FastCasNodeId(FastCasParticipantRole role, int ordinal)
    {
        if (role == FastCasParticipantRole.Client)
        {
            throw new ArgumentException("Node ids are only valid for proposer or acceptor nodes.", nameof(role));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(ordinal, 1);
        Role = role;
        Ordinal = ordinal;
    }

    public FastCasParticipantRole Role { get; }

    public int Ordinal { get; }

    public FastCasAddress Address => new(Role, Ordinal);

    public override string ToString() => Address.ToString();
}
