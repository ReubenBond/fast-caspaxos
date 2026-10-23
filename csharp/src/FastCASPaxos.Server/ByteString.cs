namespace FastCASPaxos.Server;

/// <summary>Owned, immutable bytes with structural equality for protocol retries.</summary>
public sealed class ByteString : IEquatable<ByteString>
{
    private readonly byte[] _bytes;

    public ByteString(ReadOnlySpan<byte> bytes) => _bytes = bytes.ToArray();

    public static ByteString Empty { get; } = new([]);

    public int Length => _bytes.Length;

    internal ReadOnlySpan<byte> Span => _bytes;

    /// <summary>Returns a caller-owned copy.</summary>
    public byte[] ToArray() => (byte[])_bytes.Clone();

    public bool Equals(ByteString? other) =>
        ReferenceEquals(this, other) || other is not null && _bytes.AsSpan().SequenceEqual(other._bytes);

    public override bool Equals(object? obj) => obj is ByteString other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(_bytes);
        return hash.ToHashCode();
    }
}
