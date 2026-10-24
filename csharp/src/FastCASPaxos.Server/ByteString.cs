namespace FastCASPaxos.Server;

/// <summary>Owned, immutable bytes with structural equality for protocol retries.</summary>
/// <remarks>The default value represents empty bytes. Nullable ByteString represents absence.</remarks>
public readonly struct ByteString : IEquatable<ByteString>
{
    private readonly byte[]? _bytes;

    public ByteString(ReadOnlySpan<byte> bytes) => _bytes = bytes.ToArray();

    public static ByteString Empty => default;

    public int Length => Span.Length;

    internal ReadOnlySpan<byte> Span => _bytes;

    /// <summary>Returns a caller-owned copy.</summary>
    public byte[] ToArray() => Span.ToArray();

    public bool Equals(ByteString other) => Span.SequenceEqual(other.Span);

    public override bool Equals(object? obj) => obj is ByteString other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(Span);
        return hash.ToHashCode();
    }

    public static bool operator ==(ByteString left, ByteString right) => left.Equals(right);

    public static bool operator !=(ByteString left, ByteString right) => !left.Equals(right);
}
