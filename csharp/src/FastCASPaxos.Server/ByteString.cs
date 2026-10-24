namespace FastCASPaxos.Server;

/// <summary>Owned, immutable bytes with structural equality for protocol retries.</summary>
/// <remarks>
/// The default value represents missing, uninitialized bytes. Constructed empty bytes are initialized.
/// Byte access requires an initialized value; nullable APIs use null for absence.
/// </remarks>
public readonly struct ByteString : IEquatable<ByteString>
{
    private readonly byte[]? _bytes;

    public ByteString(ReadOnlySpan<byte> bytes) => _bytes = bytes.ToArray();

    public static ByteString Empty { get; } = new([]);

    public bool IsDefault => _bytes is null;

    public bool IsEmpty => Span.IsEmpty;

    public int Length => Span.Length;

    internal ReadOnlySpan<byte> Span =>
        _bytes ?? throw new InvalidOperationException("The byte string is uninitialized.");

    /// <summary>Returns a caller-owned copy.</summary>
    public byte[] ToArray() => Span.ToArray();

    public bool Equals(ByteString other) =>
        ReferenceEquals(_bytes, other._bytes)
        || _bytes is not null && other._bytes is not null && _bytes.AsSpan().SequenceEqual(other._bytes);

    public override bool Equals(object? obj) => obj is ByteString other && Equals(other);

    public override int GetHashCode()
    {
        if (_bytes is null) return 0;
        var hash = new HashCode();
        hash.AddBytes(_bytes);
        return hash.ToHashCode();
    }

    public static bool operator ==(ByteString left, ByteString right) => left.Equals(right);

    public static bool operator !=(ByteString left, ByteString right) => !left.Equals(right);
}
