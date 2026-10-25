using System.Buffers.Binary;

namespace FastCASPaxos.Server;

/// <summary>Validates the shape of opaque native register versions.</summary>
public static class RegisterVersion
{
    /// <summary>
    /// Returns whether the bytes have the native version shape. Equality against an observed
    /// register version establishes the CAS condition; a valid shape alone establishes syntax.
    /// </summary>
    public static bool IsValid(ByteString version)
    {
        if (version.IsDefault) return false;
        var bytes = version.Span;
        return bytes.Length == 8 && BinaryPrimitives.ReadUInt64BigEndian(bytes) != 0;
    }

    internal static ByteString Next(ByteString? current)
    {
        var revision = current is { } version ? BinaryPrimitives.ReadUInt64BigEndian(version.Span) : 0;
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, checked(revision + 1));
        return new ByteString(bytes);
    }
}
