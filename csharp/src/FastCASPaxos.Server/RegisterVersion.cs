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
        return bytes.Length == 8 && BinaryPrimitives.ReadInt32BigEndian(bytes) > 0
            && BinaryPrimitives.ReadInt32BigEndian(bytes[4..]) > 0;
    }
}
