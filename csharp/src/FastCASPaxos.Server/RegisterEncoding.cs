using System.Buffers.Binary;
using FastCASPaxos.Model;

namespace FastCASPaxos.Server;

internal static class RegisterEncoding
{
    // The envelope belongs to the register layer; protocol endpoints continue to store opaque bytes.
    private static ReadOnlySpan<byte> Header => "FCR1"u8;
    public static ByteString Absent { get; } = new("FCR1\0"u8);

    public static ByteString Version(Ballot allocation)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt32BigEndian(bytes, allocation.Proposer);
        BinaryPrimitives.WriteInt32BigEndian(bytes[4..], allocation.Round);
        return new ByteString(bytes);
    }

    public static void ValidateVersion(ByteString version)
    {
        var bytes = version.ToArray();
        if (bytes.Length != 8 || BinaryPrimitives.ReadInt32BigEndian(bytes) <= 0
            || BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(4)) <= 0)
        {
            throw new ArgumentException("A register version must be a token returned by this register cluster.", nameof(version));
        }
    }

    public static ByteString Encode(ByteString version, ByteString payload)
    {
        var bytes = new byte[checked(13 + payload.Length)];
        Header.CopyTo(bytes);
        bytes[4] = 1;
        version.ToArray().CopyTo(bytes, 5);
        payload.ToArray().CopyTo(bytes, 13);
        return new ByteString(bytes);
    }

    public static RegisterValue Decode(ByteString value)
    {
        var bytes = value.ToArray().AsSpan();
        if (bytes.Length < 5 || !bytes[..4].SequenceEqual(Header))
        {
            throw new InvalidDataException("Accepted bytes are not a register envelope.");
        }

        if (bytes[4] == 0 && bytes.Length == 5)
        {
            return new RegisterValue(null, null);
        }

        if (bytes[4] != 1 || bytes.Length < 13)
        {
            throw new InvalidDataException("The accepted register envelope is malformed.");
        }

        var version = new ByteString(bytes.Slice(5, 8));
        try { ValidateVersion(version); }
        catch (ArgumentException exception) { throw new InvalidDataException("The accepted register version is malformed.", exception); }
        return new RegisterValue(new ByteString(bytes[13..]), version);
    }
}
