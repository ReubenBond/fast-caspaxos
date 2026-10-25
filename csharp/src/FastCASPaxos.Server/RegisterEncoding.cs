using System.Buffers.Binary;
using FastCASPaxos.Server.Diagnostics;

namespace FastCASPaxos.Server;

internal static class RegisterEncoding
{
    // The envelope belongs to the register layer; protocol endpoints continue to store opaque bytes.
    private static ReadOnlySpan<byte> Header => "FCR2"u8;
    public static ByteString Absent { get; } = new("FCR2\0"u8);

    public static void ValidateVersion(ByteString version)
    {
        if (!RegisterVersion.IsValid(version))
        {
            throw new ArgumentException("A register version must be a token returned by this register cluster.", nameof(version));
        }
    }

    public static ByteString Encode(ByteString version, ByteString requestIdentity, ByteString payload)
    {
        var bytes = new byte[checked(29 + payload.Length)];
        Header.CopyTo(bytes);
        bytes[4] = 1;
        version.Span.CopyTo(bytes.AsSpan(5));
        requestIdentity.Span.CopyTo(bytes.AsSpan(13));
        payload.Span.CopyTo(bytes.AsSpan(29));
        return new ByteString(bytes);
    }

    public static RegisterValue Decode(ByteString value)
    {
        var bytes = value.Span;
        var version = DecodeVersion(bytes, out var payloadOffset);
        return version is null ? new RegisterValue(null, null) : new RegisterValue(new ByteString(bytes[payloadOffset..]), version);
    }

    internal static RegisterPreview Preview(ByteString value)
    {
        try
        {
            var version = DecodeVersion(value.Span, out var payloadOffset);
            return version is null ? new("register", false, null, null)
                : new("register", true, Convert.ToBase64String(version.Value.Span), PayloadPreview.From(value.Span[payloadOffset..]));
        }
        catch (InvalidDataException)
        {
            return new("opaque", null, null, null);
        }
    }

    private static ByteString? DecodeVersion(ReadOnlySpan<byte> bytes, out int payloadOffset)
    {
        payloadOffset = 29;
        if (bytes.Length < 5)
        {
            throw new InvalidDataException("Accepted bytes are not a register envelope.");
        }

        var legacy = bytes[..4].SequenceEqual("FCR1"u8);
        if (legacy) payloadOffset = 13;
        else if (!bytes[..4].SequenceEqual(Header))
            throw new InvalidDataException("Accepted bytes are not a register envelope.");

        if (bytes[4] == 0 && bytes.Length == 5)
        {
            return null;
        }

        if (bytes[4] != 1 || bytes.Length < payloadOffset)
        {
            throw new InvalidDataException("The accepted register envelope is malformed.");
        }

        var version = new ByteString(bytes.Slice(5, 8));
        try { ValidateVersion(version); }
        catch (ArgumentException exception) { throw new InvalidDataException("The accepted register version is malformed.", exception); }
        if (legacy)
        {
            if (BinaryPrimitives.ReadInt32BigEndian(bytes[5..]) <= 0
                || BinaryPrimitives.ReadInt32BigEndian(bytes[9..]) <= 0)
                throw new InvalidDataException("The legacy register version is malformed.");
        }
        else if (BinaryPrimitives.ReadInt32BigEndian(bytes[13..]) <= 0
            || BinaryPrimitives.ReadInt32BigEndian(bytes[17..]) <= 0
            || BinaryPrimitives.ReadInt64BigEndian(bytes[21..]) <= 0)
        {
            throw new InvalidDataException("The register request identity is malformed.");
        }
        return version;
    }
}
