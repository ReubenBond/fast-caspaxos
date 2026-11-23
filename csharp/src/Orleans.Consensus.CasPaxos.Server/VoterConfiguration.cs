using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Consensus.CasPaxos.Server;

/// <summary>Lifecycle state of a voter configuration stored in <see cref="VoterConfiguration.RegisterKey"/>.</summary>
public enum VoterConfigurationState : byte
{
    Pending = 0,
    Active = 1,
}

/// <summary>Canonical identity and transport endpoint for a single voter.</summary>
public sealed record VoterDescriptor(string Id, string Endpoint = "")
{
    public void Validate()
    {
        ArgumentException.ThrowIfNullOrEmpty(Id);
        ArgumentNullException.ThrowIfNull(Endpoint);
    }
}

/// <summary>A configuration record stored in the reserved configuration register.</summary>
public sealed record VoterConfiguration(
    long Number,
    ImmutableArray<VoterDescriptor> Voters,
    VoterConfigurationState State)
{
    /// <summary>Reserved register key storing the authoritative voter configuration.</summary>
    public const string RegisterKey = "__caspaxos/config";

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(Number, 1);
        if (Voters.IsDefaultOrEmpty)
        {
            throw new ArgumentException("A voter configuration must contain at least one voter.", nameof(Voters));
        }

        if (State is not (VoterConfigurationState.Pending or VoterConfigurationState.Active))
        {
            throw new ArgumentOutOfRangeException(nameof(State), State, "Unknown voter configuration state.");
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var voter in Voters)
        {
            ArgumentNullException.ThrowIfNull(voter);
            voter.Validate();
            if (!seen.Add(voter.Id))
            {
                throw new ArgumentException($"Duplicate voter identity '{voter.Id}' in configuration {Number}.", nameof(Voters));
            }
        }
    }

    public ImmutableArray<VoterDescriptor> CanonicalVoters =>
        [.. Voters.OrderBy(static v => v.Id, StringComparer.Ordinal)];

    public bool HasSameVoters(ImmutableArray<VoterDescriptor> other)
    {
        if (Voters.Length != other.Length) return false;
        var left = CanonicalVoters;
        var right = other.OrderBy(static v => v.Id, StringComparer.Ordinal).ToArray();
        for (var i = 0; i < left.Length; i++)
        {
            if (!string.Equals(left[i].Id, right[i].Id, StringComparison.Ordinal)
                || !string.Equals(left[i].Endpoint, right[i].Endpoint, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Computes the canonical SHA-256 digest of a bootstrap voter set <c>V(1)</c> for startup agreement checks.
    /// </summary>
    public static ByteString ComputeBootstrapDigest(IEnumerable<string> voterIds)
    {
        ArgumentNullException.ThrowIfNull(voterIds);
        var sorted = voterIds.OrderBy(static id => id, StringComparer.Ordinal).ToArray();
        if (sorted.Length == 0)
        {
            throw new ArgumentException("Bootstrap voter set must contain at least one voter.", nameof(voterIds));
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lengthPrefix = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, sorted.Length);
        hash.AppendData(lengthPrefix);

        string? previous = null;
        foreach (var id in sorted)
        {
            ArgumentException.ThrowIfNullOrEmpty(id);
            if (previous is not null && string.Equals(previous, id, StringComparison.Ordinal))
            {
                throw new ArgumentException($"Duplicate voter identity '{id}' in bootstrap voter set.", nameof(voterIds));
            }

            previous = id;
            var utf8 = Encoding.UTF8.GetBytes(id);
            BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, utf8.Length);
            hash.AppendData(lengthPrefix);
            hash.AppendData(utf8);
        }

        return new ByteString(hash.GetHashAndReset());
    }
}

/// <summary>Canonical binary codec for <see cref="VoterConfiguration"/> register values.</summary>
public static class VoterConfigurationEncoding
{
    private static ReadOnlySpan<byte> Magic => "FCC1"u8;

    public static ByteString Encode(VoterConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        var voters = configuration.CanonicalVoters;
        var encodedVoters = new (byte[] Id, byte[] Endpoint)[voters.Length];
        var totalSize = 4 + 8 + 1 + 4;
        for (var i = 0; i < voters.Length; i++)
        {
            var idBytes = Encoding.UTF8.GetBytes(voters[i].Id);
            var endpointBytes = Encoding.UTF8.GetBytes(voters[i].Endpoint);
            encodedVoters[i] = (idBytes, endpointBytes);
            totalSize = checked(totalSize + 4 + idBytes.Length + 4 + endpointBytes.Length);
        }

        var buffer = new byte[totalSize];
        var offset = 0;
        Magic.CopyTo(buffer);
        offset += 4;
        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(offset), configuration.Number);
        offset += 8;
        buffer[offset++] = (byte)configuration.State;
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(offset), encodedVoters.Length);
        offset += 4;
        foreach (var (idBytes, endpointBytes) in encodedVoters)
        {
            BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(offset), idBytes.Length);
            offset += 4;
            idBytes.CopyTo(buffer, offset);
            offset += idBytes.Length;
            BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(offset), endpointBytes.Length);
            offset += 4;
            endpointBytes.CopyTo(buffer, offset);
            offset += endpointBytes.Length;
        }

        return new ByteString(buffer);
    }

    public static VoterConfiguration Decode(ByteString payload)
    {
        if (payload.IsDefault)
        {
            throw new InvalidDataException("Configuration payload is uninitialized.");
        }

        var bytes = payload.Span;
        if (bytes.Length < 17 || !bytes[..4].SequenceEqual(Magic))
        {
            throw new InvalidDataException("Configuration payload has an invalid header.");
        }

        var offset = 4;
        var number = BinaryPrimitives.ReadInt64BigEndian(bytes.Slice(offset, 8));
        offset += 8;
        var rawState = bytes[offset++];
        if (rawState > (byte)VoterConfigurationState.Active)
        {
            throw new InvalidDataException($"Unknown configuration state '{rawState}'.");
        }

        var state = (VoterConfigurationState)rawState;
        var count = BinaryPrimitives.ReadInt32BigEndian(bytes.Slice(offset, 4));
        offset += 4;
        if (number < 1 || count <= 0)
        {
            throw new InvalidDataException("Configuration payload has invalid number or voter count.");
        }

        var voters = ImmutableArray.CreateBuilder<VoterDescriptor>(count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var id = ReadString(bytes, ref offset);
            var endpoint = ReadString(bytes, ref offset);
            if (id.Length == 0 || !seen.Add(id))
            {
                throw new InvalidDataException("Configuration payload contains an empty or duplicate voter id.");
            }

            voters.Add(new VoterDescriptor(id, endpoint));
        }

        if (offset != bytes.Length)
        {
            throw new InvalidDataException("Configuration payload has trailing bytes.");
        }

        return new VoterConfiguration(number, voters.MoveToImmutable(), state);
    }

    private static string ReadString(ReadOnlySpan<byte> bytes, ref int offset)
    {
        if (offset + 4 > bytes.Length)
        {
            throw new InvalidDataException("Configuration payload is truncated.");
        }

        var length = BinaryPrimitives.ReadInt32BigEndian(bytes.Slice(offset, 4));
        offset += 4;
        if (length < 0 || offset + length > bytes.Length)
        {
            throw new InvalidDataException("Configuration payload string length is out of bounds.");
        }

        var value = Encoding.UTF8.GetString(bytes.Slice(offset, length));
        offset += length;
        return value;
    }
}

/// <summary>Thrown when a Choose step loses to a concurrent operator's different next configuration.</summary>
public sealed class ReconfigurationConflictException(VoterConfiguration winningConfiguration)
    : InvalidOperationException(
        $"Configuration {winningConfiguration.Number} was already chosen with a different voter set.")
{
    public VoterConfiguration WinningConfiguration { get; } = winningConfiguration;
}
