using FastCASPaxos.Model;
using FastCASPaxos.Server;
using ProtoBytes = Google.Protobuf.ByteString;

namespace FastCASPaxos.Grpc;

internal static class WireConvert
{
    public static string Text(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        for (var i = 0; i < value.Length; i++)
        {
            if (!char.IsSurrogate(value[i])) continue;
            if (!char.IsHighSurrogate(value[i]) || ++i == value.Length || !char.IsLowSurrogate(value[i]))
                throw new ArgumentException("Keys, prefixes, and cursors must contain well-formed UTF-16.");
        }
        return value;
    }

    public static string Key(string value, bool present)
    {
        Require(present, "A key is required; an explicitly present empty key is valid.");
        return Text(value);
    }

    public static void Require(bool condition, string message)
    {
        if (!condition) throw new ArgumentException(message);
    }

    public static Ballot Ballot(Wire.Ballot? value, bool allowZero = false)
    {
        Require(value is not null, "A ballot is required.");
        return Validate(new Ballot(value!.Round, value.ProposerId), allowZero);
    }

    public static Ballot Validate(Ballot value, bool allowZero = false)
    {
        Require((allowZero && value.IsZero) || (value.Round > 0 && value.Proposer >= 0),
            "Ballots require a positive round and nonnegative proposer id.");
        return value;
    }

    public static Wire.Ballot ToWire(Ballot value, bool allowZero = false)
    {
        Validate(value, allowZero);
        return new() { Round = value.Round, ProposerId = value.Proposer };
    }

    public static ProtoBytes ToWire(ByteString value)
    {
        Require(!value.IsDefault, "Initialized bytes are required.");
        return ProtoBytes.CopyFrom(value.ToArray());
    }

    public static ByteString Bytes(ProtoBytes value, bool present)
    {
        Require(present, "A bytes field is required; an explicitly present empty payload is valid.");
        return new(value.Span);
    }

    public static Wire.AcceptedState? Accepted(Ballot ballot, ByteString? value)
    {
        if (value is null)
        {
            Require(ballot.IsZero, "An unaccepted record must have the zero ballot.");
            return null;
        }
        return new() { Ballot = ToWire(ballot), Value = ToWire(value.Value) };
    }

    public static (Ballot Ballot, ByteString? Value) Accepted(Wire.AcceptedState? value) =>
        value is null ? (Model.Ballot.Zero, null) : (Ballot(value.Ballot), Bytes(value.Value, value.HasValue));

    public static Wire.RegisterState ToWire(RegisterValue value)
    {
        if (value.Version is null)
        {
            Require(value.Value is null, "An absent register must have absent value and version.");
            return new() { Absent = new() };
        }
        Require(value.Value is not null, "A present register must have both value and version.");
        return new() { Present = new() { Value = ToWire(value.Value!.Value), Version = ToWire(value.Version.Value) } };
    }

    public static RegisterValue Register(Wire.RegisterState? value) => value?.StateCase switch
    {
        Wire.RegisterState.StateOneofCase.Absent => new(null, null),
        Wire.RegisterState.StateOneofCase.Present => new(
            Bytes(value.Present.Value, value.Present.HasValue),
            Bytes(value.Present.Version, value.Present.HasVersion)),
        _ => throw new ArgumentException("An explicit absent or present register state is required."),
    };

    public static Wire.ListKeysRequest ListRequest(string prefix, string? afterKey, int limit)
    {
        Require(limit > 0, "The page limit must be positive.");
        var result = new Wire.ListKeysRequest { Prefix = Text(prefix), Limit = limit };
        if (afterKey is not null) result.AfterKey = Text(afterKey);
        return result;
    }

    public static AcceptedDigest Digest(Wire.AcceptedDigest? value)
    {
        Require(value is not null, "An accepted digest is required.");
        var result = new AcceptedDigest(Ballot(value!.Ballot), new(value.Sha256.Span));
        result.Validate();
        return result;
    }

    public static Wire.AcceptedDigest ToWire(AcceptedDigest value)
    {
        ArgumentNullException.ThrowIfNull(value);
        value.Validate();
        return new() { Ballot = ToWire(value.AcceptedBallot), Sha256 = ToWire(value.ValueHash) };
    }

    public static void ValidatePage(ReadOnlySpan<string> keys, string? continuation, Wire.ListKeysRequest request)
    {
        Require(keys.Length <= request.Limit, "The page exceeds its requested limit.");
        string? previous = request.HasAfterKey ? request.AfterKey : null;
        foreach (var key in keys)
        {
            Text(key);
            Require(key.StartsWith(request.Prefix, StringComparison.Ordinal)
                && (previous is null || StringComparer.Ordinal.Compare(key, previous) > 0),
                "Page keys must match the prefix and be strictly ordinal after the cursor.");
            previous = key;
        }
        if (continuation is not null)
        {
            Text(continuation);
            Require(keys.Length > 0 && continuation == keys[^1],
                "A continuation must identify the final returned key.");
        }
    }
}
