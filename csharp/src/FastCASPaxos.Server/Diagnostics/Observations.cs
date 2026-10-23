using FastCASPaxos.Model;

namespace FastCASPaxos.Server.Diagnostics;

public sealed record PayloadPreview(int Length, string Base64, bool Truncated)
{
    public const int MaximumBytes = 256;

    public static PayloadPreview? From(ByteString? value) => value is null ? null : From(value.Span);

    internal static PayloadPreview From(ReadOnlySpan<byte> value) =>
        new(value.Length, Convert.ToBase64String(value[..Math.Min(value.Length, MaximumBytes)]),
            value.Length > MaximumBytes);
}

/// <summary>Interprets an accepted envelope at the register layer; its authority remains local.</summary>
public sealed record RegisterPreview(string Format, bool? IsPresent, string? Version, PayloadPreview? Value)
{
    public static RegisterPreview FromAccepted(ByteString value) => RegisterEncoding.Preview(value);
}

// Event keys are bounded independently of protocol input limits. A truncated key invalidates the whole view.
public sealed record KeyPreview(string Text, int Length, bool Truncated, bool Lossy = false)
{
    public const int MaximumCharacters = 1024;
    public static KeyPreview? From(string? key)
    {
        if (key is null) return null;
        var length = Math.Min(key.Length, MaximumCharacters);
        if (length < key.Length && char.IsHighSurrogate(key[length - 1]) && char.IsLowSurrogate(key[length])) length--;
        var text = key[..length];
        for (var index = 0; index < text.Length; index++)
        {
            if (!char.IsSurrogate(text[index])) continue;
            if (char.IsHighSurrogate(text[index]) && index + 1 < text.Length && char.IsLowSurrogate(text[index + 1]))
            {
                index++;
                continue;
            }
            return new(System.Text.Encoding.UTF8.GetString(System.Text.Encoding.UTF8.GetBytes(text)),
                key.Length, length < key.Length, Lossy: true);
        }
        return new(text, key.Length, length < key.Length);
    }
}

public sealed record ProtocolObservation(
    string Operation, string Direction, string Phase, string? PeerId, string CallId, string? OperationId,
    KeyPreview? Key, Ballot Ballot, Ballot NextBallot, string? Outcome = null,
    Ballot? PromisedBallot = null, Ballot? AcceptedBallot = null, Ballot? ConflictingBallot = null,
    PayloadPreview? Value = null, string? Error = null);

public sealed record OperationObservation(
    string Operation, string OperationId, KeyPreview? Key, string Outcome,
    PayloadPreview? Value = null, string? Version = null, string? Error = null, bool? IsPresent = null);

public sealed record AcceptorStateObservation(
    KeyPreview Key, Ballot PromisedBallot, Ballot AcceptedBallot,
    PayloadPreview? AcceptedValue, RegisterPreview? Register)
{
    public static AcceptorStateObservation From(LocalAcceptorState state) => new(
        KeyPreview.From(state.Key)!, state.PromisedBallot, state.AcceptedBallot,
        PayloadPreview.From(state.AcceptedValue),
        state.AcceptedValue is null ? null : RegisterPreview.FromAccepted(state.AcceptedValue));
}

public sealed record StorageObservation(string Status, string? Error = null);
public sealed record ResetObservation(string Reason);
public sealed record HeartbeatObservation(string StorageStatus);

/// <summary>Correlation follows async execution; remote metadata is observational, never authorization.</summary>
public sealed record DiagnosticContext(string NodeId, string? OperationId, string? CallId)
{
    private static readonly AsyncLocal<DiagnosticContext?> Slot = new();
    public static DiagnosticContext? Current { get => Slot.Value; set => Slot.Value = value; }
}
