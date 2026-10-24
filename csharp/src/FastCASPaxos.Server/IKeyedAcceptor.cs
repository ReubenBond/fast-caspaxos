using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Server;

/// <summary>
/// A keyed protocol endpoint. Successful completion observes durably committed acceptor state.
/// Ballots have positive rounds and nonnegative proposer ids; the optional next ballot also allows Zero.
/// </summary>
public interface IKeyedAcceptor
{
    /// <summary>Reads durable accepted state while preserving acceptor state.</summary>
    /// <remarks>
    /// Each response observes a committed local snapshot taken during this call.
    /// A matching known digest returns AcceptorUnchanged; otherwise the response carries
    /// KeyedAcceptorState or AcceptorMissing. Omitting known returns the full accepted state.
    /// </remarks>
    ValueTask<AcceptorReadResult> ReadAsync(
        string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default);

    /// <summary>Promises a ballot and returns the accepted state, or the conflicting ballot.</summary>
    /// <remarks>A null accepted value means no accepted record; an empty ByteString is a value.</remarks>
    ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default);

    /// <summary>Accepts bytes and optionally promises the next ballot using the core protocol transition.</summary>
    ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists local accepted records in case-sensitive StringComparer.Ordinal key order.
    /// Prefix matching is ordinal, afterKey is exclusive, and limit must be positive.
    /// Each page is a consistent local snapshot. Continuation resumes after the final returned key.
    /// </summary>
    ValueTask<KeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100,
        CancellationToken cancellationToken = default);
}

/// <summary>A locally accepted value and its complete durable ballot state.</summary>
public sealed record KeyedAcceptorState(
    string Key, Ballot PromisedBallot, Ballot AcceptedBallot, ByteString AcceptedValue);

public sealed record AcceptorMissing;
public sealed record AcceptorUnchanged;
public union AcceptorReadResult(AcceptorMissing, AcceptorUnchanged, KeyedAcceptorState);

/// <summary>A local inventory page. A null continuation means the page exhausted its snapshot.</summary>
public sealed record KeyPage(IReadOnlyList<KeyedAcceptorState> Entries, string? ContinuationKey);
