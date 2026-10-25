using System.Collections.Immutable;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Server;

/// <summary>A linearizable conditional register per ordinal, case-sensitive key.</summary>
public interface IRegisterService
{
    ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// A null expected version creates an absent register. Applied writes receive a fresh opaque
    /// version even when the payload is equal. Unknown means the write may have taken effect.
    /// </summary>
    ValueTask<CompareExchangeResult> CompareExchangeAsync(
        string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves quorum inventory candidates through linearizable reads and returns present keys in
    /// ordinal order. Each key is observed independently; concurrent insertions before the exclusive
    /// cursor appear on a subsequent scan. A positive limit bounds the returned page.
    /// </summary>
    ValueTask<RegisterKeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default);
}

/// <summary>Absence has null value and version; a present value may contain zero bytes.</summary>
public sealed record RegisterValue(ByteString? Value, ByteString? Version)
{
    public bool IsPresent => Version is not null;
}

public enum CompareExchangeStatus { Applied, Mismatch, Unknown }

/// <summary>
/// Current is the established register state when available. An Unknown result with Current
/// identifies a later observation, which may have superseded this write.
/// </summary>
public sealed record CompareExchangeResult(
    CompareExchangeStatus Status, RegisterValue? Current, Exception? Error = null);

public sealed record RegisterKeyPage(ImmutableArray<string> Keys, string? ContinuationKey);

/// <summary>
/// Allocates strictly increasing positive rounds for a configured proposer. Reservations are durable
/// before returning, including across instances and restarts. Retain the allocator with its proposer
/// identity; all users of that identity share the same allocator state.
/// </summary>
public interface IProposerBallotSource
{
    int ProposerId { get; }
    Ballot Reserve(int minimumRound);
}

public sealed record AcceptorPeer(string Id, IKeyedAcceptor Acceptor);

public sealed class RegisterServiceOptions
{
    public ProposerModePolicy Mode { get; init; } = ProposerModePolicy.Adaptive();
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryBackoff { get; init; } = TimeSpan.FromMilliseconds(10);
    public int Capacity { get; init; } = 256;
}

/// <summary>Admission is bounded; retry this operation after existing calls finish.</summary>
public sealed class RegisterBusyException() : Exception("The register service is at its configured capacity.");

/// <summary>A quorum was unavailable before the operation's deadline.</summary>
public sealed class RegisterUnavailableException(Exception? innerException = null)
    : TimeoutException("A register quorum was unavailable before the operation deadline.", innerException);
