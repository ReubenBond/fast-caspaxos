using System.Security.Cryptography;
using FastCASPaxos.Model;

namespace FastCASPaxos.Server;

/// <summary>Reads durable accepted-state fingerprints and conditionally transfers accepted bytes.</summary>
public interface IAcceptorDigestSource
{
    /// <summary>Lists accepted keys in ordinal order, with an exclusive cursor and a positive limit.</summary>
    ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default);

    /// <summary>Returns accepted bytes only when their ballot or SHA-256 differs from the supplied digest.</summary>
    ValueTask<ConditionalAcceptorState> FetchIfDifferentAsync(
        string key, AcceptedDigest? known, CancellationToken cancellationToken = default);
}

public sealed record AcceptedDigest(Ballot AcceptedBallot, ByteString ValueHash)
{
    public static ByteString Hash(ByteString value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new(SHA256.HashData(value.ToArray()));
    }

    public void Validate()
    {
        if (AcceptedBallot.Round <= 0 || AcceptedBallot.Proposer < 0)
            throw new ArgumentException("An accepted digest requires a positive round and nonnegative proposer.");
        ArgumentNullException.ThrowIfNull(ValueHash);
        if (ValueHash.Length != 32) throw new ArgumentException("An accepted digest requires a 32-byte SHA-256.");
    }
}

public sealed record KeyDigest(string Key, AcceptedDigest Digest);
public sealed record AcceptorDigestPage(IReadOnlyList<KeyDigest> Entries, string? ContinuationKey);

public enum ConditionalAcceptorStatus { Missing, Unchanged, Different }

/// <summary>Different carries complete accepted state; Missing and Unchanged carry only their status.</summary>
public sealed record ConditionalAcceptorState(ConditionalAcceptorStatus Status, KeyedAcceptorState? State = null);

public sealed record RepairPeer(string Id, IKeyedAcceptor Acceptor, IAcceptorDigestSource Digests);

/// <summary>Coalesces requests for a local durability repair pass.</summary>
public interface IRepairScheduler
{
    void StageRepair();
}

public sealed record RepairPassResult(long Examined, long Repaired, long Pending, IReadOnlyList<Exception> Errors);

public sealed class DurabilityRepairOptions
{
    /// <summary>Zero selects a majority of the fixed membership.</summary>
    public int TargetReplicas { get; init; }
    public int PageSize { get; init; } = 128;
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryBackoff { get; init; } = TimeSpan.FromMilliseconds(10);
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
