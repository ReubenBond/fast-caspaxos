using System.Collections.Immutable;
using System.Security.Cryptography;
using FastCASPaxos.Model;

namespace FastCASPaxos.Server;

/// <summary>Lists durable accepted-state fingerprints.</summary>
public interface IAcceptorDigestSource
{
    /// <summary>Lists accepted keys in ordinal order, with an exclusive cursor and a positive limit.</summary>
    ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default);
}

public sealed record AcceptedDigest(Ballot AcceptedBallot, ByteString ValueHash)
{
    public static ByteString Hash(ByteString value)
    {
        if (value.IsDefault) throw new ArgumentException("Initialized bytes are required.", nameof(value));
        return new(SHA256.HashData(value.ToArray()));
    }

    public void Validate()
    {
        if (AcceptedBallot.Round <= 0 || AcceptedBallot.Proposer < 0)
            throw new ArgumentException("An accepted digest requires a positive round and nonnegative proposer.");
        if (ValueHash.IsDefault || ValueHash.Length != 32)
            throw new ArgumentException("An accepted digest requires a 32-byte SHA-256.");
    }
}

public sealed record KeyDigest(string Key, AcceptedDigest Digest);
public sealed record AcceptorDigestPage(ImmutableArray<KeyDigest> Entries, string? ContinuationKey);

public sealed record RepairPeer(string Id, IKeyedAcceptor Acceptor, IAcceptorDigestSource Digests);

/// <summary>Coalesces requests for a local durability repair pass.</summary>
public interface IRepairScheduler
{
    void StageRepair();
}

public sealed record RepairPassResult(long Examined, long Repaired, long Pending, ImmutableArray<Exception> Errors);

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
