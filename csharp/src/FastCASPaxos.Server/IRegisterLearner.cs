using FastCASPaxos.Model;
using System.Collections.Immutable;

namespace FastCASPaxos.Server;

/// <summary>Receives quorum conclusions from trusted proposers in the fixed cluster.</summary>
public interface IRegisterLearner
{
    void Learn(RegisterCommit commit);
}

/// <summary>
/// A quorum-committed value and optional quorum-confirmed next promise. The publishing proposer
/// establishes both quorums; receivers use the conclusion to seed their next operation.
/// </summary>
public sealed record RegisterCommit(string Key, Ballot AcceptedBallot, ByteString Value, Ballot PreparedBallot)
{
    public ImmutableArray<string> AcknowledgedPeers { get; init; } = [];
}

public interface IRegisterDigestLearner
{
    /// <summary>Returns true after learning an exact local match; false explicitly identifies a stale or missing match.</summary>
    ValueTask<bool> LearnDigestAsync(RegisterCommitDigest commit, CancellationToken cancellationToken = default);
}

public sealed record RegisterCommitDigest(string Key, AcceptedDigest Accepted, Ballot PreparedBallot)
{
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Key);
        ArgumentNullException.ThrowIfNull(Accepted);
        Accepted.Validate();
        if (!PreparedBallot.IsZero && (PreparedBallot.Round <= 0 || PreparedBallot.Proposer < 0
            || PreparedBallot <= Accepted.AcceptedBallot))
            throw new ArgumentException("A prepared successor must be higher than the accepted ballot.");
    }
}

/// <summary>A subscriber delivery failure observed after the authoritative quorum outcome.</summary>
public sealed record RegisterNotificationFailure(RegisterCommit Commit, Exception Error);
