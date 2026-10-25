using FastCASPaxos.Model;

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
public sealed record RegisterCommit(string Key, Ballot AcceptedBallot, ByteString Value, Ballot PreparedBallot);
