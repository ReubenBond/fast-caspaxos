using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Model;

namespace Orleans.Consensus.CasPaxos.Server;

/// <summary>Provider-owned read-only snapshots, including persisted promise-only records.</summary>
public interface ILocalAcceptorInspection
{
    ValueTask<LocalAcceptorBallots?> InspectBallotsAsync(string key, CancellationToken cancellationToken = default);
    ValueTask<LocalAcceptorState?> InspectAsync(string key, CancellationToken cancellationToken = default);
    ValueTask<LocalAcceptorPage> InspectKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default);
}

public sealed record LocalAcceptorBallots(Ballot PromisedBallot, Ballot AcceptedBallot)
{
    public void Validate()
    {
        var promisedValid = PromisedBallot.IsFloorBallot
            || (PromisedBallot.Configuration >= 1 && PromisedBallot.Round > 0 && PromisedBallot.Proposer >= 0);
        var acceptedValid = AcceptedBallot.IsZero
            || (AcceptedBallot.Configuration >= 1 && AcceptedBallot.Round > 0 && AcceptedBallot.Proposer >= 0);
        if (!promisedValid || !acceptedValid)
            throw new InvalidDataException("A local ballot snapshot violates the acceptor invariants.");
    }
}

public sealed record LocalAcceptorState(
    string Key, Ballot PromisedBallot, Ballot AcceptedBallot, ByteString? AcceptedValue);

public sealed record LocalAcceptorPage(ImmutableArray<LocalAcceptorState> Entries, string? ContinuationKey);
