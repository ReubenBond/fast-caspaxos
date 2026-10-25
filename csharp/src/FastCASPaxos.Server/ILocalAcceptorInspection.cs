using System.Collections.Immutable;
using FastCASPaxos.Model;

namespace FastCASPaxos.Server;

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
        if (PromisedBallot.Round <= 0 || PromisedBallot.Proposer < 0
            || (!AcceptedBallot.IsZero && (AcceptedBallot.Round <= 0 || AcceptedBallot.Proposer < 0)))
            throw new InvalidDataException("A local ballot snapshot violates the acceptor invariants.");
    }
}

public sealed record LocalAcceptorState(
    string Key, Ballot PromisedBallot, Ballot AcceptedBallot, ByteString? AcceptedValue);

public sealed record LocalAcceptorPage(ImmutableArray<LocalAcceptorState> Entries, string? ContinuationKey);
