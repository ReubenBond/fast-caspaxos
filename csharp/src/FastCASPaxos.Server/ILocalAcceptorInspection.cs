using FastCASPaxos.Model;

namespace FastCASPaxos.Server;

/// <summary>Provider-owned read-only snapshots, including persisted promise-only records.</summary>
public interface ILocalAcceptorInspection
{
    ValueTask<LocalAcceptorState?> InspectAsync(string key, CancellationToken cancellationToken = default);
    ValueTask<LocalAcceptorPage> InspectKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default);
}

public sealed record LocalAcceptorState(
    string Key, Ballot PromisedBallot, Ballot AcceptedBallot, ByteString? AcceptedValue);

public sealed record LocalAcceptorPage(IReadOnlyList<LocalAcceptorState> Entries, string? ContinuationKey);
