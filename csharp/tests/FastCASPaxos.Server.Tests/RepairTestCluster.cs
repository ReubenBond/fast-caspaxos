using System.Collections.Concurrent;
using System.Security.Cryptography;
using FastCASPaxos.Model;
using Xunit;

namespace FastCASPaxos.Server.Tests;

// Repair-only adapters reuse the existing real acceptor state machine, gates, and clock.
// Their digest behavior is fixture plumbing, not evidence for SQLite/gRPC contracts.
internal sealed class RepairTestCluster(int count = 3)
{
    public static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(5);
    public static readonly TimeSpan UnavailableRetryDelay = TimeSpan.FromSeconds(1);
    public ControlledPeer[] Peers { get; } = TestSupport.Peers(count);
    public ManualTime Clock { get; } = new();
    public RecordingRepairBallots Ballots { get; } = new();
    public RepairDigestSource[] Digests { get; private set; } = [];

    public DurabilityRepair Create(int target = 0, int pageSize = 2, int local = 0)
    {
        if (Digests.Length == 0) Digests = Peers.Select(peer => new RepairDigestSource(peer.Inner)).ToArray();
        return new($"peer-{local}", Peers.Select((peer, index) => new RepairPeer($"peer-{index}", peer, Digests[index])),
            Ballots, new()
            {
                TargetReplicas = target,
                PageSize = pageSize,
                TimeProvider = Clock,
                OperationTimeout = TimeSpan.FromSeconds(10),
                AttemptTimeout = TimeSpan.FromSeconds(1),
                RetryBackoff = TimeSpan.FromMilliseconds(1)
            });
    }

    public async Task SeedAsync(int peer, string key, ByteString value, Ballot? ballot = null)
    {
        Assert.True((await Peers[peer].Inner.AcceptAsync(key, new(ballot ?? new Ballot(1, 1), value, Ballot.Zero))).IsAccepted);
    }

    public void Offline(int peer)
    {
        Peers[peer].GoOffline();
        Digests[peer].List = (_, _, _, _) => ValueTask.FromException<AcceptorDigestPage>(new IOException("offline"));
        Digests[peer].Fetch = (_, _, _) => ValueTask.FromException<ConditionalAcceptorState>(new IOException("offline"));
    }

    public void Online(int peer)
    {
        Peers[peer].Prepare = null;
        Peers[peer].Accept = null;
        Peers[peer].List = null;
        Digests[peer].List = null;
        Digests[peer].Fetch = null;
    }

    public void AssertValue(string key, ByteString expected) =>
        Assert.All(Peers, peer => Assert.Equal(expected, peer.Inner.Snapshot(key)!.AcceptedValue));
}

internal sealed class RecordingRepairBallots : IProposerBallotSource
{
    private readonly TestBallots _inner = new(11);
    public int ProposerId => _inner.ProposerId;
    public ConcurrentQueue<Ballot> Reserved { get; } = new();
    public Ballot Reserve(int minimumRound)
    {
        var ballot = _inner.Reserve(minimumRound);
        Reserved.Enqueue(ballot);
        return ballot;
    }
}

internal sealed class RepairDigestSource(MemoryAcceptor inner) : IAcceptorDigestSource
{
    public ConcurrentQueue<(string Prefix, string? After, int Limit)> Lists { get; } = new();
    public ConcurrentQueue<(string Key, AcceptedDigest? Known)> Fetches { get; } = new();
    public Func<string, string?, int, CancellationToken, ValueTask<AcceptorDigestPage>>? List { get; set; }
    public Func<string, AcceptedDigest?, CancellationToken, ValueTask<ConditionalAcceptorState>>? Fetch { get; set; }

    public async ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        Lists.Enqueue((prefix, afterKey, limit));
        if (List is not null) return await List(prefix, afterKey, limit, cancellationToken);
        var page = await inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken);
        return new(page.Entries.Select(state => new KeyDigest(state.Key, Digest(state))).ToArray(), page.ContinuationKey);
    }

    public ValueTask<ConditionalAcceptorState> FetchIfDifferentAsync(
        string key, AcceptedDigest? known, CancellationToken cancellationToken = default)
    {
        Fetches.Enqueue((key, known));
        if (Fetch is not null) return Fetch(key, known, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        var state = inner.Snapshot(key);
        return ValueTask.FromResult(state is null
            ? new ConditionalAcceptorState(ConditionalAcceptorStatus.Missing)
            : known == Digest(state)
                ? new ConditionalAcceptorState(ConditionalAcceptorStatus.Unchanged)
                : new ConditionalAcceptorState(ConditionalAcceptorStatus.Different, state));
    }

    private static AcceptedDigest Digest(KeyedAcceptorState state) =>
        new(state.AcceptedBallot, new(SHA256.HashData(state.AcceptedValue.ToArray())));
}
