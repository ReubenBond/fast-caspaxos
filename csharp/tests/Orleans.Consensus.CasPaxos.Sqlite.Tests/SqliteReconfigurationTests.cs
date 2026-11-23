using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Server;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Sqlite.Tests;

public sealed class SqliteReconfigurationTests
{
    [Fact]
    public async Task AcceptorFloorPersistsAcrossRestartFencesAllKeysAndAllowsReads()
    {
        using var database = new TestDatabase();
        var value = new ByteString("held"u8);

        await using (var server = await database.OpenAsync())
        {
            Assert.True((await server.AcceptAsync("held", new(Ballot.InitialFast(), value, new Ballot(1, 2, 0)))).IsAccepted);

            Assert.Equal(Ballot.Floor(2), await server.SetFloorAsync(Ballot.Floor(2)));
            // Idempotent and monotonic: lower floor does not regress.
            Assert.Equal(Ballot.Floor(2), await server.SetFloorAsync(Ballot.Floor(1)));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => server.SetFloorAsync(Ballot.InitialFast()).AsTask());
        }

        await using (var reopened = await database.OpenAsync())
        {
            // Held key is fenced for config 1 prepares and new accepts.
            var heldReject = await reopened.PrepareAsync("held", new(new Ballot(1, 5, 1)));
            Assert.False(heldReject.IsPromised);
            Assert.Equal(Ballot.Floor(2), heldReject.ConflictingBallot);
            Assert.Equal(value, heldReject.AcceptedValue);

            // Unseen key is ALSO fenced by the acceptor-wide floor.
            var unseenReject = await reopened.PrepareAsync("unseen", new(Ballot.InitialClassic(1, 1)));
            Assert.False(unseenReject.IsPromised);
            Assert.Equal(Ballot.Floor(2), unseenReject.ConflictingBallot);
            Assert.Equal(Ballot.Zero, unseenReject.AcceptedBallot);

            // Exact-duplicate replay below floor succeeds read-only and echoes effPromised >= floor.
            var dup = await reopened.AcceptAsync("held", new(Ballot.InitialFast(), value, new Ballot(1, 2, 0)));
            Assert.True(dup.IsAccepted);
            Assert.Equal(Ballot.Floor(2), dup.PromisedBallot);

            // Ballot-free Read and ListKeys remain available and report effPromised >= floor.
            var read = await reopened.ReadAsync("held");
            var state = read is KeyedAcceptorState present
                ? present
                : throw new Xunit.Sdk.XunitException("Expected KeyedAcceptorState.");
            Assert.Equal(Ballot.Floor(2), state.PromisedBallot);
            Assert.Equal(value, state.AcceptedValue);

            var page = await reopened.ListKeysAsync();
            var entry = Assert.Single(page.Entries);
            Assert.Equal("held", entry.Key);
            Assert.Equal(Ballot.Floor(2), entry.PromisedBallot);

            // Config 2 ballot succeeds.
            Assert.True((await reopened.PrepareAsync("held", new(Ballot.InitialClassic(2, 1)))).IsPromised);
            Assert.True((await reopened.AcceptAsync("held", new(Ballot.InitialClassic(2, 1), value, new Ballot(2, 2, 0)))).IsAccepted);
        }
    }

    [Fact]
    public async Task ProposerBallotSourceIsLexicographicallyMonotonicAcrossConfigurationsAndRestarts()
    {
        using var database = new TestDatabase();
        using (var source = new SqliteProposerBallotSource(database.Path, 7, TestDatabase.FatalErrors))
        {
            Assert.Equal(new Ballot(1, 1, 7), source.Reserve(1, 1));
            Assert.Equal(new Ballot(1, 5, 7), source.Reserve(1, 5));
            Assert.Equal(new Ballot(2, 1, 7), source.Reserve(2, 1));
            Assert.Equal(new Ballot(2, 2, 7), source.Reserve(2, 1));
            Assert.Throws<InvalidOperationException>(() => source.Reserve(1, 10));
        }

        using (var reopened = new SqliteProposerBallotSource(database.Path, 7, TestDatabase.FatalErrors))
        {
            Assert.Throws<InvalidOperationException>(() => reopened.Reserve(1, 1));
            Assert.Equal(new Ballot(2, 3, 7), reopened.Reserve(2, 1));
            Assert.Equal(new Ballot(3, 1, 7), reopened.Reserve(3, 1));
        }
    }

    [Fact]
    public async Task VoterRegistryInitializesJoinFloorAndValidatesBootstrapDigest()
    {
        using var database = new TestDatabase();
        var digestA = VoterConfiguration.ComputeBootstrapDigest(["v1", "v2", "v3"]);
        var digestB = VoterConfiguration.ComputeBootstrapDigest(["v1", "v2", "v9"]);

        await using (var acceptor = await database.OpenAsync())
        {
            var registry = await SqliteVoterRegistry.OpenAsync(acceptor, "v4", joinConfiguration: 2, bootstrapDigest: digestA);
            Assert.Equal(2, registry.JoinConfiguration);
            Assert.Equal(digestA, registry.BootstrapDigest);

            // Join floor (2, 0, 0) was initialized atomically with the binding, fencing config 1 before any RPC.
            var reject = await acceptor.PrepareAsync("any-key", new(Ballot.InitialClassic(1, 1)));
            Assert.False(reject.IsPromised);
            Assert.Equal(Ballot.Floor(2), reject.ConflictingBallot);

            // Admission validates bootstrap digest agreement (§8).
            var otherIncarnation = Guid.NewGuid();
            Assert.Equal(otherIncarnation, await registry.AdmitAsync("v1", otherIncarnation, digestA));
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                registry.AdmitAsync("v2", Guid.NewGuid(), digestB).AsTask());
        }

        await using (var reopened = await database.OpenAsync())
        {
            // Reopening with a mismatched bootstrap digest fails startup.
            await Assert.ThrowsAsync<InvalidDataException>(() =>
                SqliteVoterRegistry.OpenAsync(reopened, "v4", joinConfiguration: 2, bootstrapDigest: digestB));

            var ok = await SqliteVoterRegistry.OpenAsync(reopened, "v4", joinConfiguration: 2, bootstrapDigest: digestA);
            Assert.Equal(2, ok.JoinConfiguration);
            Assert.Equal(digestA, ok.BootstrapDigest);
        }
    }
}
