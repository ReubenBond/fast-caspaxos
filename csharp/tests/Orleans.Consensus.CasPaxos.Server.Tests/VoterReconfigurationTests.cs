using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Server.Tests;

public sealed class VoterReconfigurationTests
{
    [Fact]
    public void ConfigurationEncodingAndBootstrapDigestAreCanonicalAndOrderIndependent()
    {
        var cfg = new VoterConfiguration(
            3,
            [new("voter-c", "http://c"), new("voter-a", "http://a"), new("voter-b", "http://b")],
            VoterConfigurationState.Active);

        var encoded = VoterConfigurationEncoding.Encode(cfg);
        var decoded = VoterConfigurationEncoding.Decode(encoded);
        Assert.Equal(3, decoded.Number);
        Assert.Equal(VoterConfigurationState.Active, decoded.State);
        Assert.Equal(["voter-a", "voter-b", "voter-c"], decoded.Voters.Select(static v => v.Id));

        var digest1 = VoterConfiguration.ComputeBootstrapDigest(["v3", "v1", "v2"]);
        var digest2 = VoterConfiguration.ComputeBootstrapDigest(["v1", "v2", "v3"]);
        var digestOther = VoterConfiguration.ComputeBootstrapDigest(["v1", "v2", "v4"]);
        Assert.Equal(digest1, digest2);
        Assert.NotEqual(digest1, digestOther);
    }

    [Fact]
    public async Task ReplaceLostVoterPreservesValuesVersionsAndRefreshesStaleProposer()
    {
        var peers = new Dictionary<string, ControlledPeer>(StringComparer.Ordinal)
        {
            ["v1"] = new(initialConfiguration: 1),
            ["v2"] = new(initialConfiguration: 1),
            ["v3"] = new(initialConfiguration: 1),
            ["v4"] = new(initialConfiguration: 2),
        };
        IKeyedAcceptor Resolve(VoterDescriptor d) => peers[d.Id];

        ImmutableArray<VoterDescriptor> c1Voters = [new("v1"), new("v2"), new("v3")];
        ImmutableArray<VoterDescriptor> c2Voters = [new("v1"), new("v2"), new("v4")];

        using var proposerBallots = new TestBallots(1);
        using var operatorBallots = new TestBallots(2);
        await using var staleProposer = new ReplicatedRegisterService(
            [new("v1", peers["v1"]), new("v2", peers["v2"]), new("v3", peers["v3"])],
            proposerBallots,
            new RegisterServiceOptions
            {
                Mode = ProposerModePolicy.Adaptive(),
                OperationTimeout = TimeSpan.FromSeconds(5),
                AttemptTimeout = TimeSpan.FromSeconds(1),
                RetryBackoff = TimeSpan.FromMilliseconds(5),
                PeerResolver = Resolve,
            });

        // Write two keys in c = 1 (including warm fast write).
        var writeA = await staleProposer.CompareExchangeAsync("alpha", null, new("one"u8));
        Assert.Equal(CompareExchangeStatus.Applied, writeA.Status);
        var writeB = await staleProposer.CompareExchangeAsync("beta", null, new("two"u8));
        Assert.Equal(CompareExchangeStatus.Applied, writeB.Status);
        var alphaVersion = writeA.Current!.Version!;

        // Permanently lose v3 before reconfiguration.
        peers["v3"].GoOffline();

        var driver = new VoterReconfigurationDriver(operatorBallots, Resolve, TestSupport.Options());
        var active = await driver.ReconfigureAsync(c1Voters, 1, c2Voters);

        Assert.Equal(2, active.Number);
        Assert.Equal(VoterConfigurationState.Active, active.State);
        Assert.True(active.HasSameVoters(c2Voters));

        // Stale proposer still on c = 1 hits floor refusal, discovers c = 2 via Read(K_cfg),
        // confirms Active on V(2), and transparently preserves logical versions across the hand-off.
        Assert.Equal(1, staleProposer.ActiveConfiguration);
        var readA = await staleProposer.ReadAsync("alpha");
        Assert.Equal(2, staleProposer.ActiveConfiguration);
        Assert.Equal(new ByteString("one"u8), readA.Value);
        Assert.Equal(alphaVersion, readA.Version);

        // Conditional write using the pre-reconfiguration version succeeds in c = 2.
        var updateA = await staleProposer.CompareExchangeAsync("alpha", alphaVersion, new("one-updated"u8));
        Assert.Equal(CompareExchangeStatus.Applied, updateA.Status);
        Assert.Equal(new ByteString("one-updated"u8), updateA.Current!.Value);
        Assert.Equal(new ByteString("two"u8), (await staleProposer.ReadAsync("beta")).Value);
    }

    [Fact]
    public async Task DisjointMigrationAndChainedReconfigurationsPreserveAllKeys()
    {
        var peers = new Dictionary<string, ControlledPeer>(StringComparer.Ordinal)
        {
            ["v1"] = new(initialConfiguration: 1),
            ["v2"] = new(initialConfiguration: 1),
            ["v3"] = new(initialConfiguration: 1),
            ["v4"] = new(initialConfiguration: 2),
            ["v5"] = new(initialConfiguration: 2),
            ["v6"] = new(initialConfiguration: 2),
            ["v7"] = new(initialConfiguration: 3),
        };
        IKeyedAcceptor Resolve(VoterDescriptor d) => peers[d.Id];

        ImmutableArray<VoterDescriptor> c1 = [new("v1"), new("v2"), new("v3")];
        ImmutableArray<VoterDescriptor> c2 = [new("v4"), new("v5"), new("v6")];
        ImmutableArray<VoterDescriptor> c3 = [new("v4"), new("v5"), new("v7")];

        using var clientBallots = new TestBallots(1);
        using var opBallots = new TestBallots(2);
        await using var client = new ReplicatedRegisterService(
            [.. c1.Select(v => new AcceptorPeer(v.Id, peers[v.Id]))],
            clientBallots,
            new RegisterServiceOptions
            {
                Mode = ProposerModePolicy.Adaptive(),
                OperationTimeout = TimeSpan.FromSeconds(5),
                AttemptTimeout = TimeSpan.FromSeconds(1),
                RetryBackoff = TimeSpan.FromMilliseconds(5),
                PeerResolver = Resolve,
            });

        var w1 = await client.CompareExchangeAsync("k", null, new("v1"u8));
        Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

        var driver = new VoterReconfigurationDriver(opBallots, Resolve, TestSupport.Options());
        var active2 = await driver.ReconfigureAsync(c1, 1, c2);
        Assert.Equal(2, active2.Number);

        // Decommission all of V(1) after Activate(2) commits.
        peers["v1"].GoOffline();
        peers["v2"].GoOffline();
        peers["v3"].GoOffline();

        await using var client2 = new ReplicatedRegisterService(
            [.. c2.Select(v => new AcceptorPeer(v.Id, peers[v.Id]))],
            new TestBallots(3),
            new RegisterServiceOptions
            {
                Mode = ProposerModePolicy.Adaptive(),
                InitialConfiguration = 2,
                OperationTimeout = TimeSpan.FromSeconds(5),
                AttemptTimeout = TimeSpan.FromSeconds(1),
                RetryBackoff = TimeSpan.FromMilliseconds(5),
                PeerResolver = Resolve,
            });

        var w2 = await client2.CompareExchangeAsync("k", w1.Current!.Version, new("v2"u8));
        Assert.Equal(CompareExchangeStatus.Applied, w2.Status);

        // Chain 2 -> 3 with v6 offline.
        peers["v6"].GoOffline();
        var active3 = await driver.ReconfigureAsync(c2, 2, c3);
        Assert.Equal(3, active3.Number);

        // Client still on c = 2 refreshes to c = 3 automatically and reads v2.
        var finalRead = await client2.ReadAsync("k");
        Assert.Equal(3, client2.ActiveConfiguration);
        Assert.Equal(new ByteString("v2"u8), finalRead.Value);
        Assert.Equal(w2.Current!.Version, finalRead.Version);
    }

    [Fact]
    public async Task CrashMidInstallAndConcurrentChooseConflictAreHandledSafely()
    {
        var peers = new Dictionary<string, ControlledPeer>(StringComparer.Ordinal)
        {
            ["v1"] = new(initialConfiguration: 1),
            ["v2"] = new(initialConfiguration: 1),
            ["v3"] = new(initialConfiguration: 1),
            ["v4"] = new(initialConfiguration: 2),
            ["v5"] = new(initialConfiguration: 2),
        };
        IKeyedAcceptor Resolve(VoterDescriptor d) => peers[d.Id];

        ImmutableArray<VoterDescriptor> c1 = [new("v1"), new("v2"), new("v3")];
        ImmutableArray<VoterDescriptor> c2A = [new("v1"), new("v2"), new("v4")];
        ImmutableArray<VoterDescriptor> c2B = [new("v1"), new("v3"), new("v5")];

        using var writerBallots = new TestBallots(1);
        await using var writer = new ReplicatedRegisterService(
            [.. c1.Select(v => new AcceptorPeer(v.Id, peers[v.Id]))],
            writerBallots,
            TestSupport.Options("classic"));

        Assert.Equal(CompareExchangeStatus.Applied, (await writer.CompareExchangeAsync("k1", null, new("a"u8))).Status);
        Assert.Equal(CompareExchangeStatus.Applied, (await writer.CompareExchangeAsync("k2", null, new("b"u8))).Status);

        // Simulate Step 1 Choose committing c2A (Pending) in c = 1, then operator crashing before Step 2.
        var pendingA = new VoterConfiguration(2, c2A, VoterConfigurationState.Pending);
        var choose = await writer.CompareExchangeAsync(
            VoterConfiguration.RegisterKey, null, VoterConfigurationEncoding.Encode(pendingA));
        Assert.Equal(CompareExchangeStatus.Applied, choose.Status);

        // A competing operator attempting to choose c2B observes ReconfigurationConflictException with c2A.
        var competingDriver = new VoterReconfigurationDriver(new TestBallots(2), Resolve, TestSupport.Options());
        var conflict = await Assert.ThrowsAsync<ReconfigurationConflictException>(
            () => competingDriver.ReconfigureAsync(c1, 1, c2B));
        Assert.Equal(2, conflict.WinningConfiguration.Number);
        Assert.True(conflict.WinningConfiguration.HasSameVoters(c2A));

        // Even if K_cfg was prematurely transferred while k2 was not, ResumeAsync re-runs Fence -> Install all -> Activate.
        var resumingDriver = new VoterReconfigurationDriver(new TestBallots(3), Resolve, TestSupport.Options());
        await resumingDriver.TransferKeyHandOffAsync(
            VoterConfiguration.RegisterKey,
            2,
            [.. c1.Select(v => new AcceptorPeer(v.Id, peers[v.Id]))],
            [.. c2A.Select(v => new AcceptorPeer(v.Id, peers[v.Id]))]);
        Assert.Null(peers["v4"].Inner.Snapshot("k2"));

        var active = await resumingDriver.ResumeAsync(c1, 1);
        Assert.Equal(2, active.Number);
        Assert.Equal(VoterConfigurationState.Active, active.State);

        // Both user keys are now present on V(2) including v4.
        Assert.NotNull(peers["v4"].Inner.Snapshot("k1"));
        Assert.NotNull(peers["v4"].Inner.Snapshot("k2"));

        // Re-running ResumeAsync on an already-active configuration is idempotent.
        var idempotent = await resumingDriver.ResumeAsync(c1, 1);
        Assert.Equal(2, idempotent.Number);
        Assert.Equal(VoterConfigurationState.Active, idempotent.State);
    }
}
