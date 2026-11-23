using System.Buffers.Binary;
using System.Collections.Immutable;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Protocol;
using Orleans.Consensus.CasPaxos.Server;
using Orleans.Consensus.CasPaxos.Testing;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class VoterReconfigurationSimulationTests
{
    private static ulong ReadRevision(RegisterValue value)
    {
        Assert.True(value.IsPresent);
        return BinaryPrimitives.ReadUInt64BigEndian(value.Version!.Value.ToArray());
    }

    [Fact]
    public async Task ReplaceLostVoterWithDropsAndDelaysIsDeterministicAndLinearizable()
    {
        var trace1 = await RunReplaceLostVoterAsync(seed: 1001);
        var trace2 = await RunReplaceLostVoterAsync(seed: 1001);
        Assert.Equal(trace1, trace2);
    }

    private static async Task<string[]> RunReplaceLostVoterAsync(int seed)
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v4");

        cluster.Run(async sim =>
        {
            var client = sim.CreateClient(proposerId: 1, initialConfiguration: 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await client.CompareExchangeAsync("alpha", null, new("v1-a"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);
            var w2 = await client.CompareExchangeAsync("beta", null, new("v1-b"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
            var w1b = await client.CompareExchangeAsync("alpha", w1.Current!.Version, new("v1-a2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1b.Status);

            // Permanently lose v3 (disk wiped, never restarts) and inject packet drops + jitter.
            sim.LoseVoter("v3");
            sim.SetMessageDropRate(0.10);

            var op = sim.CreateOperator(proposerId: 2);
            var active = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active.Number);
            Assert.Equal(VoterConfigurationState.Active, active.State);
            Assert.True(active.HasSameVoters(c2));

            // Stale client refreshes to c=2 under message drops and preserves logical versions.
            var readAlpha = await client.ReadAsync("alpha");
            Assert.Equal(2, client.ActiveConfiguration);
            Assert.Equal(new ByteString("v1-a2"u8), readAlpha.Value);
            Assert.Equal(w1b.Current!.Version, readAlpha.Version);

            var w3 = await client.CompareExchangeAsync("alpha", readAlpha.Version, new("v2-a3"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w3.Status);
            Assert.Equal(3UL, ReadRevision(w3.Current!));

            var readBeta = await client.ReadAsync("beta");
            Assert.Equal(new ByteString("v1-b"u8), readBeta.Value);
            Assert.Equal(w2.Current!.Version, readBeta.Version);
        });

        return cluster.TraceRecorder.Events
            .Select(static e => $"{e.Tick}:{e.Kind}:{e.Key}:{e.AcceptorId}:{e.Ballot}")
            .ToArray();
    }

    [Fact]
    public async Task AddOneThenRemoveOneVoterPreservesStateAndQuorums()
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1002);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v3", "v4");
        var c3 = cluster.RegisterConfiguration(3, "v2", "v3", "v4");

        cluster.Run(async sim =>
        {
            var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var r1 = await client.CompareExchangeAsync("k", null, new("one"u8));
            Assert.Equal(CompareExchangeStatus.Applied, r1.Status);

            // Add-1: 3 -> 4 voters (fast quorum 3 -> 3, classic quorum 2 -> 3).
            var op1 = sim.CreateOperator(2);
            var active2 = await op1.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active2.Number);
            Assert.Equal(VoterConfigurationState.Active, active2.State);

            var r2 = await client.CompareExchangeAsync("k", r1.Current!.Version, new("two"u8));
            Assert.Equal(CompareExchangeStatus.Applied, r2.Status);
            Assert.Equal(2, client.ActiveConfiguration);

            // Remove-1: 4 -> 3 voters (retiring v1), with v1 temporarily partitioned during removal.
            sim.CrashVoter("v1");
            var op2 = sim.CreateOperator(3);
            var active3 = await op2.ReconfigureAsync(c2, 2, c3);
            Assert.Equal(3, active3.Number);
            Assert.Equal(VoterConfigurationState.Active, active3.State);

            // v1 is permanently retired; reads and writes succeed in c = 3.
            var r3 = await client.CompareExchangeAsync("k", r2.Current!.Version, new("three"u8));
            Assert.Equal(CompareExchangeStatus.Applied, r3.Status);
            Assert.Equal(3, client.ActiveConfiguration);
            Assert.Equal(3UL, ReadRevision(r3.Current!));
            Assert.Equal(new ByteString("three"u8), (await client.ReadAsync("k")).Value);
        });
    }

    [Fact]
    public async Task ChainedDoubleReplaceOneToTwoToThreeWithStaleTwoHopRefresh()
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1003);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v4");
        var c3 = cluster.RegisterConfiguration(3, "v1", "v4", "v5");

        cluster.Run(async sim =>
        {
            var staleClient = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await staleClient.CompareExchangeAsync("chain", null, new("c1"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            // Reconfigure 1 -> 2 after losing v3.
            sim.LoseVoter("v3");
            var op = sim.CreateOperator(2);
            var active2 = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active2.Number);

            var c2Client = sim.CreateClient(3, 2, ["v1", "v2", "v4"], ProposerModePolicy.Adaptive());
            var w2 = await c2Client.CompareExchangeAsync("chain", w1.Current!.Version, new("c2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);

            // Reconfigure 2 -> 3 after losing v2.
            sim.LoseVoter("v2");
            var active3 = await op.ReconfigureAsync(c2, 2, c3);
            Assert.Equal(3, active3.Number);

            // Stale client still at c = 1 (with v2 and v3 lost) cannot form a quorum of V(1),
            // whereas a client at c = 2 (v1, v4 alive) refreshes directly to c = 3!
            var readFromC2 = await c2Client.ReadAsync("chain");
            Assert.Equal(3, c2Client.ActiveConfiguration);
            Assert.Equal(new ByteString("c2"u8), readFromC2.Value);
            Assert.Equal(w2.Current!.Version, readFromC2.Version);

            var w3 = await c2Client.CompareExchangeAsync("chain", readFromC2.Version, new("c3"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w3.Status);
            Assert.Equal(3UL, ReadRevision(w3.Current!));
        });
    }

    [Fact]
    public async Task DisjointMigrationRefreshesStaleAbsentReadsAndListKeys()
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1004);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v4", "v5", "v6");

        cluster.Run(async sim =>
        {
            var staleClient = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var staleLister = sim.CreateClient(4, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var w1 = await staleClient.CompareExchangeAsync("existing", null, new("before"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            var op = sim.CreateOperator(2);
            var active2 = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active2.Number);

            // Create a brand-new key strictly in c = 2 (vacuously complete across fenced V(1)).
            var c2Client = sim.CreateClient(3, 2, ["v4", "v5", "v6"], ProposerModePolicy.Adaptive());
            var wNew = await c2Client.CompareExchangeAsync("fresh/key", null, new("after"u8));
            Assert.Equal(CompareExchangeStatus.Applied, wNew.Status);

            // Stale client still on c = 1 reads the brand-new key; must NOT return stale Absent!
            var readNew = await staleClient.ReadAsync("fresh/key");
            Assert.Equal(2, staleClient.ActiveConfiguration);
            Assert.Equal(new ByteString("after"u8), readNew.Value);
            Assert.Equal(wNew.Current!.Version, readNew.Version);

            // Stale lister still on c = 1 lists prefix "fresh/"; must see floor and refresh to c = 2.
            var page = await staleLister.ListKeysAsync(prefix: "fresh/");
            Assert.Equal(2, staleLister.ActiveConfiguration);
            Assert.Equal<string>(["fresh/key"], page.Keys);

            // Decommission V(1) completely after Activate(2) — V(2) continues independently.
            sim.LoseVoter("v1");
            sim.LoseVoter("v2");
            sim.LoseVoter("v3");
            var w2 = await staleClient.CompareExchangeAsync("existing", w1.Current!.Version, new("after-decommission"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
            Assert.Equal(new ByteString("after-decommission"u8), (await c2Client.ReadAsync("existing")).Value);
        });
    }

    public enum OperatorCrashPoint
    {
        AfterChoose,
        MidFence,
        AfterFenceBeforeInstall,
        MidInstall,
        MidInstallPrematureConfigTransfer,
        AfterInstallBeforeActivate,
        DuringActivateReplyLost,
    }

    [Theory]
    [InlineData(OperatorCrashPoint.AfterChoose)]
    [InlineData(OperatorCrashPoint.MidFence)]
    [InlineData(OperatorCrashPoint.AfterFenceBeforeInstall)]
    [InlineData(OperatorCrashPoint.MidInstall)]
    [InlineData(OperatorCrashPoint.MidInstallPrematureConfigTransfer)]
    [InlineData(OperatorCrashPoint.AfterInstallBeforeActivate)]
    [InlineData(OperatorCrashPoint.DuringActivateReplyLost)]
    public async Task OperatorCrashAndResumeAtEveryStepPreservesAllKeysAndLinearizability(OperatorCrashPoint crashPoint)
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1100 + (int)crashPoint);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v4");

        cluster.Run(async sim =>
        {
            var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
            var k1 = await client.CompareExchangeAsync("k1", null, new("val1"u8));
            var k2 = await client.CompareExchangeAsync("k2", null, new("val2"u8));
            var k3 = await client.CompareExchangeAsync("k3", null, new("val3"u8));
            Assert.Equal(CompareExchangeStatus.Applied, k1.Status);
            Assert.Equal(CompareExchangeStatus.Applied, k2.Status);
            Assert.Equal(CompareExchangeStatus.Applied, k3.Status);

            if (crashPoint == OperatorCrashPoint.MidInstallPrematureConfigTransfer)
            {
                // Choose Pending in c = 1, fence V(1), then transfer ONLY K_cfg into c = 2 and crash!
                var pending = new VoterConfiguration(2, c2, VoterConfigurationState.Pending, c1);
                var chosen = await client.CompareExchangeAsync(
                    VoterConfiguration.RegisterKey, null, VoterConfigurationEncoding.Encode(pending));
                Assert.Equal(CompareExchangeStatus.Applied, chosen.Status);

                var helperOp = sim.CreateOperator(2);
                var resolveHelper = sim.CreatePeerResolver("helper:premature-cfg");
                ImmutableArray<AcceptorPeer> prevPeers = [.. c1.Select(d => new AcceptorPeer(d.Id, resolveHelper(d)))];
                ImmutableArray<AcceptorPeer> nextPeers = [.. c2.Select(d => new AcceptorPeer(d.Id, resolveHelper(d)))];
                await prevPeers[0].Acceptor.SetFloorAsync(Ballot.Floor(2));
                await prevPeers[1].Acceptor.SetFloorAsync(Ballot.Floor(2));
                await helperOp.Driver.TransferKeyHandOffAsync(VoterConfiguration.RegisterKey, 2, prevPeers, nextPeers);
                Assert.Null(sim.InspectVoterKey("v4", "k2"));
            }
            else
            {
                using var crashCts = new CancellationTokenSource();
                var setFloorResponses = 0;
                var installAcceptResponses = 0;
                var listKeysResponses = 0;
                var activateAcceptResponses = 0;

                var crashingOp = sim.CreateOperator(
                    proposerId: 2,
                    rpcObserver: ctx =>
                    {
                        if (crashCts.IsCancellationRequested) return;

                        switch (crashPoint)
                        {
                            case OperatorCrashPoint.AfterChoose:
                                // Crash right before first SetFloor after Choose committed.
                                if (!ctx.IsResponse && ctx.Kind == SimulatedRpcKind.SetFloor)
                                {
                                    crashCts.Cancel();
                                }
                                break;
                            case OperatorCrashPoint.MidFence:
                                // Crash after 1st SetFloor response on V(1) (before classic quorum of 2).
                                if (ctx.IsResponse && ctx.Kind == SimulatedRpcKind.SetFloor && ctx.VoterId is "v1" or "v2" or "v3")
                                {
                                    if (++setFloorResponses == 1) crashCts.Cancel();
                                }
                                break;
                            case OperatorCrashPoint.AfterFenceBeforeInstall:
                                // Crash right after Fence finishes, before ListKeys completes.
                                if (!ctx.IsResponse && ctx.Kind == SimulatedRpcKind.ListKeys)
                                {
                                    crashCts.Cancel();
                                }
                                break;
                            case OperatorCrashPoint.MidInstall:
                                // Crash after 2 Accept responses in c = 2 (1 key transferred, remaining keys pending).
                                if (ctx.IsResponse && ctx.Kind == SimulatedRpcKind.Accept && ctx.Ballot.Configuration == 2)
                                {
                                    if (++installAcceptResponses == 2) crashCts.Cancel();
                                }
                                break;
                            case OperatorCrashPoint.AfterInstallBeforeActivate:
                                // Crash after K_cfg transferred in c = 2, before Activate writes Active.
                                if (!ctx.IsResponse && ctx.Kind == SimulatedRpcKind.Prepare
                                    && ctx.Ballot.Configuration == 2
                                    && ctx.Key == VoterConfiguration.RegisterKey
                                    && listKeysResponses > 0)
                                {
                                    // First c=2 Prepare on K_cfg is hand-off transfer; second is Activate!
                                    if (++activateAcceptResponses == 2) crashCts.Cancel();
                                }
                                else if (ctx.IsResponse && ctx.Kind == SimulatedRpcKind.ListKeys)
                                {
                                    listKeysResponses++;
                                }
                                break;
                            case OperatorCrashPoint.DuringActivateReplyLost:
                                // Let Activate's Accept reach quorum on V(2), then cancel right on the 2nd Accept reply.
                                if (ctx.IsResponse && ctx.Kind == SimulatedRpcKind.Accept
                                    && ctx.Ballot.Configuration == 2
                                    && ctx.Key == VoterConfiguration.RegisterKey)
                                {
                                    // First 2-3 acks are Install transfer of K_cfg; next 2 acks are Activate.
                                    if (++activateAcceptResponses >= 5) crashCts.Cancel();
                                }
                                break;
                        }
                    });

                try
                {
                    await crashingOp.ReconfigureAsync(c1, 1, c2, crashCts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Expected simulated operator crash.
                }
            }

            // A brand-new operator (proposerId: 3) resumes from c = 1 seed and completes cleanly.
            var resumingOp = sim.CreateOperator(proposerId: 3);
            var active = await resumingOp.ResumeAsync(c1, 1);
            Assert.Equal(2, active.Number);
            Assert.Equal(VoterConfigurationState.Active, active.State);
            Assert.True(active.HasSameVoters(c2));

            // Every user key and its exact logical version survived intact on V(2).
            var v2Client = sim.CreateClient(4, 2, ["v1", "v2", "v4"], ProposerModePolicy.Adaptive());
            var rk1 = await v2Client.ReadAsync("k1");
            var rk2 = await v2Client.ReadAsync("k2");
            var rk3 = await v2Client.ReadAsync("k3");
            Assert.Equal(k1.Current, rk1);
            Assert.Equal(k2.Current, rk2);
            Assert.Equal(k3.Current, rk3);

            // Subsequent conditional updates succeed in c = 2.
            var uk2 = await v2Client.CompareExchangeAsync("k2", rk2.Version, new("val2-next"u8));
            Assert.Equal(CompareExchangeStatus.Applied, uk2.Status);
        });
    }

    [Fact]
    public async Task FiveVoterReconfigurationToleratesAdditionalVoterLossDuringInstall()
    {
        // Spec §9.2: N = 5, f_lost = 1 (v5 already lost) tolerates 1 additional loss in V(1) during Install.
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1201);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3", "v4", "v5");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v3", "v4", "v6");

        cluster.Run(async sim =>
        {
            var client = sim.CreateClient(1, 1, ["v1", "v2", "v3", "v4", "v5"], ProposerModePolicy.Adaptive());
            var w1 = await client.CompareExchangeAsync("doc", null, new("hello-5"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);

            // Lose v5 permanently before reconfiguration starts.
            sim.LoseVoter("v5");

            // Crash v1 right as ListKeys begins during Step 3 Install, forcing Step 3 to fence surviving v4
            // and enumerate from {v2, v3, v4} (a fresh classic quorum F' of V(1)).
            var crashedV1 = false;
            var op = sim.CreateOperator(
                proposerId: 2,
                rpcObserver: ctx =>
                {
                    if (!crashedV1 && !ctx.IsResponse && ctx.Kind == SimulatedRpcKind.ListKeys && ctx.VoterId == "v1")
                    {
                        crashedV1 = true;
                        sim.CrashVoter("v1");
                    }
                });

            var active = await op.ReconfigureAsync(c1, 1, c2);
            Assert.True(crashedV1);
            Assert.Equal(2, active.Number);
            Assert.Equal(VoterConfigurationState.Active, active.State);

            // Read succeeds on surviving V(2) quorum {v2, v3, v4, v6}.
            var read = await client.ReadAsync("doc");
            Assert.Equal(2, client.ActiveConfiguration);
            Assert.Equal(w1.Current, read);
        });
    }

    [Fact]
    public async Task RacingConcurrentOperatorsSameAndConflictingTargetsSerializeSafely()
    {
        // Part 1: Two concurrent operators racing with the SAME target V(2) both succeed idempotently.
        await using (var sameCluster = new SimulatedReconfigurationCluster(seed: 1301))
        {
            var c1 = sameCluster.RegisterConfiguration(1, "v1", "v2", "v3");
            var c2 = sameCluster.RegisterConfiguration(2, "v1", "v2", "v4");

            sameCluster.Run(async sim =>
            {
                var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
                var w = await client.CompareExchangeAsync("shared", null, new("v1"u8));
                Assert.Equal(CompareExchangeStatus.Applied, w.Status);

                var opA = sim.CreateOperator(2);
                var opB = sim.CreateOperator(3);
                var tA = opA.ReconfigureAsync(c1, 1, c2);
                var tB = opB.ReconfigureAsync(c1, 1, c2);
                var results = await Task.WhenAll(tA, tB);

                Assert.All(results, r =>
                {
                    Assert.Equal(2, r.Number);
                    Assert.Equal(VoterConfigurationState.Active, r.State);
                    Assert.True(r.HasSameVoters(c2));
                });
                Assert.Equal(w.Current, await client.ReadAsync("shared"));
            });
        }

        // Part 2: Two concurrent operators racing with CONFLICTING targets V_A vs V_B — exactly one wins.
        await using (var conflictCluster = new SimulatedReconfigurationCluster(seed: 1302))
        {
            var c1 = conflictCluster.RegisterConfiguration(1, "v1", "v2", "v3");
            conflictCluster.AddVoter("v4", initialConfiguration: 2);
            conflictCluster.AddVoter("v5", initialConfiguration: 2);
            ImmutableArray<VoterDescriptor> c2A = [new("v1", "sim://v1"), new("v2", "sim://v2"), new("v4", "sim://v4")];
            ImmutableArray<VoterDescriptor> c2B = [new("v1", "sim://v1"), new("v3", "sim://v3"), new("v5", "sim://v5")];

            // Register both candidate c2 sets lazily via whichever wins — use a fresh cluster without pre-registering conflicting c=2.
            conflictCluster.Run(async sim =>
            {
                var client = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive());
                var w = await client.CompareExchangeAsync("k", null, new("init"u8));
                Assert.Equal(CompareExchangeStatus.Applied, w.Status);

                var opA = sim.CreateOperator(2);
                var opB = sim.CreateOperator(3);
                var tA = CaptureOutcomeAsync(opA.ReconfigureAsync(c1, 1, c2A));
                var tB = CaptureOutcomeAsync(opB.ReconfigureAsync(c1, 1, c2B));
                var (outA, outB) = (await tA, await tB);

                // Exactly one operator won (Active) and the other observed ReconfigurationConflictException.
                var winner = outA.Config ?? outB.Config;
                var conflict = outA.Conflict ?? outB.Conflict;
                Assert.NotNull(winner);
                Assert.NotNull(conflict);
                Assert.Equal(2, winner!.Number);
                Assert.Equal(VoterConfigurationState.Active, winner.State);
                Assert.Equal(2, conflict!.WinningConfiguration.Number);
                Assert.True(winner.HasSameVoters(conflict.WinningConfiguration.Voters));
            });
        }
    }

    [Fact]
    public async Task StaleProposerHoldingPreparedFastBallotIsFencedAndRecoversToNextConfig()
    {
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1401);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v4");

        cluster.Run(async sim =>
        {
            // StaticFast client writes in c = 1 using (1, 1, 0) and caches piggybacked fast ballot (1, 2, 0).
            var staleFastClient = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.StaticFast);
            var first = await staleFastClient.CompareExchangeAsync("fastkey", null, new("fast-1"u8));
            Assert.Equal(CompareExchangeStatus.Applied, first.Status);
            // Verify initial write skipped prepare (1-RTT initial fast ballot (1, 1, 0)).
            Assert.Equal(0, staleFastClient.PrepareCount);

            // Reconfigure 1 -> 2 while staleFastClient still holds prepared fast ballot (1, 2, 0)!
            var op = sim.CreateOperator(2);
            var active2 = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active2.Number);
            Assert.Equal(VoterConfigurationState.Active, active2.State);

            // Stale client attempts its next write using cached fast ballot (1, 2, 0):
            // it hits floor (2, 0, 0), discards the stale prepared ballot, refreshes to c = 2, and succeeds!
            Assert.Equal(1, staleFastClient.ActiveConfiguration);
            var second = await staleFastClient.CompareExchangeAsync("fastkey", first.Current!.Version, new("after-fence"u8));
            Assert.Equal(CompareExchangeStatus.Applied, second.Status);
            Assert.Equal(2, staleFastClient.ActiveConfiguration);
            Assert.Equal(new ByteString("after-fence"u8), second.Current!.Value);
            Assert.Equal(2UL, ReadRevision(second.Current));
        });
    }

    [Fact]
    public async Task PostReconfigurationOneRttFastCommitsSucceedInConfigGreaterThanOne()
    {
        // Use a 4-voter cluster (classic quorum = 3, fast quorum = 3) so the first classic write
        // in c = 2 immediately certifies piggybacked fast ballot (2, 2, 0) even under network jitter.
        await using var cluster = new SimulatedReconfigurationCluster(seed: 1501);
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3", "v4");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v3", "v5");

        cluster.Run(async sim =>
        {
            var op = sim.CreateOperator(2);
            var active2 = await op.ReconfigureAsync(c1, 1, c2);
            Assert.Equal(2, active2.Number);
            Assert.Equal(VoterConfigurationState.Active, active2.State);

            var fastClient = sim.CreateClient(1, 2, ["v1", "v2", "v3", "v5"], ProposerModePolicy.StaticFast);

            // First write in c = 2 MUST NOT use an implicitly prepared (2, 1, 0) fast ballot (§4.2):
            // it runs classic prepare + accept and piggybacks (2, 2, 0).
            fastClient.ResetRpcCounters();
            var w1 = await fastClient.CompareExchangeAsync("post-reconfig", null, new("first-in-c2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w1.Status);
            Assert.True(fastClient.PrepareCount > 0, "First write in c > 1 must execute a classic Prepare.");

            // Second write in c = 2 reuses piggybacked fast ballot (2, 2, 0) in 1 RTT with ZERO prepares!
            fastClient.ResetRpcCounters();
            var w2 = await fastClient.CompareExchangeAsync("post-reconfig", w1.Current!.Version, new("fast-in-c2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w2.Status);
            Assert.Equal(0, fastClient.PrepareCount);
            Assert.True(fastClient.AcceptCount > 0);
            Assert.Equal(2UL, ReadRevision(w2.Current!));

            // Third consecutive 1-RTT fast write in c = 2 using piggybacked (2, 3, 0)!
            fastClient.ResetRpcCounters();
            var w3 = await fastClient.CompareExchangeAsync("post-reconfig", w2.Current!.Version, new("fast2-in-c2"u8));
            Assert.Equal(CompareExchangeStatus.Applied, w3.Status);
            Assert.Equal(0, fastClient.PrepareCount);
            Assert.Equal(3UL, ReadRevision(w3.Current!));
        });
    }

    [Theory]
    [InlineData(2001)]
    [InlineData(2002)]
    [InlineData(2003)]
    [InlineData(2004)]
    [InlineData(2005)]
    public async Task ChaosSeedSweepWithConcurrentClientsDropsPartitionsAndChainedReconfigMaintainsSafety(int seed)
    {
        await using var cluster = new SimulatedReconfigurationCluster(
            seed,
            baseMessageDelay: TimeSpan.FromMilliseconds(2),
            maxJitter: TimeSpan.FromMilliseconds(5));
        var c1 = cluster.RegisterConfiguration(1, "v1", "v2", "v3");
        var c2 = cluster.RegisterConfiguration(2, "v1", "v2", "v4");
        var c3 = cluster.RegisterConfiguration(3, "v1", "v2", "v4", "v5");

        cluster.Run(async sim =>
        {
            var clientA = sim.CreateClient(1, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive(), operationTimeout: TimeSpan.FromSeconds(8));
            var clientB = sim.CreateClient(2, 1, ["v1", "v2", "v3"], ProposerModePolicy.Adaptive(), operationTimeout: TimeSpan.FromSeconds(8));
            var op = sim.CreateOperator(3, operationTimeout: TimeSpan.FromSeconds(15));

            // Seed two keys before chaos begins.
            var initA = await clientA.CompareExchangeAsync("hot", null, new("s0"u8));
            Assert.Equal(CompareExchangeStatus.Applied, initA.Status);
            var initB = await clientB.CompareExchangeAsync("cold", null, new("c0"u8));
            Assert.Equal(CompareExchangeStatus.Applied, initB.Status);

            // Inject message drops and transient voter crash/restart alongside concurrent client traffic + reconfiguration 1 -> 2.
            sim.SetMessageDropRate(0.05);

            var clientTask = RunClientWorkloadAsync(sim, clientA, "hot", iterations: 6, seed);
            var readerTask = RunReadWorkloadAsync(sim, clientB, "hot", iterations: 6);
            var crashTask = CrashAndRestartVoterAsync(sim, "v3", TimeSpan.FromMilliseconds(15), TimeSpan.FromMilliseconds(40));
            var reconfig1Task = RunWithTransientFaultAsync(sim, async () =>
            {
                await sim.DelayAsync(TimeSpan.FromMilliseconds(10));
                return await op.ReconfigureAsync(c1, 1, c2);
            });

            var active2 = await reconfig1Task;
            Assert.Equal(2, active2.Number);
            Assert.Equal(VoterConfigurationState.Active, active2.State);
            await Task.WhenAll(clientTask, readerTask, crashTask);

            // Chain 2 -> 3 (add-1 to 4 voters) with temporary isolation of v4 then heal.
            sim.IsolateVoter("v4");
            var healTask = HealAfterAsync(sim, TimeSpan.FromMilliseconds(45));
            var active3 = await op.ReconfigureAsync(c2, 2, c3);
            await healTask;
            Assert.Equal(3, active3.Number);
            Assert.Equal(VoterConfigurationState.Active, active3.State);

            // Heal drops and verify both clients converge to c = 3 and observe consistent final state.
            sim.SetMessageDropRate(0);
            var finalA = await clientA.ReadAsync("hot");
            var finalB = await clientB.ReadAsync("hot");
            Assert.Equal(3, clientA.ActiveConfiguration);
            Assert.Equal(3, clientB.ActiveConfiguration);
            Assert.Equal(finalA, finalB);
            Assert.Equal(initB.Current, await clientA.ReadAsync("cold"));
        });
    }

    [Fact]
    public void RegisterLinearizabilityCheckerDetectsStaleReadViolation()
    {
        var checker = new RegisterLinearizabilityChecker();
        var rev1 = new ByteString([0, 0, 0, 0, 0, 0, 0, 1]);
        var val1 = new RegisterValue(new ByteString("a"u8), rev1);

        var op1 = checker.BeginOperation("k", RecordedRegisterOpKind.CompareExchange, invokeTick: 10, expectedVersion: null, writePayload: new ByteString("a"u8));
        checker.CompleteCompareExchange(op1, completeTick: 20, new CompareExchangeResult(CompareExchangeStatus.Applied, val1));

        // Stale read invoked strictly after write completed returns Absent — must be rejected!
        var op2 = checker.BeginOperation("k", RecordedRegisterOpKind.Read, invokeTick: 30);
        checker.CompleteRead(op2, completeTick: 40, new RegisterValue(null, null));

        var ex = Assert.Throws<InvalidOperationException>(() => checker.AssertLinearizable());
        Assert.Contains("Linearizability violated", ex.Message, StringComparison.Ordinal);
    }

    private static async Task RunClientWorkloadAsync(
        SimulatedReconfigurationCluster sim,
        TrackedRegisterClient client,
        string key,
        int iterations,
        int seed)
    {
        for (var i = 0; i < iterations; i++)
        {
            try
            {
                var cur = await client.ReadAsync(key);
                var payload = new ByteString(System.Text.Encoding.UTF8.GetBytes($"s{seed}-{i}"));
                await client.CompareExchangeAsync(key, cur.Version, payload);
            }
            catch (RegisterUnavailableException)
            {
                // Transient unavailability during Fence -> Activate pause is expected (§9.1).
            }

            await sim.DelayAsync(TimeSpan.FromMilliseconds(8));
        }
    }

    private static async Task RunReadWorkloadAsync(
        SimulatedReconfigurationCluster sim,
        TrackedRegisterClient client,
        string key,
        int iterations)
    {
        for (var i = 0; i < iterations; i++)
        {
            try
            {
                await client.ReadAsync(key);
            }
            catch (RegisterUnavailableException)
            {
                // Transient unavailability during Fence -> Activate pause is expected (§9.1).
            }

            await sim.DelayAsync(TimeSpan.FromMilliseconds(10));
        }
    }

    private static async Task<VoterConfiguration> RunWithTransientFaultAsync(
        SimulatedReconfigurationCluster sim,
        Func<Task<VoterConfiguration>> action) =>
        await action();

    private static async Task CrashAndRestartVoterAsync(
        SimulatedReconfigurationCluster sim,
        string voterId,
        TimeSpan crashAfter,
        TimeSpan downDuration)
    {
        await sim.DelayAsync(crashAfter);
        sim.CrashVoter(voterId);
        await sim.DelayAsync(downDuration);
        sim.RestartVoter(voterId);
    }

    private static async Task HealAfterAsync(SimulatedReconfigurationCluster sim, TimeSpan delay)
    {
        await sim.DelayAsync(delay);
        sim.ReconnectVoter("v4");
    }

    private static async Task<(VoterConfiguration? Config, ReconfigurationConflictException? Conflict)> CaptureOutcomeAsync(
        Task<VoterConfiguration> task)
    {
        try
        {
            return (await task, null);
        }
        catch (ReconfigurationConflictException conflict)
        {
            return (null, conflict);
        }
    }
}
