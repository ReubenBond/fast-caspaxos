using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Transport;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class RegisterRestartSimulationTests
{
    [Theory]
    [InlineData("classic")]
    [InlineData("leader")]
    public async Task Restart_LaggingReplicaAcceptRetryPreservesNewerQuorum(string mode)
    {
        // Every request/reply runs through Clockwork queues. A fixed seed, fixed 1ms links
        // and one completed exchange per phase pin the trace without sleeps or thread-pool races.
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 12989184,
            initialValue: default,
            options: new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.Parse(mode),
                CrashRestartStateModel = FastCasCrashRestartStateModel.PreserveDurableAcceptorState
            },
            startDateTime: DateTimeOffset.UnixEpoch);
        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 3);
        cluster.Network.EnableDelays = true;
        cluster.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(1);
        cluster.Network.MaxJitter = TimeSpan.Zero;
        var probe = FastCasAddress.Client(42);
        var replies = new List<IFastCasTransportMessage<TestValue>>();
        cluster.Transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            probe, cluster.TaskQueue, replies.Add));
        var acceptors = cluster.AcceptorNodes.Select(node => Assert.IsType<FastCasAcceptorHost<TestValue>>(node.Host)).ToArray();
        var oldValue = new TestValue(1, 1);
        var newValue = new TestValue(2, 2);
        var oldPrepare = new PrepareRequest(new Ballot(2, 8));
        var oldAccept = new AcceptRequest<TestValue>(
            oldPrepare.Ballot, oldValue, mode == "leader" ? new Ballot(3, 8) : Ballot.Zero);
        var newPrepare = new PrepareRequest(new Ballot(5, 8));
        var newAccept = new AcceptRequest<TestValue>(
            newPrepare.Ballot, newValue, mode == "leader" ? new Ballot(6, 8) : Ballot.Zero);

        IFastCasTransportMessage<TestValue> Exchange(IFastCasTransportMessage<TestValue> request)
        {
            Assert.Empty(replies);
            var sentAt = cluster.Clock.UtcNow;
            Assert.Equal(DeliveryStatus.Success, cluster.Transport.Send(request));
            Assert.True(cluster.RunUntil(() => replies.Count == 1, maxIterations: 1000));
            Assert.Equal(sentAt + TimeSpan.FromMilliseconds(2), cluster.Clock.UtcNow);
            var reply = Assert.Single(replies);
            Assert.Equal(request.To, reply.From);
            Assert.Equal(probe, reply.To);
            replies.Clear();
            return reply;
        }

        // Phase 1: prepare/accept the original pair on every node, then crash/restart
        // all participants. The simulator's durable stores must retain the exact state.
        foreach (var acceptor in acceptors)
        {
            Assert.IsType<FastCasPreparePromiseMessage<TestValue>>(Exchange(
                new FastCasPrepareRequestMessage<TestValue>(probe, acceptor.Address, oldPrepare)));
            Assert.IsType<FastCasAcceptAcceptedMessage<TestValue>>(Exchange(
                new FastCasAcceptRequestMessage<TestValue>(probe, acceptor.Address, oldAccept)));
            Assert.Equal(oldAccept.Ballot, acceptor.State.AcceptedBallot);
            Assert.Equal(oldValue, acceptor.State.AcceptedValue);
        }
        var durable = acceptors.Select(Snapshot).ToArray();
        cluster.CrashNode(FastCasAddress.Proposer(1));
        foreach (var acceptor in acceptors) cluster.CrashNode(acceptor.Address);
        foreach (var acceptor in acceptors) cluster.RestartNode(acceptor.Address);
        cluster.RestartNode(FastCasAddress.Proposer(1));
        Assert.Equal(durable, acceptors.Select(Snapshot).ToArray());

        // Phase 2: deliberately lose the restarted writer's messages to node 3.
        // Exactly nodes 1+2 accept the newer value; node 3 remains at the old pair.
        var lagging = acceptors[2];
        cluster.Network.CreateBidirectionalPartition(probe.NetworkAddress, lagging.Address.NetworkAddress);
        foreach (var acceptor in acceptors.Take(2))
        {
            Assert.IsType<FastCasPreparePromiseMessage<TestValue>>(Exchange(
                new FastCasPrepareRequestMessage<TestValue>(probe, acceptor.Address, newPrepare)));
            Assert.IsType<FastCasAcceptAcceptedMessage<TestValue>>(Exchange(
                new FastCasAcceptRequestMessage<TestValue>(probe, acceptor.Address, newAccept)));
            Assert.Equal(newAccept.Ballot, acceptor.State.AcceptedBallot);
            Assert.Equal(newValue, acceptor.State.AcceptedValue);
        }
        Assert.Equal(DeliveryStatus.Partitioned, cluster.Transport.Send(
            new FastCasPrepareRequestMessage<TestValue>(probe, lagging.Address, newPrepare)));
        Assert.Equal(DeliveryStatus.Partitioned, cluster.Transport.Send(
            new FastCasAcceptRequestMessage<TestValue>(probe, lagging.Address, newAccept)));
        Assert.Empty(replies);
        Assert.Equal(durable[2], Snapshot(lagging));

        // Phase 3: heal, but deliver only a newer prepare to node 3. Its promise
        // advances while its accepted pair stays old, the exact CI counterexample.
        cluster.Network.HealBidirectionalPartition(probe.NetworkAddress, lagging.Address.NetworkAddress);
        Assert.IsType<FastCasPreparePromiseMessage<TestValue>>(Exchange(
            new FastCasPrepareRequestMessage<TestValue>(probe, lagging.Address, newPrepare)));
        Assert.Equal(newPrepare.Ballot, lagging.State.PromisedBallot);
        Assert.Equal(oldAccept.Ballot, lagging.State.AcceptedBallot);
        Assert.Equal(oldValue, lagging.State.AcceptedValue);

        // Phase 4: delayed old requests must not change any accepted/promise/value state.
        foreach (var acceptor in acceptors)
        {
            var before = Snapshot(acceptor);
            var highestBallot = Ballot.Max(before.Promised, before.Accepted);
            var prepare = Assert.IsType<FastCasPrepareRejectionMessage<TestValue>>(Exchange(
                new FastCasPrepareRequestMessage<TestValue>(probe, acceptor.Address, oldPrepare)));
            Assert.Equal(oldPrepare.Ballot.Round, prepare.Payload.Round);
            Assert.Equal(highestBallot, prepare.Payload.ConflictBallot);
            Assert.Equal(before.Accepted, prepare.Payload.AcceptedBallot);
            Assert.Equal(before.Value, prepare.Payload.AcceptedValue);
            Assert.Equal(before, Snapshot(acceptor));

            var currentPrepare = Assert.IsType<FastCasPreparePromiseMessage<TestValue>>(Exchange(
                new FastCasPrepareRequestMessage<TestValue>(probe, acceptor.Address, new(before.Promised))));
            Assert.Equal(before.Accepted, currentPrepare.Payload.AcceptedBallot);
            Assert.Equal(before.Value, currentPrepare.Payload.AcceptedValue);
            Assert.Equal(before, Snapshot(acceptor));

            var accept = Exchange(new FastCasAcceptRequestMessage<TestValue>(probe, acceptor.Address, oldAccept));
            if (ReferenceEquals(acceptor, lagging))
            {
                // This schedule fixes exactly one possible response: node 3 still owns
                // the old pair, so it ACKs read-only and reports its newer current promise.
                var accepted = Assert.IsType<FastCasAcceptAcceptedMessage<TestValue>>(accept);
                Assert.Equal(oldAccept.Ballot.Round, accepted.Payload.Round);
                Assert.Equal(acceptor.Address, accepted.Payload.Acceptor);
                Assert.Equal(before.Promised, accepted.Payload.PromisedBallot);
            }
            else
            {
                var superseded = Assert.IsType<FastCasAcceptRejectedMessage<TestValue>>(accept);
                Assert.Equal(oldAccept.Ballot.Round, superseded.Payload.Round);
                Assert.Equal(acceptor.Address, superseded.Payload.Acceptor);
                Assert.Equal(highestBallot, superseded.Payload.ConflictingBallot);
            }
            Assert.Equal(before, Snapshot(acceptor));

            var wrongValue = oldAccept with { Value = newValue, NextBallotToPrepare = highestBallot.NextRound(8) };
            var rejected = Assert.IsType<FastCasAcceptRejectedMessage<TestValue>>(Exchange(
                new FastCasAcceptRequestMessage<TestValue>(probe, acceptor.Address, wrongValue)));
            Assert.Equal(oldAccept.Ballot.Round, rejected.Payload.Round);
            Assert.Equal(highestBallot, rejected.Payload.ConflictingBallot);
            Assert.Equal(before, Snapshot(acceptor));
        }

        // Phase 5: the restarted proposer has no cached result. A real identity accept
        // quorum, driven entirely by Clockwork, must recover the newer value, not node 3's old one.
        var read = cluster.RunProposal(FastCasAddress.Proposer(1),
            new Operation<int, TestValue>
            {
                Input = 0,
                Name = "read-after-delayed-retries",
                Apply = static (current, _) => (OperationStatus.NotApplicable, current)
            },
            maxIterations: 1000);
        Assert.Equal(newValue, read.CommittedValue);
        cluster.AssertSafetyInvariants();
    }

    private static (Ballot Promised, Ballot Accepted, TestValue Value) Snapshot(FastCasAcceptorHost<TestValue> host) =>
        (host.State.PromisedBallot, host.State.AcceptedBallot, host.State.AcceptedValue);

    private readonly record struct TestValue(int Version, int Payload) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) => Version == predecessor.Version + 1;
    }
}
