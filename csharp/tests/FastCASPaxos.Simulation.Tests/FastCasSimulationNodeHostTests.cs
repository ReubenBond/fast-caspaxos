using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Hosts;
using FastCASPaxos.Simulation.Nodes;
using FastCASPaxos.Simulation.Transport;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationNodeHostTests
{
    [Fact]
    public void ProposerAndAcceptorsReachConsensusThroughSimulationTransport()
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var clusterQueue = new SimulationTaskQueue(clock, guard);
        List<SimulationNode> nodes = [];
        var network = new SimulationNetwork(() => [.. nodes], new SimulationRandom(321));
        var transport = new FastCasSimulationTransport<TestValue>(network);
        var options = new FastCasSimulationOptions
        {
            ProposerModePolicy = ProposerModePolicy.StaticClassic,
        };

        var acceptorNodes = new List<FastCasSimulationNode<TestValue>>();
        var acceptorHosts = new List<FastCasAcceptorHost<TestValue>>();
        var acceptorAddresses = new List<FastCasAddress>();
        for (var i = 1; i <= 5; i++)
        {
            var nodeId = new FastCasNodeId(FastCasParticipantRole.Acceptor, i);
            var host = new FastCasAcceptorHost<TestValue>(nodeId, transport, default, options: options);
            var node = new FastCasSimulationNode<TestValue>(
                host,
                clock,
                guard,
                new SimulationRandom(100 + i),
                clusterQueue,
                transport);

            acceptorHosts.Add(host);
            acceptorNodes.Add(node);
            acceptorAddresses.Add(nodeId.Address);
            nodes.Add(node);
        }

        var proposerHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerNode = new FastCasSimulationNode<TestValue>(
            proposerHost,
            clock,
            guard,
            new SimulationRandom(500),
            clusterQueue,
            transport);
        nodes.Add(proposerNode);

        var clientQueue = new SimulationTaskQueue(clock, guard);
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientQueue,
            message => clientMessages.Add(message)));

        var proposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerHost.Address,
            1,
            AppendOperation(1, "A"));

        Assert.Equal(DeliveryStatus.Success, transport.Send(proposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var completion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), completion.Payload.CommittedValue);
        Assert.Equal(new TestValue(1, "A"), proposerHost.CachedValue);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(1, "A"), host.State.AcceptedValue));
    }

    [Fact]
    public void ProposerLearnerAdoptsCommittedValueAndSupportsNextProposal()
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var clusterQueue = new SimulationTaskQueue(clock, guard);
        List<SimulationNode> nodes = [];
        var network = new SimulationNetwork(() => [.. nodes], new SimulationRandom(654));
        var transport = new FastCasSimulationTransport<TestValue>(network);
        var options = new FastCasSimulationOptions
        {
            ProposerModePolicy = ProposerModePolicy.StaticClassic,
        };

        var acceptorHosts = new List<FastCasAcceptorHost<TestValue>>();
        var acceptorAddresses = new List<FastCasAddress>();
        for (var i = 1; i <= 5; i++)
        {
            var nodeId = new FastCasNodeId(FastCasParticipantRole.Acceptor, i);
            var host = new FastCasAcceptorHost<TestValue>(nodeId, transport, default, options: options);
            var node = new FastCasSimulationNode<TestValue>(
                host,
                clock,
                guard,
                new SimulationRandom(200 + i),
                clusterQueue,
                transport);

            acceptorHosts.Add(host);
            acceptorAddresses.Add(nodeId.Address);
            nodes.Add(node);
        }

        var proposerOneHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerOneNode = new FastCasSimulationNode<TestValue>(
            proposerOneHost,
            clock,
            guard,
            new SimulationRandom(700),
            clusterQueue,
            transport);
        proposerOneHost.BindSimulationContext(proposerOneNode.Context);
        nodes.Add(proposerOneNode);

        var proposerTwoHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 2),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerTwoNode = new FastCasSimulationNode<TestValue>(
            proposerTwoHost,
            clock,
            guard,
            new SimulationRandom(701),
            clusterQueue,
            transport);
        proposerTwoHost.BindSimulationContext(proposerTwoNode.Context);
        nodes.Add(proposerTwoNode);

        var clientQueue = new SimulationTaskQueue(clock, guard);
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientQueue,
            message => clientMessages.Add(message)));

        var firstProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerOneHost.Address,
            1,
            AppendOperation(1, "A"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(firstProposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var firstCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), firstCompletion.Payload.CommittedValue);

        Assert.Equal(
            DeliveryStatus.Success,
            transport.Send(new FastCasLearnCommittedValueMessage<TestValue>(
                proposerOneHost.Address,
                proposerTwoHost.Address,
                proposerOneHost.Ballot,
                proposerOneHost.CachedValue)));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        Assert.Equal(proposerOneHost.Ballot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(1, "A"), proposerTwoHost.CachedValue);
        Assert.Null(proposerTwoHost.PreparedBallot);

        clientMessages.Clear();
        var secondProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerTwoHost.Address,
            2,
            AppendOperation(2, "B"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(secondProposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var secondCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(2, "AB"), secondCompletion.Payload.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(2, "AB"), host.State.AcceptedValue));
    }

    [Fact]
    public void ProposerLearnerFastModeLearnedCommitArrivesAfterNetworkDelayAndCarriesPreparedBallot()
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var clusterQueue = new SimulationTaskQueue(clock, guard);
        List<SimulationNode> nodes = [];
        var network = new SimulationNetwork(() => [.. nodes], new SimulationRandom(777));
        var transport = new FastCasSimulationTransport<TestValue>(network);
        var options = new FastCasSimulationOptions
        {
            ProposerModePolicy = ProposerModePolicy.StaticFast,
        };

        var acceptorHosts = new List<FastCasAcceptorHost<TestValue>>();
        var acceptorAddresses = new List<FastCasAddress>();
        for (var i = 1; i <= 5; i++)
        {
            var nodeId = new FastCasNodeId(FastCasParticipantRole.Acceptor, i);
            var host = new FastCasAcceptorHost<TestValue>(nodeId, transport, default, options: options);
            var node = new FastCasSimulationNode<TestValue>(
                host,
                clock,
                guard,
                new SimulationRandom(300 + i),
                clusterQueue,
                transport);

            acceptorHosts.Add(host);
            acceptorAddresses.Add(nodeId.Address);
            nodes.Add(node);
        }

        var proposerOneHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerOneNode = new FastCasSimulationNode<TestValue>(
            proposerOneHost,
            clock,
            guard,
            new SimulationRandom(800),
            clusterQueue,
            transport);
        proposerOneHost.BindSimulationContext(proposerOneNode.Context);
        nodes.Add(proposerOneNode);

        var proposerTwoHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 2),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerTwoNode = new FastCasSimulationNode<TestValue>(
            proposerTwoHost,
            clock,
            guard,
            new SimulationRandom(801),
            clusterQueue,
            transport);
        proposerTwoHost.BindSimulationContext(proposerTwoNode.Context);
        nodes.Add(proposerTwoNode);

        var clientQueue = new SimulationTaskQueue(clock, guard);
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientQueue,
            message => clientMessages.Add(message)));

        var firstProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerOneHost.Address,
            1,
            AppendOperation(1, "A"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(firstProposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var firstCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), firstCompletion.Payload.CommittedValue);

        var learnedPreparedBallot = Assert.IsType<Ballot>(proposerOneHost.PreparedBallot);
        network.EnableDelays = true;
        network.BaseMessageDelay = TimeSpan.Zero;
        network.MaxJitter = TimeSpan.Zero;
        var learnDelay = TimeSpan.FromMilliseconds(50);
        network.SetLinkLatency(
            proposerOneHost.Address.NetworkAddress,
            proposerTwoHost.Address.NetworkAddress,
            learnDelay,
            TimeSpan.Zero);

        Assert.Equal(
            DeliveryStatus.Success,
            transport.Send(new FastCasLearnCommittedValueMessage<TestValue>(
                proposerOneHost.Address,
                proposerTwoHost.Address,
                proposerOneHost.Ballot,
                proposerOneHost.CachedValue,
                learnedPreparedBallot)));

        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);
        Assert.Equal(default, proposerTwoHost.CachedValue);
        Assert.Equal(Ballot.Zero, proposerTwoHost.Ballot);
        Assert.Null(proposerTwoHost.PreparedBallot);

        clock.Advance(learnDelay);
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        Assert.Equal(proposerOneHost.Ballot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(1, "A"), proposerTwoHost.CachedValue);
        Assert.Equal(learnedPreparedBallot, proposerTwoHost.PreparedBallot);

        clientMessages.Clear();
        var secondProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerTwoHost.Address,
            2,
            AppendOperation(2, "B"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(secondProposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var secondCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(2, "AB"), secondCompletion.Payload.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        var replaySafeBallot = proposerTwoHost.Ballot;
        var replaySafePreparedBallot = Assert.IsType<Ballot>(proposerTwoHost.PreparedBallot);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(2, "AB"), host.State.AcceptedValue));

        var replayDelay = TimeSpan.FromMilliseconds(25);
        network.SetLinkLatency(
            proposerOneHost.Address.NetworkAddress,
            proposerTwoHost.Address.NetworkAddress,
            replayDelay,
            TimeSpan.Zero);
        Assert.Equal(
            DeliveryStatus.Success,
            transport.Send(new FastCasLearnCommittedValueMessage<TestValue>(
                proposerOneHost.Address,
                proposerTwoHost.Address,
                proposerOneHost.Ballot,
                proposerOneHost.CachedValue,
                learnedPreparedBallot)));

        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);
        Assert.Equal(replaySafeBallot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.Equal(replaySafePreparedBallot, proposerTwoHost.PreparedBallot);

        clock.Advance(replayDelay);
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        Assert.Equal(replaySafeBallot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.Equal(replaySafePreparedBallot, proposerTwoHost.PreparedBallot);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void QueuedReadCompletesThroughItsOwnAcceptQuorum(bool forwarded)
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var acceptorQueue = new SimulationTaskQueue(clock, guard);
        var clientQueue = new SimulationTaskQueue(clock, guard);
        var network = new SimulationNetwork(() => [], new SimulationRandom(918));
        var transport = new FastCasSimulationTransport<TestValue>(network);
        FastCasAddress[] acceptors = [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(2), FastCasAddress.Acceptor(3)];
        List<IFastCasTransportMessage<TestValue>> requests = [];
        List<IFastCasTransportMessage<TestValue>> responses = [];
        foreach (var acceptor in acceptors)
        {
            transport.RegisterEndpoint(new(acceptor, acceptorQueue, requests.Add));
        }

        var caller = FastCasAddress.Client();
        var forwardingProposer = FastCasAddress.Proposer(2);
        transport.RegisterEndpoint(new(caller, clientQueue, responses.Add));
        transport.RegisterEndpoint(new(forwardingProposer, clientQueue, responses.Add));
        var host = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptors],
            transport,
            default,
            new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
                ProposerResponseTimeout = TimeSpan.Zero,
            });

        host.Deliver(new FastCasProposeRequestMessage<TestValue>(caller, host.Address, 1, AppendOperation(1, "A")));
        var read = new Operation<int, TestValue>
        {
            Input = 0,
            Apply = static (current, _) => (OperationStatus.NotApplicable, current),
        };
        if (forwarded)
        {
            host.Deliver(new FastCasForwardProposeRequestMessage<TestValue>(
                forwardingProposer, host.Address, caller, 2, read));
        }
        else
        {
            host.Deliver(new FastCasProposeRequestMessage<TestValue>(caller, host.Address, 2, read));
        }

        var writeBallot = host.Ballot;
        Promise(acceptors[0], Ballot.Zero, default);
        Promise(acceptors[1], Ballot.Zero, default);
        Accept(acceptors[0]);
        Accept(acceptors[1]);
        RunUntilIdle(acceptorQueue, clientQueue, []);

        var value = new TestValue(1, "A");
        var writeResponse = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(responses));
        Assert.Equal(1, writeResponse.RequestId);
        Assert.Equal(value, writeResponse.Payload.CommittedValue);
        Assert.True(host.HasPendingClientRequests);
        Assert.True(host.Ballot > writeBallot);
        Assert.Equal(6, requests.OfType<FastCasPrepareRequestMessage<TestValue>>().Count());

        Promise(acceptors[0], writeBallot, value);
        Promise(acceptors[1], writeBallot, value);
        RunUntilIdle(acceptorQueue, clientQueue, []);
        var readAccepts = requests.OfType<FastCasAcceptRequestMessage<TestValue>>()
            .Where(message => message.Payload.Ballot == host.Ballot).ToArray();
        Assert.Equal(3, readAccepts.Length);
        Assert.All(readAccepts, message => Assert.Equal(value, message.Payload.Value));
        Assert.Single(responses);

        Accept(acceptors[0]);
        RunUntilIdle(acceptorQueue, clientQueue, []);
        Assert.Single(responses);
        Accept(acceptors[1]);
        RunUntilIdle(acceptorQueue, clientQueue, []);

        Assert.Equal(2, responses.Count);
        if (forwarded)
        {
            var response = Assert.IsType<FastCasForwardProposeResponseMessage<TestValue>>(responses[1]);
            Assert.Equal(forwardingProposer, response.Destination);
            Assert.Equal(caller, response.OriginalCaller);
            Assert.Equal(2, response.RequestId);
            Assert.Equal(value, response.Payload.CommittedValue);
        }
        else
        {
            var response = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(responses[1]);
            Assert.Equal(caller, response.Destination);
            Assert.Equal(2, response.RequestId);
            Assert.Equal(value, response.Payload.CommittedValue);
        }

        Assert.False(host.HasPendingClientRequests);

        void Promise(FastCasAddress acceptor, Ballot acceptedBallot, TestValue acceptedValue) =>
            host.Deliver(new FastCasPreparePromiseMessage<TestValue>(
                acceptor, host.Address, new(host.Ballot.Round, acceptor, acceptedBallot, acceptedValue)));

        void Accept(FastCasAddress acceptor) =>
            host.Deliver(new FastCasAcceptAcceptedMessage<TestValue>(
                acceptor, host.Address, new(host.Ballot.Round, acceptor, Ballot.Zero)));
    }

    private static Operation<TestValue, TestValue> AppendOperation(int expectedVersion, string suffix) =>
        new Operation<TestValue, TestValue>
        {
            Input = new TestValue(expectedVersion, suffix),
            Name = $"Append '{suffix}' at version {expectedVersion}",
            Apply = static (current, input) =>
            {
                if (input.Version == current.Version + 1)
                {
                    var currentValue = current.Value ?? string.Empty;
                    return (OperationStatus.Success, new TestValue(input.Version, currentValue + input.Value));
                }

                if (current.Version >= input.Version)
                {
                    return (OperationStatus.NotApplicable, current);
                }

                return (OperationStatus.Failed, current);
            },
        };

    private static void RunUntilIdle(
        SimulationTaskQueue clusterQueue,
        SimulationTaskQueue clientQueue,
        ReadOnlySpan<SimulationTaskQueue> nodeQueues)
    {
        for (var i = 0; i < 1000; i++)
        {
            var madeProgress = false;
            madeProgress |= clusterQueue.RunOnce();
            madeProgress |= clientQueue.RunOnce();

            foreach (var queue in nodeQueues)
            {
                madeProgress |= queue.RunOnce();
            }

            if (!madeProgress)
            {
                return;
            }
        }

        throw new InvalidOperationException("Simulation queues did not quiesce.");
    }

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
