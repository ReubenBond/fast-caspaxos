using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Hosts;
using Orleans.Consensus.CasPaxos.Simulation.Nodes;
using Orleans.Consensus.CasPaxos.Simulation.Transport;
using Orleans.Testing.Simulation;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class FastCasSimulationNodeHostTests
{
    [Fact]
    public async Task ProposerAndAcceptorsReachConsensusThroughSimulationTransport()
    {
        await using var cluster = CreateCluster(seed: 321);
        var transport = new FastCasSimulationTransport<TestValue>(cluster.Network);
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
            Attach(cluster, host, transport);

            acceptorHosts.Add(host);
            acceptorAddresses.Add(nodeId.Address);
        }

        var proposerHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        Attach(cluster, proposerHost, transport);

        var clientLane = cluster.SchedulerLane;
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientLane,
            message => clientMessages.Add(message)));

        var proposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerHost.Address,
            1,
            AppendOperation(1, "A"));

        Assert.Equal(DeliveryStatus.Success, transport.Send(proposal));
        RunUntilIdle(cluster);

        var completion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), completion.Payload.CommittedValue);
        Assert.Equal(new TestValue(1, "A"), proposerHost.CachedValue);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(1, "A"), host.State.AcceptedValue));
    }

    [Fact]
    public async Task ProposerLearnerAdoptsCommittedValueAndSupportsNextProposal()
    {
        await using var cluster = CreateCluster(seed: 654);
        var transport = new FastCasSimulationTransport<TestValue>(cluster.Network);
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
            Attach(cluster, host, transport);

            acceptorHosts.Add(host);
            acceptorAddresses.Add(nodeId.Address);
        }

        var proposerOneHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerOneNode = Attach(cluster, proposerOneHost, transport);
        proposerOneHost.BindSimulationContext(proposerOneNode.Context);

        var proposerTwoHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 2),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerTwoNode = Attach(cluster, proposerTwoHost, transport);
        proposerTwoHost.BindSimulationContext(proposerTwoNode.Context);

        var clientLane = cluster.SchedulerLane;
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientLane,
            message => clientMessages.Add(message)));

        var firstProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerOneHost.Address,
            1,
            AppendOperation(1, "A"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(firstProposal));
        RunUntilIdle(cluster);

        var firstCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), firstCompletion.Payload.CommittedValue);

        Assert.Equal(
            DeliveryStatus.Success,
            transport.Send(new FastCasLearnCommittedValueMessage<TestValue>(
                proposerOneHost.Address,
                proposerTwoHost.Address,
                proposerOneHost.Ballot,
                proposerOneHost.CachedValue)));
        RunUntilIdle(cluster);

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
        RunUntilIdle(cluster);

        var secondCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(2, "AB"), secondCompletion.Payload.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(2, "AB"), host.State.AcceptedValue));
    }

    [Fact]
    public async Task ProposerLearnerFastModeLearnedCommitArrivesAfterNetworkDelayAndCarriesPreparedBallot()
    {
        await using var cluster = CreateCluster(seed: 777);
        var transport = new FastCasSimulationTransport<TestValue>(cluster.Network);
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
            Attach(cluster, host, transport);

            acceptorHosts.Add(host);
            acceptorAddresses.Add(nodeId.Address);
        }

        var proposerOneHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 1),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerOneNode = Attach(cluster, proposerOneHost, transport);
        proposerOneHost.BindSimulationContext(proposerOneNode.Context);

        var proposerTwoHost = new FastCasProposerHost<TestValue>(
            new FastCasNodeId(FastCasParticipantRole.Proposer, 2),
            [.. acceptorAddresses],
            transport,
            default,
            options);
        var proposerTwoNode = Attach(cluster, proposerTwoHost, transport);
        proposerTwoHost.BindSimulationContext(proposerTwoNode.Context);

        var clientLane = cluster.SchedulerLane;
        List<IFastCasTransportMessage<TestValue>> clientMessages = [];
        var clientAddress = FastCasAddress.Client();
        transport.RegisterEndpoint(new FastCasTransportEndpointRegistration<TestValue>(
            clientAddress,
            clientLane,
            message => clientMessages.Add(message)));

        var firstProposal = new FastCasProposeRequestMessage<TestValue>(
            clientAddress,
            proposerOneHost.Address,
            1,
            AppendOperation(1, "A"));
        Assert.Equal(DeliveryStatus.Success, transport.Send(firstProposal));
        RunUntilIdle(cluster);

        var firstCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), firstCompletion.Payload.CommittedValue);

        var learnedPreparedBallot = Assert.IsType<Ballot>(proposerOneHost.PreparedBallot);
        transport.EnableDelays = true;
        transport.Network.BaseMessageDelay = TimeSpan.Zero;
        transport.Network.MaxJitter = TimeSpan.Zero;
        var learnDelay = TimeSpan.FromMilliseconds(50);
        transport.Network.SetLinkLatency(
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

        RunUntilIdle(cluster);
        Assert.Equal(default, proposerTwoHost.CachedValue);
        Assert.Equal(Ballot.Zero, proposerTwoHost.Ballot);
        Assert.Null(proposerTwoHost.PreparedBallot);

        cluster.RunFor(learnDelay, CancellationToken.None);

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
        RunUntilIdle(cluster);

        var secondCompletion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(2, "AB"), secondCompletion.Payload.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        var replaySafeBallot = proposerTwoHost.Ballot;
        var replaySafePreparedBallot = Assert.IsType<Ballot>(proposerTwoHost.PreparedBallot);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(2, "AB"), host.State.AcceptedValue));

        var replayDelay = TimeSpan.FromMilliseconds(25);
        transport.Network.SetLinkLatency(
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

        RunUntilIdle(cluster);
        Assert.Equal(replaySafeBallot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.Equal(replaySafePreparedBallot, proposerTwoHost.PreparedBallot);

        cluster.RunFor(replayDelay, CancellationToken.None);

        Assert.Equal(replaySafeBallot, proposerTwoHost.Ballot);
        Assert.Equal(new TestValue(2, "AB"), proposerTwoHost.CachedValue);
        Assert.Equal(replaySafePreparedBallot, proposerTwoHost.PreparedBallot);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QueuedReadCompletesThroughItsOwnAcceptQuorum(bool forwarded)
    {
        await using var cluster = CreateCluster(seed: 918);
        var acceptorLane = cluster.AddNode("acceptors").Context.SchedulerLane;
        var clientLane = cluster.AddNode("clients").Context.SchedulerLane;
        var transport = new FastCasSimulationTransport<TestValue>(cluster.Network);
        FastCasAddress[] acceptors = [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(2), FastCasAddress.Acceptor(3)];
        List<IFastCasTransportMessage<TestValue>> requests = [];
        List<IFastCasTransportMessage<TestValue>> responses = [];
        foreach (var acceptor in acceptors)
        {
            transport.RegisterEndpoint(new(acceptor, acceptorLane, requests.Add));
        }

        var caller = FastCasAddress.Client();
        var forwardingProposer = FastCasAddress.Proposer(2);
        transport.RegisterEndpoint(new(caller, clientLane, responses.Add));
        transport.RegisterEndpoint(new(forwardingProposer, clientLane, responses.Add));
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
        RunUntilIdle(cluster);

        var value = new TestValue(1, "A");
        var writeResponse = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(responses));
        Assert.Equal(1, writeResponse.RequestId);
        Assert.Equal(value, writeResponse.Payload.CommittedValue);
        Assert.True(host.HasPendingClientRequests);
        Assert.True(host.Ballot > writeBallot);
        Assert.Equal(6, requests.OfType<FastCasPrepareRequestMessage<TestValue>>().Count());

        Promise(acceptors[0], writeBallot, value);
        Promise(acceptors[1], writeBallot, value);
        RunUntilIdle(cluster);
        var readAccepts = requests.OfType<FastCasAcceptRequestMessage<TestValue>>()
            .Where(message => message.Payload.Ballot == host.Ballot).ToArray();
        Assert.Equal(3, readAccepts.Length);
        Assert.All(readAccepts, message => Assert.Equal(value, message.Payload.Value));
        Assert.Single(responses);

        Accept(acceptors[0]);
        RunUntilIdle(cluster);
        Assert.Single(responses);
        Accept(acceptors[1]);
        RunUntilIdle(cluster);

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

    private static SimulationCluster CreateCluster(int seed) => new(seed, DateTimeOffset.UnixEpoch);

    private static FastCasSimulationNode<TestValue> Attach(
        SimulationCluster cluster,
        IFastCasNodeHost<TestValue> host,
        FastCasSimulationTransport<TestValue> transport) =>
        cluster.AddCustomNode(
            host.Address.NetworkAddress,
            context => new FastCasSimulationNode<TestValue>(host, context, transport));

    /// <summary>Runs the work which is due now, without advancing virtual time.</summary>
    private static void RunUntilIdle(SimulationCluster cluster) =>
        cluster.SchedulerLane.RunUntilIdle(CancellationToken.None);

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
