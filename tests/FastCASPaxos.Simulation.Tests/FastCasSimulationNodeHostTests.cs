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
    public void ProposerAndAcceptors_ReachConsensusThroughSimulationTransport()
    {
        var clock = new SimulationClock(DateTimeOffset.UnixEpoch);
        var guard = new SingleThreadedGuard();
        var clusterQueue = new SimulationTaskQueue(clock, guard);
        List<SimulationNode> nodes = [];
        var network = new SimulationNetwork(() => nodes, new SimulationRandom(321));
        var transport = new FastCasSimulationTransport<TestValue>(network);
        var options = new FastCasSimulationOptions
        {
            EnableFastCommit = false,
            EnableDistinguishedLeader = false,
        };

        var acceptorNodes = new List<FastCasSimulationNode<TestValue>>();
        var acceptorHosts = new List<FastCasAcceptorHost<TestValue>>();
        var acceptorAddresses = new List<FastCasAddress>();
        for (var i = 1; i <= 5; i++)
        {
            var nodeId = new FastCasNodeId(FastCasParticipantRole.Acceptor, i);
            var host = new FastCasAcceptorHost<TestValue>(nodeId, transport, options: options);
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
            acceptorAddresses,
            transport,
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
            AppendOperation(1, "A"));

        Assert.Equal(DeliveryStatus.Success, transport.Send(proposal));
        RunUntilIdle(clusterQueue, clientQueue, [.. nodes.Select(node => node.Context.TaskQueue)]);

        var completion = Assert.IsType<FastCasProposeResponseMessage<TestValue>>(Assert.Single(clientMessages));
        Assert.Equal(new TestValue(1, "A"), completion.Payload.CommittedValue);
        Assert.Equal(new TestValue(1, "A"), proposerHost.CachedValue);
        Assert.All(acceptorHosts, host => Assert.Equal(new TestValue(1, "A"), host.State.AcceptedValue));
    }

    private static IOperation<TestValue> AppendOperation(int expectedVersion, string suffix) =>
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
        IReadOnlyList<SimulationTaskQueue> nodeQueues)
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

