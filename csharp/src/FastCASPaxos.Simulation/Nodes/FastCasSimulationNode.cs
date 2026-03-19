using Clockwork;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Transport;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Simulation.Nodes;

public sealed class FastCasSimulationNode<TValue> : SimulationNode
{
    private readonly FastCasSimulationTransport<TValue> _transport;

    public FastCasSimulationNode(
        IFastCasNodeHost<TValue> host,
        SimulationClock clock,
        SingleThreadedGuard guard,
        SimulationRandom random,
        SimulationTaskQueue externalTaskQueue,
        FastCasSimulationTransport<TValue> transport,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(guard);
        ArgumentNullException.ThrowIfNull(random);
        ArgumentNullException.ThrowIfNull(externalTaskQueue);

        Host = host;
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        Context = new SimulationNodeContext(clock, guard, random, externalTaskQueue, logger);

        Registration = new FastCasTransportEndpointRegistration<TValue>(
            Host.Address,
            Context.TaskQueue,
            Host.Deliver);

        _transport.RegisterEndpoint(Registration);
    }

    public IFastCasNodeHost<TValue> Host { get; }

    public FastCasTransportEndpointRegistration<TValue> Registration { get; }

    public override SimulationNodeContext Context { get; }

    public override string NetworkAddress => Host.Address.NetworkAddress;

    public override bool IsInitialized => true;

    public bool IsRunning => Host.IsRunning;

    public void Crash() => Host.Crash();

    public void Restart() => Host.Restart();

    public void Unregister() => _transport.UnregisterEndpoint(Host.Address);
}
