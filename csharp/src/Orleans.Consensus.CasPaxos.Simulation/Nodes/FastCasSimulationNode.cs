using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Transport;
using Orleans.Testing.Simulation;

namespace Orleans.Consensus.CasPaxos.Simulation.Nodes;

/// <summary>
/// A Fast CASPaxos host attached to a <see cref="SimulationCluster"/> through
/// <see cref="SimulationCluster.AddCustomNode{TNode}(string, Func{SimulationNodeContext, TNode})"/>.
/// The host's messages are delivered on the node's scheduler lane, and disposal removes its transport endpoint.
/// </summary>
public sealed class FastCasSimulationNode<TValue> : SimulationNode, IDisposable
{
    private readonly FastCasSimulationTransport<TValue> _transport;

    public FastCasSimulationNode(
        IFastCasNodeHost<TValue> host,
        SimulationNodeContext context,
        FastCasSimulationTransport<TValue> transport)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(transport);

        Host = host;
        Context = context;
        _transport = transport;

        Registration = new FastCasTransportEndpointRegistration<TValue>(
            Host.Address,
            Context.SchedulerLane,
            Host.Deliver);

        if (Host is IFastCasProposerNodeHost<TValue> proposerHost)
        {
            proposerHost.BindSimulationContext(Context);
        }

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

    public void Dispose() => _transport.UnregisterEndpoint(Host.Address);
}
