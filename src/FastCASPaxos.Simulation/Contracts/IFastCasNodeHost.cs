
namespace FastCASPaxos.Simulation.Contracts;

public interface IFastCasNodeHost<TValue>
{
    FastCasNodeId NodeId { get; }

    FastCasAddress Address { get; }

    FastCasCrashRestartStateModel CrashRestartStateModel { get; }

    bool IsRunning { get; }

    void Deliver(IFastCasTransportMessage<TValue> message);

    void Crash();

    void Restart();
}
