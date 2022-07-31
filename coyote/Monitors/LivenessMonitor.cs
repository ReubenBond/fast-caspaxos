using FastCASPaxos.Events;
using FastCASPaxos.Model;
using Microsoft.Coyote;
using Microsoft.Coyote.Specifications;

namespace FastCASPaxos.Monitors;

public class LivenessMonitor<TValue, TVersionedValue> : Monitor where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
{
    private int _totalAcceptors;
    private int _availableAcceptors;
    private int _activeProposers;

    [Start]
    [Cold]
    [OnEventDoAction(typeof(TestConfigEventHolder), nameof(OnInit))]
    [OnEventDoAction(typeof(OperationStarted), nameof(OnOperationStarted))]
    [OnEventDoAction(typeof(OperationCompleted), nameof(OnOperationCompleted))]
    [OnEventDoAction(typeof(AcceptorUp), nameof(OnAcceptorUp))]
    [OnEventDoAction(typeof(AcceptorDown), nameof(OnAcceptorDown))]
    public class Idle : State { }

    [Hot]
    [OnEventDoAction(typeof(OperationStarted), nameof(OnOperationStarted))]
    [OnEventDoAction(typeof(OperationCompleted), nameof(OnOperationCompleted))]
    public class Busy : State { }

    public void OnInit(Event e)
    {
        var config = (TestConfigEvent<TValue, TVersionedValue>)((TestConfigEventHolder)e).Value;

        _totalAcceptors = config.NumAcceptors;
    }

    public void OnOperationStarted(Event e)
    {
        ++_activeProposers;
        UpdateState();
    }

    public void OnOperationCompleted(Event e)
    {
        --_activeProposers;
        UpdateState();
    }

    public void OnAcceptorUp(Event e)
    {
        ++_availableAcceptors;
        UpdateState();
    }

    public void OnAcceptorDown(Event e)
    {
        --_availableAcceptors;
        UpdateState();
    }

    private void UpdateState()
    {
        if (_activeProposers > 0 && CanFormQuorum())
        {
            RaiseGotoStateEvent<Busy>();
        }
        else
        {
            RaiseGotoStateEvent<Idle>();
        }
    }

    public bool CanFormQuorum() => 2 * _availableAcceptors > _totalAcceptors;
}
