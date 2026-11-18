using Orleans.Consensus.CasPaxos.Messages;

namespace Orleans.Consensus.CasPaxos.Protocol;

public readonly record struct RoutedProposeResponse<TValue, TRoute>(TRoute Proposer, ProposeResponse<TValue> Response)
{
    public int Round => Response.Round;

    public TValue CommittedValue => Response.CommittedValue;
}
