using Orleans.Consensus.CasPaxos.Model;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Tests;

public sealed class BallotOverflowTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public void NextRoundAtMaximumThrowsRatherThanWrapping(int proposer)
    {
        Assert.Equal(new Ballot(int.MaxValue, proposer), new Ballot(int.MaxValue - 1, 3).NextRound(proposer));
        Assert.Throws<OverflowException>(() => new Ballot(int.MaxValue, 3).NextRound(proposer));
    }
}
