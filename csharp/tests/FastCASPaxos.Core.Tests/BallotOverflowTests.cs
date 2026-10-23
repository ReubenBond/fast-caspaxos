using FastCASPaxos.Model;
using Xunit;

namespace FastCASPaxos.Core.Tests;

public sealed class BallotOverflowTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(7)]
    public void NextRound_AtMaximumThrowsRatherThanWrapping(int proposer)
    {
        Assert.Equal(new Ballot(int.MaxValue, proposer), new Ballot(int.MaxValue - 1, 3).NextRound(proposer));
        Assert.Throws<OverflowException>(() => new Ballot(int.MaxValue, 3).NextRound(proposer));
    }
}
