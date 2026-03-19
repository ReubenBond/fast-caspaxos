using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Invariants;
using FastCASPaxos.Simulation.Scenarios;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSafetyInvariantCheckerTests
{
    [Fact]
    public void AssertSafety_RejectsDivergentCommittedValueAtSameVersion()
    {
        var histories = new Dictionary<FastCasAddress, IReadOnlyList<StringValue>>
        {
            [FastCasAddress.Proposer(1)] = [new StringValue(1, "A")],
            [FastCasAddress.Proposer(2)] = [new StringValue(1, "B")],
        };

        var error = Assert.Throws<InvalidOperationException>(() =>
            FastCasSafetyInvariantChecker.AssertSafety<StringValue>(
                histories, Array.Empty<RoutedProposeResponse<StringValue, FastCasAddress>>()));

        Assert.Contains("does not agree across proposers", error.Message);
    }

    [Fact]
    public void AssertSafety_RejectsNonLinearHistory()
    {
        var histories = new Dictionary<FastCasAddress, IReadOnlyList<StringValue>>
        {
            [FastCasAddress.Proposer(1)] = [new StringValue(1, "A"), new StringValue(1, "B")],
        };

        var error = Assert.Throws<InvalidOperationException>(() =>
            FastCasSafetyInvariantChecker.AssertSafety<StringValue>(
                histories, Array.Empty<RoutedProposeResponse<StringValue, FastCasAddress>>()));

        Assert.Contains("Non-linearizable history", error.Message);
    }

    [Fact]
    public void AssertSafety_RejectsResponseHistoryThatRegressesHighestVisibleVersion()
    {
        var histories = new Dictionary<FastCasAddress, IReadOnlyList<StringValue>>
        {
            [FastCasAddress.Proposer(1)] = [new StringValue(1, "A"), new StringValue(2, "AB"), new StringValue(3, "ABC")],
        };

        var proposer = FastCasAddress.Proposer(1);
        var responses = new[]
        {
            CreateResponse(proposer, requestId: 1, new StringValue(2, "AB")),
            CreateResponse(proposer, requestId: 2, new StringValue(1, "A")),
        };

        var error = Assert.Throws<InvalidOperationException>(() =>
            FastCasSafetyInvariantChecker.AssertSafety<StringValue>(histories, responses));

        Assert.Contains("regressed", error.Message);
    }

    [Fact]
    public void AssertSafety_AllowsResponseHistoryToReplaceFrontierValue()
    {
        var histories = new Dictionary<FastCasAddress, IReadOnlyList<StringValue>>
        {
            [FastCasAddress.Proposer(1)] = [new StringValue(1, "B"), new StringValue(2, "BC")],
            [FastCasAddress.Proposer(2)] = [new StringValue(1, "B"), new StringValue(2, "BC")],
        };

        var proposer = FastCasAddress.Proposer(1);
        var responses = new[]
        {
            CreateResponse(proposer, requestId: 1, new StringValue(1, "A")),
            CreateResponse(proposer, requestId: 2, new StringValue(1, "B")),
            CreateResponse(proposer, requestId: 3, new StringValue(2, "BC")),
        };

        FastCasSafetyInvariantChecker.AssertSafety<StringValue>(histories, responses);
    }

    [Fact]
    public void AssertSafety_RejectsInvalidSuccessorChain()
    {
        var histories = new Dictionary<FastCasAddress, IReadOnlyList<StringValue>>
        {
            [FastCasAddress.Proposer(1)] = [new StringValue(1, "A"), new StringValue(2, "Z")],
            [FastCasAddress.Proposer(2)] = [new StringValue(1, "A"), new StringValue(2, "Z")],
        };

        var error = Assert.Throws<InvalidOperationException>(() =>
            FastCasSafetyInvariantChecker.AssertSafety<StringValue>(
                histories, Array.Empty<RoutedProposeResponse<StringValue, FastCasAddress>>()));

        Assert.Contains("not a valid successor", error.Message);
    }

    private static RoutedProposeResponse<StringValue, FastCasAddress> CreateResponse(
        FastCasAddress proposer,
        int requestId,
        StringValue committedValue) =>
        new(proposer, new ProposeResponse<StringValue>(requestId, committedValue));
}

