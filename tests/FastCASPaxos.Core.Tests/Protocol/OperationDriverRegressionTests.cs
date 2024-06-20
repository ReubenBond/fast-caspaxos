using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class OperationDriverRegressionTests
{
    [Fact]
    public void OperationDriver_StartReadyRequests_WrapsScheduledOperationWithDriverCaller()
    {
        var driver = new RecordingOperationDriver<TestValue, string>("client", ["p1"]);
        var operation = AppendOperation(1, "A");
        driver.Enqueue("p1", operation);

        driver.StartReadyRequests();
        var scheduledOperation = Assert.Single(driver.SentProposals).Operation;
        var routedOperation = Assert.IsType<RoutedOperation<TestValue, string>>(scheduledOperation);

        Assert.Equal("client", routedOperation.Caller);
        Assert.Same(operation, routedOperation.Operation);
    }

    [Fact]
    public void OperationDriver_Completion_PrefersLatestResponsePerProposer()
    {
        var driver = new RecordingOperationDriver<TestValue, string>("client", ["p1"]);
        driver.Enqueue("p1", AppendOperation(1, "A"));
        driver.Enqueue("p1", ReadOperation());

        driver.ResetRecording();
        driver.StartReadyRequests();
        var firstRequest = Assert.Single(driver.SentProposals).Operation;

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>(
            "p1",
            new ProposeResponse<TestValue>(0, new TestValue(2, "AB"))));
        Assert.Null(driver.LastCompletion);
        var secondRequest = Assert.Single(driver.SentProposals).Operation;

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>(
            "p1",
            new ProposeResponse<TestValue>(1, new TestValue(1, "A"))));

        Assert.NotNull(driver.LastCompletion);
        // Latest response (highest round) wins, regardless of value content.
        Assert.Equal(new TestValue(1, "A"), driver.LastCompletion!.FinalValues["p1"]);
        Assert.Equal(new TestValue(1, "A"), driver.LastCompletion!.LatestValue);
    }

    [Fact]
    public void OperationDriver_Completion_PrefersLatestSameVersionResponsePerProposer()
    {
        var driver = new RecordingOperationDriver<TestValue, string>("client", ["p1"]);
        driver.Enqueue("p1", ReadOperation());
        driver.Enqueue("p1", ReadOperation());

        driver.ResetRecording();
        driver.StartReadyRequests();
        var firstRequest = Assert.Single(driver.SentProposals).Operation;

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>(
            "p1",
            new ProposeResponse<TestValue>(0, new TestValue(1, "A"))));
        Assert.Null(driver.LastCompletion);
        var secondRequest = Assert.Single(driver.SentProposals).Operation;

        driver.ResetRecording();
        driver.HandleResponse(new RoutedProposeResponse<TestValue, string>(
            "p1",
            new ProposeResponse<TestValue>(1, new TestValue(1, "B"))));

        Assert.NotNull(driver.LastCompletion);
        Assert.Equal(new TestValue(1, "B"), driver.LastCompletion!.FinalValues["p1"]);
    }

    [Fact]
    public void OperationDriver_HandleResponse_AcceptsAnyRoundForScheduledRequest()
    {
        var driver = new RecordingOperationDriver<TestValue, string>("client", ["p1"]);
        driver.Enqueue("p1", AppendOperation(1, "A"));
        driver.StartReadyRequests();

        // Round 999 doesn't match what was scheduled, but OperationDriver no longer
        // validates round values — it only tracks whether a request is outstanding.
        var response = new RoutedProposeResponse<TestValue, string>(
            "p1",
            new ProposeResponse<TestValue>(999, new TestValue(1, "A")));
        driver.HandleResponse(response);

        Assert.NotNull(driver.LastCompletion);
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

    private static IOperation<TestValue> ReadOperation() =>
        new Operation<TestValue, TestValue>
        {
            Input = default,
            Name = "Read current value",
            Apply = static (current, _) => (OperationStatus.NotApplicable, current),
        };

    private readonly record struct TestValue(int Version, string Value)
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
