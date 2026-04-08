using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using Xunit;

namespace FastCASPaxos.Core.Tests.Protocol;

public sealed class ConsensusRegisterTests
{
    [Fact]
    public void LocalRegister_ProcessesRequestWithoutExternalProtocolMessages()
    {
        var register = CreateRegister();

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        var messages = register.DrainMessages();

        Assert.Equal(2, messages.Count);
        var learn = Assert.IsType<LearnCommittedValueOutputMessage<TestValue, string>>(messages[0]);
        Assert.Equal(new TestValue(1, "A"), learn.Value);

        var response = Assert.IsType<ProposeResponseOutputMessage<TestValue, string>>(messages[1]);
        Assert.Equal("client", response.To);
        Assert.Equal(1, response.RequestId);
        Assert.Equal(new TestValue(1, "A"), response.Payload.CommittedValue);
        Assert.Equal(new TestValue(1, "A"), register.ProposerCachedValue);
        Assert.Equal(new TestValue(1, "A"), register.AcceptorState.AcceptedValue);
        Assert.False(register.HasPendingRequests);
    }

    [Fact]
    public void LocalRegister_QueuesSecondRequestUntilFirstCompletes()
    {
        var register = CreateRegister(modePolicy: ProposerModePolicy.StaticClassic);

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        register.SubmitClientRequest("client", 2, AppendOperation(2, "B"));

        var messages = register.DrainMessages();

        Assert.Equal(4, messages.Count);
        var responses = messages.OfType<ProposeResponseOutputMessage<TestValue, string>>().ToList();
        Assert.Equal(2, responses.Count);
        Assert.Equal([1, 2], responses.Select(message => message.RequestId));
        Assert.Equal(new TestValue(2, "AB"), responses[1].Payload.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), register.ProposerCachedValue);
    }

    [Fact]
    public void LocalRegister_DefersLearnedCommitUntilQueueDrains()
    {
        var register = CreateRegister(
            modePolicy: ProposerModePolicy.StaticClassic,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            acceptorRoute: "a1");

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        register.SubmitClientRequest("client", 2, AppendOperation(2, "B"));
        register.LearnCommittedValue(new Ballot(3, 4), new TestValue(3, "XYZ"), new Ballot(4, 4));

        var firstPrepare = register.DrainMessages();
        Assert.Equal(4, firstPrepare.OfType<PrepareRequestOutputMessage<TestValue, string>>().Count());

        DeliverClassicPrepareQuorum(register);
        var firstAccept = register.DrainMessages();
        Assert.Equal(4, firstAccept.OfType<AcceptRequestOutputMessage<TestValue, string>>().Count());

        DeliverClassicAcceptQuorum(register);
        Assert.Equal(new TestValue(1, "A"), register.ProposerCachedValue);
        Assert.Equal(new TestValue(1, "A"), register.AcceptorState.AcceptedValue);

        var afterFirst = register.DrainMessages();
        Assert.Contains(afterFirst, message => message is ProposeResponseOutputMessage<TestValue, string> { RequestId: 1 });
        Assert.Equal(4, afterFirst.OfType<PrepareRequestOutputMessage<TestValue, string>>().Count());

        DeliverClassicPrepareQuorum(register);
        var secondAccept = register.DrainMessages();
        Assert.Equal(4, secondAccept.OfType<AcceptRequestOutputMessage<TestValue, string>>().Count());

        DeliverClassicAcceptQuorum(register);
        Assert.Equal(new Ballot(3, 4), register.ProposerBallot);
        Assert.Equal(new TestValue(3, "XYZ"), register.ProposerCachedValue);
        Assert.Equal(new TestValue(2, "AB"), register.AcceptorState.AcceptedValue);
        Assert.Equal(new Ballot(4, 4), register.ProposerPreparedBallot);
    }

    [Fact]
    public void LocalRegister_ForwardsCompletionMetadataForForwardedRequests()
    {
        var register = CreateRegister();

        register.DeliverForwardedRequest(new ConsensusRegisterForwardedRequest<TestValue, string>(
            From: "proposer-2",
            OriginalCaller: "client-7",
            RequestId: 99,
            Operation: AppendOperation(1, "A")));

        var messages = register.DrainMessages();
        Assert.Equal(2, messages.Count);
        Assert.IsType<LearnCommittedValueOutputMessage<TestValue, string>>(messages[0]);
        var response = Assert.IsType<ForwardProposeResponseOutputMessage<TestValue, string>>(messages[1]);
        Assert.Equal("proposer-2", response.To);
        Assert.Equal("client-7", response.OriginalCaller);
        Assert.Equal(99, response.RequestId);
        Assert.Equal(new TestValue(1, "A"), response.Payload.CommittedValue);
        Assert.Equal(register.ProposerBallot, response.AcceptedBallot);
    }

    [Fact]
    public void RemoteRegister_EmitsPrepareAcceptAndLearnMessages()
    {
        var register = CreateRegister(acceptors: ["a1", "a2", "a3", "a4", "a5"], acceptorRoute: "a1");

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        var initialMessages = register.DrainMessages();

        Assert.Equal(4, initialMessages.OfType<AcceptRequestOutputMessage<TestValue, string>>().Count());

        DeliverAcceptAccepted(register, "a2");
        Assert.Empty(register.DrainMessages());

        DeliverAcceptAccepted(register, "a3");
        Assert.Empty(register.DrainMessages());

        DeliverAcceptAccepted(register, "a4");
        var completionMessages = register.DrainMessages();

        var learn = Assert.IsType<LearnCommittedValueOutputMessage<TestValue, string>>(completionMessages[0]);
        Assert.Equal(new TestValue(1, "A"), learn.Value);

        var response = Assert.IsType<ProposeResponseOutputMessage<TestValue, string>>(completionMessages[1]);
        Assert.Equal("client", response.To);
        Assert.Equal(new TestValue(1, "A"), response.Payload.CommittedValue);
    }

    [Fact]
    public void Timeout_PropagatesRetryMessages()
    {
        var register = CreateRegister(modePolicy: ProposerModePolicy.StaticClassic, acceptors: ["a1", "a2", "a3", "a4", "a5"], acceptorRoute: "a1");

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        var initial = register.DrainMessages();
        var prepare = Assert.IsType<PrepareRequestOutputMessage<TestValue, string>>(initial[0]);

        Assert.True(register.HandleResponseTimeout(prepare.Payload.Ballot.Round));
        var retry = register.DrainMessages();

        Assert.Contains(retry, message => message is PrepareRequestOutputMessage<TestValue, string>);
    }

    [Fact]
    public void PrepareRecovery_KeepsRecoveredProposerValueSeparateFromLocalAcceptorState()
    {
        var register = CreateRegister(
            modePolicy: ProposerModePolicy.StaticClassic,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            acceptorRoute: "a1");

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        _ = register.DrainMessages();

        var recoveredValue = new TestValue(1, "REMOTE");
        register.DeliverPreparePromise(new PreparePromise<TestValue, string>(
            register.ProposerBallot.Round,
            "a2",
            Ballot.InitialFast(),
            recoveredValue));

        Assert.Equal(ProposerValueStatus.Cached, register.ProposerValueStatus);
        Assert.Equal(recoveredValue, register.ProposerCurrentValue);
        Assert.Equal(recoveredValue, register.ProposerCachedValue);
        Assert.Equal(default, register.AcceptorState.AcceptedValue);
    }

    [Fact]
    public void PendingAccept_UsesSingleCurrentValueView()
    {
        var register = CreateRegister(
            modePolicy: ProposerModePolicy.StaticClassic,
            acceptors: ["a1", "a2", "a3", "a4", "a5"],
            acceptorRoute: "a1");

        register.SubmitClientRequest("client", 1, AppendOperation(1, "A"));
        _ = register.DrainMessages();

        DeliverClassicPrepareQuorum(register);
        var acceptMessages = register.DrainMessages();
        Assert.Equal(4, acceptMessages.OfType<AcceptRequestOutputMessage<TestValue, string>>().Count());

        Assert.Equal(ProposerValueStatus.PendingAccept, register.ProposerValueStatus);
        Assert.Equal(new TestValue(1, "A"), register.ProposerCurrentValue);
        Assert.Equal(new TestValue(1, "A"), register.ProposerPendingAcceptValue);
        Assert.Equal(new TestValue(1, "A"), register.AcceptorState.AcceptedValue);
        _ = Assert.Throws<InvalidOperationException>(() => register.ProposerCachedValue);
    }

    private static ConsensusRegister<TestValue, string> CreateRegister(
        ProposerModePolicy? modePolicy = null,
        string proposerRoute = "p1",
        string acceptorRoute = "p1",
        IReadOnlyList<string>? acceptors = null)
    {
        return new ConsensusRegister<TestValue, string>(
            proposerRoute,
            acceptorRoute,
            proposerId: 1,
            initialValue: default,
            acceptors ?? [acceptorRoute],
            modePolicy ?? ProposerModePolicy.StaticFast);
    }

    private static void DeliverAcceptAccepted(
        ConsensusRegister<TestValue, string> register,
        string acceptor)
    {
        register.DeliverAcceptAccepted(new AcceptAccepted<string>(
            register.ProposerBallot.Round,
            acceptor,
            register.ProposerBallot));
    }

    private static void DeliverClassicPrepareQuorum(ConsensusRegister<TestValue, string> register)
    {
        register.DeliverPreparePromise(new PreparePromise<TestValue, string>(
            register.ProposerBallot.Round,
            "a2",
            Ballot.Zero,
            default));
        register.DeliverPreparePromise(new PreparePromise<TestValue, string>(
            register.ProposerBallot.Round,
            "a3",
            Ballot.Zero,
            default));
    }

    private static void DeliverClassicAcceptQuorum(ConsensusRegister<TestValue, string> register)
    {
        DeliverAcceptAccepted(register, "a2");
        DeliverAcceptAccepted(register, "a3");
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

    private readonly record struct TestValue(int Version, string? Value)
    {
        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
