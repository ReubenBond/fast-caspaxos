using Clockwork;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationClusterTests
{
    [Fact]
    public async Task RunProposalCompletesAgainstConfiguredQuorum()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 123,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var response = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));

        Assert.Equal(new TestValue(1, "A"), response.CommittedValue);
        Assert.Equal("RunProposalCompletesAgainstConfiguredQuorum (seed: 123)", cluster.GetReproductionHint(nameof(RunProposalCompletesAgainstConfiguredQuorum)));
    }

    [Fact]
    public async Task SendProposalStoresClientResponseByRequestId()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 456,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var requestId = cluster.SendProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"), requestId: 42);
        Assert.True(cluster.RunUntil(() => cluster.TryGetResponse(requestId, out _), maxIterations: 1000));
        Assert.True(cluster.TryGetResponse(requestId, out var response));
        Assert.Equal(Ballot.InitialClassic(1).Round, response.Round);
    }

    [Fact]
    public async Task RunProposalBroadcastsCommittedValueToPeerProposers()
    {
        var history = new RecordingHistoryObserver();
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 457,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticFast,
                HistoryObserver = history,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var sourceProposer = FastCasAddress.Proposer(1);
        var peerProposer = FastCasAddress.Proposer(2);
        var response = cluster.RunProposal(sourceProposer, AppendOperation(1, "A"));
        Assert.Equal(new TestValue(1, "A"), response.CommittedValue);

        var peerHost = Assert.IsType<FastCasProposerHost<TestValue>>(cluster.ProposerNodes[1].Host);
        Assert.True(
            cluster.RunUntil(
                () => EqualityComparer<TestValue>.Default.Equals(peerHost.CachedValue, new TestValue(1, "A")),
                maxIterations: 5000),
            "Expected peer proposer to learn the committed value from the winner.");

        var learnEvent = Assert.Single(history.LearnEvents);
        Assert.Equal(cluster.GetProposerBallot(sourceProposer), peerHost.Ballot);
        Assert.Equal(cluster.GetProposerPreparedBallot(sourceProposer), peerHost.PreparedBallot);
        Assert.Equal(cluster.GetProposerBallot(sourceProposer), learnEvent.AcceptedBallot);
        Assert.Equal(new TestValue(1, "A"), learnEvent.Value);
        Assert.Equal(cluster.GetProposerPreparedBallot(sourceProposer) ?? Ballot.Zero, learnEvent.PreparedBallot);
        Assert.Equal(peerProposer, peerHost.Address);
        Assert.Equal(peerProposer, learnEvent.TargetProposer);
    }

    [Fact]
    public async Task RunProposalBroadcastsClassicPreparedBallotToPeerProposers()
    {
        var history = new RecordingHistoryObserver();
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 458,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticLeader,
                HistoryObserver = history,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var sourceProposer = FastCasAddress.Proposer(1);
        var response = cluster.RunProposal(sourceProposer, AppendOperation(1, "A"));
        Assert.Equal(new TestValue(1, "A"), response.CommittedValue);

        var peerHost = Assert.IsType<FastCasProposerHost<TestValue>>(cluster.ProposerNodes[1].Host);
        Assert.True(
            cluster.RunUntil(
                () => EqualityComparer<TestValue>.Default.Equals(peerHost.CachedValue, new TestValue(1, "A")),
                maxIterations: 5000),
            "Expected peer proposer to learn the committed value from the winner.");

        var learnEvent = Assert.Single(history.LearnEvents);
        Assert.Equal(cluster.GetProposerBallot(sourceProposer), peerHost.Ballot);
        Assert.Equal(cluster.GetProposerPreparedBallot(sourceProposer), peerHost.PreparedBallot);
        Assert.Equal(cluster.GetProposerPreparedBallot(sourceProposer) ?? Ballot.Zero, learnEvent.PreparedBallot);
    }

    [Fact]
    public async Task AdaptiveModeForwardsRequestsToLearnedLeader()
    {
        var transportObserver = new RecordingTransportObserver();
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 459,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.Adaptive(new AdaptiveProposerPolicy
                {
                    StartInFastMode = false,
                    EnableForwarding = true,
                    LeaderMissThreshold = 2,
                }),
                TransportObserver = transportObserver,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var leader = FastCasAddress.Proposer(1);
        var follower = FastCasAddress.Proposer(2);
        var leaderResponse = cluster.RunProposal(leader, AppendOperation(1, "A"));
        Assert.Equal(new TestValue(1, "A"), leaderResponse.CommittedValue);

        var followerHost = Assert.IsType<FastCasProposerHost<TestValue>>(cluster.ProposerNodes[1].Host);
        Assert.True(
            cluster.RunUntil(
                () => EqualityComparer<TestValue>.Default.Equals(followerHost.CachedValue, new TestValue(1, "A")),
                maxIterations: 5000),
            "Expected follower to learn the leader's committed value before forwarding.");

        var requestId = cluster.SendProposal(follower, AppendOperation(2, "B"), requestId: 91);
        Assert.True(cluster.RunUntil(() => cluster.TryGetRoutedResponse(requestId, out _), maxIterations: 5000));
        Assert.True(cluster.TryGetRoutedResponse(requestId, out var routedResponse));

        Assert.Equal(follower, routedResponse.Proposer);
        Assert.Equal(new TestValue(2, "AB"), routedResponse.Response.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), followerHost.CachedValue);
        Assert.Equal(leader.Ordinal, followerHost.Ballot.Proposer);
        Assert.True(transportObserver.Count(FastCasTransportMessageKind.ForwardProposeRequest, DeliveryStatus.Success) >= 1);
        Assert.True(transportObserver.Count(FastCasTransportMessageKind.ForwardProposeResponse, DeliveryStatus.Success) >= 1);
    }

    [Fact]
    public async Task AdaptiveModeForwardFailureFallsBackToLocalTakeover()
    {
        var transportObserver = new RecordingTransportObserver();
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 460,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.Adaptive(new AdaptiveProposerPolicy
                {
                    StartInFastMode = false,
                    EnableForwarding = true,
                    LeaderMissThreshold = 1,
                }),
                TransportObserver = transportObserver,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var leader = FastCasAddress.Proposer(1);
        var follower = FastCasAddress.Proposer(2);
        _ = cluster.RunProposal(leader, AppendOperation(1, "A"));

        var followerHost = Assert.IsType<FastCasProposerHost<TestValue>>(cluster.ProposerNodes[1].Host);
        Assert.True(
            cluster.RunUntil(
                () => EqualityComparer<TestValue>.Default.Equals(followerHost.CachedValue, new TestValue(1, "A")),
                maxIterations: 5000),
            "Expected follower to learn the leader's committed value before takeover.");

        cluster.CreateBidirectionalPartition(follower, leader);

        var requestId = cluster.SendProposal(follower, AppendOperation(2, "B"), requestId: 92);
        Assert.True(cluster.RunUntil(() => cluster.TryGetRoutedResponse(requestId, out _), maxIterations: 5000));
        Assert.True(cluster.TryGetRoutedResponse(requestId, out var routedResponse));

        Assert.Equal(follower, routedResponse.Proposer);
        Assert.Equal(new TestValue(2, "AB"), routedResponse.Response.CommittedValue);
        Assert.Equal(new TestValue(2, "AB"), followerHost.CachedValue);
        Assert.Equal(follower.Ordinal, followerHost.Ballot.Proposer);
        Assert.False(followerHost.Ballot.IsFastRoundBallot);
        Assert.True(transportObserver.Count(FastCasTransportMessageKind.ForwardProposeRequest, DeliveryStatus.Partitioned) >= 1);
    }

    [Fact]
    public async Task FaultOperationsBlockThenRestoreProgress()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 789,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var proposer = FastCasAddress.Proposer(1);
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        var requestId = cluster.SendProposal(proposer, AppendOperation(1, "A"), requestId: 100);
        Assert.False(cluster.RunUntil(() => cluster.TryGetResponse(requestId, out _), maxIterations: 100));

        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.HealBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        var healedResponse = cluster.RunProposal(proposer, AppendOperation(1, "A"));
        Assert.Equal(new TestValue(1, "A"), healedResponse.CommittedValue);
        cluster.AssertSafetyInvariants();
    }

    [Fact]
    public async Task CrashRestartPreservesAcceptorState()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 246,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);
        _ = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));

        cluster.CrashNode(FastCasAddress.Acceptor(1));
        cluster.RestartNode(FastCasAddress.Acceptor(1));

        var acceptor = Assert.IsType<FastCasAcceptorHost<TestValue>>(cluster.AcceptorNodes[0].Host);
        Assert.Equal(new TestValue(1, "A"), acceptor.State.AcceptedValue);
    }

    [Fact]
    public async Task CreateConfigurationConfiguresZeroLatencyForCollocatedProposerAndAcceptor()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 741,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 2, acceptorCount: 5);

        var proposer1ToAcceptor1 = cluster.Network.GetLinkLatency("proposer-1", "acceptor-1");
        var acceptor1ToProposer1 = cluster.Network.GetLinkLatency("acceptor-1", "proposer-1");
        Assert.True(proposer1ToAcceptor1.HasValue);
        Assert.True(acceptor1ToProposer1.HasValue);
        Assert.Equal(TimeSpan.FromMicroseconds(1), proposer1ToAcceptor1.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, proposer1ToAcceptor1.Value.MaxJitter);
        Assert.Equal(TimeSpan.FromMicroseconds(1), acceptor1ToProposer1.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, acceptor1ToProposer1.Value.MaxJitter);
        Assert.Null(cluster.Network.GetLinkLatency("proposer-1", "acceptor-2"));
    }

    [Fact]
    public async Task ClearLinkLatenciesRetainsZeroLatencyForCollocatedPairs()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 742,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);
        cluster.ConfigureBidirectionalLinkLatency(
            FastCasAddress.Proposer(1),
            FastCasAddress.Acceptor(1),
            TimeSpan.FromMilliseconds(25),
            TimeSpan.FromMilliseconds(5));
        cluster.ConfigureBidirectionalLinkLatency(
            FastCasAddress.Proposer(1),
            FastCasAddress.Acceptor(2),
            TimeSpan.FromMilliseconds(25),
            TimeSpan.FromMilliseconds(5));

        var collocatedLatency = cluster.Network.GetLinkLatency("proposer-1", "acceptor-1");
        var remoteLatency = cluster.Network.GetLinkLatency("proposer-1", "acceptor-2");
        Assert.True(collocatedLatency.HasValue);
        Assert.True(remoteLatency.HasValue);
        Assert.Equal(TimeSpan.FromMicroseconds(1), collocatedLatency.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, collocatedLatency.Value.MaxJitter);
        Assert.Equal(TimeSpan.FromMilliseconds(25), remoteLatency.Value.BaseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(5), remoteLatency.Value.MaxJitter);

        cluster.ClearBidirectionalLinkLatency(FastCasAddress.Proposer(1), FastCasAddress.Acceptor(1));
        var collocatedAfterSpecificClear = cluster.Network.GetLinkLatency("proposer-1", "acceptor-1");
        Assert.True(collocatedAfterSpecificClear.HasValue);
        Assert.Equal(TimeSpan.FromMicroseconds(1), collocatedAfterSpecificClear.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, collocatedAfterSpecificClear.Value.MaxJitter);

        cluster.ClearLinkLatencies();

        var collocatedAfterClearAll = cluster.Network.GetLinkLatency("proposer-1", "acceptor-1");
        Assert.True(collocatedAfterClearAll.HasValue);
        Assert.Equal(TimeSpan.FromMicroseconds(1), collocatedAfterClearAll.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, collocatedAfterClearAll.Value.MaxJitter);
        Assert.Null(cluster.Network.GetLinkLatency("proposer-1", "acceptor-2"));
    }

    [Fact]
    public async Task CreateConsensusRegisterConfigurationCreatesCombinedRegisterEndpoints()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 743,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConsensusRegisterConfiguration(registerCount: 3);

        Assert.Equal(3, cluster.ConsensusRegisters.Count);

        var register = cluster.GetConsensusRegister(2);
        Assert.Equal(FastCasAddress.Proposer(2), register.ProposerAddress);
        Assert.Equal(FastCasAddress.Acceptor(2), register.AcceptorAddress);

        var proposerToAcceptor = cluster.Network.GetLinkLatency("proposer-2", "acceptor-2");
        Assert.True(proposerToAcceptor.HasValue);
        Assert.Equal(TimeSpan.FromMicroseconds(1), proposerToAcceptor.Value.BaseDelay);
        Assert.Equal(TimeSpan.Zero, proposerToAcceptor.Value.MaxJitter);
    }

    [Fact]
    public async Task ConsensusRegisterConfigurationRunsProposalsAndBroadcastsLearns()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 744,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticFast,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConsensusRegisterConfiguration(registerCount: 5);

        var response = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));
        Assert.Equal(new TestValue(1, "A"), response.CommittedValue);

        var peerRegister = cluster.GetConsensusRegister(2);
        Assert.True(
            cluster.RunUntil(
                () => EqualityComparer<TestValue>.Default.Equals(peerRegister.ProposerHost.CachedValue, new TestValue(1, "A")),
                maxIterations: 5000),
            "Expected peer register proposer role to learn the committed value.");

        Assert.All(
            cluster.ConsensusRegisters.Values,
            register => Assert.Equal(new TestValue(1, "A"), register.AcceptorHost.State.AcceptedValue));
    }

    [Fact]
    public async Task RunProposalUpdatesAcceptorSafetyMonitorHistory()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 135,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));

        var artifact = cluster.AcceptorSafetyMonitor.CreateArtifact();
        Assert.Single(artifact.Commits);
        Assert.Equal(1, artifact.Commits[0].Version);
        Assert.True(artifact.Commits[0].Witnesses.Length >= 3);
        Assert.Equal(5, artifact.FinalSnapshot.Length);
    }

    [Fact]
    public async Task RunProposalWhenQuorumUnavailableThrowsWithDiagnostics()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 654,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);
        var proposer = FastCasAddress.Proposer(1);
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(1));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(2));
        cluster.CreateBidirectionalPartition(proposer, FastCasAddress.Acceptor(3));

        var error = Assert.Throws<InvalidOperationException>(() => cluster.RunProposal(proposer, AppendOperation(1, "A"), maxIterations: 50));
        Assert.Contains("seed: 654", error.Message);
        Assert.Contains("reachableQuorumAvailable=False", error.Message);
    }

    [Fact]
    public async Task DiagnosticsIncludeSeedNodeStateAndFaultHistory()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 987,
            initialValue: default,
            options:new FastCasSimulationOptions
            {
                ProposerModePolicy = ProposerModePolicy.StaticClassic,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 3);
        cluster.SetMessageDropRate(0.25);
        cluster.SuspendNode(FastCasAddress.Acceptor(1));

        var diagnostics = cluster.GetFailureDiagnostics(nameof(DiagnosticsIncludeSeedNodeStateAndFaultHistory));

        Assert.Contains("seed: 987", diagnostics);
        Assert.Contains("proposer-1", diagnostics);
        Assert.Contains("acceptor-1", diagnostics);
        Assert.Contains("drop-rate 25", diagnostics);
        Assert.Contains("suspend acceptor-1", diagnostics);
    }

    private static Operation<TestValue, TestValue> AppendOperation(int expectedVersion, string suffix) =>
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

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }

    private readonly record struct LearnEvent(
        FastCasAddress SourceProposer,
        FastCasAddress TargetProposer,
        Ballot AcceptedBallot,
        TestValue Value,
        Ballot PreparedBallot);

    private sealed class RecordingHistoryObserver : IFastCasSimulationHistoryObserver
    {
        public List<LearnEvent> LearnEvents { get; } = [];

        public void OnProposalSent<TValue>(
            DateTimeOffset timestamp,
            FastCasAddress proposer,
            int requestId,
            IOperation<TValue> operation)
            where TValue : IVersionedValue<TValue>
        {
        }

        public void OnProposalCompleted<TValue>(
            DateTimeOffset timestamp,
            FastCasAddress proposer,
            int requestId,
            ProposeResponse<TValue> response)
            where TValue : IVersionedValue<TValue>
        {
        }

        public void OnLearnCommittedValue<TValue>(
            DateTimeOffset timestamp,
            FastCasAddress sourceProposer,
            FastCasAddress targetProposer,
            Ballot acceptedBallot,
            TValue value,
            Ballot preparedBallot)
            where TValue : IVersionedValue<TValue>
        {
            if (value is not TestValue typedValue)
            {
                throw new InvalidOperationException($"Unexpected learned value type '{typeof(TValue).Name}'.");
            }

            LearnEvents.Add(new LearnEvent(sourceProposer, targetProposer, acceptedBallot, typedValue, preparedBallot));
        }
    }

    private sealed class RecordingTransportObserver : IFastCasSimulationTransportObserver
    {
        private readonly List<(FastCasTransportMessageKind Kind, DeliveryStatus Status)> _messages = [];

        public void OnTransportMessage(FastCasTransportMessageKind kind, DeliveryStatus status) =>
            _messages.Add((kind, status));

        public int Count(FastCasTransportMessageKind kind, DeliveryStatus status) =>
            _messages.Count(message => message.Kind == kind && message.Status == status);
    }
}

