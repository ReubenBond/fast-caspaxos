using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Hosts;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class FastCasSimulationClusterTests
{
    [Fact]
    public async Task RunProposal_CompletesAgainstConfiguredQuorum()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 123,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var response = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));

        Assert.Equal(new TestValue(1, "A"), response.CommittedValue);
        Assert.Equal("RunProposal_CompletesAgainstConfiguredQuorum (seed: 123)", cluster.GetReproductionHint(nameof(RunProposal_CompletesAgainstConfiguredQuorum)));
    }

    [Fact]
    public async Task SendProposal_StoresClientResponseByRequestId()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 456,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        var requestId = cluster.SendProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"), requestId: 42);
        Assert.True(cluster.RunUntil(() => cluster.TryGetResponse(requestId, out _), maxIterations: 1000));
        Assert.True(cluster.TryGetResponse(requestId, out var response));
        Assert.Equal(Ballot.InitialClassic(1).Round, response.Round);
    }

    [Fact]
    public async Task FaultOperations_BlockThenRestoreProgress()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 789,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
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
    public async Task CrashRestart_PreservesAcceptorState()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 246,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
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
    public async Task RunProposal_UpdatesAcceptorSafetyMonitorHistory()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 135,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 5);

        _ = cluster.RunProposal(FastCasAddress.Proposer(1), AppendOperation(1, "A"));

        var artifact = cluster.AcceptorSafetyMonitor.CreateArtifact();
        Assert.Single(artifact.Commits);
        Assert.Equal(1, artifact.Commits[0].Version);
        Assert.True(artifact.Commits[0].Witnesses.Count >= 3);
        Assert.Equal(5, artifact.FinalSnapshot.Count);
    }

    [Fact]
    public async Task RunProposal_WhenQuorumUnavailable_ThrowsWithDiagnostics()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 654,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
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
    public async Task Diagnostics_IncludeSeedNodeStateAndFaultHistory()
    {
        await using var cluster = new FastCasSimulationCluster<TestValue>(
            seed: 987,
            options: new FastCasSimulationOptions
            {
                EnableFastCommit = false,
                EnableDistinguishedLeader = false,
            },
            startDateTime: DateTimeOffset.UnixEpoch);

        cluster.CreateConfiguration(proposerCount: 1, acceptorCount: 3);
        cluster.SetMessageDropRate(0.25);
        cluster.SuspendNode(FastCasAddress.Acceptor(1));

        var diagnostics = cluster.GetFailureDiagnostics(nameof(Diagnostics_IncludeSeedNodeStateAndFaultHistory));

        Assert.Contains("seed: 987", diagnostics);
        Assert.Contains("proposer-1", diagnostics);
        Assert.Contains("acceptor-1", diagnostics);
        Assert.Contains("drop-rate 25", diagnostics);
        Assert.Contains("suspend acceptor-1", diagnostics);
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

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}

