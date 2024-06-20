using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;
using FastCASPaxos.Simulation.Invariants;
using Xunit;

namespace FastCASPaxos.Simulation.Tests;

public sealed class AcceptorSafetyMonitorTests
{
    [Fact]
    public void Observe_TracksClassicCommitHistoryWhenCommittedValueChanges()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        var firstBallot = Ballot.InitialClassic(1);
        var secondBallot = firstBallot.NextRound(1);

        monitor.Observe(
            DateTimeOffset.UnixEpoch,
            FastCasAddress.Acceptor(3),
            [
                Snapshot(1, acceptedBallot: firstBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(2, acceptedBallot: firstBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(3, acceptedBallot: firstBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(4),
                Snapshot(5),
            ]);

        monitor.Observe(
            DateTimeOffset.UnixEpoch.AddSeconds(1),
            FastCasAddress.Acceptor(4),
            [
                Snapshot(1, acceptedBallot: secondBallot, acceptedValue: new TestValue(2, "AB")),
                Snapshot(2, acceptedBallot: secondBallot, acceptedValue: new TestValue(2, "AB")),
                Snapshot(3, acceptedBallot: secondBallot, acceptedValue: new TestValue(2, "AB")),
                Snapshot(4, acceptedBallot: secondBallot, acceptedValue: new TestValue(2, "AB")),
                Snapshot(5, acceptedBallot: firstBallot, acceptedValue: new TestValue(1, "A")),
            ]);

        var artifact = monitor.CreateArtifact();
        Assert.Equal(2, artifact.Commits.Count);
        Assert.Equal(1, artifact.Commits[0].Version);
        Assert.Equal(2, artifact.Commits[1].Version);
        Assert.Equal("Ballot(r1, p1)", artifact.Commits[0].Ballot);
        Assert.Equal("Ballot(r2, p1)", artifact.Commits[1].Ballot);
    }

    [Fact]
    public void Observe_RequiresFastQuorumBeforeInferringCommit()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        var fastBallot = Ballot.InitialFast();

        monitor.Observe(
            DateTimeOffset.UnixEpoch,
            FastCasAddress.Acceptor(3),
            [
                Snapshot(1, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(2, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(3, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(4),
                Snapshot(5),
            ]);
        Assert.Empty(monitor.CreateArtifact().Commits);

        monitor.Observe(
            DateTimeOffset.UnixEpoch.AddSeconds(1),
            FastCasAddress.Acceptor(4),
            [
                Snapshot(1, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(2, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(3, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(4, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(5),
            ]);

        var artifact = monitor.CreateArtifact();
        Assert.Single(artifact.Commits);
        Assert.Equal(4, artifact.Commits[0].Witnesses.Count);
        Assert.Equal(1, artifact.Commits[0].Version);
    }

    [Fact]
    public void Observe_AllowsConflictingFastRoundValuesUntilOneValueWinsFastQuorum()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        var fastBallot = Ballot.InitialFast();

        monitor.Observe(
            DateTimeOffset.UnixEpoch,
            FastCasAddress.Acceptor(4),
            [
                Snapshot(1, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(2, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "A")),
                Snapshot(3, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "B")),
                Snapshot(4, acceptedBallot: fastBallot, acceptedValue: new TestValue(1, "B")),
                Snapshot(5),
            ]);

        Assert.Empty(monitor.CreateArtifact().Commits);
    }

    [Fact]
    public void Observe_ThrowsWhenAcceptedBallotCarriesConflictingValues()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        var classicBallot = Ballot.InitialClassic(1);

        var error = Assert.Throws<InvalidOperationException>(
            () => monitor.Observe(
                DateTimeOffset.UnixEpoch,
                FastCasAddress.Acceptor(3),
                [
                    Snapshot(1, acceptedBallot: classicBallot, acceptedValue: new TestValue(1, "A")),
                    Snapshot(2, acceptedBallot: classicBallot, acceptedValue: new TestValue(1, "A")),
                    Snapshot(3, acceptedBallot: classicBallot, acceptedValue: new TestValue(1, "B")),
                    Snapshot(4),
                    Snapshot(5),
                ]));

        Assert.Contains("Accepted ballot Ballot(r1, p1) has conflicting values", error.Message, StringComparison.Ordinal);
        Assert.Contains("acceptor-3=Val(B@1)", error.Message, StringComparison.Ordinal);
    }

    private static FastCasAcceptorStateSnapshot<TestValue> Snapshot(
        int ordinal,
        Ballot promisedBallot = default,
        Ballot acceptedBallot = default,
        TestValue acceptedValue = default) =>
        new(
            Acceptor: FastCasAddress.Acceptor(ordinal),
            IsRunning: true,
            IsSuspended: false,
            PromisedBallot: promisedBallot,
            AcceptedBallot: acceptedBallot,
            AcceptedValue: acceptedValue);

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
