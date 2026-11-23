using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Simulation.Contracts;
using Orleans.Consensus.CasPaxos.Simulation.Invariants;
using Xunit;

namespace Orleans.Consensus.CasPaxos.Simulation.Tests;

public sealed class AcceptorSafetyMonitorTests
{
    [Fact]
    public void ObserveTracksClassicCommitHistoryWhenCommittedValueChanges()
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
        Assert.Equal(2, artifact.Commits.Length);
        Assert.Equal(1, artifact.Commits[0].Version);
        Assert.Equal(2, artifact.Commits[1].Version);
        Assert.Equal("Ballot(r1, p1)", artifact.Commits[0].Ballot);
        Assert.Equal("Ballot(r2, p1)", artifact.Commits[1].Ballot);
    }

    [Fact]
    public void ObserveRequiresFastQuorumBeforeInferringCommit()
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
        Assert.Equal(4, artifact.Commits[0].Witnesses.Length);
        Assert.Equal(1, artifact.Commits[0].Version);
    }

    [Fact]
    public void ObserveAllowsConflictingFastRoundValuesUntilOneValueWinsFastQuorum()
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
    public void ObserveThrowsWhenAcceptedBallotCarriesConflictingValues()
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

    [Fact]
    public void ObserveTracksCrossConfigurationCommitsAndScopesQuorumsToBallotConfiguration()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        monitor.RegisterConfiguration(1, [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(2), FastCasAddress.Acceptor(3)]);
        monitor.RegisterConfiguration(2, [FastCasAddress.Acceptor(4), FastCasAddress.Acceptor(5), FastCasAddress.Acceptor(6)]);

        var c1Ballot = new Ballot(1, 1, 1);
        var c2Ballot = new Ballot(2, 1, 1);

        // Non-voter acceptor-4 holding c1Ballot does NOT count toward V(1)'s quorum.
        monitor.Observe(
            DateTimeOffset.UnixEpoch,
            FastCasAddress.Acceptor(1),
            [
                Snapshot(1, acceptedBallot: c1Ballot, acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(1)),
                Snapshot(2, floorBallot: Ballot.Floor(1)),
                Snapshot(3, floorBallot: Ballot.Floor(1)),
                Snapshot(4, acceptedBallot: c2Ballot, acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(2)),
                Snapshot(5, floorBallot: Ballot.Floor(2)),
                Snapshot(6, floorBallot: Ballot.Floor(2)),
            ]);
        Assert.Empty(monitor.CreateArtifact().Commits);

        // Commit in c=1 on {1,2}, raise floor to 2, and commit successor in c=2 on {4,5} simultaneously.
        monitor.Observe(
            DateTimeOffset.UnixEpoch.AddSeconds(1),
            FastCasAddress.Acceptor(2),
            [
                Snapshot(1, acceptedBallot: c1Ballot, acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(2)),
                Snapshot(2, acceptedBallot: c1Ballot, acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(2)),
                Snapshot(3, floorBallot: Ballot.Floor(1)),
                Snapshot(4, acceptedBallot: c2Ballot, acceptedValue: new TestValue(2, "AB"), floorBallot: Ballot.Floor(2)),
                Snapshot(5, acceptedBallot: c2Ballot, acceptedValue: new TestValue(2, "AB"), floorBallot: Ballot.Floor(2)),
                Snapshot(6, floorBallot: Ballot.Floor(2)),
            ]);

        var artifact = monitor.CreateArtifact();
        Assert.Equal(2, artifact.Commits.Length);
        Assert.Equal(1, artifact.Commits[0].Version);
        Assert.Equal(2, artifact.Commits[1].Version);
    }

    [Fact]
    public void ObserveRejectsFloorRegressionAndNewAcceptanceBelowFloor()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        monitor.Observe(
            DateTimeOffset.UnixEpoch,
            FastCasAddress.Acceptor(1),
            [
                Snapshot(1, promisedBallot: Ballot.Floor(2), floorBallot: Ballot.Floor(2)),
                Snapshot(2, floorBallot: Ballot.Floor(1)),
                Snapshot(3, floorBallot: Ballot.Floor(1)),
            ]);

        var regressed = Assert.Throws<InvalidOperationException>(() =>
            monitor.Observe(
                DateTimeOffset.UnixEpoch.AddSeconds(1),
                FastCasAddress.Acceptor(1),
                [
                    Snapshot(1, promisedBallot: Ballot.Floor(1), floorBallot: Ballot.Floor(1)),
                    Snapshot(2, floorBallot: Ballot.Floor(1)),
                    Snapshot(3, floorBallot: Ballot.Floor(1)),
                ]));
        Assert.Contains("Floor monotonicity", regressed.Message, StringComparison.Ordinal);

        var belowFloor = Assert.Throws<InvalidOperationException>(() =>
            monitor.Observe(
                DateTimeOffset.UnixEpoch.AddSeconds(2),
                FastCasAddress.Acceptor(1),
                [
                    Snapshot(1, promisedBallot: Ballot.Floor(2), acceptedBallot: new Ballot(1, 1, 1), acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(2)),
                    Snapshot(2, floorBallot: Ballot.Floor(1)),
                    Snapshot(3, floorBallot: Ballot.Floor(1)),
                ]));
        Assert.Contains("below durable floor", belowFloor.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ObserveRejectsPreJoinAcceptanceAndPermanentRetirementViolation()
    {
        var monitor = new FastCasAcceptorSafetyMonitor<TestValue>();
        monitor.RegisterConfiguration(1, [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(2), FastCasAddress.Acceptor(3)]);
        monitor.RegisterConfiguration(2, [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(2), FastCasAddress.Acceptor(4)]);

        var preJoin = Assert.Throws<InvalidOperationException>(() =>
            monitor.Observe(
                DateTimeOffset.UnixEpoch,
                FastCasAddress.Acceptor(4),
                [
                    Snapshot(1, floorBallot: Ballot.Floor(1)),
                    Snapshot(2, floorBallot: Ballot.Floor(1)),
                    Snapshot(3, floorBallot: Ballot.Floor(1)),
                    Snapshot(4, promisedBallot: new Ballot(1, 1, 1), acceptedBallot: new Ballot(1, 1, 1), acceptedValue: new TestValue(1, "A"), floorBallot: Ballot.Floor(1)),
                ]));
        Assert.Contains("Pre-join acceptance", preJoin.Message, StringComparison.Ordinal);

        var retirement = Assert.Throws<InvalidOperationException>(() =>
            monitor.RegisterConfiguration(3, [FastCasAddress.Acceptor(1), FastCasAddress.Acceptor(3), FastCasAddress.Acceptor(4)]));
        Assert.Contains("Permanent retirement", retirement.Message, StringComparison.Ordinal);
    }

    private static FastCasAcceptorStateSnapshot<TestValue> Snapshot(
        int ordinal,
        Ballot promisedBallot = default,
        Ballot acceptedBallot = default,
        TestValue acceptedValue = default,
        Ballot floorBallot = default) =>
        new(
            Acceptor: FastCasAddress.Acceptor(ordinal),
            IsRunning: true,
            IsSuspended: false,
            PromisedBallot: promisedBallot,
            AcceptedBallot: acceptedBallot,
            AcceptedValue: acceptedValue,
            FloorBallot: floorBallot);

    private readonly record struct TestValue(int Version, string Value) : IVersionedValue<TestValue>
    {
        public bool IsValidSuccessorTo(TestValue predecessor) =>
            predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";
    }
}
