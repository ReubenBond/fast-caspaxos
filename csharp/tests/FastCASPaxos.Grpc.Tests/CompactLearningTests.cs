using System.Security.Cryptography;
using FastCASPaxos.Host;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using FastCASPaxos.Sqlite;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using static FastCASPaxos.Grpc.Tests.FastHandoffContractTests;

namespace FastCASPaxos.Grpc.Tests;

public sealed class CompactLearningTests
{
    [Theory]
    [InlineData(11)]
    [InlineData(26)]
    [InlineData(5)]
    public async Task SqliteBallotInspectionClassifiesCorruptionSeparatelyFromOrdinaryFailure(int code)
    {
        using var directory = new TestDirectory();
        var store = await SqliteAcceptorServer.OpenAsync(directory.Database("inspection"));
        var error = new Microsoft.Data.Sqlite.SqliteException("injected read failure", code);
        try
        {
            store.BeforeCommit = (_, _, _) => throw error;
            var thrown = await Record.ExceptionAsync(() => store.InspectBallotsAsync("key").AsTask());
            if (code is 11 or 26) Assert.Same(error, Assert.IsType<InvalidDataException>(thrown).InnerException);
            else Assert.Same(error, thrown);
        }
        finally
        {
            store.BeforeCommit = null;
            // The injected writer failure is terminal; disposal reports that
            // same original failure after releasing the owned SQLite resources.
            Assert.Same(error, await Record.ExceptionAsync(() => store.DisposeAsync().AsTask()));
        }
    }

    public enum LocalMatch { Exact, Missing, Older, Newer, DifferentHash }

    [Theory]
    [InlineData(LocalMatch.Exact)]
    [InlineData(LocalMatch.Missing)]
    [InlineData(LocalMatch.Older)]
    [InlineData(LocalMatch.Newer)]
    [InlineData(LocalMatch.DifferentHash)]
    public async Task CompactLearnResolvesOnlyExactLocalBallotAndHash(LocalMatch match)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(HostLifecycleTests.Options(directory));
        var envelope = Envelope(1, Payload);
        var commit = new RegisterCommitDigest("key", new(new(5, 0), new(SHA256.HashData(envelope.ToArray()))), new(6, 0));
        if (match != LocalMatch.Missing)
        {
            var localBallot = new Ballot(match == LocalMatch.Older ? 4 : match == LocalMatch.Newer ? 7 : 5, 0);
            var localValue = match == LocalMatch.DifferentHash ? Envelope(1, ByteString.Empty) : envelope;
            Assert.True((await node.Acceptor.AcceptAsync("key", new(localBallot, localValue, localBallot.NextRound(0)))).IsAccepted);
        }
        var before = await node.Inspection.InspectAsync("key");
        Assert.Equal(match == LocalMatch.Exact, await node.LearnDigestAsync(commit));
        Assert.Equal(before, await node.Inspection.InspectAsync("key")); // Learning itself must not write.
        var read = await node.Registers.ReadAsync("key");
        Assert.Equal(match == LocalMatch.Missing ? new RegisterValue(null, null)
            : new RegisterValue(match == LocalMatch.DifferentHash ? ByteString.Empty : Payload, Revision(1)), read);
    }

    [Fact]
    public async Task CompactLearnReorderingCannotRollBackNewerCertificate()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(HostLifecycleTests.Options(directory));
        var first = Envelope(1, Payload);
        var latest = Envelope(2, ByteString.Empty);
        var old = new RegisterCommitDigest("key", new(new(5, 0), new(SHA256.HashData(first.ToArray()))), new(6, 0));
        var current = new RegisterCommitDigest("key", new(new(8, 0), new(SHA256.HashData(latest.ToArray()))), new(9, 0));
        Assert.True((await node.Acceptor.AcceptAsync("key", new(current.Accepted.AcceptedBallot, latest, current.PreparedBallot))).IsAccepted);
        Assert.True(await node.LearnDigestAsync(current));
        Assert.False(await node.LearnDigestAsync(old));
        Assert.True(await node.LearnDigestAsync(current));
        var result = await node.Registers.CompareExchangeAsync("key", Revision(2), Payload);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(3)), result.Current);
        Assert.Equal(new Ballot(9, 0), (await node.Inspection.InspectAsync("key"))!.AcceptedBallot);
    }

    [Fact]
    public async Task CompactLearnMatchingHashWithCorruptEnvelopeIsAnError()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(HostLifecycleTests.Options(directory));
        var invalid = new ByteString("not a register"u8);
        Assert.True((await node.Acceptor.AcceptAsync("key", new(new(5, 0), invalid, new(6, 0)))).IsAccepted);
        var before = await node.Inspection.InspectAsync("key");
        var commit = new RegisterCommitDigest("key", new(new(5, 0), new(SHA256.HashData(invalid.ToArray()))), new(6, 0));
        await Assert.ThrowsAsync<InvalidDataException>(() => node.LearnDigestAsync(commit).AsTask());
        Assert.Equal(before, await node.Inspection.InspectAsync("key"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompactResolutionCancellationOrDeadlineCannotLearnLateAcrossReset(bool cancel)
    {
        using var directory = new TestDirectory();
        var options = HostLifecycleTests.Options(directory);
        options.Capacity = 1;
        options.AttemptTimeout = TimeSpan.FromSeconds(1);
        var clock = new RepairClock();
        await using var node = await NodeRuntime.OpenAsync(options, clock);
        var value = Envelope(1, Payload);
        Assert.True((await node.Acceptor.AcceptAsync("key", new(new(5, 0), value, new(6, 0)))).IsAccepted);
        var commit = new RegisterCommitDigest("key", new(new(5, 0), new(SHA256.HashData(value.ToArray()))), new(6, 0));
        var store = Assert.IsType<SqliteAcceptorServer>(node.Inspection);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.BeforeCommit = (_, _, _) =>
        {
            entered.TrySetResult();
            if (!release.Wait(Watchdog)) throw new TimeoutException("Resolution gate was not released.");
        };
        using var cancellation = new CancellationTokenSource();
        var previous = DiagnosticContext.Current;
        DiagnosticContext.Current = new("sender", "bounded-resolution", "bounded-call");
        var cursor = node.Diagnostics.Cursor;
        Task<NodeAdministration>? preparing = null;
        var request = HostLifecycleTests.NewReset();
        try
        {
            var learning = node.LearnDigestAsync(commit, cancellation.Token).AsTask();
            await entered.Task.WaitAsync(Watchdog);
            if (cancel)
            {
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => learning.WaitAsync(Watchdog));
            }
            else
            {
                await clock.WaitForTimerAsync(options.AttemptTimeout);
                clock.Advance(options.AttemptTimeout);
                await Assert.ThrowsAsync<TimeoutException>(() => learning.WaitAsync(Watchdog));
            }
            // Settings must not replace the executor and admit a second physical SQLite read.
            await node.ApplySettingsAsync(new() { Mode = "fast" });
            await Assert.ThrowsAsync<RegisterBusyException>(() => node.LearnDigestAsync(commit).AsTask());
            preparing = node.ApplyResetPhaseAsync(request, "prepare");
            Assert.Equal("prepared", (await preparing.WaitAsync(Watchdog)).State);
            Assert.False(release.IsSet); // Resolution's expired waiter no longer holds serving admission.
        }
        finally
        {
            store.BeforeCommit = null;
            release.Set();
            DiagnosticContext.Current = previous;
        }
        await node.ApplyResetPhaseAsync(request, "clear").WaitAsync(Watchdog);
        await node.ApplyResetPhaseAsync(request, "activate").WaitAsync(Watchdog);
        Assert.Null(await node.Inspection.InspectAsync("key"));
        var created = await node.Registers.CompareExchangeAsync("key", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, new ByteString([.. Revision(1).ToArray(), .. Revision(1).ToArray()])), created.Current);
        var end = node.Diagnostics.Cursor;
        using var timeout = new CancellationTokenSource(Watchdog);
        var observations = new List<ProtocolObservation>();
        await foreach (var item in node.Diagnostics.ReadAsync(cursor, Watchdog, timeout.Token))
        {
            if (item.Data is ProtocolObservation observation && observation.OperationId == "bounded-resolution")
                observations.Add(observation);
            if (item.Id == end) break;
        }
        Assert.Contains(observations, item => item.Operation == "learn-digest" && item.Phase == "fault-observed");
        Assert.DoesNotContain(observations, item => item.Operation == "learn-digest" && item.Phase == "response-produced");
    }
}
