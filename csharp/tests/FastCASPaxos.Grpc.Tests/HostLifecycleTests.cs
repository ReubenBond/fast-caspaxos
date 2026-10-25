using FastCASPaxos.Host;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using FastCASPaxos.Server.Diagnostics;
using System.Buffers.Binary;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;
using static FastCASPaxos.Grpc.Tests.FastHandoffContractTests;

namespace FastCASPaxos.Grpc.Tests;

public sealed class HostLifecycleTests
{
    public enum BadOptions
    {
        MissingAcceptorId, MissingAcceptorPath, MissingProposerPath, SameDatabasePath, InvalidProposerId,
        EmptyMembership, MissingLocalMember, DuplicateId, DuplicateEndpoint, MissingPeerId, NullPeer,
        NullPeers, RelativeEndpoint, UnsupportedScheme, EndpointPath, EndpointQuery, EndpointFragment,
        EndpointCredentials, ZeroQueue, ZeroBatch, ZeroCapacity, ZeroReceiveLimit, ZeroSendLimit,
        ZeroConnectionLimit, ZeroStreamLimit, InvalidMode, ZeroOperationTimeout, ZeroAttemptTimeout,
        ZeroRetryBackoff, ZeroPeerTimeout, ExcessiveOperationTimeout, ExcessiveAttemptTimeout,
        ExcessiveRetryBackoff, ExcessivePeerTimeout,
    }

    public static IEnumerable<object[]> InvalidConfigurations() =>
        Enum.GetValues<BadOptions>().Select(value => new object[] { value });

    [Theory]
    [MemberData(nameof(InvalidConfigurations))]
    public void NodeOptionsRejectInvalidConfiguration(BadOptions scenario)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        switch (scenario)
        {
            case BadOptions.MissingAcceptorId: options.AcceptorId = " "; break;
            case BadOptions.MissingAcceptorPath: options.AcceptorDatabase = ""; break;
            case BadOptions.MissingProposerPath: options.ProposerDatabase = ""; break;
            case BadOptions.SameDatabasePath: options.ProposerDatabase = options.AcceptorDatabase; break;
            case BadOptions.InvalidProposerId: options.ProposerId = 0; break;
            case BadOptions.EmptyMembership: options.Peers.Clear(); break;
            case BadOptions.MissingLocalMember: options.Peers[0].Id = "other"; break;
            case BadOptions.DuplicateId: options.Peers.Add(new() { Id = "a", Endpoint = "http://127.0.0.1:2", HttpEndpoint = "http://127.0.0.1:12" }); break;
            case BadOptions.DuplicateEndpoint: options.Peers.Add(new() { Id = "b", Endpoint = "http://127.0.0.1:1/", HttpEndpoint = "http://127.0.0.1:12" }); break;
            case BadOptions.MissingPeerId: options.Peers[0].Id = ""; break;
            case BadOptions.NullPeer: options.Peers.Add(null!); break;
            case BadOptions.NullPeers: options.Peers = null!; break;
            case BadOptions.RelativeEndpoint: options.Peers[0].Endpoint = "/peer"; break;
            case BadOptions.UnsupportedScheme: options.Peers[0].Endpoint = "ftp://127.0.0.1"; break;
            case BadOptions.EndpointPath: options.Peers[0].Endpoint = "http://127.0.0.1/path"; break;
            case BadOptions.EndpointQuery: options.Peers[0].Endpoint = "http://127.0.0.1/?key=value"; break;
            case BadOptions.EndpointFragment: options.Peers[0].Endpoint = "http://127.0.0.1/#fragment"; break;
            case BadOptions.EndpointCredentials: options.Peers[0].Endpoint = "http://user:password@127.0.0.1"; break;
            case BadOptions.ZeroQueue: options.QueueCapacity = 0; break;
            case BadOptions.ZeroBatch: options.MaxBatchSize = 0; break;
            case BadOptions.ZeroCapacity: options.Capacity = 0; break;
            case BadOptions.ZeroReceiveLimit: options.MaxReceiveMessageBytes = 0; break;
            case BadOptions.ZeroSendLimit: options.MaxSendMessageBytes = 0; break;
            case BadOptions.ZeroConnectionLimit: options.MaxConcurrentConnections = 0; break;
            case BadOptions.ZeroStreamLimit: options.MaxStreamsPerConnection = 0; break;
            case BadOptions.InvalidMode: options.Mode = "invalid-mode"; break;
            case BadOptions.ZeroOperationTimeout: options.OperationTimeout = TimeSpan.Zero; break;
            case BadOptions.ZeroAttemptTimeout: options.AttemptTimeout = TimeSpan.Zero; break;
            case BadOptions.ZeroRetryBackoff: options.RetryBackoff = TimeSpan.Zero; break;
            case BadOptions.ZeroPeerTimeout: options.PeerCallTimeout = TimeSpan.Zero; break;
            case BadOptions.ExcessiveOperationTimeout: options.OperationTimeout = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
            case BadOptions.ExcessiveAttemptTimeout: options.AttemptTimeout = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
            case BadOptions.ExcessiveRetryBackoff: options.RetryBackoff = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
            case BadOptions.ExcessivePeerTimeout: options.PeerCallTimeout = TimeSpan.FromDays(1) + TimeSpan.FromTicks(1); break;
        }
        Assert.ThrowsAny<ArgumentException>(options.Validate);
        Assert.Empty(Directory.GetFiles(directory.Path)); // Validation must not create persistent state.
    }

    [Fact]
    public void NodeOptionsRequireExplicitMembershipAndAcceptBoundaryConfiguration()
    {
        Assert.Empty(new NodeOptions().Peers);
        using var directory = new TestDirectory();
        var options = Options(directory);
        options.QueueCapacity = options.MaxBatchSize = options.Capacity = 1;
        options.MaxReceiveMessageBytes = options.MaxSendMessageBytes = 1;
        options.MaxConcurrentConnections = options.MaxStreamsPerConnection = 1;
        foreach (var duration in new[] { TimeSpan.FromTicks(1), TimeSpan.FromDays(1) })
        {
            options.OperationTimeout = duration;
            options.AttemptTimeout = duration;
            options.RetryBackoff = duration;
            options.PeerCallTimeout = duration;
            foreach (var mode in new[] { "classic", "fast", "leader" })
            {
                options.Mode = mode;
                options.Validate();
            }
        }
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public void NodeOptionsRejectNegativeBoundsAsWellAsZero()
    {
        using var directory = new TestDirectory();
        Action<NodeOptions>[] invalid =
        [
            options => options.ProposerId = -1,
            options => options.QueueCapacity = -1,
            options => options.MaxBatchSize = -1,
            options => options.Capacity = -1,
            options => options.MaxReceiveMessageBytes = -1,
            options => options.MaxSendMessageBytes = -1,
            options => options.MaxConcurrentConnections = -1,
            options => options.MaxStreamsPerConnection = -1,
            options => options.OperationTimeout = TimeSpan.FromTicks(-1),
            options => options.AttemptTimeout = TimeSpan.FromTicks(-1),
            options => options.RetryBackoff = TimeSpan.FromTicks(-1),
            options => options.PeerCallTimeout = TimeSpan.FromTicks(-1),
        ];
        foreach (var change in invalid)
        {
            var options = Options(directory);
            change(options);
            Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        }
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task NodeRuntimeOfflineMembershipDoesNotRequireConnectivityAtStartup()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        options.Peers.Add(new() { Id = "b", Endpoint = "http://127.0.0.1:2", HttpEndpoint = "http://127.0.0.1:12" });
        options.Peers.Add(new() { Id = "c", Endpoint = "https://127.0.0.1:3", HttpEndpoint = "https://127.0.0.1:13" });
        await using var runtime = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
        // Only the local acceptor is exercised; constructing remote channels must not connect.
        var result = await runtime.Acceptor.AcceptAsync("local", new(new(2, 1), Payload, new())).AsTask().WaitAsync(Watchdog);
        Assert.True(result.IsAccepted);
        var entry = Assert.Single((await runtime.Acceptor.ListKeysAsync()).Entries);
        Assert.Equal("local", entry.Key);
        Assert.Equal(Payload, entry.AcceptedValue);
        Assert.True(File.Exists(options.AcceptorDatabase));
        Assert.True(File.Exists(options.ProposerDatabase));
    }

    [Fact]
    public async Task NodeRuntimeDisposalIsIdempotentAndReopenPreservesState()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        RegisterValue current;
        await using (var runtime = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog))
        {
            var client = new GrpcRegisterService(new ContractCallInvoker(register: Service(runtime.Registers)));
            var created = await client.CompareExchangeAsync("durable", null, Payload).AsTask().WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, created.Status);
            current = created.Current!;
            // Enqueue before disposal; the bounded writer must drain accepted work.
            var pending = Enumerable.Range(0, 16).Select(index => runtime.Acceptor.AcceptAsync(
                $"queued-{index:D2}", new(new(2, 1), new ByteString([(byte)index]), new())).AsTask()).ToArray();
            var first = runtime.DisposeAsync().AsTask();
            var second = runtime.DisposeAsync().AsTask();
            Assert.Same(first, second);
            await Task.WhenAll(first, second).WaitAsync(Watchdog);
            Assert.All(await Task.WhenAll(pending).WaitAsync(Watchdog), reply => Assert.True(reply.IsAccepted));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => runtime.Registers.ReadAsync("durable").AsTask());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => runtime.Acceptor.ListKeysAsync().AsTask());
        }
        await using var reopened = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
        Assert.Equal(current, await reopened.Registers.ReadAsync("durable").AsTask().WaitAsync(Watchdog));
        var page = await reopened.Acceptor.ListKeysAsync("queued-");
        Assert.Equal(16, page.Entries.Length);
        for (var index = 0; index < 16; index++)
        {
            Assert.Equal($"queued-{index:D2}", page.Entries[index].Key);
            Assert.Equal(new ByteString([(byte)index]), page.Entries[index].AcceptedValue);
        }
        var updated = await reopened.Registers.CompareExchangeAsync("durable", current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, updated.Status);
        Assert.NotEqual(current.Version, updated.Current!.Version);
        Assert.Equal(Payload, updated.Current.Value);
    }

    [Fact]
    public async Task NodeRuntimePreCancelledStartupCreatesNoDatabase()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => NodeRuntime.OpenAsync(options, cancellationToken: cancellation.Token));
        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task NodeRuntimeInvalidStartupRejectsBeforeCreatingDatabase()
    {
        using var directory = new TestDirectory();
        await Assert.ThrowsAsync<ArgumentNullException>(() => NodeRuntime.OpenAsync(null!));
        var options = Options(directory);
        options.Peers.Clear();
        await Assert.ThrowsAsync<ArgumentException>(() => NodeRuntime.OpenAsync(options));
        Assert.Empty(Directory.GetFiles(directory.Path));
    }

    [Fact]
    public async Task NodeRuntimeFailedProposerInitializationReleasesAcceptor()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        using (var other = new SqliteProposerBallotSource(options.ProposerDatabase, 2))
            Assert.Equal(new FastCASPaxos.Model.Ballot(1, 2), other.Reserve(1));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => NodeRuntime.OpenAsync(options).WaitAsync(Watchdog));
        Assert.Contains("different proposer identity", error.Message, StringComparison.Ordinal);
        // No leaked SQLite owner may retain an exclusive file handle after partial startup.
        using (File.Open(options.AcceptorDatabase, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) { }
        options.ProposerId = 2;
        await using var recovered = await NodeRuntime.OpenAsync(options).WaitAsync(Watchdog);
        var created = await recovered.Registers.CompareExchangeAsync("recovered", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(CompareExchangeStatus.Applied, created.Status);
        Assert.Equal(Payload, created.Current!.Value);
    }

    [Theory]
    [InlineData("classic", true, false)]
    [InlineData("fast", false, true)]
    [InlineData("leader", true, true)]
    [InlineData("fast+leader", false, true)]
    [InlineData("adaptive", false, true)]
    public async Task ProposerSettingsApplyEachSupportedMode(string mode, bool prepares, bool preparesNext)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var registers = node.Registers;
        var status = await node.ApplySettingsAsync(new() { Mode = " " + mode.ToUpperInvariant() + " " });
        Assert.Equal(mode, status.Settings.Mode);
        Assert.Same(registers, node.Registers);
        var cursor = node.Diagnostics.Cursor;

        var result = await registers.CompareExchangeAsync("mode", null, Payload).AsTask().WaitAsync(Watchdog);

        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), result.Current);
        var requests = await AdministrationProtocolRequests(node, cursor);
        Assert.Equal(prepares ? 1 : 0, requests.Count(item => item.Operation == "prepare"));
        var accept = Assert.Single(requests, item => item.Operation == "accept");
        Assert.Equal(!preparesNext, accept.NextBallot.IsZero);
        Assert.Equal(prepares, accept.Ballot.Proposer == node.ProposerId);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ProposerSettingsPersistedSelectionOverridesStartupDefaults(bool enabled, bool forwarding)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        options.Mode = "classic";
        await using (var node = await NodeRuntime.OpenAsync(options))
            await node.ApplySettingsAsync(SelectedSettings(enabled) with { EnableForwarding = forwarding });
        await using var reopened = await NodeRuntime.OpenAsync(options);

        AssertSelectedSettings(reopened.Administration.Settings, enabled, forwarding);
        var cursor = reopened.Diagnostics.Cursor;
        var created = await reopened.Registers.CompareExchangeAsync("startup", null, Payload).AsTask().WaitAsync(Watchdog);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), created.Current);
        var requests = await AdministrationProtocolRequests(reopened, cursor);
        Assert.Equal(enabled ? 0 : 1, requests.Count(item => item.Operation == "prepare"));
    }

    public enum InvalidSettings
    {
        UnknownMode, NullMode, NegativeIncrement, NegativeDecay, NegativeReopen,
        ZeroMisses, NegativeMisses, UnrepresentableLeaderedDebt,
    }

    [Theory]
    [InlineData(InvalidSettings.UnknownMode)]
    [InlineData(InvalidSettings.NullMode)]
    [InlineData(InvalidSettings.NegativeIncrement)]
    [InlineData(InvalidSettings.NegativeDecay)]
    [InlineData(InvalidSettings.NegativeReopen)]
    [InlineData(InvalidSettings.ZeroMisses)]
    [InlineData(InvalidSettings.NegativeMisses)]
    [InlineData(InvalidSettings.UnrepresentableLeaderedDebt)]
    public async Task ProposerSettingsRejectInvalidValuesWithoutMutation(InvalidSettings scenario)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var created = await node.Registers.CompareExchangeAsync("retained", null, Payload);
        var before = node.Administration;
        var durable = await node.Inspection.InspectAsync("retained");
        var settings = scenario switch
        {
            InvalidSettings.UnknownMode => new ProposerSettings { Mode = "unknown" },
            InvalidSettings.NullMode => new() { Mode = null! },
            InvalidSettings.NegativeIncrement => new() { Mode = "classic", ConflictDebtIncrement = -1 },
            InvalidSettings.NegativeDecay => new() { ConflictDebtDecay = -1 },
            InvalidSettings.NegativeReopen => new() { FastReopenThreshold = -1 },
            InvalidSettings.ZeroMisses => new() { LeaderMissThreshold = 0 },
            InvalidSettings.NegativeMisses => new() { LeaderMissThreshold = -1 },
            _ => new() { StartInFastMode = false, FastReopenThreshold = int.MaxValue },
        };

        await Assert.ThrowsAnyAsync<ArgumentException>(() => node.ApplySettingsAsync(settings));

        Assert.Equal(before, node.Administration);
        Assert.Equal(durable, await node.Inspection.InspectAsync("retained"));
        Assert.Equal(created.Current, await node.Registers.ReadAsync("retained"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProposerSettingsAcceptInt32Boundaries(bool maximum)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var result = await node.ApplySettingsAsync(new()
        {
            ConflictDebtIncrement = maximum ? int.MaxValue : 0,
            ConflictDebtDecay = maximum ? int.MaxValue : 0,
            FastReopenThreshold = maximum ? int.MaxValue : 0,
            LeaderMissThreshold = maximum ? int.MaxValue : 1,
        });
        Assert.Equal(maximum ? int.MaxValue : 0, result.Settings.ConflictDebtIncrement);
        Assert.Equal(maximum ? int.MaxValue : 0, result.Settings.ConflictDebtDecay);
        Assert.Equal(maximum ? int.MaxValue : 0, result.Settings.FastReopenThreshold);
        Assert.Equal(maximum ? int.MaxValue : 1, result.Settings.LeaderMissThreshold);
        var created = await node.Registers.CompareExchangeAsync("boundary", null, Payload);
        Assert.Equal(new RegisterValue(Payload, Revision(1)), created.Current);
        var leadered = await node.ApplySettingsAsync(new() { StartInFastMode = false, FastReopenThreshold = int.MaxValue - 1 });
        Assert.False(leadered.Settings.StartInFastMode);
        Assert.Equal(int.MaxValue - 1, leadered.Settings.FastReopenThreshold);
    }

    [Fact]
    public async Task ProposerSettingsDrainAdmittedWorkBeforeFacadeSwap()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var registers = node.Registers;
        var acceptor = node.Acceptor;
        var digests = node.Digests;
        var store = Assert.IsType<SqliteAcceptorServer>(node.Inspection);
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.BeforeCommit = (_, _, _) =>
        {
            entered.TrySetResult();
            if (!release.Wait(Watchdog)) throw new TimeoutException("Test did not release the register commit.");
        };
        try
        {
            var write = registers.CompareExchangeAsync("admitted", null, Payload).AsTask();
            await entered.Task.WaitAsync(Watchdog);
            var changing = node.ApplySettingsAsync(new() { Mode = "classic" }, cancellation.Token);
            cancellation.Cancel();
            Assert.Equal("classic", node.Administration.Settings.Mode); // Persisted before draining.
            Assert.False(changing.IsCompleted);
            Assert.False(write.IsCompleted);
            await Assert.ThrowsAsync<RegisterUnavailableException>(() => registers.ReadAsync("late").AsTask());
            store.BeforeCommit = null;
            release.Set();
            var result = await write.WaitAsync(Watchdog);
            Assert.Equal(CompareExchangeStatus.Applied, result.Status);
            await changing.WaitAsync(Watchdog);
            Assert.Same(registers, node.Registers);
            Assert.Same(acceptor, node.Acceptor);
            Assert.Same(digests, node.Digests);
            Assert.Equal(result.Current, await registers.ReadAsync("admitted"));
            var updated = await registers.CompareExchangeAsync("admitted", result.Current!.Version, ByteString.Empty);
            Assert.Equal(new RegisterValue(ByteString.Empty, Revision(2)), updated.Current);
        }
        finally { store.BeforeCommit = null; release.Set(); }
    }

    [Fact]
    public async Task ProposerSettingsSwapRetainsAcceptorDataAndPromises()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        RegisterValue value;
        LocalAcceptorState? promise;
        LocalAcceptorState? accepted;
        await using (var node = await NodeRuntime.OpenAsync(options))
        {
            value = (await node.Registers.CompareExchangeAsync("", null, ByteString.Empty)).Current!;
            Assert.True((await node.Acceptor.AcceptAsync("binary", new(new(17, 1), Payload, new(18, 1)))).IsAccepted);
            Assert.True((await node.Acceptor.PrepareAsync("promise-only", new(new(19, 1)))).IsPromised);
            accepted = await node.Inspection.InspectAsync("binary");
            promise = await node.Inspection.InspectAsync("promise-only");
            foreach (var mode in new[] { "classic", "fast", "leader", "adaptive" })
            {
                await node.ApplySettingsAsync(new() { Mode = mode });
                Assert.Equal(value, await node.Registers.ReadAsync(""));
                Assert.Equal(accepted, await node.Inspection.InspectAsync("binary"));
                Assert.Equal(promise, await node.Inspection.InspectAsync("promise-only"));
            }
        }
        await using var reopened = await NodeRuntime.OpenAsync(options);
        Assert.Equal(value, await reopened.Registers.ReadAsync(""));
        Assert.Equal(accepted, await reopened.Inspection.InspectAsync("binary"));
        Assert.Equal(promise, await reopened.Inspection.InspectAsync("promise-only"));
    }

    [Fact]
    public async Task ProposerSettingsSwapDiscardsLearnedPolicyCache()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        await node.ApplySettingsAsync(new() { Mode = "fast" });
        node.Learn(new("learned-only", new(50, 0), Envelope(7, Payload), new(51, 0)));
        var registers = node.Registers;
        await node.ApplySettingsAsync(new() { Mode = "fast" });
        var result = await registers.CompareExchangeAsync("learned-only", null, ByteString.Empty);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        Assert.Equal(new RegisterValue(ByteString.Empty, Revision(1)), result.Current);
        Assert.Equal(new FastCASPaxos.Model.Ballot(1, 0),
            (await node.Inspection.InspectAsync("learned-only"))!.AcceptedBallot);
    }

    [Fact]
    public async Task ClusterAdministrationPreCancelledCommandsLeaveDurableStateUnchanged()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var written = await node.Registers.CompareExchangeAsync("retained", null, Payload);
        var before = node.Administration;
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.ApplySettingsAsync(new() { Mode = "classic" }, cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.ApplyClusterSettingsAsync(new() { Mode = "leader" }, cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.ResetClusterAsync(NewReset(), cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => node.ApplyResetPhaseAsync(NewReset(), "prepare", cancellation.Token));
        Assert.Equal(before, node.Administration);
        Assert.Equal(written.Current, await node.Registers.ReadAsync("retained"));
    }

    [Fact]
    public async Task ClusterSettingsCoordinatorAppliesAndReportsLocalMember()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var result = await node.ApplyClusterSettingsAsync(SelectedSettings(false));
        Assert.Equal("complete", result.State);
        Assert.Equal(new AdminNodeResult("a", "active"), Assert.Single(result.Nodes));
        Assert.Null(result.Reset);
        AssertSelectedSettings(node.Administration.Settings, false);
        Assert.Equal(1, node.Administration.SchemaVersion);
        Assert.Equal("a", node.Administration.CoordinatorId);
    }

    [Fact]
    public async Task ClusterCommandsRejectNonOrdinalCoordinatorBeforeContactingPeers()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        options.AcceptorId = "b";
        options.Peers =
        [
            new() { Id = "b", Endpoint = "http://127.0.0.1:1", HttpEndpoint = "http://127.0.0.1:11" },
            new() { Id = "a2", Endpoint = "http://127.0.0.1:2", HttpEndpoint = "http://127.0.0.1:12" },
            new() { Id = "a10", Endpoint = "http://127.0.0.1:3", HttpEndpoint = "http://127.0.0.1:13" },
        ];
        await using var node = await NodeRuntime.OpenAsync(options);
        Assert.Equal("a10", node.CoordinatorId);
        Assert.Contains("'a10'", (await Assert.ThrowsAsync<InvalidOperationException>(() =>
            node.ApplyClusterSettingsAsync(new()))).Message, StringComparison.Ordinal);
        Assert.Contains("'a10'", (await Assert.ThrowsAsync<InvalidOperationException>(() =>
            node.ResetClusterAsync(NewReset()))).Message, StringComparison.Ordinal);
        Assert.Equal("active", node.Administration.State);
        Assert.Equal("0", node.Administration.Epoch);
        Assert.Null(node.Administration.ClusterReset);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ClusterAdminMutationsSerializeWithPrepareAndShutdown(bool shutdown)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        await using var node = await NodeRuntime.OpenAsync(options);
        var selected = node.Administration.Settings;
        IDisposable? lease = node.EnterProtocol("0");
        try
        {
            var prepare = node.ApplyResetPhaseAsync(NewReset(), "prepare");
            Assert.Equal("preparing", node.Administration.State);
            var settings = node.ApplySettingsAsync(new() { Mode = "leader" });
            var stop = shutdown ? node.StopAsync() : Task.CompletedTask;
            Assert.False(prepare.IsCompleted);
            Assert.False(settings.IsCompleted);
            if (shutdown) Assert.False(stop.IsCompleted);
            lease.Dispose();
            lease = null;
            Assert.Equal("prepared", (await prepare.WaitAsync(Watchdog)).State);
            await Assert.ThrowsAsync<InvalidOperationException>(() => settings.WaitAsync(Watchdog));
            await stop.WaitAsync(Watchdog);
            Assert.Equal(selected, node.Administration.Settings);
            Assert.Equal("prepared", node.Administration.State);
            Assert.Equal(shutdown, node.IsShutdownComplete);
            Assert.Throws<RegisterUnavailableException>(() => node.EnterProtocol("0"));
        }
        finally { lease?.Dispose(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ClusterResetClearsEmptyAndPopulatedStateRetainingSettings(bool populated)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        await using (var node = await NodeRuntime.OpenAsync(options))
        {
            await node.ApplySettingsAsync(SelectedSettings(false));
            if (populated)
            {
                foreach (var key in new[] { "", "a +/&?é", "third" })
                    Assert.Equal(CompareExchangeStatus.Applied,
                        (await node.Registers.CompareExchangeAsync(key, null, key == "" ? ByteString.Empty : Payload)).Status);
                Assert.True((await node.Acceptor.PrepareAsync("promise-only", new(new(90, 1)))).IsPromised);
                node.Learn(new("learned-only", new(99, 0), Envelope(1, Payload), new(100, 0)));
                Assert.Equal(4, (await node.Inspection.InspectKeysAsync()).Entries.Length);
            }
            var result = await node.ResetClusterAsync(NewReset()).WaitAsync(Watchdog);
            Assert.Equal("complete", result.State);
            Assert.Equal(new AdminNodeResult("a", "active"), Assert.Single(result.Nodes));
            Assert.Equal("1", node.Administration.Epoch);
            Assert.Equal("complete", node.Administration.ClusterReset!.Phase);
            AssertSelectedSettings(node.Administration.Settings, false);
            Assert.Empty((await node.Inspection.InspectKeysAsync(limit: 1)).Entries);
            Assert.Empty((await node.Acceptor.ListKeysAsync(limit: 1)).Entries);
            Assert.Empty((await node.Digests.ListDigestsAsync(limit: 1)).Entries);
            Assert.Empty((await node.Registers.ListKeysAsync(limit: 1)).Keys);
            foreach (var key in new[] { "", "a +/&?é", "third", "promise-only", "learned-only" })
            {
                Assert.Null(await node.Inspection.InspectAsync(key));
                Assert.Equal(new RegisterValue(null, null), await node.Registers.ReadAsync(key));
            }
            var next = await node.Registers.CompareExchangeAsync("learned-only", null, ByteString.Empty);
            Assert.Equal(new RegisterValue(ByteString.Empty, EpochVersion(1, 1)), next.Current);
        }
        await using var reopened = await NodeRuntime.OpenAsync(options);
        Assert.Equal("1", reopened.Administration.Epoch);
        AssertSelectedSettings(reopened.Administration.Settings, false);
        Assert.Equal(new RegisterValue(ByteString.Empty, EpochVersion(1, 1)), await reopened.Registers.ReadAsync("learned-only"));
        Assert.Null(await reopened.Inspection.InspectAsync("promise-only"));
    }

    [Theory]
    [InlineData("prepare", "prepared")]
    [InlineData("clear", "cleared")]
    public async Task InterruptedClusterResetRemainsFencedAcrossRestart(string phase, string durablePhase)
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        var request = NewReset();
        await using (var node = await NodeRuntime.OpenAsync(options))
        {
            await node.Registers.CompareExchangeAsync("old", null, Payload);
            await node.ApplyResetPhaseAsync(request, "prepare");
            if (phase == "clear") await node.ApplyResetPhaseAsync(request, "clear");
        }
        await using var reopened = await NodeRuntime.OpenAsync(options);
        Assert.Equal(durablePhase, reopened.Administration.State);
        Assert.Equal(request, reopened.Administration.PendingReset);
        Assert.Equal("0", reopened.Administration.Epoch);
        Assert.Throws<RegisterUnavailableException>(() => reopened.EnterProtocol("0"));
        await Assert.ThrowsAsync<RegisterUnavailableException>(() => reopened.Registers.ReadAsync("old").AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.ApplySettingsAsync(new()));
        if (phase == "prepare")
            Assert.Equal("old", (await reopened.Inspection.InspectAsync("old"))!.Key);
        else Assert.Null(await reopened.Inspection.InspectAsync("old"));
        await reopened.ApplyResetPhaseAsync(request, "prepare");
        await reopened.ApplyResetPhaseAsync(request, "clear");
        await reopened.ApplyResetPhaseAsync(request, "activate");
        Assert.Equal("1", reopened.Administration.Epoch);
        Assert.Equal("active", reopened.Administration.State);
        Assert.Null(reopened.Administration.PendingReset);
        Assert.Equal(new RegisterValue(null, null), await reopened.Registers.ReadAsync("old"));
    }

    [Fact]
    public async Task CompletedClusterResetReplayPreservesNewGenerationWritesAndRejectsChangedEpoch()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        var first = NewReset();
        await using (var node = await NodeRuntime.OpenAsync(options))
            Assert.Equal("complete", (await node.ResetClusterAsync(first)).State);
        await using var reopened = await NodeRuntime.OpenAsync(options);
        var written = await reopened.Registers.CompareExchangeAsync("new", null, Payload);
        foreach (var phase in new[] { "prepare", "clear", "activate" })
        {
            await reopened.ApplyResetPhaseAsync(first, phase);
            Assert.Equal(written.Current, await reopened.Registers.ReadAsync("new"));
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                reopened.ApplyResetPhaseAsync(first with { ExpectedEpoch = "2" }, phase));
        }
        Assert.Equal("complete", (await reopened.ResetClusterAsync(first)).State);
        Assert.Equal(written.Current, await reopened.Registers.ReadAsync("new"));
        var reused = first with { ExpectedEpoch = "1" };
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.ResetClusterAsync(reused));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.ApplyResetPhaseAsync(reused, "prepare"));
        Assert.Equal(written.Current, await reopened.Registers.ReadAsync("new"));
        var second = NewReset() with { ExpectedEpoch = "1" };
        Assert.Equal("complete", (await reopened.ResetClusterAsync(second)).State);
        Assert.Equal("2", reopened.Administration.Epoch);
        var secondWrite = await reopened.Registers.CompareExchangeAsync("new", null, ByteString.Empty);
        Assert.Equal(new RegisterValue(ByteString.Empty, EpochVersion(2, 1)), secondWrite.Current);
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.ResetClusterAsync(first));
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.ApplyResetPhaseAsync(first, "clear"));
        Assert.Equal(secondWrite.Current, await reopened.Registers.ReadAsync("new"));
    }

    [Fact]
    public async Task ClusterResetRejectsPreResetRegisterPreconditions()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var original = await node.Registers.CompareExchangeAsync("key", null, Payload);
        Assert.Equal(Revision(1), original.Current!.Version);
        await node.ResetClusterAsync(NewReset());
        var missing = await node.Registers.CompareExchangeAsync("key", original.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, missing.Status);
        Assert.Equal(new RegisterValue(null, null), missing.Current);
        var fresh = await node.Registers.CompareExchangeAsync("key", null, ByteString.Empty);
        Assert.Equal(EpochVersion(1, 1), fresh.Current!.Version);
        var stale = await node.Registers.CompareExchangeAsync("key", original.Current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Mismatch, stale.Status);
        Assert.Equal(fresh.Current, stale.Current);
        var updated = await node.Registers.CompareExchangeAsync("key", fresh.Current.Version, Payload);
        Assert.Equal(new RegisterValue(Payload, EpochVersion(1, 2)), updated.Current);
    }

    [Fact]
    public async Task ClusterResetRejectsConflictingOrOutOfOrderOperations()
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var request = NewReset();
        await node.Registers.CompareExchangeAsync("retained", null, Payload);
        var before = await node.Inspection.InspectAsync("retained");
        foreach (var phase in new[] { "clear", "activate" })
            await Assert.ThrowsAsync<InvalidOperationException>(() => node.ApplyResetPhaseAsync(request, phase));
        Assert.Equal("active", node.Administration.State);
        Assert.Equal(before, await node.Inspection.InspectAsync("retained"));
        await node.ApplyResetPhaseAsync(request, "prepare");
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.ApplyResetPhaseAsync(NewReset(), "prepare"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.ApplyResetPhaseAsync(request with { ExpectedEpoch = "1" }, "prepare"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => node.ApplyResetPhaseAsync(request, "activate"));
        Assert.Equal("prepared", node.Administration.State);
        Assert.Equal(before, await node.Inspection.InspectAsync("retained"));
        await node.ApplyResetPhaseAsync(request, "clear");
        await node.ApplyResetPhaseAsync(request, "clear");
        Assert.Equal("cleared", node.Administration.State);
        Assert.Null(await node.Inspection.InspectAsync("retained"));
        await node.ApplyResetPhaseAsync(request, "activate");
    }

    public enum InvalidReset
    {
        ZeroId, UppercaseId, MalformedId, DashedId, EmptyEpoch, LeadingZero, NegativeEpoch,
        SignedEpoch, WhitespaceEpoch, FractionalEpoch, FutureOverflow, MaximumEpoch,
    }

    [Theory]
    [InlineData(InvalidReset.ZeroId)]
    [InlineData(InvalidReset.UppercaseId)]
    [InlineData(InvalidReset.MalformedId)]
    [InlineData(InvalidReset.DashedId)]
    [InlineData(InvalidReset.EmptyEpoch)]
    [InlineData(InvalidReset.LeadingZero)]
    [InlineData(InvalidReset.NegativeEpoch)]
    [InlineData(InvalidReset.SignedEpoch)]
    [InlineData(InvalidReset.WhitespaceEpoch)]
    [InlineData(InvalidReset.FractionalEpoch)]
    [InlineData(InvalidReset.FutureOverflow)]
    [InlineData(InvalidReset.MaximumEpoch)]
    public async Task ClusterResetRejectsNoncanonicalIdentityWithoutMutation(InvalidReset scenario)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        var request = scenario switch
        {
            InvalidReset.ZeroId => new ResetRequest(new string('0', 32), "0"),
            InvalidReset.UppercaseId => new(new string('A', 32), "0"),
            InvalidReset.MalformedId => new("not-an-id", "0"),
            InvalidReset.DashedId => new("12345678-1234-1234-1234-1234567890ab", "0"),
            InvalidReset.EmptyEpoch => NewReset() with { ExpectedEpoch = "" },
            InvalidReset.LeadingZero => NewReset() with { ExpectedEpoch = "00" },
            InvalidReset.NegativeEpoch => NewReset() with { ExpectedEpoch = "-1" },
            InvalidReset.SignedEpoch => NewReset() with { ExpectedEpoch = "+0" },
            InvalidReset.WhitespaceEpoch => NewReset() with { ExpectedEpoch = " 0" },
            InvalidReset.FractionalEpoch => NewReset() with { ExpectedEpoch = "0.0" },
            InvalidReset.FutureOverflow => NewReset() with { ExpectedEpoch = "18446744073709551616" },
            _ => NewReset() with { ExpectedEpoch = "18446744073709551615" },
        };
        var before = node.Administration;
        await Assert.ThrowsAnyAsync<ArgumentException>(() => node.ResetClusterAsync(request));
        Assert.Equal(before, node.Administration);
        using var lease = node.EnterProtocol("0");
    }

    [Theory]
    [InlineData("9007199254740993")]
    [InlineData("18446744073709551614")]
    public async Task ClusterResetAcceptsFullWidthDecimalEpochSyntaxButRejectsUnobservedEpoch(string epoch)
    {
        using var directory = new TestDirectory();
        await using var node = await NodeRuntime.OpenAsync(Options(directory));
        // Valid UInt64 syntax beyond double precision must reach the durable epoch conflict check,
        // rather than fail input validation (ArgumentException).
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            node.ResetClusterAsync(NewReset() with { ExpectedEpoch = epoch }));
        Assert.Equal("0", node.Administration.Epoch);
        Assert.Null(node.Administration.ClusterReset);
        Assert.Equal("active", node.Administration.State);
    }

    [Fact]
    public async Task ClusterResetPreservesAllocatorIdentityUniqueness()
    {
        using var directory = new TestDirectory();
        var options = Options(directory);
        int before;
        int after;
        await using (var node = await NodeRuntime.OpenAsync(options))
        {
            before = await WriteIncarnation(node);
            await node.ResetClusterAsync(NewReset());
            after = await WriteIncarnation(node);
            Assert.True(after > before, $"Reset must retain the durable allocator: {before} -> {after}.");
        }
        await using var reopened = await NodeRuntime.OpenAsync(options);
        var restart = await WriteIncarnation(reopened);
        Assert.True(restart > after, $"Restart must reserve a new incarnation: {after} -> {restart}.");
    }

    private static async Task<int> WriteIncarnation(NodeRuntime node)
    {
        var current = await node.Registers.ReadAsync("identity");
        var result = await node.Registers.CompareExchangeAsync("identity", current.Version, Payload);
        Assert.Equal(CompareExchangeStatus.Applied, result.Status);
        var envelope = (await node.Inspection.InspectAsync("identity"))!.AcceptedValue!.Value.ToArray();
        Assert.Equal("FCR2"u8.ToArray(), envelope[..4]);
        Assert.Equal(1, BinaryPrimitives.ReadInt32BigEndian(envelope.AsSpan(13)));
        return BinaryPrimitives.ReadInt32BigEndian(envelope.AsSpan(17));
    }

    internal static ResetRequest NewReset() => new(Guid.NewGuid().ToString("N"), "0");
    private static ProposerSettings SelectedSettings(bool enabled) => new()
    {
        StartInFastMode = enabled, EnableForwarding = enabled,
        ConflictDebtIncrement = 7, ConflictDebtDecay = 2, FastReopenThreshold = 3, LeaderMissThreshold = 5,
    };
    private static void AssertSelectedSettings(ProposerSettings settings, bool enabled, bool? forwarding = null)
    {
        Assert.Equal("adaptive", settings.Mode);
        Assert.Equal(enabled, settings.StartInFastMode);
        Assert.Equal(forwarding ?? enabled, settings.EnableForwarding);
        Assert.Equal(7, settings.ConflictDebtIncrement);
        Assert.Equal(2, settings.ConflictDebtDecay);
        Assert.Equal(3, settings.FastReopenThreshold);
        Assert.Equal(5, settings.LeaderMissThreshold);
    }
    private static ByteString EpochVersion(ulong epoch, ulong revision) =>
        new([.. Revision(epoch).ToArray(), .. Revision(revision).ToArray()]);

    private static async Task<List<ProtocolObservation>> AdministrationProtocolRequests(NodeRuntime node, string cursor)
    {
        var end = node.Diagnostics.Cursor;
        var result = new List<ProtocolObservation>();
        using var timeout = new CancellationTokenSource(Watchdog);
        await foreach (var entry in node.Diagnostics.ReadAsync(cursor, Watchdog, timeout.Token))
        {
            if (entry.Data is ProtocolObservation { Phase: "request-initiated" } observation)
                result.Add(observation);
            if (entry.Id == end) return result;
        }
        throw new InvalidOperationException("The diagnostic journal ended before the captured operation.");
    }

    internal static NodeOptions Options(TestDirectory directory) => new()
    {
        AcceptorId = "a",
        AcceptorDatabase = directory.Database("acceptor"),
        ProposerId = 1,
        ProposerDatabase = directory.Database("proposer"),
        Peers = [new() { Id = "a", Endpoint = "http://127.0.0.1:1", HttpEndpoint = "http://127.0.0.1:11" }],
        QueueCapacity = 64,
        MaxBatchSize = 8,
    };
}
