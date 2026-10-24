using FastCASPaxos.Host;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Xunit;
using static FastCASPaxos.Grpc.Tests.ContractTestSupport;

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

    private static NodeOptions Options(TestDirectory directory) => new()
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
