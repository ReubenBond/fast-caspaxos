using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class RegisterValidationTests
{
    [Fact]
    public void Constructor_RejectsInvalidMembershipWithoutContactingPeers()
    {
        var peers = Peers();
        var routes = Routes(peers);
        var ballots = new TestBallots(1);
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService(null!, ballots));
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService(routes, null!));
        Assert.Throws<ArgumentException>(() => new ReplicatedRegisterService([], ballots));
        Assert.Throws<ArgumentException>(() => new ReplicatedRegisterService([routes[0], routes[0]], ballots));
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService([null!], ballots));
        Assert.Throws<ArgumentException>(() => new ReplicatedRegisterService([new("", peers[0])], ballots));
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService([new("valid", null!)], ballots));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplicatedRegisterService(routes, new TestBallots(0)));
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
    }

    [Fact]
    public void Constructor_RejectsInvalidOptions()
    {
        var routes = Routes(Peers());
        var ballots = new TestBallots(1);
        RegisterServiceOptions[] invalid =
        [
            new() { Capacity = 0 }, new() { Capacity = -1 },
            new() { OperationTimeout = TimeSpan.Zero }, new() { OperationTimeout = Timeout.InfiniteTimeSpan },
            new() { OperationTimeout = TimeSpan.MaxValue },
            new() { AttemptTimeout = TimeSpan.Zero }, new() { AttemptTimeout = Timeout.InfiniteTimeSpan },
            new() { AttemptTimeout = TimeSpan.MaxValue },
            new() { RetryBackoff = TimeSpan.Zero }, new() { RetryBackoff = TimeSpan.FromTicks(-1) },
            new() { RetryBackoff = TimeSpan.MaxValue }
        ];
        foreach (var options in invalid)
            Assert.Throws<ArgumentOutOfRangeException>(() => new ReplicatedRegisterService(routes, ballots, options));
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService(routes, ballots, new() { Mode = null! }));
        Assert.Throws<ArgumentNullException>(() => new ReplicatedRegisterService(routes, ballots, new() { TimeProvider = null! }));
    }

    [Fact]
    public async Task InvalidCalls_FailBeforeNetworkOrAdmission()
    {
        var peers = Peers();
        await using var service = new ReplicatedRegisterService(Routes(peers), new TestBallots(1), Options());
        await Assert.ThrowsAsync<ArgumentNullException>(() => service.ReadAsync(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => service.CompareExchangeAsync(null!, null, Payload).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => service.CompareExchangeAsync("key", null, null!).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => service.ListKeysAsync(null!).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => service.ListKeysAsync(limit: 0).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => service.ListKeysAsync(limit: -1).AsTask());
        ByteString[] invalidVersions = [ByteString.Empty, new(new byte[7]), new(new byte[9]),
            Version(0, 1), Version(-1, 1), Version(1, 0), Version(1, -1)];
        foreach (var version in invalidVersions)
            await Assert.ThrowsAsync<ArgumentException>(() => service.CompareExchangeAsync("key", version, Payload).AsTask());
        Assert.All(peers, peer => Assert.Empty(peer.Prepares));
        Assert.All(peers, peer => Assert.Empty(peer.Accepts));
        // Empty key and payload are valid, unlike null.
        Assert.Equal(CompareExchangeStatus.Applied, (await service.CompareExchangeAsync("", null, ByteString.Empty)).Status);
        Assert.Equal(ByteString.Empty, (await service.ReadAsync("")).Value);
    }
}
