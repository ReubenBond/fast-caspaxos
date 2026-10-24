using FastCASPaxos.Server;
using Grpc.Core;

namespace FastCASPaxos.Grpc;

public sealed class GrpcAcceptorDigestSource : IAcceptorDigestSource
{
    private readonly Wire.AcceptorDigests.AcceptorDigestsClient _client;
    private readonly ClientCalls _calls;

    public GrpcAcceptorDigestSource(CallInvoker callInvoker, TimeSpan? callTimeout = null, TimeProvider? timeProvider = null)
    {
        _client = new(callInvoker);
        _calls = new(callTimeout, timeProvider ?? TimeProvider.System);
    }

    public async ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        var request = WireConvert.ListRequest(prefix, afterKey, limit);
        var response = await _calls.Invoke(o => _client.ListDigestsAsync(request, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            var entries = response.Entries.Select(entry =>
                new KeyDigest(WireConvert.Key(entry.Key, entry.HasKey), WireConvert.Digest(entry.Digest))).ToArray();
            var cursor = response.HasContinuationKey ? response.ContinuationKey : null;
            WireConvert.ValidatePage(entries.Select(e => e.Key).ToArray(), cursor, request);
            return new AcceptorDigestPage(Array.AsReadOnly(entries), cursor);
        });
    }
}
