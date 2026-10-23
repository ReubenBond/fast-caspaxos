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

    public async ValueTask<ConditionalAcceptorState> FetchIfDifferentAsync(
        string key, AcceptedDigest? known, CancellationToken cancellationToken = default)
    {
        var request = new Wire.FetchIfDifferentRequest
        {
            Key = WireConvert.Text(key),
            Known = known is null ? null : WireConvert.ToWire(known),
        };
        var response = await _calls.Invoke(o => _client.FetchIfDifferentAsync(request, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            switch (response.OutcomeCase)
            {
                case Wire.ConditionalAcceptorState.OutcomeOneofCase.Missing:
                    return new ConditionalAcceptorState(ConditionalAcceptorStatus.Missing);
                case Wire.ConditionalAcceptorState.OutcomeOneofCase.Unchanged:
                    WireConvert.Require(known is not null, "Unchanged requires a supplied digest.");
                    return new ConditionalAcceptorState(ConditionalAcceptorStatus.Unchanged);
                case Wire.ConditionalAcceptorState.OutcomeOneofCase.Different:
                    var entry = response.Different;
                    WireConvert.Require(WireConvert.Key(entry.Key, entry.HasKey) == key, "The response must identify the requested key.");
                    var (ballot, value) = WireConvert.Accepted(entry.Accepted);
                    WireConvert.Require(value is not null, "Different requires accepted bytes.");
                    WireConvert.Require(known is null || known != new AcceptedDigest(ballot, AcceptedDigest.Hash(value!)),
                        "Different must differ from the supplied digest.");
                    return new ConditionalAcceptorState(ConditionalAcceptorStatus.Different,
                        new(key, WireConvert.Ballot(entry.PromisedBallot), ballot, value!));
                default:
                    throw new ArgumentException("An explicit conditional-fetch outcome is required.");
            }
        });
    }
}
