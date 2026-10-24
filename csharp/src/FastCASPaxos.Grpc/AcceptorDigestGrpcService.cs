using FastCASPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

public sealed class AcceptorDigestGrpcService(
    IAcceptorDigestSource source, IKeyedAcceptor acceptor, ILogger<AcceptorDigestGrpcService> logger)
    : Wire.AcceptorDigests.AcceptorDigestsBase
{
    public override Task<Wire.AcceptorDigestPage> ListDigests(Wire.ListKeysRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var cursor = request.HasAfterKey ? request.AfterKey : null;
            WireConvert.ListRequest(request.Prefix, cursor, request.Limit);
            var page = await source.ListDigestsAsync(request.Prefix, cursor, request.Limit, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                WireConvert.ValidatePage(page.Entries.Select(e => e.Key).ToArray(), page.ContinuationKey, request);
                var response = new Wire.AcceptorDigestPage();
                foreach (var entry in page.Entries)
                    response.Entries.Add(new Wire.KeyDigest { Key = WireConvert.Text(entry.Key), Digest = WireConvert.ToWire(entry.Digest) });
                if (page.ContinuationKey is not null) response.ContinuationKey = page.ContinuationKey;
                return response;
            });
        }, context, logger);

    public override Task<Wire.ConditionalAcceptorState> FetchIfDifferent(Wire.FetchIfDifferentRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var key = WireConvert.Key(request.Key, request.HasKey);
            var known = request.Known is null ? null : WireConvert.Digest(request.Known);
            var result = await acceptor.ReadAsync(key, known, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                if (result is KeyedAcceptorState state)
                {
                    WireConvert.Require(state.Key == key, "Different must carry the requested key.");
                    WireConvert.Require(known is null || known != new AcceptedDigest(state.AcceptedBallot, AcceptedDigest.Hash(state.AcceptedValue)),
                        "Different must differ from the supplied digest.");
                    return new Wire.ConditionalAcceptorState { Different = new()
                    {
                        Key = WireConvert.Text(state.Key),
                        PromisedBallot = WireConvert.ToWire(state.PromisedBallot),
                        Accepted = WireConvert.Accepted(state.AcceptedBallot, state.AcceptedValue),
                    } };
                }
                return result switch
                {
                    AcceptorMissing => new Wire.ConditionalAcceptorState { Missing = new() },
                    AcceptorUnchanged when known is not null => new Wire.ConditionalAcceptorState { Unchanged = new() },
                    _ => throw new ArgumentException("A valid explicit read outcome is required."),
                };
            });
        }, context, logger);
}
