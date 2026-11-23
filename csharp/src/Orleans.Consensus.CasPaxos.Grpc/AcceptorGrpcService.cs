using Orleans.Consensus.CasPaxos.Messages;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Consensus.CasPaxos.Grpc;

public sealed class AcceptorGrpcService(IKeyedAcceptor acceptor, ILogger<AcceptorGrpcService> logger)
    : Wire.Acceptor.AcceptorBase
{
    public override Task<Wire.PrepareResponse> Prepare(Wire.PrepareRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var key = WireConvert.Key(request.Key, request.HasKey);
            var ballot = WireConvert.Ballot(request.Ballot);
            var result = await acceptor.PrepareAsync(key, new PrepareRequest(ballot), context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                var response = new Wire.PrepareResponse
                {
                    Accepted = WireConvert.Accepted(result.AcceptedBallot, result.AcceptedValue),
                };
                if (result.IsPromised) response.Promised = WireConvert.ToWire(ballot);
                else response.Rejected = WireConvert.ToWire(result.ConflictingBallot, allowFloor: true);
                return response;
            });
        }, context, logger);

    public override Task<Wire.AcceptResponse> Accept(Wire.AcceptRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var key = WireConvert.Key(request.Key, request.HasKey);
            var ballot = WireConvert.Ballot(request.Ballot);
            var value = WireConvert.Bytes(request.Value, request.HasValue);
            var next = request.NextBallot is null ? Ballot.Zero : WireConvert.Ballot(request.NextBallot, allowZero: true);
            var result = await acceptor.AcceptAsync(key, new AcceptRequest<ByteString>(ballot, value, next), context.CancellationToken);
            return RpcErrors.Response(() => result.IsAccepted
                ? new Wire.AcceptResponse { Accepted = WireConvert.ToWire(result.PromisedBallot, allowFloor: true) }
                : new Wire.AcceptResponse { Rejected = WireConvert.ToWire(result.ConflictingBallot, allowFloor: true) });
        }, context, logger);

    public override Task<Wire.SetFloorResponse> SetFloor(Wire.SetFloorRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var targetFloor = WireConvert.FloorBallot(request.Floor);
            var floor = await acceptor.SetFloorAsync(targetFloor, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                WireConvert.Require(floor.IsFloorBallot && floor >= targetFloor,
                    "The returned floor ballot must be a valid floor at least as large as the requested floor.");
                return new Wire.SetFloorResponse { Floor = WireConvert.ToWire(floor, allowFloor: true) };
            });
        }, context, logger);

    public override Task<Wire.AcceptorKeyPage> ListKeys(Wire.ListKeysRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var cursor = request.HasAfterKey ? request.AfterKey : null;
            WireConvert.ListRequest(request.Prefix, cursor, request.Limit);
            var page = await acceptor.ListKeysAsync(request.Prefix, cursor, request.Limit, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                WireConvert.ValidatePage(page.Entries.Select(e => e.Key).ToArray(), page.ContinuationKey, request);
                var response = new Wire.AcceptorKeyPage();
                foreach (var entry in page.Entries)
                    response.Entries.Add(new Wire.AcceptorEntry
                    {
                        Key = WireConvert.Text(entry.Key),
                        PromisedBallot = WireConvert.ToWire(entry.PromisedBallot, allowFloor: true),
                        Accepted = WireConvert.Accepted(entry.AcceptedBallot, entry.AcceptedValue),
                    });
                if (page.ContinuationKey is not null) response.ContinuationKey = page.ContinuationKey;
                if (!page.FloorBallot.IsZero) response.FloorBallot = WireConvert.ToWire(page.FloorBallot, allowFloor: true);
                return response;
            });
        }, context, logger);
}
