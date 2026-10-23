using FastCASPaxos.Server;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace FastCASPaxos.Grpc;

public sealed class RegisterGrpcService(IRegisterService registers, ILogger<RegisterGrpcService> logger)
    : Wire.Register.RegisterBase
{
    public override Task<Wire.RegisterState> Read(Wire.ReadRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var value = await registers.ReadAsync(WireConvert.Key(request.Key, request.HasKey), context.CancellationToken);
            return RpcErrors.Response(() => WireConvert.ToWire(value));
        }, context, logger);

    public override Task<Wire.CompareExchangeResponse> CompareExchange(
        Wire.CompareExchangeRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var key = WireConvert.Key(request.Key, request.HasKey);
            var value = WireConvert.Bytes(request.Value, request.HasValue);
            var expected = request.HasExpectedVersion ? new ByteString(request.ExpectedVersion.Span) : null;
            var result = await registers.CompareExchangeAsync(key, expected, value, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                switch (result.Status)
                {
                    case CompareExchangeStatus.Applied:
                        WireConvert.Require(result.Current is { IsPresent: true }, "Applied requires a present current value.");
                        return new Wire.CompareExchangeResponse { Applied = WireConvert.ToWire(result.Current!) };
                    case CompareExchangeStatus.Mismatch:
                        WireConvert.Require(result.Current is not null, "Mismatch requires an established current value.");
                        return new Wire.CompareExchangeResponse { Mismatch = WireConvert.ToWire(result.Current!) };
                    case CompareExchangeStatus.Unknown:
                        if (result.Error is not null) logger.LogWarning(result.Error, "CompareExchange has an unknown outcome.");
                        return new Wire.CompareExchangeResponse
                        {
                            Unknown = new()
                            {
                                Current = result.Current is null ? null : WireConvert.ToWire(result.Current),
                                Error = result.Error is null ? null : RpcErrors.ToWire(result.Error),
                            },
                        };
                    default:
                        throw new ArgumentException("An explicit CompareExchange outcome is required.");
                }
            });
        }, context, logger);

    public override Task<Wire.RegisterKeyPage> ListKeys(Wire.ListKeysRequest request, ServerCallContext context) =>
        RpcErrors.Serve(async () =>
        {
            var cursor = request.HasAfterKey ? request.AfterKey : null;
            WireConvert.ListRequest(request.Prefix, cursor, request.Limit);
            var page = await registers.ListKeysAsync(request.Prefix, cursor, request.Limit, context.CancellationToken);
            return RpcErrors.Response(() =>
            {
                WireConvert.ValidatePage(page.Keys, page.ContinuationKey, request);
                var response = new Wire.RegisterKeyPage();
                response.Keys.AddRange(page.Keys);
                if (page.ContinuationKey is not null) response.ContinuationKey = page.ContinuationKey;
                return response;
            });
        }, context, logger);
}
