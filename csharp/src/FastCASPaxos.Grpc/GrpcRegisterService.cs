using System.Collections.Immutable;
using FastCASPaxos.Server;
using Grpc.Core;

namespace FastCASPaxos.Grpc;

/// <summary>A remote register adapter. The caller owns the channel supplying the call invoker.</summary>
public sealed class GrpcRegisterService : IRegisterService
{
    private readonly Wire.Register.RegisterClient _client;
    private readonly ClientCalls _calls;

    public GrpcRegisterService(CallInvoker callInvoker, TimeSpan? callTimeout = null, TimeProvider? timeProvider = null)
    {
        _client = new(callInvoker);
        _calls = new(callTimeout, timeProvider ?? TimeProvider.System);
    }

    public async ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
    {
        var request = new Wire.ReadRequest { Key = WireConvert.Text(key) };
        var response = await _calls.Invoke(o => _client.ReadAsync(request, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() => WireConvert.Register(response));
    }

    public async ValueTask<CompareExchangeResult> CompareExchangeAsync(
        string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default)
    {
        var request = new Wire.CompareExchangeRequest { Key = WireConvert.Text(key), Value = WireConvert.ToWire(value) };
        if (expectedVersion is { } expected) request.ExpectedVersion = WireConvert.ToWire(expected);
        var options = _calls.Options(cancellationToken);
        try
        {
            using var call = _client.CompareExchangeAsync(request, options);
            var response = await call.ResponseAsync.ConfigureAwait(false);
            return RpcErrors.Response(() =>
            {
                switch (response.OutcomeCase)
                {
                    case Wire.CompareExchangeResponse.OutcomeOneofCase.Applied:
                        var current = WireConvert.Register(response.Applied);
                        WireConvert.Require(current.IsPresent, "An applied write must return a present value.");
                        WireConvert.Require(value == current.Value && expectedVersion != current.Version,
                            "An applied write must return its requested payload with a fresh version.");
                        return new CompareExchangeResult(CompareExchangeStatus.Applied, current);
                    case Wire.CompareExchangeResponse.OutcomeOneofCase.Mismatch:
                        var mismatch = WireConvert.Register(response.Mismatch);
                        WireConvert.Require(expectedVersion != mismatch.Version,
                            "A mismatch must report a different version than the expected version.");
                        return new CompareExchangeResult(CompareExchangeStatus.Mismatch, mismatch);
                    case Wire.CompareExchangeResponse.OutcomeOneofCase.Unknown:
                        return new CompareExchangeResult(CompareExchangeStatus.Unknown,
                            response.Unknown.Current is null ? null : WireConvert.Register(response.Unknown.Current),
                            response.Unknown.Error is null ? null : RpcErrors.FromWire(response.Unknown.Error));
                    default:
                        throw new ArgumentException("An explicit CompareExchange outcome is required.");
                }
            });
        }
        catch (RpcException error) when (RpcErrors.IsDefinite(error))
        {
            throw RpcErrors.FromRpc(error, cancellationToken);
        }
        catch (Exception error)
        {
            // Once dispatched, even a lost or malformed response can conceal a committed write.
            return new CompareExchangeResult(CompareExchangeStatus.Unknown, null,
                error is RpcException rpc ? RpcErrors.FromRpc(rpc, cancellationToken) : error);
        }
    }

    public async ValueTask<RegisterKeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        var request = WireConvert.ListRequest(prefix, afterKey, limit);
        var response = await _calls.Invoke(o => _client.ListKeysAsync(request, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            ImmutableArray<string> keys = [.. response.Keys];
            var cursor = response.HasContinuationKey ? response.ContinuationKey : null;
            WireConvert.ValidatePage(keys.AsSpan(), cursor, request);
            return new RegisterKeyPage(keys, cursor);
        });
    }
}
