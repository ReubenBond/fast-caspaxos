using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using Grpc.Core;

namespace FastCASPaxos.Grpc;

/// <summary>A remote acceptor adapter. The caller owns the channel supplying the call invoker.</summary>
public sealed class GrpcKeyedAcceptor : IKeyedAcceptor
{
    private readonly Wire.Acceptor.AcceptorClient _client;
    private readonly ClientCalls _calls;

    public GrpcKeyedAcceptor(CallInvoker callInvoker, TimeSpan? callTimeout = null)
    {
        _client = new(callInvoker);
        _calls = new(callTimeout);
    }

    public async ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        var wire = new Wire.PrepareRequest { Key = WireConvert.Text(key), Ballot = WireConvert.ToWire(request.Ballot) };
        var response = await _calls.Invoke(o => _client.PrepareAsync(wire, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            var (ballot, value) = WireConvert.Accepted(response.Accepted);
            switch (response.OutcomeCase)
            {
                case Wire.PrepareResponse.OutcomeOneofCase.Promised:
                    WireConvert.Require(WireConvert.Ballot(response.Promised) == request.Ballot,
                        "The promised ballot must echo the requested ballot.");
                    WireConvert.Require(ballot <= request.Ballot, "The accepted ballot exceeds the promised ballot.");
                    return AcceptorPrepareResult.Promised(ballot, value);
                case Wire.PrepareResponse.OutcomeOneofCase.Rejected:
                    var conflict = WireConvert.Ballot(response.Rejected);
                    WireConvert.Require(conflict > request.Ballot && conflict >= ballot,
                        "A prepare rejection must report a higher conflicting ballot.");
                    return AcceptorPrepareResult.Rejected(ballot, value, conflict);
                default:
                    throw new ArgumentException("An explicit prepare outcome is required.");
            }
        });
    }

    public async ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        var wire = new Wire.AcceptRequest
        {
            Key = WireConvert.Text(key),
            Ballot = WireConvert.ToWire(request.Ballot),
            Value = WireConvert.ToWire(request.Value),
            NextBallot = WireConvert.ToWire(request.NextBallotToPrepare, allowZero: true),
        };
        var response = await _calls.Invoke(o => _client.AcceptAsync(wire, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            switch (response.OutcomeCase)
            {
                case Wire.AcceptResponse.OutcomeOneofCase.Accepted:
                    return AcceptorAcceptResult.Accepted(WireConvert.Ballot(response.Accepted));
                case Wire.AcceptResponse.OutcomeOneofCase.Rejected:
                    var conflict = WireConvert.Ballot(response.Rejected);
                    WireConvert.Require(conflict >= request.Ballot, "An accept rejection must report a conflicting ballot.");
                    return AcceptorAcceptResult.Rejected(conflict);
                default:
                    throw new ArgumentException("An explicit accept outcome is required.");
            }
        });
    }

    public async ValueTask<KeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        var request = WireConvert.ListRequest(prefix, afterKey, limit);
        var response = await _calls.Invoke(o => _client.ListKeysAsync(request, o), cancellationToken).ConfigureAwait(false);
        return RpcErrors.Response(() =>
        {
            var entries = response.Entries.Select(entry =>
            {
                var (ballot, value) = WireConvert.Accepted(entry.Accepted);
                WireConvert.Require(value is not null, "Inventory entries must contain an accepted value.");
                return new KeyedAcceptorState(WireConvert.Key(entry.Key, entry.HasKey),
                    WireConvert.Ballot(entry.PromisedBallot), ballot, value!);
            }).ToArray();
            var cursor = response.HasContinuationKey ? response.ContinuationKey : null;
            WireConvert.ValidatePage(entries.Select(e => e.Key).ToArray(), cursor, request);
            return new KeyPage(Array.AsReadOnly(entries), cursor);
        });
    }
}
