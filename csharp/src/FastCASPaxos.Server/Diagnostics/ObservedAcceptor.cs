using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;

namespace FastCASPaxos.Server.Diagnostics;

/// <summary>Observes the actual acceptor boundary while preserving its response and cancellation contract.</summary>
public sealed class ObservedAcceptor(
    IKeyedAcceptor inner, NodeEventJournal journal, string direction, string? peerId) : IKeyedAcceptor
{
    public async ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        var previous = DiagnosticContext.Current;
        var context = Begin(previous);
        try
        {
            Record("prepare", RequestPhase, context, key, request.Ballot, Ballot.Zero);
            var result = await inner.PrepareAsync(key, request, cancellationToken).ConfigureAwait(false);
            Record("prepare", ResponsePhase, context, key, request.Ballot, Ballot.Zero,
                result.IsPromised ? "promised" : "rejected",
                result.IsPromised ? request.Ballot : null, result.AcceptedBallot, result.ConflictingBallot,
                result.AcceptedValue);
            return result;
        }
        catch (Exception error)
        {
            Record("prepare", "fault-observed", context, key, request.Ballot, Ballot.Zero, error: error.GetType().Name);
            throw;
        }
        finally { DiagnosticContext.Current = previous; }
    }

    public async ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        var previous = DiagnosticContext.Current;
        var context = Begin(previous);
        try
        {
            Record("accept", RequestPhase, context, key, request.Ballot, request.NextBallotToPrepare, value: request.Value);
            var result = await inner.AcceptAsync(key, request, cancellationToken).ConfigureAwait(false);
            Record("accept", ResponsePhase, context, key, request.Ballot, request.NextBallotToPrepare,
                result.IsAccepted ? "accepted" : "rejected", result.PromisedBallot,
                conflicting: result.ConflictingBallot);
            return result;
        }
        catch (Exception error)
        {
            Record("accept", "fault-observed", context, key, request.Ballot, request.NextBallotToPrepare,
                error: error.GetType().Name);
            throw;
        }
        finally { DiagnosticContext.Current = previous; }
    }

    public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100,
        CancellationToken cancellationToken = default) => inner.ListKeysAsync(prefix, afterKey, limit, cancellationToken);

    private string RequestPhase => direction == "inbound" ? "request-received" : "request-initiated";
    private string ResponsePhase => direction == "inbound" ? "response-produced" : "response-observed";

    private DiagnosticContext Begin(DiagnosticContext? previous)
    {
        var context = new DiagnosticContext(
            direction == "inbound" ? previous?.NodeId ?? "" : journal.NodeId,
            previous?.OperationId,
            direction == "inbound" ? previous?.CallId ?? Guid.NewGuid().ToString("N") : Guid.NewGuid().ToString("N"));
        DiagnosticContext.Current = context;
        return context;
    }

    private void Record(string operation, string phase, DiagnosticContext context, string key, Ballot ballot,
        Ballot next, string? outcome = null, Ballot? promised = null, Ballot? accepted = null,
        Ballot? conflicting = null, ByteString? value = null, string? error = null) =>
        journal.Publish("protocol", new ProtocolObservation(operation, direction, phase,
            direction == "inbound" ? (context.NodeId.Length == 0 ? null : context.NodeId) : peerId,
            context.CallId!, context.OperationId, KeyPreview.From(key), ballot, next,
            outcome, promised, accepted, conflicting, PayloadPreview.From(value), error));
}
