using System.Text.Json;
using System.Text.Json.Serialization;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Scenarios;

public sealed record PorcupineHistoryArtifact(
    string SchemaVersion,
    string ModelKind,
    string ScenarioName,
    int Round,
    int Seed,
    IReadOnlyList<PorcupineHistoryEvent> Events);

public sealed record PorcupineHistoryEvent(
    long Sequence,
    string Kind,
    int OperationId,
    int RequestId,
    int ClientId,
    string Client,
    string Proposer,
    DateTimeOffset Timestamp,
    PorcupineOperationInput? Input,
    PorcupineValueOutput? Output,
    int? ProtocolRound);

public sealed record PorcupineOperationInput(
    string Kind,
    int? ExpectedVersion,
    string? Value);

public sealed record PorcupineValueOutput(
    int Version,
    string? StringValue,
    IReadOnlyList<string>? SetValue)
{
    public static PorcupineValueOutput FromString(StringValue value) =>
        new(
            value.Version,
            value.Value ?? string.Empty,
            SetValue: null);

    public static PorcupineValueOutput FromSet(SetValue value) =>
        new(
            value.Version,
            StringValue: null,
            SetValue: (value.Value ?? []).OrderBy(item => item, StringComparer.Ordinal).ToArray());
}

internal interface IPorcupineHistoryOperationDescriptor<TValue>
{
    string ModelKind { get; }

    PorcupineOperationInput CreatePorcupineInput();
}

internal static class PorcupineModelKinds
{
    public const string String = "string";
    public const string Set = "set";
}

internal sealed class PorcupineHistoryBuilder
{
    private readonly List<PorcupineHistoryEvent> _events = [];
    private readonly Dictionary<int, PendingOperation> _pendingOperations = [];
    private long _nextSequence = 1;
    private string? _modelKind;

    public string? UnsupportedReason { get; private set; }

    public bool HasEvents => _events.Count > 0;

    public void OnProposalSent<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        IOperation<TValue> operation)
        where TValue : Contracts.IVersionedValue<TValue>
    {
        if (UnsupportedReason is not null)
        {
            return;
        }

        if (!TryDescribeOperation(operation, out var modelKind, out var input))
        {
            UnsupportedReason = $"unsupported operation type '{UnwrapOperation(operation).GetType().FullName}'";
            return;
        }

        if (_modelKind is null)
        {
            _modelKind = modelKind;
        }
        else if (!string.Equals(_modelKind, modelKind, StringComparison.Ordinal))
        {
            UnsupportedReason = $"mixed Porcupine model kinds are not supported within the same run: '{_modelKind}' and '{modelKind}'.";
            return;
        }

        var clientId = proposer.Ordinal;
        var participant = proposer.NetworkAddress;
        _pendingOperations[requestId] = new PendingOperation(
            requestId,
            clientId,
            participant,
            proposer.NetworkAddress);
        _events.Add(new PorcupineHistoryEvent(
            Sequence: _nextSequence++,
            Kind: "call",
            OperationId: requestId,
            RequestId: requestId,
            ClientId: clientId,
            Client: participant,
            Proposer: proposer.NetworkAddress,
            Timestamp: timestamp,
            Input: input,
            Output: null,
            ProtocolRound: null));
    }

    public void OnProposalCompleted<TValue>(
        DateTimeOffset timestamp,
        FastCasAddress proposer,
        int requestId,
        ProposeResponse<TValue> response)
        where TValue : Contracts.IVersionedValue<TValue>
    {
        if (UnsupportedReason is not null)
        {
            return;
        }

        if (!_pendingOperations.TryGetValue(requestId, out var pending))
        {
            UnsupportedReason = $"missing pending Porcupine request '{requestId}' for proposer '{proposer}'.";
            return;
        }

        if (!TryCreateOutput(response.CommittedValue, out var output))
        {
            UnsupportedReason = $"unsupported committed value type '{response.CommittedValue.GetType().FullName ?? typeof(TValue).FullName}'.";
            return;
        }

        _events.Add(new PorcupineHistoryEvent(
            Sequence: _nextSequence++,
            Kind: "return",
            OperationId: pending.OperationId,
            RequestId: requestId,
            ClientId: pending.ClientId,
            Client: pending.Client,
            Proposer: pending.Proposer,
            Timestamp: timestamp,
            Input: null,
            Output: output,
            ProtocolRound: response.Round));
        _pendingOperations.Remove(requestId);
    }

    public PorcupineHistoryArtifact? CreateArtifact(string scenarioName, int round, int seed)
    {
        if (UnsupportedReason is not null || _events.Count == 0 || _modelKind is null)
        {
            return null;
        }

        return new PorcupineHistoryArtifact(
            SchemaVersion: "fast-caspaxos-porcupine-history-v1",
            ModelKind: _modelKind,
            ScenarioName: scenarioName,
            Round: round,
            Seed: seed,
            Events: _events.ToArray());
    }

    private static bool TryDescribeOperation<TValue>(
        IOperation<TValue> operation,
        out string modelKind,
        out PorcupineOperationInput input)
        where TValue : Contracts.IVersionedValue<TValue>
    {
        if (UnwrapOperation(operation) is IPorcupineHistoryOperationDescriptor<TValue> descriptor)
        {
            modelKind = descriptor.ModelKind;
            input = descriptor.CreatePorcupineInput();
            return true;
        }

        modelKind = string.Empty;
        input = null!;
        return false;
    }

    private static bool TryCreateOutput<TValue>(TValue committedValue, out PorcupineValueOutput output)
        where TValue : Contracts.IVersionedValue<TValue> =>
        committedValue switch
        {
            StringValue stringValue => Return(PorcupineValueOutput.FromString(stringValue), out output),
            SetValue setValue => Return(PorcupineValueOutput.FromSet(setValue), out output),
            _ => Return(null, out output),
        };

    private static IOperation<TValue> UnwrapOperation<TValue>(IOperation<TValue> operation)
    {
        var current = operation;
        while (current is RoutedOperation<TValue, FastCasAddress> routed)
        {
            current = routed.Operation;
        }

        return current;
    }

    private static bool Return(PorcupineValueOutput? output, out PorcupineValueOutput value)
    {
        value = output!;
        return output is not null;
    }

    private readonly record struct PendingOperation(
        int OperationId,
        int ClientId,
        string Client,
        string Proposer);
}

internal static class PorcupineHistorySerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static Task WriteAsync(
        string path,
        PorcupineHistoryArtifact history,
        CancellationToken cancellationToken) =>
        File.WriteAllTextAsync(
            path,
            JsonSerializer.Serialize(history, Options),
            cancellationToken);
}
