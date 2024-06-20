using FastCASPaxos.Model;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Simulation.Scenarios;

public readonly struct StringValue(int version, string value) : IVersionedValue<StringValue>
{
    public string Value { get; } = value;

    public int Version { get; } = version;

    public override bool Equals(object? obj) => obj is StringValue other && Equals(other);

    public bool Equals(StringValue other) => Version == other.Version && string.Equals(Value, other.Value, StringComparison.Ordinal);

    public override int GetHashCode() => HashCode.Combine(Version, Value);

    public bool IsValidSuccessorTo(StringValue predecessor) =>
        predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value, StringComparison.Ordinal));

    public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"{Value}@{Version}")})";

    public static bool operator ==(StringValue left, StringValue right) => left.Equals(right);

    public static bool operator !=(StringValue left, StringValue right) => !(left == right);
}

public static class StringScenarioOperations
{
    public static IReadOnlyList<IOperation<StringValue>> AppendSequence(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        List<IOperation<StringValue>> operations = [];
        var version = 0;
        foreach (var character in value)
        {
            operations.Add(AppendAtVersion(++version, character.ToString()));
        }

        return operations;
    }

    public static IOperation<StringValue> AppendAtVersion(int version, string segment) =>
        new AppendAtVersionStringScenarioOperation(version, segment);

    public static IOperation<StringValue> AppendCharacter(char character) =>
        new AppendCharacterStringScenarioOperation(character);

    public static IOperation<StringValue> Read() =>
        new ReadStringScenarioOperation();
}

public readonly struct SetValue(int version, HashSet<string> value) : IVersionedValue<SetValue>
{
    public HashSet<string> Value { get; } = value;

    public int Version { get; } = version;

    public override bool Equals(object? obj) => obj is SetValue other && Equals(other);

    public bool Equals(SetValue other)
    {
        if (Version != other.Version)
        {
            return false;
        }

        if (Value is null ^ other.Value is null)
        {
            return false;
        }

        if (Value is null)
        {
            return true;
        }

        return other.Value is not null && Value.SetEquals(other.Value);
    }

    public override int GetHashCode() => HashCode.Combine(Version, Value);

    public bool IsValidSuccessorTo(SetValue predecessor)
    {
        if (predecessor.Value is null)
        {
            return true;
        }

        return Value.IsProperSupersetOf(predecessor.Value);
    }

    public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"[{string.Join(", ", (Value ?? []).OrderBy(s => s))}]@{Version}")})";

    public static bool operator ==(SetValue left, SetValue right) => left.Equals(right);

    public static bool operator !=(SetValue left, SetValue right) => !(left == right);
}

public readonly struct SetOperation(bool add, string value)
{
    public bool Add { get; } = add;

    public string Value { get; } = value;
}

public static class SetScenarioOperations
{
    public static IOperation<SetValue> Add(string value) =>
        new AddSetScenarioOperation(value);

    public static IOperation<SetValue> Read() =>
        new ReadSetScenarioOperation();

    internal static (OperationStatus Status, SetValue Result) Apply(SetValue current, SetOperation input)
    {
        if (current.Value is null)
        {
            current = new SetValue(current.Version, new(StringComparer.Ordinal));
        }

        if (input.Add)
        {
            if (!current.Value.Contains(input.Value))
            {
                var result = new SetValue(current.Version + 1, new HashSet<string>(current.Value, StringComparer.Ordinal)
                {
                    input.Value,
                });

                return (OperationStatus.Success, result);
            }

            return (OperationStatus.NotApplicable, current);
        }

        return (OperationStatus.NotApplicable, current);
    }
}

file abstract class StringScenarioOperation :
    IOperation<StringValue>,
    IPorcupineHistoryOperationDescriptor<StringValue>
{
    public string ModelKind => PorcupineModelKinds.String;

    public abstract (OperationStatus Status, StringValue Result) Apply(StringValue current);

    public abstract PorcupineOperationInput CreatePorcupineInput();
}

file sealed class AppendAtVersionStringScenarioOperation(int version, string segment)
    : StringScenarioOperation
{
    public override (OperationStatus Status, StringValue Result) Apply(StringValue current)
    {
        var input = new StringValue(version, segment);
        if (input.Version == current.Version + 1)
        {
            var newValue = (current.Value ?? string.Empty) + input.Value;
            return (OperationStatus.Success, new StringValue(input.Version, newValue));
        }

        if (current.Version >= input.Version)
        {
            return (OperationStatus.NotApplicable, current);
        }

        return (OperationStatus.Failed, current);
    }

    public override PorcupineOperationInput CreatePorcupineInput() =>
        new(
            Kind: "append_at_version",
            ExpectedVersion: version,
            Value: segment);

    public override string ToString() => $"Append '{segment}' at version {version}";
}

file sealed class AppendCharacterStringScenarioOperation(char character)
    : StringScenarioOperation
{
    public override (OperationStatus Status, StringValue Result) Apply(StringValue current)
    {
        var newValue = (current.Value ?? string.Empty) + character;
        return (OperationStatus.Success, new StringValue(current.Version + 1, newValue));
    }

    public override PorcupineOperationInput CreatePorcupineInput() =>
        new(
            Kind: "append_character",
            ExpectedVersion: null,
            Value: character.ToString());

    public override string ToString() => $"Append '{character}'";
}

file sealed class ReadStringScenarioOperation
    : StringScenarioOperation
{
    public override (OperationStatus Status, StringValue Result) Apply(StringValue current) =>
        (OperationStatus.NotApplicable, current);

    public override PorcupineOperationInput CreatePorcupineInput() =>
        new(
            Kind: "read",
            ExpectedVersion: null,
            Value: null);

    public override string ToString() => "Read current value";
}

file abstract class SetScenarioOperation :
    IOperation<SetValue>,
    IPorcupineHistoryOperationDescriptor<SetValue>
{
    public string ModelKind => PorcupineModelKinds.Set;

    public abstract (OperationStatus Status, SetValue Result) Apply(SetValue current);

    public abstract PorcupineOperationInput CreatePorcupineInput();
}

file sealed class AddSetScenarioOperation(string value)
    : SetScenarioOperation
{
    public override (OperationStatus Status, SetValue Result) Apply(SetValue current) =>
        SetScenarioOperations.Apply(
            current,
            new SetOperation(add: true, value));

    public override PorcupineOperationInput CreatePorcupineInput() =>
        new(
            Kind: "add",
            ExpectedVersion: null,
            Value: value);

    public override string ToString() => $"Add {value}";
}

file sealed class ReadSetScenarioOperation
    : SetScenarioOperation
{
    public override (OperationStatus Status, SetValue Result) Apply(SetValue current) =>
        (OperationStatus.NotApplicable, current);

    public override PorcupineOperationInput CreatePorcupineInput() =>
        new(
            Kind: "read",
            ExpectedVersion: null,
            Value: null);

    public override string ToString() => "Read current set";
}
