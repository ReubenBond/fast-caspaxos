namespace FastCASPaxos.Model;

public enum OperationStatus
{
    Failed,
    /// <summary>
    /// Establishes the current value through an identity accept quorum and returns it unchanged.
    /// </summary>
    NotApplicable,
    Success
}

public interface IOperation<TValue>
{
    (OperationStatus Status, TValue Result) Apply(TValue current);
}

public interface IRoutedOperation<out TRoute>
{
    TRoute Caller { get; }
}

public delegate (OperationStatus Status, TValue Result) Apply<TInput, TValue>(
    TValue existing,
    TInput input);

public sealed class RoutedOperation<TValue, TRoute> : IOperation<TValue>, IRoutedOperation<TRoute>
    where TRoute : notnull
{
    public RoutedOperation(TRoute caller, IOperation<TValue> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Caller = caller;
        Operation = operation;
    }

    public TRoute Caller { get; }

    public IOperation<TValue> Operation { get; }

    (OperationStatus Status, TValue Result) IOperation<TValue>.Apply(TValue current) =>
        Operation.Apply(current);

    public override string ToString() => Operation.ToString() ?? string.Empty;
}

public sealed class Operation<TInput, TValue> : IOperation<TValue>
{
    public required TInput Input { get; init; }

    public required Apply<TInput, TValue> Apply { get; init; }

    public string Name { get; init; } = string.Empty;

    (OperationStatus Status, TValue Result) IOperation<TValue>.Apply(TValue current) =>
        Apply(current, Input);

    public override string ToString() => Name;
}
