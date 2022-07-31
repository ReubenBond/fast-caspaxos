namespace FastCASPaxos.Model
{
    public enum OperationStatus
    {
        Failed,
        NotApplicable,
        Success
    }

    public interface IOperation<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
    {
        (OperationStatus Status, TVersionedValue Result) Apply(TVersionedValue current);
    }

    public delegate (OperationStatus Status, TVersionedValue Result) Apply<TInput, TValue, TVersionedValue>(TVersionedValue existing, TInput input) where TVersionedValue : IVersionedValue<TValue, TVersionedValue>;

    public class Operation<TInput, TValue, TVersionedValue> : IOperation<TValue, TVersionedValue> where TVersionedValue : IVersionedValue<TValue, TVersionedValue>
    {
        public TInput Input { get; set; }
        public Apply<TInput, TValue, TVersionedValue> Apply { get; set; }
        public string Name { get; set; }

        (OperationStatus Status, TVersionedValue Result) IOperation<TValue, TVersionedValue>.Apply(TVersionedValue current)
        {
            return Apply(current, Input);
        }

        public override string ToString() => Name;
    }
}
