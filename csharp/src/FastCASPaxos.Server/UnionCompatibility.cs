#if !NET11_0_OR_GREATER
namespace System.Runtime.CompilerServices;

// C# union metadata for the .NET 10 target; .NET 11 supplies these types.
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
internal sealed class UnionAttribute : Attribute;

internal interface IUnion
{
    object? Value { get; }
}
#endif
