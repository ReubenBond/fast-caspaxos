using Microsoft.Coyote;
using System;

namespace FastCASPaxos.Events;

public class ProposeResponseHolder : Event
{
    public object Value { get; set; }
    public override string ToString() => Value?.ToString();
    public override int GetHashCode() => HashCode.Combine(Value);
}
