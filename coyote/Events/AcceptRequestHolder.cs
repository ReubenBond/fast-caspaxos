using Microsoft.Coyote;
using System;

namespace FastCASPaxos.Events;

public class AcceptRequestHolder : Event
{
    public object Value { get; set; }
    public override string ToString() => Value?.ToString();
    public override int GetHashCode() => HashCode.Combine(Value);
}
