﻿using Microsoft.Coyote;
using System;

namespace FastCASPaxos.Events;

public class PrepareResponseHolder : Event
{
    public object Value { get; set; }
    public override string ToString() => Value?.ToString();
    public override int GetHashCode() => HashCode.Combine(Value);
}