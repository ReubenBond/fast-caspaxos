using Microsoft.Coyote;

namespace FastCASPaxos.Events;

public class TestConfigEventHolder : Event
{
    public object Value { get; set; }
}
