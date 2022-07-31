using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using System.Collections.Generic;

namespace FastCASPaxos.Events;

public class InitProposerEvent : Event
{
    public InitProposerEvent(int id, List<ActorId> acceptors, bool enableFastCommit, bool enableDistinguishedProposer)
    {
        Id = id;
        Acceptors = acceptors;
        EnableFastCommit = enableFastCommit;
        EnableDistinguishedProposer = enableDistinguishedProposer;
    }

    public int Id { get; }
    public List<ActorId> Acceptors { get; }
    public bool EnableDistinguishedProposer { get; }
    public bool EnableFastCommit { get; }
}
