namespace FastCASPaxos.Protocol;

public enum ProposerValueStatus : byte
    {
        None = 0,
        Cached = 1,
        PendingAccept = 2,
    }

