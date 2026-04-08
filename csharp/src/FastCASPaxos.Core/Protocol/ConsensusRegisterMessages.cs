using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

public readonly record struct ConsensusRegisterClientRequest<TValue, TRoute>(
    TRoute From,
    int RequestId,
    IOperation<TValue> Operation)
    where TRoute : notnull;

public readonly record struct ConsensusRegisterForwardedRequest<TValue, TRoute>(
    TRoute From,
    TRoute OriginalCaller,
    int RequestId,
    IOperation<TValue> Operation)
    where TRoute : notnull;

public abstract record ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record PrepareRequestOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    PrepareRequest Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record AcceptRequestOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    AcceptRequest<TValue> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record PreparePromiseOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    PreparePromise<TValue, TRoute> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record PrepareRejectionOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    PrepareRejection<TValue, TRoute> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record AcceptAcceptedOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    AcceptAccepted<TRoute> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record AcceptRejectedOutputMessage<TValue, TRoute>(
    TRoute From,
    TRoute To,
    AcceptRejected<TRoute> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record ProposeResponseOutputMessage<TValue, TRoute>(
    TRoute To,
    int RequestId,
    ProposeResponse<TValue> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record ForwardProposeRequestOutputMessage<TValue, TRoute>(
    int LeaderProposerId,
    TRoute OriginalCaller,
    int RequestId,
    IOperation<TValue> Operation)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record ForwardProposeResponseOutputMessage<TValue, TRoute>(
    TRoute To,
    TRoute OriginalCaller,
    int RequestId,
    Ballot AcceptedBallot,
    Ballot PreparedBallot,
    ProposeResponse<TValue> Payload)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;

public sealed record LearnCommittedValueOutputMessage<TValue, TRoute>(
    Ballot AcceptedBallot,
    TValue Value,
    Ballot PreparedBallot)
    : ConsensusRegisterOutputMessage<TValue, TRoute>
    where TRoute : notnull;
