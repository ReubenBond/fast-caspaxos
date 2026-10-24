using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Contracts;

public interface IFastCasTransportMessage<TValue>
{
    FastCasAddress From { get; }

    FastCasAddress Destination { get; }
}

public sealed record FastCasProposeRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    int RequestId,
    IOperation<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasForwardProposeRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    FastCasAddress OriginalCaller,
    int RequestId,
    IOperation<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPrepareRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    PrepareRequest Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPreparePromiseMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    PreparePromise<TValue, FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPrepareRejectionMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    PrepareRejection<TValue, FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    AcceptRequest<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptAcceptedMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    AcceptAccepted<FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptRejectedMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    AcceptRejected<FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasProposeResponseMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    int RequestId,
    ProposeResponse<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasForwardProposeResponseMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    FastCasAddress OriginalCaller,
    int RequestId,
    Ballot AcceptedBallot,
    Ballot PreparedBallot,
    ProposeResponse<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasLearnCommittedValueMessage<TValue>(
    FastCasAddress From,
    FastCasAddress Destination,
    Ballot AcceptedBallot,
    TValue Value,
    Ballot PreparedBallot = default)
    : IFastCasTransportMessage<TValue>;
