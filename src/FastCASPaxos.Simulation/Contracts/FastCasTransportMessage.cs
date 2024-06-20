using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Contracts;

public interface IFastCasTransportMessage<TValue>
{
    FastCasAddress From { get; }

    FastCasAddress To { get; }
}

public sealed record FastCasProposeRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    IOperation<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPrepareRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    PrepareRequest Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPreparePromiseMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    PreparePromise<TValue, FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasPrepareRejectionMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    PrepareRejection<TValue, FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptRequestMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    AcceptRequest<TValue> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptAcceptedMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    AcceptAccepted<FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasAcceptRejectedMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    AcceptRejected<FastCasAddress> Payload)
    : IFastCasTransportMessage<TValue>;

public sealed record FastCasProposeResponseMessage<TValue>(
    FastCasAddress From,
    FastCasAddress To,
    ProposeResponse<TValue> Payload)
    : IFastCasTransportMessage<TValue>;
