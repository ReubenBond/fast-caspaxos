//! Combined Fast `CASPaxos` register host.
//!
//! [`ConsensusRegister`] composes the existing proposer and acceptor simulation
//! hosts behind a single per-ordinal abstraction. It keeps the role-specific
//! protocol logic intact while giving callers one object that owns:
//!
//! - proposer/coordinator request handling
//! - acceptor durability and protocol responses
//! - learner updates applied to the local proposer cache
//! - combined message draining for simple local or simulated routing

use fast_caspaxos_core::acceptor::AcceptorState;
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, PreparePromise, PrepareRejection,
    PrepareRequest, ProposeResponse,
};
use fast_caspaxos_core::proposer::{AdaptivePolicy, ValueStatus};

use crate::acceptor_host::{AcceptorHost, AcceptorOutputMessage};
use crate::address::Address;
use crate::proposer_host::{
    ClientProposal, PendingForwardRequest, ProposerAdaptiveState, ProposerHost,
    ProposerOutputMessage,
};
use crate::state_store::{AcceptorStateStore, InMemoryAcceptorStateStore};

/// A combined message emitted by a [`ConsensusRegister`].
#[derive(Debug)]
pub enum ConsensusRegisterOutputMessage<V> {
    Acceptor(AcceptorOutputMessage<V>),
    Proposer(ProposerOutputMessage<V>),
    ForwardRequest(PendingForwardRequest<V>),
}

/// A combined per-server register that owns proposer, acceptor, learner, and
/// request-coordination behavior for one ordinal.
pub struct ConsensusRegister<V, S: AcceptorStateStore<V>> {
    ordinal: u32,
    proposer: ProposerHost<V>,
    acceptor: AcceptorHost<V, S>,
}

impl<V, S> ConsensusRegister<V, S>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
    S: AcceptorStateStore<V>,
{
    /// Creates a new combined register using the supplied acceptor store.
    pub fn new(
        ordinal: u32,
        acceptors: Vec<Address>,
        enable_fast_commit: bool,
        enable_distinguished_leader: bool,
        adaptive_policy: Option<AdaptivePolicy>,
        state_store: S,
    ) -> Self {
        assert!(ordinal > 0, "ordinal must be positive");

        let proposer_address = Address::proposer(ordinal);
        let proposer = ProposerHost::new(
            proposer_address,
            ordinal,
            acceptors,
            enable_fast_commit,
            enable_distinguished_leader,
            adaptive_policy,
        );
        let acceptor = AcceptorHost::new(Address::acceptor(ordinal), state_store);

        Self {
            ordinal,
            proposer,
            acceptor,
        }
    }

    /// Register ordinal shared by the proposer and acceptor roles.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Address of the local proposer/coordinator role.
    pub fn proposer_address(&self) -> Address {
        self.proposer.address()
    }

    /// Address of the local acceptor role.
    pub fn acceptor_address(&self) -> Address {
        self.acceptor.address()
    }

    /// Returns the local proposer ballot.
    pub fn proposer_ballot(&self) -> Ballot {
        self.proposer.ballot()
    }

    /// Returns the local prepared ballot, if any.
    pub fn proposer_prepared_ballot(&self) -> Option<Ballot> {
        self.proposer.prepared_ballot()
    }

    /// Returns the local proposer's current value status.
    pub fn proposer_value_status(&self) -> ValueStatus {
        self.proposer.value_status()
    }

    /// Returns the local proposer's current value view.
    pub fn proposer_current_value(&self) -> &V {
        self.proposer.current_value()
    }

    /// Returns `true` if the local proposer has queued or in-flight work.
    pub fn proposer_has_pending_requests(&self) -> bool {
        self.proposer.has_pending_requests()
    }

    /// Returns the local proposer adaptive state.
    pub fn proposer_adaptive_state(&self) -> ProposerAdaptiveState {
        self.proposer.adaptive_state.clone()
    }

    /// Returns the local acceptor durable state snapshot.
    pub fn acceptor_state(&self) -> AcceptorState<V> {
        self.acceptor.state()
    }

    /// Returns whether both roles are currently running.
    pub fn is_running(&self) -> bool {
        self.proposer.is_running() && self.acceptor.is_running()
    }

    /// Submit a client request to the local coordinator/proposer role.
    pub fn submit_client_request(&mut self, proposal: ClientProposal<V>) {
        self.proposer.submit_client_request(proposal);
    }

    /// Deliver a prepare request to the local acceptor.
    pub fn deliver_prepare(&mut self, from: Address, request: &PrepareRequest) {
        self.acceptor.deliver_prepare(from, request);
    }

    /// Deliver an accept request to the local acceptor.
    pub fn deliver_accept(&mut self, from: Address, request: &AcceptRequest<V>) {
        self.acceptor.deliver_accept(from, request);
    }

    /// Deliver a prepare promise to the local proposer.
    pub fn deliver_prepare_promise(&mut self, msg: PreparePromise<V, Address>) {
        self.proposer.deliver_prepare_promise(msg);
    }

    /// Deliver a prepare rejection to the local proposer.
    pub fn deliver_prepare_rejection(&mut self, msg: PrepareRejection<V, Address>) {
        self.proposer.deliver_prepare_rejection(msg);
    }

    /// Deliver an accept-accepted response to the local proposer.
    pub fn deliver_accept_accepted(&mut self, msg: AcceptAccepted<Address>) {
        self.proposer.deliver_accept_accepted(msg);
    }

    /// Deliver an accept-rejected response to the local proposer.
    pub fn deliver_accept_rejected(&mut self, msg: AcceptRejected<Address>) {
        self.proposer.deliver_accept_rejected(msg);
    }

    /// Deliver a learned committed value to the local learner/proposer view.
    pub fn deliver_learn_committed_value(
        &mut self,
        ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    ) {
        self.proposer
            .deliver_learn_committed_value(ballot, value, prepared_ballot);
    }

    /// Deliver a forwarded request to the local proposer.
    pub fn deliver_forwarded_request(&mut self, req: PendingForwardRequest<V>) {
        self.proposer.deliver_forwarded_request(req);
    }

    /// Deliver a forwarded response to the local proposer.
    pub fn deliver_forward_response(
        &mut self,
        request_id: u32,
        response: ProposeResponse<V>,
        accepted_ballot: Ballot,
        prepared_ballot: Ballot,
    ) {
        self.proposer.deliver_forward_response(
            request_id,
            response,
            accepted_ballot,
            prepared_ballot,
        );
    }

    /// Notify the local proposer that a forwarded request could not be delivered.
    pub fn handle_failed_forward_request(&mut self, req: PendingForwardRequest<V>) {
        self.proposer.handle_failed_forward_request(req);
    }

    /// Trigger a timeout against the local proposer.
    pub fn handle_timeout(&mut self, round: u32) -> bool {
        self.proposer.handle_timeout(round)
    }

    /// Drain all output produced by both roles.
    pub fn drain_messages(&mut self) -> Vec<ConsensusRegisterOutputMessage<V>> {
        let mut messages = Vec::new();
        messages.extend(
            self.acceptor
                .drain_outbox()
                .into_iter()
                .map(ConsensusRegisterOutputMessage::Acceptor),
        );
        messages.extend(
            self.proposer
                .drain_outbox()
                .into_iter()
                .map(ConsensusRegisterOutputMessage::Proposer),
        );
        messages.extend(
            self.proposer
                .drain_forward_requests()
                .into_iter()
                .map(ConsensusRegisterOutputMessage::ForwardRequest),
        );
        messages
    }

    /// Crash both roles in the combined register.
    pub fn crash(&mut self) {
        self.proposer.crash();
        self.acceptor.crash();
    }

    /// Restart both roles in the combined register.
    pub fn restart(&mut self) {
        self.acceptor.restart();
        self.proposer.restart();
    }
}

impl<V> ConsensusRegister<V, InMemoryAcceptorStateStore<V>>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    /// Convenience constructor using the in-memory acceptor state store.
    pub fn new_in_memory(
        ordinal: u32,
        acceptors: Vec<Address>,
        enable_fast_commit: bool,
        enable_distinguished_leader: bool,
        adaptive_policy: Option<AdaptivePolicy>,
    ) -> Self {
        Self::new(
            ordinal,
            acceptors,
            enable_fast_commit,
            enable_distinguished_leader,
            adaptive_policy,
            InMemoryAcceptorStateStore::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::messages::ProposeResponse;
    use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult};

    fn write_op(value: i32) -> Box<dyn Operation<i32>> {
        Box::new(FnOperation::new("write", move |_: &i32| {
            OperationResult::Success(value)
        }))
    }

    fn pump_single_register(
        register: &mut ConsensusRegister<i32, InMemoryAcceptorStateStore<i32>>,
    ) -> (Vec<(u32, ProposeResponse<i32>)>, Vec<(Ballot, i32, Ballot)>) {
        let mut client_responses = Vec::new();
        let mut broadcasts = Vec::new();

        loop {
            let messages = register.drain_messages();
            if messages.is_empty() {
                break;
            }

            for message in messages {
                match message {
                    ConsensusRegisterOutputMessage::Acceptor(
                        AcceptorOutputMessage::PreparePromise { payload, .. },
                    ) => register.deliver_prepare_promise(payload),
                    ConsensusRegisterOutputMessage::Acceptor(
                        AcceptorOutputMessage::PrepareRejection { payload, .. },
                    ) => register.deliver_prepare_rejection(payload),
                    ConsensusRegisterOutputMessage::Acceptor(
                        AcceptorOutputMessage::AcceptAccepted { payload, .. },
                    ) => register.deliver_accept_accepted(payload),
                    ConsensusRegisterOutputMessage::Acceptor(
                        AcceptorOutputMessage::AcceptRejected { payload, .. },
                    ) => register.deliver_accept_rejected(payload),
                    ConsensusRegisterOutputMessage::Proposer(
                        ProposerOutputMessage::SendPrepare(request),
                    ) => register.deliver_prepare(register.proposer_address(), &request),
                    ConsensusRegisterOutputMessage::Proposer(
                        ProposerOutputMessage::SendAccept(request),
                    ) => register.deliver_accept(register.proposer_address(), &request),
                    ConsensusRegisterOutputMessage::Proposer(
                        ProposerOutputMessage::ProposeResponse {
                            request_id,
                            response,
                            ..
                        },
                    ) => client_responses.push((request_id, response)),
                    ConsensusRegisterOutputMessage::Proposer(
                        ProposerOutputMessage::BroadcastCommittedValue {
                            ballot,
                            value,
                            prepared_ballot,
                        },
                    ) => broadcasts.push((ballot, value, prepared_ballot)),
                    ConsensusRegisterOutputMessage::ForwardRequest(req) => {
                        register.deliver_forwarded_request(req);
                    }
                }
            }
        }

        (client_responses, broadcasts)
    }

    #[test]
    fn combined_register_completes_local_request() {
        let acceptors = vec![Address::acceptor(1)];
        let mut register = ConsensusRegister::new_in_memory(1, acceptors, true, false, None);

        register.submit_client_request(ClientProposal {
            from: Address::client(0),
            request_id: 1,
            operation: write_op(42),
        });

        let (responses, broadcasts) = pump_single_register(&mut register);

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].0, 1);
        assert_eq!(responses[0].1.committed_value, 42);
        assert_eq!(register.acceptor_state().accepted_value, 42);
        assert_eq!(*register.proposer_current_value(), 42);
        assert_eq!(register.proposer_value_status(), ValueStatus::Cached);
        assert_eq!(broadcasts.len(), 1);
    }

    #[test]
    fn combined_register_applies_learned_commit_to_peer_view() {
        let mut source =
            ConsensusRegister::new_in_memory(1, vec![Address::acceptor(1)], true, false, None);
        let mut peer =
            ConsensusRegister::new_in_memory(2, vec![Address::acceptor(2)], true, false, None);

        source.submit_client_request(ClientProposal {
            from: Address::client(0),
            request_id: 7,
            operation: write_op(99),
        });

        let (_, broadcasts) = pump_single_register(&mut source);
        let (accepted_ballot, value, prepared_ballot) = broadcasts
            .into_iter()
            .next()
            .expect("expected a committed-value broadcast");

        peer.deliver_learn_committed_value(accepted_ballot, value, prepared_ballot);

        assert_eq!(peer.proposer_ballot(), accepted_ballot);
        assert_eq!(*peer.proposer_current_value(), 99);
        assert_eq!(peer.proposer_prepared_ballot(), Some(prepared_ballot));
    }
}
