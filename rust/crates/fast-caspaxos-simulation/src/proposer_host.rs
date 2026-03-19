//! Proposer host adapter for the simulation.
//!
//! Bridges [`ProposerEngine`] to the simulation transport by collecting
//! protocol messages into an outbox, managing a FIFO client request queue,
//! and handling crash/restart cycles.

use std::collections::VecDeque;

use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::*;
use fast_caspaxos_core::operation::Operation;
use fast_caspaxos_core::proposer::{ProposerConfig, ProposerEngine, ProposerHandler, ValueStatus};

use crate::address::Address;

// ---------------------------------------------------------------------------
// Output messages
// ---------------------------------------------------------------------------

/// Messages produced by a proposer host.
#[derive(Debug, Clone)]
pub enum ProposerOutputMessage<V> {
    /// Send prepare request to all acceptors.
    SendPrepare(PrepareRequest),
    /// Send accept request to all acceptors.
    SendAccept(AcceptRequest<V>),
    /// Response to a client.
    ProposeResponse {
        to: Address,
        request_id: u32,
        response: ProposeResponse<V>,
    },
    /// Broadcast committed value to other proposers.
    BroadcastCommittedValue {
        ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    },
}

// ---------------------------------------------------------------------------
// Client proposal
// ---------------------------------------------------------------------------

/// A client proposal request queued for processing.
pub struct ClientProposal<V> {
    pub from: Address,
    pub request_id: u32,
    pub operation: Box<dyn Operation<V>>,
}

impl<V> std::fmt::Debug for ClientProposal<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientProposal")
            .field("from", &self.from)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Private handler — routes callbacks into the outbox
// ---------------------------------------------------------------------------

struct HostHandler<'a, V> {
    active_request_from: Option<Address>,
    active_request_id: Option<u32>,
    outbox: &'a mut Vec<ProposerOutputMessage<V>>,
    last_committed_value: &'a mut Option<V>,
}

impl<V: Clone + PartialEq + Default> ProposerHandler<V, Address> for HostHandler<'_, V> {
    fn send_prepare(&mut self, request: PrepareRequest) {
        self.outbox
            .push(ProposerOutputMessage::SendPrepare(request));
    }

    fn send_accept(&mut self, request: AcceptRequest<V>) {
        self.outbox
            .push(ProposerOutputMessage::SendAccept(request));
    }

    fn proposal_completed(&mut self, response: ProposeResponse<V>) {
        if let (Some(to), Some(id)) = (self.active_request_from, self.active_request_id) {
            self.outbox.push(ProposerOutputMessage::ProposeResponse {
                to,
                request_id: id,
                response,
            });
        }
    }

    fn value_committed(&mut self, committed: CommittedValue<V>) {
        *self.last_committed_value = Some(committed.value);
    }

    fn cached_value_changed(&mut self, _value: &V) {}
    fn pending_accept_started(&mut self, _value: &V) {}
    fn pending_accept_committed(&mut self, _value: &V) {}
    fn pending_accept_cleared(&mut self, _value: &V) {}
}

// ---------------------------------------------------------------------------
// ProposerHost
// ---------------------------------------------------------------------------

/// Wraps a [`ProposerEngine`] with a client request queue and message routing.
///
/// Protocol messages are collected in an outbox. Client proposals are queued
/// in FIFO order and dispatched one at a time to the engine.
pub struct ProposerHost<V> {
    address: Address,
    engine: ProposerEngine<V, Address>,
    acceptors: Vec<Address>,
    is_running: bool,
    request_queue: VecDeque<ClientProposal<V>>,
    active_request_id: Option<u32>,
    active_request_from: Option<Address>,
    outbox: Vec<ProposerOutputMessage<V>>,
    enable_fast_commit: bool,
    enable_distinguished_leader: bool,
    proposer_id: u32,
}

impl<V> ProposerHost<V>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    /// Creates a new proposer host.
    pub fn new(
        address: Address,
        proposer_id: u32,
        acceptors: Vec<Address>,
        enable_fast_commit: bool,
        enable_distinguished_leader: bool,
    ) -> Self {
        let config = ProposerConfig {
            proposer_id,
            enable_fast_commit,
            enable_distinguished_leader,
        };
        let engine = ProposerEngine::new(config, acceptors.clone());
        Self {
            address,
            engine,
            acceptors,
            is_running: true,
            request_queue: VecDeque::new(),
            active_request_id: None,
            active_request_from: None,
            outbox: Vec::new(),
            enable_fast_commit,
            enable_distinguished_leader,
            proposer_id,
        }
    }

    // -- Accessors ----------------------------------------------------------

    /// Returns the address of this proposer.
    pub fn address(&self) -> Address {
        self.address
    }

    /// Returns `true` if the host is running (not crashed).
    pub fn is_running(&self) -> bool {
        self.is_running
    }

    /// Returns the engine's current ballot.
    pub fn ballot(&self) -> Ballot {
        self.engine.ballot()
    }

    /// Returns the engine's prepared (piggybacked) ballot, if any.
    pub fn prepared_ballot(&self) -> Option<Ballot> {
        self.engine.prepared_ballot()
    }

    /// Returns `true` if there are queued or in-flight requests.
    pub fn has_pending_requests(&self) -> bool {
        self.active_request_id.is_some() || !self.request_queue.is_empty()
    }

    /// Returns the engine's current value status.
    pub fn value_status(&self) -> ValueStatus {
        self.engine.value_status()
    }

    /// Returns a reference to the engine's current value.
    pub fn current_value(&self) -> &V {
        self.engine.current_value()
    }

    // -- Client requests ----------------------------------------------------

    /// Submits a client proposal. If no operation is in flight, starts
    /// immediately; otherwise queues it.
    pub fn submit_client_request(&mut self, proposal: ClientProposal<V>) {
        if !self.is_running {
            return;
        }
        if self.active_request_id.is_some() {
            self.request_queue.push_back(proposal);
        } else {
            self.start_proposal(proposal);
        }
    }

    // -- Deliver protocol messages ------------------------------------------

    /// Delivers a prepare promise from an acceptor.
    pub fn deliver_prepare_promise(&mut self, msg: PreparePromise<V, Address>) {
        if !self.is_running {
            return;
        }
        self.run_with_handler(|engine, handler| {
            engine.handle_prepare_promised(msg, handler);
        });
    }

    /// Delivers a prepare rejection from an acceptor.
    pub fn deliver_prepare_rejection(&mut self, msg: PrepareRejection<V, Address>) {
        if !self.is_running {
            return;
        }
        self.run_with_handler(|engine, handler| {
            engine.handle_prepare_rejected(msg, handler);
        });
    }

    /// Delivers an accept-accepted from an acceptor.
    pub fn deliver_accept_accepted(&mut self, msg: AcceptAccepted<Address>) {
        if !self.is_running {
            return;
        }
        self.run_with_handler(|engine, handler| {
            engine.handle_accept_accepted(msg, handler);
        });
    }

    /// Delivers an accept-rejected from an acceptor.
    pub fn deliver_accept_rejected(&mut self, msg: AcceptRejected<Address>) {
        if !self.is_running {
            return;
        }
        self.run_with_handler(|engine, handler| {
            engine.handle_accept_rejected(msg, handler);
        });
    }

    /// Learns a committed value from another proposer's broadcast.
    pub fn deliver_learn_committed_value(
        &mut self,
        ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    ) {
        if !self.is_running {
            return;
        }
        self.run_with_handler(|engine, handler| {
            engine.learn_committed_value(ballot, value, prepared_ballot, handler);
        });
    }

    /// Handles a response timeout for the given round.
    ///
    /// Returns `true` if a retry was triggered, `false` if the timeout was stale.
    pub fn handle_timeout(&mut self, round: u32) -> bool {
        if !self.is_running {
            return false;
        }
        let mut retried = false;
        self.run_with_handler(|engine, handler| {
            retried = engine.handle_response_timeout(round, handler);
        });
        retried
    }

    // -- Outbox -------------------------------------------------------------

    /// Takes all accumulated output messages, leaving the outbox empty.
    pub fn drain_outbox(&mut self) -> Vec<ProposerOutputMessage<V>> {
        std::mem::take(&mut self.outbox)
    }

    // -- Crash / restart ----------------------------------------------------

    /// Simulates a crash — stops processing and clears all in-flight state.
    pub fn crash(&mut self) {
        self.is_running = false;
        self.outbox.clear();
        self.request_queue.clear();
        self.active_request_id = None;
        self.active_request_from = None;
    }

    /// Restarts after a crash by recreating the engine from scratch.
    pub fn restart(&mut self) {
        let config = ProposerConfig {
            proposer_id: self.proposer_id,
            enable_fast_commit: self.enable_fast_commit,
            enable_distinguished_leader: self.enable_distinguished_leader,
        };
        self.engine = ProposerEngine::new(config, self.acceptors.clone());
        self.is_running = true;
        self.outbox.clear();
        self.request_queue.clear();
        self.active_request_id = None;
        self.active_request_from = None;
    }

    // -- Private helpers ----------------------------------------------------

    /// Runs a closure with a handler that borrows disjoint fields of self.
    /// After the closure, emits any committed value broadcast and advances
    /// the request queue if the current operation completed.
    fn run_with_handler<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ProposerEngine<V, Address>, &mut HostHandler<'_, V>),
    {
        let mut committed_value: Option<V> = None;

        {
            let mut handler = HostHandler {
                active_request_from: self.active_request_from,
                active_request_id: self.active_request_id,
                outbox: &mut self.outbox,
                last_committed_value: &mut committed_value,
            };
            f(&mut self.engine, &mut handler);
        }

        if let Some(value) = committed_value {
            let ballot = self.engine.ballot();
            let prepared_ballot = self.engine.prepared_ballot().unwrap_or(Ballot::ZERO);
            self.outbox
                .push(ProposerOutputMessage::BroadcastCommittedValue {
                    ballot,
                    value,
                    prepared_ballot,
                });
        }

        // If the engine finished the current operation, advance the queue.
        if !self.engine.has_pending_proposal() && self.active_request_id.is_some() {
            self.active_request_id = None;
            self.active_request_from = None;
            self.try_advance_queue();
        }
    }

    fn start_proposal(&mut self, proposal: ClientProposal<V>) {
        self.active_request_id = Some(proposal.request_id);
        self.active_request_from = Some(proposal.from);

        let operation = proposal.operation;
        self.run_with_handler(|engine, handler| {
            engine.start_proposal(operation, handler);
        });
    }

    fn try_advance_queue(&mut self) {
        if self.active_request_id.is_some() {
            return;
        }
        if let Some(next) = self.request_queue.pop_front() {
            self.start_proposal(next);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::ballot::Ballot;
    use fast_caspaxos_core::operation::{FnOperation, OperationResult, OperationStatus};

    fn make_acceptors(count: u32) -> Vec<Address> {
        (0..count).map(Address::acceptor).collect()
    }

    fn make_host(proposer_id: u32) -> ProposerHost<Option<i32>> {
        ProposerHost::new(
            Address::proposer(proposer_id),
            proposer_id,
            make_acceptors(3),
            true,
            false,
        )
    }

    fn set_op(value: i32) -> Box<dyn Operation<Option<i32>>> {
        Box::new(FnOperation::new(format!("set({value})"), move |_: &Option<i32>| {
            OperationResult::new(OperationStatus::Success, Some(value))
        }))
    }

    fn client_proposal(id: u32, value: i32) -> ClientProposal<Option<i32>> {
        ClientProposal {
            from: Address::client(0),
            request_id: id,
            operation: set_op(value),
        }
    }

    #[test]
    fn submit_proposal_produces_message() {
        let mut host = make_host(1);
        host.submit_client_request(client_proposal(1, 42));

        let msgs = host.drain_outbox();
        // Should produce either SendPrepare or SendAccept (fast commit).
        assert!(
            !msgs.is_empty(),
            "Expected at least one output message after submit"
        );
        let has_protocol_msg = msgs.iter().any(|m| {
            matches!(
                m,
                ProposerOutputMessage::SendPrepare(_)
                    | ProposerOutputMessage::SendAccept(_)
            )
        });
        assert!(has_protocol_msg);
    }

    #[test]
    fn second_proposal_queues_behind_first() {
        let mut host = make_host(1);
        host.submit_client_request(client_proposal(1, 10));
        host.drain_outbox();

        host.submit_client_request(client_proposal(2, 20));
        // No new protocol messages should appear — second is queued.
        let msgs = host.drain_outbox();
        assert!(
            msgs.is_empty(),
            "Second proposal should be queued, not started"
        );
        assert!(host.has_pending_requests());
    }

    #[test]
    fn crash_clears_pending_requests() {
        let mut host = make_host(1);
        host.submit_client_request(client_proposal(1, 42));
        host.drain_outbox();

        host.crash();
        assert!(!host.is_running());
        assert!(!host.has_pending_requests());
    }

    #[test]
    fn restart_resets_engine() {
        let mut host = make_host(1);
        host.submit_client_request(client_proposal(1, 42));
        host.drain_outbox();

        host.crash();
        host.restart();

        assert!(host.is_running());
        assert!(!host.has_pending_requests());
        assert_eq!(host.ballot(), Ballot::ZERO);
    }

    #[test]
    fn learn_committed_value_updates_state() {
        let mut host = make_host(1);

        let ballot = Ballot::initial_classic(2);
        let prepared = Ballot::new(2, 0);
        host.deliver_learn_committed_value(ballot, Some(99), prepared);

        assert_eq!(host.value_status(), ValueStatus::Cached);
        assert_eq!(*host.current_value(), Some(99));
    }

    #[test]
    fn not_running_ignores_messages() {
        let mut host = make_host(1);
        host.crash();

        host.submit_client_request(client_proposal(1, 42));
        assert!(host.drain_outbox().is_empty());

        host.deliver_prepare_promise(PreparePromise {
            round: 1,
            acceptor: Address::acceptor(0),
            accepted_ballot: Ballot::ZERO,
            accepted_value: None,
        });
        assert!(host.drain_outbox().is_empty());
    }
}
