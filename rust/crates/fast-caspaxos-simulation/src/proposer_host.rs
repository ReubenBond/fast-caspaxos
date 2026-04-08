//! Proposer host adapter for the simulation.
//!
//! Bridges [`ProposerEngine`] to the simulation transport by collecting
//! protocol messages into an outbox, managing a FIFO client request queue,
//! and handling crash/restart cycles.
//!
//! ## Adaptive mode
//!
//! When constructed with an [`AdaptivePolicy`] the host maintains local
//! [`ProposerAdaptiveState`] that tracks conflict debt, the current
//! [`AdaptiveMode`], a leader hint, and forward-attempt counters.
//!
//! In [`AdaptiveMode::Classic`] with a valid leader hint the host *forwards*
//! incoming client requests to the hinted leader instead of running them
//! locally.  After [`AdaptivePolicy::max_forward_attempts`] unacknowledged
//! forwards it falls back to a local classic takeover.
//!
//! ## Proposer-to-proposer forwarding
//!
//! Forwarded requests are drained separately via [`ProposerHost::drain_forward_requests`]
//! (called by the cluster each step).  When the leader completes a forwarded
//! request it emits a [`ProposerOutputMessage::ProposeResponse`] addressed to
//! the forwarder; the cluster detects this (recipient is a proposer address)
//! and routes it via [`ProposerHost::deliver_forward_response`], which looks
//! up the original client and relays the final response.

use std::collections::{HashMap, VecDeque};

use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, CommittedValue, PreparePromise,
    PrepareRejection, PrepareRequest, ProposeResponse,
};
use fast_caspaxos_core::operation::Operation;
use fast_caspaxos_core::proposer::{
    AdaptiveMode, AdaptivePolicy, ProposerConfig, ProposerEngine, ProposerHandler, ValueStatus,
};

use crate::address::Address;

// ---------------------------------------------------------------------------
// Adaptive state
// ---------------------------------------------------------------------------

/// Per-proposer adaptive state: tracks the current mode, conflict debt,
/// leader hint, and forward-attempt counters.
#[derive(Debug, Clone)]
pub struct ProposerAdaptiveState {
    pub mode: AdaptiveMode,
    pub conflict_debt: u32,
    /// The proposer we believe is the current classic leader.
    pub leader_hint: Option<Address>,
    /// Consecutive forwarding attempts that have not yet received a response.
    pub forward_attempts: u32,
}

impl Default for ProposerAdaptiveState {
    fn default() -> Self {
        Self {
            mode: AdaptiveMode::Fast,
            conflict_debt: 0,
            leader_hint: None,
            forward_attempts: 0,
        }
    }
}

impl ProposerAdaptiveState {
    /// Apply a contention event: increase debt, update leader hint, maybe
    /// switch to classic mode.
    pub fn apply_contention(&mut self, conflict_ballot: Ballot, policy: &AdaptivePolicy) {
        self.conflict_debt = self
            .conflict_debt
            .saturating_add(policy.conflict_debt_per_event);
        if conflict_ballot.is_classic_round() {
            self.leader_hint = Some(Address::proposer(conflict_ballot.proposer));
        }
        if self.conflict_debt >= policy.classic_entry_threshold && self.mode == AdaptiveMode::Fast {
            self.mode = AdaptiveMode::Classic;
        }
    }

    /// Apply a calm classic commit: drain debt and optionally reopen fast path.
    /// Only effective when the proposer that committed *is* the leader (this
    /// proposer's id matches the ballot's proposer).
    pub fn apply_calm_commit(&mut self, ballot: Ballot, policy: &AdaptivePolicy, my_id: u32) {
        if ballot.proposer != my_id {
            return;
        }
        self.conflict_debt = self
            .conflict_debt
            .saturating_sub(policy.calm_drain_per_commit);
        self.forward_attempts = 0;
        if self.conflict_debt <= policy.fast_probe_threshold {
            self.mode = AdaptiveMode::Fast;
            self.leader_hint = None;
        }
    }

    /// Learned a fast committed ballot: the fast path is healthy.
    /// If debt has drained to zero, reopen fast mode.
    pub fn apply_fast_ballot_learned(&mut self) {
        if self.mode == AdaptiveMode::Classic && self.conflict_debt == 0 {
            self.mode = AdaptiveMode::Fast;
            self.leader_hint = None;
        }
    }

    /// Learned a classic committed ballot from another proposer: update hint.
    pub fn apply_classic_ballot_learned(
        &mut self,
        ballot: Ballot,
        policy: &AdaptivePolicy,
        my_id: u32,
    ) {
        if ballot.is_classic_round() && ballot.proposer != my_id {
            self.leader_hint = Some(Address::proposer(ballot.proposer));
            // Learning someone else's classic commit counts as soft contention
            // if we are currently trying to use the fast path.
            if self.mode == AdaptiveMode::Fast {
                self.conflict_debt = self
                    .conflict_debt
                    .saturating_add(policy.conflict_debt_per_event);
                if self.conflict_debt >= policy.classic_entry_threshold {
                    self.mode = AdaptiveMode::Classic;
                }
            }
        }
    }

    /// Returns the leader address we should forward to, or `None` if we
    /// should handle the request locally.
    pub fn should_forward(&self, my_id: u32, policy: &AdaptivePolicy) -> Option<Address> {
        if self.mode != AdaptiveMode::Classic {
            return None;
        }
        let hint = self.leader_hint?;
        if hint == Address::proposer(my_id) {
            return None; // We ARE the leader.
        }
        if self.forward_attempts >= policy.max_forward_attempts {
            return None; // Exceeded retry budget → local takeover.
        }
        Some(hint)
    }

    /// Note that a forward request was sent.
    pub fn note_forward_attempt(&mut self) {
        self.forward_attempts += 1;
    }

    /// A forwarded request received a successful response.
    pub fn forward_succeeded(&mut self) {
        self.forward_attempts = 0;
    }
}

// ---------------------------------------------------------------------------
// Forward request (proposer → leader)
// ---------------------------------------------------------------------------

/// A client request that should be forwarded to the current classic leader.
///
/// The cluster routes this to the leader host; the leader processes it and
/// returns a response addressed to `forwarded_from` (this proposer).  The
/// cluster then calls [`ProposerHost::deliver_forward_response`] on this host,
/// which relays to `original_client`.
pub struct PendingForwardRequest<V> {
    /// Leader proposer that should receive and process this request.
    pub to: Address,
    /// This proposer's address — the leader responds here.
    pub forwarded_from: Address,
    /// Original client address (used for the final relay).
    pub original_client: Address,
    pub request_id: u32,
    pub operation: Box<dyn Operation<V>>,
}

impl<V> std::fmt::Debug for PendingForwardRequest<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingForwardRequest")
            .field("to", &self.to)
            .field("forwarded_from", &self.forwarded_from)
            .field("original_client", &self.original_client)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

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
    /// Response to a client (or forwarder).
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
    adaptive_state: &'a mut ProposerAdaptiveState,
    adaptive_policy: Option<&'a AdaptivePolicy>,
    proposer_id: u32,
}

impl<V: Clone + PartialEq + Default> ProposerHandler<V, Address> for HostHandler<'_, V> {
    fn send_prepare(&mut self, request: PrepareRequest) {
        self.outbox
            .push(ProposerOutputMessage::SendPrepare(request));
    }

    fn send_accept(&mut self, request: AcceptRequest<V>) {
        self.outbox.push(ProposerOutputMessage::SendAccept(request));
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

    fn contention_detected(&mut self, conflict_ballot: Ballot) {
        if let Some(policy) = self.adaptive_policy {
            self.adaptive_state
                .apply_contention(conflict_ballot, policy);
        }
    }

    fn calm_classic_committed(&mut self, ballot: Ballot) {
        if let Some(policy) = self.adaptive_policy {
            self.adaptive_state
                .apply_calm_commit(ballot, policy, self.proposer_id);
        }
    }
}

// ---------------------------------------------------------------------------
// ProposerHost
// ---------------------------------------------------------------------------

/// Wraps a [`ProposerEngine`] with a client request queue and message routing.
///
/// Protocol messages are collected in an outbox. Client proposals are queued
/// in FIFO order and dispatched one at a time to the engine.
///
/// When constructed with an [`AdaptivePolicy`] the host automatically manages
/// fast/classic mode selection and proposer-to-proposer forwarding.
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
    /// Optional adaptive policy; `None` preserves legacy static behavior.
    pub adaptive_policy: Option<AdaptivePolicy>,
    /// Mutable adaptive state, managed by this host.
    pub adaptive_state: ProposerAdaptiveState,
    /// Forward requests pending delivery to the leader.
    pending_forward_requests: Vec<PendingForwardRequest<V>>,
    /// Maps in-flight forwarded request_id → original_client address.
    forward_return_map: HashMap<u32, Address>,
}

impl<V> ProposerHost<V>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    /// Creates a new proposer host with the static (legacy) configuration.
    pub fn new(
        address: Address,
        proposer_id: u32,
        acceptors: Vec<Address>,
        enable_fast_commit: bool,
        enable_distinguished_leader: bool,
        adaptive_policy: Option<AdaptivePolicy>,
    ) -> Self {
        let config = ProposerConfig {
            proposer_id,
            enable_fast_commit,
            enable_distinguished_leader,
            ballot_seed: Ballot::ZERO,
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
            adaptive_policy,
            adaptive_state: ProposerAdaptiveState::default(),
            pending_forward_requests: Vec::new(),
            forward_return_map: HashMap::new(),
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
        self.active_request_id.is_some()
            || !self.request_queue.is_empty()
            || !self.forward_return_map.is_empty()
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

    /// Submits a client proposal.
    ///
    /// In adaptive mode with a leader hint and classic stickiness active the
    /// request is forwarded to the leader instead of being processed locally.
    /// After [`AdaptivePolicy::max_forward_attempts`] unacknowledged forwards
    /// the proposer attempts a local classic takeover.
    pub fn submit_client_request(&mut self, proposal: ClientProposal<V>) {
        if !self.is_running {
            return;
        }

        // Adaptive forwarding: only when there is no in-flight local operation.
        if let Some(policy) = &self.adaptive_policy {
            if self.active_request_id.is_none() {
                if let Some(leader) = self.adaptive_state.should_forward(self.proposer_id, policy) {
                    let request_id = proposal.request_id;
                    let original_client = proposal.from;
                    self.forward_return_map.insert(request_id, original_client);
                    self.adaptive_state.note_forward_attempt();
                    self.pending_forward_requests.push(PendingForwardRequest {
                        to: leader,
                        forwarded_from: self.address,
                        original_client,
                        request_id,
                        operation: proposal.operation,
                    });
                    return;
                }
            }
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
    ///
    /// In adaptive mode, updates the leader hint and potentially reopens the
    /// fast path if the committed ballot is a fast ballot.
    pub fn deliver_learn_committed_value(
        &mut self,
        ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    ) {
        if !self.is_running {
            return;
        }

        // Update adaptive state based on the ballot type.
        if let Some(policy) = self.adaptive_policy.clone() {
            if ballot.is_fast_round() {
                self.adaptive_state.apply_fast_ballot_learned();
            } else {
                self.adaptive_state
                    .apply_classic_ballot_learned(ballot, &policy, self.proposer_id);
            }
        }

        self.run_with_handler(|engine, handler| {
            engine.learn_committed_value(ballot, value, prepared_ballot, handler);
        });
    }

    /// Delivers a forwarded request from another proposer.
    ///
    /// The leader (this host) processes the request as a regular local proposal.
    /// The response is addressed to `req.forwarded_from` so the cluster can
    /// route it back to the forwarder.
    pub fn deliver_forwarded_request(&mut self, req: PendingForwardRequest<V>) {
        if !self.is_running {
            return;
        }
        // Run the forwarded operation as if it came from the forwarder proposer.
        // active_request_from = forwarder address → response goes back to forwarder.
        let proposal = ClientProposal {
            from: req.forwarded_from,
            request_id: req.request_id,
            operation: req.operation,
        };
        if self.active_request_id.is_some() {
            self.request_queue.push_back(proposal);
        } else {
            self.start_proposal(proposal);
        }
    }

    /// Receives a response for a previously forwarded request.
    ///
    /// Looks up the original client, emits a [`ProposerOutputMessage::ProposeResponse`]
    /// addressed to them, and resets the forward-attempt counter.
    pub fn deliver_forward_response(
        &mut self,
        request_id: u32,
        response: ProposeResponse<V>,
        accepted_ballot: Ballot,
        prepared_ballot: Ballot,
    ) {
        if !self.is_running {
            return;
        }
        if self.adaptive_policy.is_some() {
            self.adaptive_state.forward_succeeded();
        }
        if !accepted_ballot.is_zero() {
            self.deliver_learn_committed_value(
                accepted_ballot,
                response.committed_value.clone(),
                prepared_ballot,
            );
        }
        if let Some(original_client) = self.forward_return_map.remove(&request_id) {
            self.outbox.push(ProposerOutputMessage::ProposeResponse {
                to: original_client,
                request_id,
                response,
            });
        }
        // Advance the queue in case other requests were waiting.
        self.try_advance_queue();
    }

    /// Called by the cluster when a forwarded request could not be delivered
    /// to the hinted leader (drop, partition, or crashed leader).
    ///
    /// The same request stays live: retry forwarding while budget remains,
    /// otherwise switch to local classic takeover and execute it here.
    pub fn handle_failed_forward_request(&mut self, mut req: PendingForwardRequest<V>) {
        if !self.is_running {
            return;
        }

        if let Some(policy) = &self.adaptive_policy {
            if let Some(leader) = self.adaptive_state.should_forward(self.proposer_id, policy) {
                self.adaptive_state.note_forward_attempt();
                req.to = leader;
                req.forwarded_from = self.address;
                self.pending_forward_requests.push(req);
                return;
            }

            self.adaptive_state.mode = AdaptiveMode::Classic;
            self.adaptive_state.leader_hint = Some(self.address);
            self.adaptive_state.forward_attempts = 0;
            self.adaptive_state.conflict_debt = self
                .adaptive_state
                .conflict_debt
                .max(policy.classic_entry_threshold);
        }

        self.forward_return_map.remove(&req.request_id);
        let proposal = ClientProposal {
            from: req.original_client,
            request_id: req.request_id,
            operation: req.operation,
        };
        if self.active_request_id.is_some() {
            self.request_queue.push_front(proposal);
        } else {
            self.start_proposal(proposal);
        }
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

    /// Takes all pending forward requests, leaving the list empty.
    pub fn drain_forward_requests(&mut self) -> Vec<PendingForwardRequest<V>> {
        std::mem::take(&mut self.pending_forward_requests)
    }

    // -- Crash / restart ----------------------------------------------------

    /// Simulates a crash — stops processing and clears all in-flight state.
    pub fn crash(&mut self) {
        self.is_running = false;
        self.outbox.clear();
        self.request_queue.clear();
        self.active_request_id = None;
        self.active_request_from = None;
        self.pending_forward_requests.clear();
        self.forward_return_map.clear();
    }

    /// Restarts after a crash by recreating the engine from scratch.
    ///
    /// The adaptive *policy* is preserved; the adaptive *state* (debt, hint)
    /// is reset — after a crash the proposer starts fresh in fast mode.
    pub fn restart(&mut self) {
        let config = ProposerConfig {
            proposer_id: self.proposer_id,
            enable_fast_commit: self.enable_fast_commit,
            enable_distinguished_leader: self.enable_distinguished_leader,
            ballot_seed: Ballot::ZERO,
        };
        self.engine = ProposerEngine::new(config, self.acceptors.clone());
        self.is_running = true;
        self.outbox.clear();
        self.request_queue.clear();
        self.active_request_id = None;
        self.active_request_from = None;
        self.pending_forward_requests.clear();
        self.forward_return_map.clear();
        // Reset adaptive state but keep policy.
        self.adaptive_state = ProposerAdaptiveState::default();
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
                adaptive_state: &mut self.adaptive_state,
                adaptive_policy: self.adaptive_policy.as_ref(),
                proposer_id: self.proposer_id,
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

        // Apply adaptive mode to the engine before starting.
        if self.adaptive_policy.is_some() {
            let force_classic = self.adaptive_state.mode == AdaptiveMode::Classic;
            self.engine.set_force_classic(force_classic);
        }

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
            // Re-check forwarding for the dequeued request.
            self.submit_client_request(next);
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
    use fast_caspaxos_core::operation::{FnOperation, OperationResult};

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
            None,
        )
    }

    fn make_adaptive_host(proposer_id: u32) -> ProposerHost<Option<i32>> {
        ProposerHost::new(
            Address::proposer(proposer_id),
            proposer_id,
            make_acceptors(5),
            true,
            false,
            Some(AdaptivePolicy::default()),
        )
    }

    fn set_op(value: i32) -> Box<dyn Operation<Option<i32>>> {
        Box::new(FnOperation::new(
            format!("set({value})"),
            move |_: &Option<i32>| OperationResult::Success(Some(value)),
        ))
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
                ProposerOutputMessage::SendPrepare(_) | ProposerOutputMessage::SendAccept(_)
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
        let prepared = Ballot::new(0, 2, 0);
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

    // -- Adaptive state tests -----------------------------------------------

    #[test]
    fn adaptive_starts_in_fast_mode() {
        let host = make_adaptive_host(1);
        assert_eq!(host.adaptive_state.mode, AdaptiveMode::Fast);
        assert_eq!(host.adaptive_state.conflict_debt, 0);
        assert!(host.adaptive_state.leader_hint.is_none());
    }

    #[test]
    fn adaptive_contention_accumulates_debt() {
        let mut host = make_adaptive_host(1);
        let policy = AdaptivePolicy::default();
        let conflict = Ballot::new(0, 3, 2);
        host.adaptive_state.apply_contention(conflict, &policy);
        assert_eq!(
            host.adaptive_state.conflict_debt,
            policy.conflict_debt_per_event
        );
        assert_eq!(host.adaptive_state.leader_hint, Some(Address::proposer(2)));
    }

    #[test]
    fn adaptive_switches_to_classic_at_threshold() {
        let mut host = make_adaptive_host(1);
        let policy = AdaptivePolicy::default();
        let conflict = Ballot::new(0, 3, 2);
        // Apply enough contention to reach the threshold.
        for _ in 0..(policy.classic_entry_threshold / policy.conflict_debt_per_event) + 1 {
            host.adaptive_state.apply_contention(conflict, &policy);
        }
        assert_eq!(host.adaptive_state.mode, AdaptiveMode::Classic);
    }

    #[test]
    fn adaptive_calm_drains_debt_and_reopens_fast() {
        let mut host = make_adaptive_host(1);
        let policy = AdaptivePolicy::default();
        let conflict = Ballot::new(0, 3, 2);

        // Push into classic.
        for _ in 0..(policy.classic_entry_threshold / policy.conflict_debt_per_event) + 1 {
            host.adaptive_state.apply_contention(conflict, &policy);
        }
        assert_eq!(host.adaptive_state.mode, AdaptiveMode::Classic);
        let initial_debt = host.adaptive_state.conflict_debt;

        // Calm commits drain debt (our ballot proposer = 1).
        let our_ballot = Ballot::new(0, 5, 1);
        let drains_needed = initial_debt / policy.calm_drain_per_commit + 1;
        for _ in 0..drains_needed {
            host.adaptive_state
                .apply_calm_commit(our_ballot, &policy, 1);
        }
        assert_eq!(host.adaptive_state.mode, AdaptiveMode::Fast);
        assert_eq!(host.adaptive_state.conflict_debt, 0);
    }

    #[test]
    fn adaptive_forward_request_generated_when_classic_with_hint() {
        let mut host = make_adaptive_host(2); // proposer 2
        let policy = AdaptivePolicy::default();

        // Set up classic mode with leader hint = proposer 1.
        host.adaptive_state.mode = AdaptiveMode::Classic;
        host.adaptive_state.leader_hint = Some(Address::proposer(1));
        host.adaptive_state.forward_attempts = 0;
        drop(policy);

        host.submit_client_request(client_proposal(1, 42));

        // Should produce a forward request, not a protocol message.
        assert!(
            host.drain_outbox().is_empty(),
            "Expected no direct outbox messages when forwarding"
        );
        let forwards = host.drain_forward_requests();
        assert_eq!(forwards.len(), 1);
        assert_eq!(forwards[0].to, Address::proposer(1));
        assert_eq!(forwards[0].forwarded_from, Address::proposer(2));
        assert_eq!(forwards[0].request_id, 1);
    }

    #[test]
    fn adaptive_local_takeover_after_max_forward_attempts() {
        let mut host = make_adaptive_host(2);

        // Set classic mode with exhausted forward budget.
        host.adaptive_state.mode = AdaptiveMode::Classic;
        host.adaptive_state.leader_hint = Some(Address::proposer(1));
        let policy = host.adaptive_policy.as_ref().unwrap();
        host.adaptive_state.forward_attempts = policy.max_forward_attempts;

        // Now submit — should NOT forward, should do local proposal.
        host.submit_client_request(client_proposal(1, 42));
        assert!(
            host.drain_forward_requests().is_empty(),
            "Should not forward after max attempts"
        );
        let msgs = host.drain_outbox();
        assert!(
            !msgs.is_empty(),
            "Expected local protocol message (prepare/accept) after takeover"
        );
    }

    #[test]
    fn deliver_forward_response_relays_to_original_client() {
        let mut host = make_adaptive_host(2);

        // Simulate a pending forward: insert into return map directly.
        let original_client = Address::client(42);
        host.forward_return_map.insert(99, original_client);

        let response = ProposeResponse {
            round: 5,
            committed_value: Some(100_i32),
        };
        host.deliver_forward_response(99, response.clone(), Ballot::new(0, 5, 1), Ballot::ZERO);

        let msgs = host.drain_outbox();
        let relay = msgs.iter().find_map(|m| match m {
            ProposerOutputMessage::ProposeResponse {
                to,
                request_id,
                response: r,
            } => Some((*to, *request_id, r.clone())),
            _ => None,
        });
        assert!(relay.is_some(), "Expected relayed ProposeResponse");
        let (to, rid, r) = relay.unwrap();
        assert_eq!(to, original_client);
        assert_eq!(rid, 99);
        assert_eq!(r.committed_value, response.committed_value);
    }

    #[test]
    fn restart_preserves_policy_but_resets_adaptive_state() {
        let mut host = make_adaptive_host(1);
        host.adaptive_state.mode = AdaptiveMode::Classic;
        host.adaptive_state.conflict_debt = 8;
        host.adaptive_state.leader_hint = Some(Address::proposer(2));

        host.crash();
        host.restart();

        assert!(host.adaptive_policy.is_some(), "Policy should be preserved");
        assert_eq!(host.adaptive_state.mode, AdaptiveMode::Fast);
        assert_eq!(host.adaptive_state.conflict_debt, 0);
        assert!(host.adaptive_state.leader_hint.is_none());
    }
}
