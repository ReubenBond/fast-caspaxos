//! Simulation cluster — central orchestrator for Fast `CASPaxos` testing.
//!
//! Creates and manages proposer/acceptor hosts, routes messages through
//! a deterministic simulation network, and provides a synchronous step-based
//! execution model for testing.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::SeedableRng;

use fast_caspaxos_core::acceptor::AcceptorState;
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, PreparePromise, PrepareRejection,
    PrepareRequest, ProposeResponse,
};
use fast_caspaxos_core::operation::Operation;
use fast_caspaxos_core::proposer::{AdaptiveMode, AdaptivePolicy};

use crate::acceptor_host::{AcceptorHost, AcceptorOutputMessage};
use crate::address::{Address, ParticipantRole};
use crate::diagnostics;
use crate::network::{DeliveryStatus, LinkLatency, SimulationNetwork};
use crate::proposer_host::{
    ClientProposal, PendingForwardRequest, ProposerAdaptiveState, ProposerHost,
    ProposerOutputMessage,
};
use crate::state_store::InMemoryAcceptorStateStore;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for cluster creation.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub num_acceptors: u32,
    pub num_proposers: u32,
    pub enable_fast_commit: bool,
    pub enable_distinguished_leader: bool,
    pub seed: u64,
    pub default_latency: LinkLatency,
    /// Optional adaptive policy.  When `Some`, all proposers are created in
    /// adaptive mode and will dynamically switch between fast and classic.
    pub adaptive_policy: Option<AdaptivePolicy>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_acceptors: 5,
            num_proposers: 3,
            enable_fast_commit: true,
            enable_distinguished_leader: false,
            seed: 42,
            default_latency: LinkLatency::zero(),
            adaptive_policy: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Client response
// ---------------------------------------------------------------------------

/// A response received by a client from a proposer.
#[derive(Debug, Clone)]
pub struct ClientResponse<V> {
    pub request_id: u32,
    pub from: Address,
    pub response: ProposeResponse<V>,
}

// ---------------------------------------------------------------------------
// Internal message envelope
// ---------------------------------------------------------------------------

/// In-flight message waiting to be delivered.
enum PendingMessage<V> {
    // Proposer → Acceptor
    PrepareRequest {
        from: Address,
        to: Address,
        payload: PrepareRequest,
    },
    AcceptRequest {
        from: Address,
        to: Address,
        payload: AcceptRequest<V>,
    },
    // Acceptor → Proposer
    PreparePromise {
        from: Address,
        to: Address,
        payload: PreparePromise<V, Address>,
    },
    PrepareRejection {
        from: Address,
        to: Address,
        payload: PrepareRejection<V, Address>,
    },
    AcceptAccepted {
        from: Address,
        to: Address,
        payload: AcceptAccepted<Address>,
    },
    AcceptRejected {
        from: Address,
        to: Address,
        payload: AcceptRejected<Address>,
    },
    // Proposer → Proposer (learn)
    LearnCommittedValue {
        from: Address,
        to: Address,
        ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    },
    /// Proposer → Proposer (forward request: non-leader sends work to leader).
    ForwardRequest {
        from: Address,
        to: Address,
        payload: PendingForwardRequest<V>,
    },
    /// Proposer → Proposer (forward response: leader sends result back to
    /// the forwarder, which then relays to the original client).
    ForwardResponse {
        from: Address,
        to: Address,
        request_id: u32,
        accepted_ballot: Ballot,
        prepared_ballot: Ballot,
        response: ProposeResponse<V>,
    },
}

// ---------------------------------------------------------------------------
// SimulationCluster
// ---------------------------------------------------------------------------

/// Central simulation orchestrator.
///
/// Manages all proposer/acceptor hosts and routes messages through a
/// deterministic simulation network. Execution is synchronous and driven
/// by explicit `step` / `run_*` calls.
pub struct SimulationCluster<V: Clone> {
    acceptor_hosts: BTreeMap<Address, AcceptorHost<V, InMemoryAcceptorStateStore<V>>>,
    proposer_hosts: BTreeMap<Address, ProposerHost<V>>,
    network: SimulationNetwork,
    rng: StdRng,
    pending_messages: Vec<PendingMessage<V>>,
    client_responses: BTreeMap<u32, ClientResponse<V>>,
    next_request_id: u32,
    fault_history: Vec<String>,
    config: ClusterConfig,
}

impl<V> SimulationCluster<V>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    /// Create a new cluster from configuration.
    pub fn new(config: ClusterConfig) -> Self {
        let mut acceptor_hosts = BTreeMap::new();
        let mut acceptor_addrs = Vec::new();

        for i in 1..=config.num_acceptors {
            let addr = Address::acceptor(i);
            acceptor_addrs.push(addr);
            let store = InMemoryAcceptorStateStore::new();
            acceptor_hosts.insert(addr, AcceptorHost::new(addr, store));
        }

        let mut proposer_hosts = BTreeMap::new();
        for i in 1..=config.num_proposers {
            let addr = Address::proposer(i);
            proposer_hosts.insert(
                addr,
                ProposerHost::new(
                    addr,
                    i,
                    acceptor_addrs.clone(),
                    config.enable_fast_commit,
                    config.enable_distinguished_leader,
                    config.adaptive_policy.clone(),
                ),
            );
        }

        let network = SimulationNetwork::new(config.default_latency);
        let rng = StdRng::seed_from_u64(config.seed);

        Self {
            acceptor_hosts,
            proposer_hosts,
            network,
            rng,
            pending_messages: Vec::new(),
            client_responses: BTreeMap::new(),
            next_request_id: 1,
            fault_history: Vec::new(),
            config,
        }
    }

    // -- Proposal submission ------------------------------------------------

    /// Submit a client proposal to a specific proposer.
    ///
    /// Returns the auto-assigned request ID.
    pub fn send_proposal(&mut self, proposer: Address, operation: Box<dyn Operation<V>>) -> u32 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        diagnostics::trace_proposal_submitted(proposer, request_id);
        let client = Address::client(0);
        let proposal = ClientProposal {
            from: client,
            request_id,
            operation,
        };
        self.proposer_hosts
            .get_mut(&proposer)
            .expect("unknown proposer address")
            .submit_client_request(proposal);
        self.collect_outbox_messages();
        request_id
    }

    // -- Execution ----------------------------------------------------------

    /// Run one step: deliver the next pending message.
    ///
    /// Returns `true` if a message was delivered, `false` if the queue was
    /// empty.
    pub fn step(&mut self) -> bool {
        if self.pending_messages.is_empty() {
            return false;
        }
        let msg = self.pending_messages.remove(0);
        self.deliver_message(msg);
        self.collect_outbox_messages();
        true
    }

    /// Run until a specific request gets a response, or `max_steps` is
    /// exceeded.
    pub fn run_until_response(
        &mut self,
        request_id: u32,
        max_steps: usize,
    ) -> Option<&ClientResponse<V>> {
        for _ in 0..max_steps {
            if self.client_responses.contains_key(&request_id) {
                return self.client_responses.get(&request_id);
            }
            if !self.step() {
                break;
            }
        }
        self.client_responses.get(&request_id)
    }

    /// Run until no more pending messages or `max_steps` exceeded.
    ///
    /// Returns the number of steps taken.
    pub fn run_to_completion(&mut self, max_steps: usize) -> usize {
        let mut steps = 0;
        while steps < max_steps && self.step() {
            steps += 1;
        }
        steps
    }

    // -- Timeout ------------------------------------------------------------

    /// Trigger a response timeout on a proposer for its current round.
    ///
    /// This causes the proposer to retry the in-flight operation (if any)
    /// with a new ballot.
    pub fn trigger_timeout(&mut self, addr: Address) {
        diagnostics::trace_timeout_triggered(addr);
        if let Some(host) = self.proposer_hosts.get_mut(&addr) {
            let round = host.ballot().round;
            host.handle_timeout(round);
        }
        self.collect_outbox_messages();
    }

    // -- Fault injection ----------------------------------------------------

    /// Create a bidirectional network partition between two addresses.
    pub fn create_partition(&mut self, a: Address, b: Address) {
        self.update_partition_state(a, b, false);
    }

    /// Heal a previously created partition.
    pub fn heal_partition(&mut self, a: Address, b: Address) {
        self.update_partition_state(a, b, true);
    }

    /// Crash a node (acceptor or proposer). Crashed nodes ignore all
    /// delivered messages until restarted.
    pub fn crash_node(&mut self, addr: Address) {
        diagnostics::trace_node_crashed(addr);
        self.set_node_running_state(addr, false);
        self.fault_history.push(format!("crash: {addr}"));
    }

    /// Restart a previously crashed node.
    pub fn restart_node(&mut self, addr: Address) {
        diagnostics::trace_node_restarted(addr);
        self.set_node_running_state(addr, true);
        self.fault_history.push(format!("restart: {addr}"));
    }

    /// Set the drop rate for the directed link `from → to`.
    pub fn set_drop_rate(&mut self, from: Address, to: Address, rate: f64) {
        self.network.set_drop_rate(from, to, rate);
    }

    /// Set latency for the directed link `from → to`.
    pub fn set_link_latency(&mut self, from: Address, to: Address, latency: LinkLatency) {
        self.network.set_link_latency(from, to, latency);
    }

    // -- Query --------------------------------------------------------------

    /// All client responses received so far, keyed by request ID.
    pub fn client_responses(&self) -> &BTreeMap<u32, ClientResponse<V>> {
        &self.client_responses
    }

    /// Snapshot the current state of an acceptor.
    pub fn acceptor_state(&self, addr: Address) -> AcceptorState<V> {
        self.acceptor_hosts
            .get(&addr)
            .expect("unknown acceptor address")
            .state()
    }

    /// Current ballot of a proposer.
    pub fn proposer_ballot(&self, addr: Address) -> Ballot {
        self.proposer_hosts
            .get(&addr)
            .expect("unknown proposer address")
            .ballot()
    }

    /// Snapshot the current adaptive state of a proposer (if in adaptive mode).
    pub fn proposer_adaptive_state(&self, addr: Address) -> ProposerAdaptiveState {
        self.proposer_hosts
            .get(&addr)
            .expect("unknown proposer address")
            .adaptive_state
            .clone()
    }

    /// Returns the current [`AdaptiveMode`] of a proposer, or `None` if the
    /// proposer is not running in adaptive mode.
    pub fn proposer_adaptive_mode(&self, addr: Address) -> Option<AdaptiveMode> {
        let host = self
            .proposer_hosts
            .get(&addr)
            .expect("unknown proposer address");
        if host.adaptive_policy.is_some() {
            Some(host.adaptive_state.mode)
        } else {
            None
        }
    }

    /// Directly overwrite the adaptive state of a proposer.
    ///
    /// Useful in tests to seed a specific mode/hint without running through
    /// the full contention protocol.
    pub fn set_proposer_adaptive_state(&mut self, addr: Address, state: ProposerAdaptiveState) {
        self.proposer_hosts
            .get_mut(&addr)
            .expect("unknown proposer address")
            .adaptive_state = state;
    }

    /// Number of messages waiting to be delivered.
    pub fn num_pending_messages(&self) -> usize {
        self.pending_messages.len()
    }

    /// Fault-injection history for diagnostics.
    pub fn fault_history(&self) -> &[String] {
        &self.fault_history
    }

    /// Addresses of all acceptors in the cluster.
    pub fn acceptor_addresses(&self) -> Vec<Address> {
        self.acceptor_hosts.keys().copied().collect()
    }

    /// Addresses of all proposers in the cluster.
    pub fn proposer_addresses(&self) -> Vec<Address> {
        self.proposer_hosts.keys().copied().collect()
    }

    /// The configuration used to create this cluster.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    fn update_partition_state(&mut self, a: Address, b: Address, healed: bool) {
        if healed {
            diagnostics::trace_partition_healed(a, b);
            self.network.heal_partition(a, b);
            self.fault_history.push(format!("heal: {a} <-> {b}"));
        } else {
            diagnostics::trace_partition_created(a, b);
            self.network.create_partition(a, b);
            self.fault_history.push(format!("partition: {a} <-> {b}"));
        }
    }

    fn set_node_running_state(&mut self, addr: Address, running: bool) {
        if let Some(host) = self.acceptor_hosts.get_mut(&addr) {
            if running {
                host.restart();
            } else {
                host.crash();
            }
        }
        if let Some(host) = self.proposer_hosts.get_mut(&addr) {
            if running {
                host.restart();
            } else {
                host.crash();
            }
        }
    }

    // -- Internal: outbox collection ----------------------------------------

    /// Drain output messages from all hosts into the pending-message queue.
    ///
    /// `ProposeResponse` messages are recorded as client responses immediately.
    /// `BroadcastCommittedValue` is fanned out as individual learn messages
    /// to each other proposer.
    fn collect_outbox_messages(&mut self) {
        let acceptor_addrs: Vec<Address> = self.acceptor_hosts.keys().copied().collect();
        let proposer_addrs: Vec<Address> = self.proposer_hosts.keys().copied().collect();

        self.collect_acceptor_outboxes(&acceptor_addrs);
        self.collect_proposer_outboxes(&acceptor_addrs, &proposer_addrs);
    }

    fn collect_acceptor_outboxes(&mut self, acceptor_addrs: &[Address]) {
        for &addr in acceptor_addrs {
            let outbox = {
                let host = self.acceptor_hosts.get_mut(&addr).unwrap();
                host.drain_outbox()
            };
            for msg in outbox {
                self.enqueue_acceptor_message(addr, msg);
            }
        }
    }

    fn enqueue_acceptor_message(&mut self, from: Address, msg: AcceptorOutputMessage<V>) {
        match msg {
            AcceptorOutputMessage::PreparePromise { to, payload } => {
                self.pending_messages
                    .push(PendingMessage::PreparePromise { from, to, payload });
            }
            AcceptorOutputMessage::PrepareRejection { to, payload } => {
                self.pending_messages
                    .push(PendingMessage::PrepareRejection { from, to, payload });
            }
            AcceptorOutputMessage::AcceptAccepted { to, payload } => {
                self.pending_messages
                    .push(PendingMessage::AcceptAccepted { from, to, payload });
            }
            AcceptorOutputMessage::AcceptRejected { to, payload } => {
                self.pending_messages
                    .push(PendingMessage::AcceptRejected { from, to, payload });
            }
        }
    }

    fn collect_proposer_outboxes(
        &mut self,
        acceptor_addrs: &[Address],
        proposer_addrs: &[Address],
    ) {
        for &addr in proposer_addrs {
            let (outbox, forward_requests, accepted_ballot, prepared_ballot) = {
                let host = self.proposer_hosts.get_mut(&addr).unwrap();
                let accepted_ballot = host.ballot();
                let prepared_ballot = host.prepared_ballot().unwrap_or(Ballot::ZERO);
                let outbox = host.drain_outbox();
                let forward_requests = host.drain_forward_requests();
                (outbox, forward_requests, accepted_ballot, prepared_ballot)
            };

            for msg in outbox {
                self.enqueue_proposer_message(
                    addr,
                    msg,
                    acceptor_addrs,
                    proposer_addrs,
                    accepted_ballot,
                    prepared_ballot,
                );
            }

            for payload in forward_requests {
                let to = payload.to;
                self.pending_messages.push(PendingMessage::ForwardRequest {
                    from: addr,
                    to,
                    payload,
                });
            }
        }
    }

    fn enqueue_proposer_message(
        &mut self,
        from: Address,
        msg: ProposerOutputMessage<V>,
        acceptor_addrs: &[Address],
        proposer_addrs: &[Address],
        accepted_ballot: Ballot,
        host_prepared_ballot: Ballot,
    ) {
        match msg {
            ProposerOutputMessage::SendPrepare(req) => {
                for &to in acceptor_addrs {
                    self.pending_messages.push(PendingMessage::PrepareRequest {
                        from,
                        to,
                        payload: PrepareRequest { ballot: req.ballot },
                    });
                }
            }
            ProposerOutputMessage::SendAccept(req) => {
                for &to in acceptor_addrs {
                    self.pending_messages.push(PendingMessage::AcceptRequest {
                        from,
                        to,
                        payload: req.clone(),
                    });
                }
            }
            ProposerOutputMessage::ProposeResponse {
                to,
                request_id,
                response,
            } => {
                if to.role == ParticipantRole::Proposer {
                    self.pending_messages.push(PendingMessage::ForwardResponse {
                        from,
                        to,
                        request_id,
                        accepted_ballot,
                        prepared_ballot: host_prepared_ballot,
                        response,
                    });
                } else {
                    diagnostics::trace_proposal_completed(request_id, response.round);
                    self.client_responses.insert(
                        request_id,
                        ClientResponse {
                            request_id,
                            from,
                            response,
                        },
                    );
                }
            }
            ProposerOutputMessage::BroadcastCommittedValue {
                ballot,
                value,
                prepared_ballot,
            } => {
                for &to in proposer_addrs {
                    if to != from {
                        self.pending_messages
                            .push(PendingMessage::LearnCommittedValue {
                                from,
                                to,
                                ballot,
                                value: value.clone(),
                                prepared_ballot,
                            });
                    }
                }
            }
        }
    }

    // -- Internal: message delivery -----------------------------------------

    /// Deliver one message, checking network conditions first.
    fn deliver_message(&mut self, msg: PendingMessage<V>) {
        match msg {
            PendingMessage::PrepareRequest { from, to, payload } => {
                if self.try_deliver(from, to, "PrepareRequest") {
                    if let Some(host) = self.acceptor_hosts.get_mut(&to) {
                        host.deliver_prepare(from, &payload);
                    }
                }
            }
            PendingMessage::AcceptRequest { from, to, payload } => {
                if self.try_deliver(from, to, "AcceptRequest") {
                    if let Some(host) = self.acceptor_hosts.get_mut(&to) {
                        host.deliver_accept(from, &payload);
                    }
                }
            }
            PendingMessage::PreparePromise { from, to, payload } => {
                if self.try_deliver(from, to, "PreparePromise") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_prepare_promise(payload);
                    }
                }
            }
            PendingMessage::PrepareRejection { from, to, payload } => {
                if self.try_deliver(from, to, "PrepareRejection") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_prepare_rejection(payload);
                    }
                }
            }
            PendingMessage::AcceptAccepted { from, to, payload } => {
                if self.try_deliver(from, to, "AcceptAccepted") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_accept_accepted(payload);
                    }
                }
            }
            PendingMessage::AcceptRejected { from, to, payload } => {
                if self.try_deliver(from, to, "AcceptRejected") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_accept_rejected(payload);
                    }
                }
            }
            PendingMessage::LearnCommittedValue {
                from,
                to,
                ballot,
                value,
                prepared_ballot,
            } => {
                if self.try_deliver(from, to, "LearnCommittedValue") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_learn_committed_value(ballot, value, prepared_ballot);
                    }
                }
            }
            PendingMessage::ForwardRequest { from, to, payload } => {
                if self.try_deliver(from, to, "ForwardRequest") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_forwarded_request(payload);
                    }
                } else if let Some(host) = self.proposer_hosts.get_mut(&from) {
                    host.handle_failed_forward_request(payload);
                }
            }
            PendingMessage::ForwardResponse {
                from,
                to,
                request_id,
                accepted_ballot,
                prepared_ballot,
                response,
            } => {
                if self.try_deliver(from, to, "ForwardResponse") {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_forward_response(
                            request_id,
                            response,
                            accepted_ballot,
                            prepared_ballot,
                        );
                    }
                    // collect_outbox_messages will pick up the relay response.
                    self.collect_outbox_messages();
                }
            }
        }
    }

    /// Check whether a message from `from` to `to` is delivered by the
    /// simulated network. Traces the outcome.
    fn try_deliver(&mut self, from: Address, to: Address, msg_type: &str) -> bool {
        match self.network.check_delivery(from, to, &mut self.rng) {
            DeliveryStatus::Success => {
                diagnostics::trace_message_delivered(from, to, msg_type);
                true
            }
            DeliveryStatus::Dropped => {
                diagnostics::trace_message_dropped(from, to, msg_type, "random drop");
                false
            }
            DeliveryStatus::Partitioned => {
                diagnostics::trace_message_dropped(from, to, msg_type, "partitioned");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::operation::{FnOperation, OperationResult};

    fn write_op(value: i32) -> Box<dyn Operation<i32>> {
        Box::new(FnOperation::new("write", move |_: &i32| {
            OperationResult::Success(value)
        }))
    }

    // -- Basic operation ----------------------------------------------------

    #[test]
    fn single_proposer_write() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        let req_id = cluster.send_proposal(proposer, write_op(42));
        let response = cluster.run_until_response(req_id, 1000);

        assert!(response.is_some(), "expected a response");
        assert_eq!(response.unwrap().response.committed_value, 42);
    }

    #[test]
    fn multiple_sequential_writes() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        for value in [10, 20, 30] {
            let req_id = cluster.send_proposal(proposer, write_op(value));
            let response = cluster.run_until_response(req_id, 1000);
            assert!(response.is_some(), "write of {value} should succeed");
            assert_eq!(response.unwrap().response.committed_value, value);
        }
    }

    #[test]
    fn two_proposer_writes() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 2,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let p1 = Address::proposer(1);
        let p2 = Address::proposer(2);

        // First proposer writes
        let req1 = cluster.send_proposal(p1, write_op(10));
        let resp1 = cluster.run_until_response(req1, 1000);
        assert!(resp1.is_some(), "first proposer write should succeed");

        // Second proposer writes
        let req2 = cluster.send_proposal(p2, write_op(20));
        let resp2 = cluster.run_until_response(req2, 1000);
        assert!(resp2.is_some(), "second proposer write should succeed");

        // Values should reflect the most recent write
        let final_value = resp2.unwrap().response.committed_value;
        assert_eq!(final_value, 20);
    }

    // -- Fault injection ----------------------------------------------------

    #[test]
    fn partition_blocks_proposal() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        // Partition all acceptors from the proposer.
        for i in 1..=5 {
            cluster.create_partition(proposer, Address::acceptor(i));
        }

        let req_id = cluster.send_proposal(proposer, write_op(42));

        // Messages are sent but all dropped due to partition.
        cluster.run_to_completion(100);
        assert!(
            !cluster.client_responses().contains_key(&req_id),
            "should not get a response while partitioned"
        );

        // Heal partition and retry via timeout.
        for i in 1..=5 {
            cluster.heal_partition(proposer, Address::acceptor(i));
        }
        cluster.trigger_timeout(proposer);

        let response = cluster.run_until_response(req_id, 1000);
        assert!(
            response.is_some(),
            "should get a response after healing the partition"
        );
        assert_eq!(response.unwrap().response.committed_value, 42);
    }

    #[test]
    fn crash_acceptor_and_recover() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        // Crash one acceptor — quorum is still reachable.
        cluster.crash_node(Address::acceptor(1));

        let req_id = cluster.send_proposal(proposer, write_op(42));
        let response = cluster.run_until_response(req_id, 1000);
        assert!(
            response.is_some(),
            "write should succeed with one acceptor down"
        );
        assert_eq!(response.unwrap().response.committed_value, 42);

        // Restart the crashed acceptor.
        cluster.restart_node(Address::acceptor(1));

        // Verify the restarted acceptor still has its persisted state.
        let state = cluster.acceptor_state(Address::acceptor(1));
        // The default acceptor state has ZERO accepted ballot — crash before
        // any messages were delivered to it, so the store only has initial state.
        assert_eq!(state.accepted_ballot, Ballot::ZERO);
    }

    // -- Fast vs classic paths ----------------------------------------------

    #[test]
    fn fast_commit_mode() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            enable_fast_commit: true,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        let req_id = cluster.send_proposal(proposer, write_op(42));
        let fast_steps = cluster.run_to_completion(1000);

        assert!(
            cluster.client_responses().contains_key(&req_id),
            "fast commit should complete"
        );
        // Fast path: accept(5) + responses(5) + learn broadcasts = ~12 steps
        assert!(
            fast_steps <= 20,
            "fast commit should finish quickly, took {fast_steps} steps"
        );
    }

    #[test]
    fn classic_mode() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            enable_fast_commit: false,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        let req_id = cluster.send_proposal(proposer, write_op(42));
        let classic_steps = cluster.run_to_completion(1000);

        assert!(
            cluster.client_responses().contains_key(&req_id),
            "classic mode should complete"
        );
        assert_eq!(
            cluster.client_responses()[&req_id].response.committed_value,
            42
        );
        // Classic path: prepare(5) + promise(5) + accept(5) + accepted(5) = ~20
        assert!(
            classic_steps > 10,
            "classic mode should take more steps, took {classic_steps}"
        );
    }

    // -- Message drops ------------------------------------------------------

    #[test]
    fn message_drop_eventually_succeeds() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            enable_fast_commit: false,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let proposer = Address::proposer(1);

        // Set 50% drop rate on all proposer↔acceptor links.
        for i in 1..=5 {
            let acc = Address::acceptor(i);
            cluster.set_drop_rate(proposer, acc, 0.5);
            cluster.set_drop_rate(acc, proposer, 0.5);
        }

        let req_id = cluster.send_proposal(proposer, write_op(42));

        // Retry with timeouts until the proposal completes.
        for _ in 0..500 {
            if cluster.client_responses().contains_key(&req_id) {
                break;
            }
            cluster.run_to_completion(50);
            if !cluster.client_responses().contains_key(&req_id) {
                cluster.trigger_timeout(proposer);
            }
        }

        assert!(
            cluster.client_responses().contains_key(&req_id),
            "should eventually succeed despite 50% message drops"
        );
        assert_eq!(
            cluster.client_responses()[&req_id].response.committed_value,
            42
        );
    }

    // -- Query helpers ------------------------------------------------------

    #[test]
    fn cluster_query_methods() {
        let config = ClusterConfig {
            num_acceptors: 3,
            num_proposers: 2,
            ..Default::default()
        };
        let cluster = SimulationCluster::<i32>::new(config);

        assert_eq!(cluster.acceptor_addresses().len(), 3);
        assert_eq!(cluster.proposer_addresses().len(), 2);
        assert_eq!(cluster.num_pending_messages(), 0);
        assert!(cluster.fault_history().is_empty());
        assert_eq!(cluster.config().num_acceptors, 3);
    }

    #[test]
    fn fault_history_tracks_events() {
        let config = ClusterConfig {
            num_acceptors: 3,
            num_proposers: 1,
            ..Default::default()
        };
        let mut cluster = SimulationCluster::<i32>::new(config);
        let p = Address::proposer(1);
        let a = Address::acceptor(1);

        cluster.create_partition(p, a);
        cluster.crash_node(a);
        cluster.restart_node(a);
        cluster.heal_partition(p, a);

        assert_eq!(cluster.fault_history().len(), 4);
    }

    #[test]
    #[ignore = "tracing subscriber conflicts when run in parallel with other tests"]
    fn tracing_emits_events() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct CountingSubscriber {
            info_count: Arc<AtomicUsize>,
            debug_count: Arc<AtomicUsize>,
            warn_count: Arc<AtomicUsize>,
        }

        impl tracing::Subscriber for CountingSubscriber {
            fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
                true
            }

            fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }

            fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }

            fn event(&self, event: &tracing::Event<'_>) {
                match *event.metadata().level() {
                    tracing::Level::INFO => {
                        self.info_count.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::Level::DEBUG => {
                        self.debug_count.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::Level::WARN => {
                        self.warn_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }

            fn enter(&self, _span: &tracing::span::Id) {}
            fn exit(&self, _span: &tracing::span::Id) {}
        }

        let info_count = Arc::new(AtomicUsize::new(0));
        let debug_count = Arc::new(AtomicUsize::new(0));
        let warn_count = Arc::new(AtomicUsize::new(0));

        let subscriber = CountingSubscriber {
            info_count: Arc::clone(&info_count),
            debug_count: Arc::clone(&debug_count),
            warn_count: Arc::clone(&warn_count),
        };

        tracing::subscriber::with_default(subscriber, || {
            let config = ClusterConfig {
                num_acceptors: 5,
                num_proposers: 1,
                ..Default::default()
            };
            let mut cluster = SimulationCluster::<i32>::new(config);
            let proposer = Address::proposer(1);

            let req_id = cluster.send_proposal(proposer, write_op(42));
            cluster.run_until_response(req_id, 1000);

            let acc = Address::acceptor(1);
            cluster.crash_node(acc);
            cluster.restart_node(acc);
            cluster.create_partition(proposer, acc);
            cluster.heal_partition(proposer, acc);
        });

        // proposal_submitted + proposal_completed + node_restarted + partition_healed
        assert!(
            info_count.load(Ordering::Relaxed) >= 4,
            "expected at least 4 INFO events, got {}",
            info_count.load(Ordering::Relaxed),
        );
        // message deliveries at DEBUG level
        assert!(
            debug_count.load(Ordering::Relaxed) >= 1,
            "expected at least 1 DEBUG event, got {}",
            debug_count.load(Ordering::Relaxed),
        );
        // crash + partition → WARN
        assert!(
            warn_count.load(Ordering::Relaxed) >= 2,
            "expected at least 2 WARN events, got {}",
            warn_count.load(Ordering::Relaxed),
        );
    }
}
