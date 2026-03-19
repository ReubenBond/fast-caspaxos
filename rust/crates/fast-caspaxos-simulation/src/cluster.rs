//! Simulation cluster — central orchestrator for Fast CASPaxos testing.
//!
//! Creates and manages proposer/acceptor hosts, routes messages through
//! a deterministic simulation network, and provides a synchronous step-based
//! execution model for testing.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::SeedableRng;

use fast_caspaxos_core::acceptor::AcceptorState;
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::messages::*;
use fast_caspaxos_core::operation::Operation;

use crate::acceptor_host::{AcceptorHost, AcceptorOutputMessage};
use crate::address::Address;
use crate::network::{DeliveryStatus, LinkLatency, SimulationNetwork};
use crate::proposer_host::{ClientProposal, ProposerHost, ProposerOutputMessage};
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
    pub fn send_proposal(
        &mut self,
        proposer: Address,
        operation: Box<dyn Operation<V>>,
    ) -> u32 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
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
        if let Some(host) = self.proposer_hosts.get_mut(&addr) {
            let round = host.ballot().round;
            host.handle_timeout(round);
        }
        self.collect_outbox_messages();
    }

    // -- Fault injection ----------------------------------------------------

    /// Create a bidirectional network partition between two addresses.
    pub fn create_partition(&mut self, a: Address, b: Address) {
        self.network.create_partition(a, b);
        self.fault_history.push(format!("partition: {a} <-> {b}"));
    }

    /// Heal a previously created partition.
    pub fn heal_partition(&mut self, a: Address, b: Address) {
        self.network.heal_partition(a, b);
        self.fault_history.push(format!("heal: {a} <-> {b}"));
    }

    /// Crash a node (acceptor or proposer). Crashed nodes ignore all
    /// delivered messages until restarted.
    pub fn crash_node(&mut self, addr: Address) {
        if let Some(host) = self.acceptor_hosts.get_mut(&addr) {
            host.crash();
        }
        if let Some(host) = self.proposer_hosts.get_mut(&addr) {
            host.crash();
        }
        self.fault_history.push(format!("crash: {addr}"));
    }

    /// Restart a previously crashed node.
    pub fn restart_node(&mut self, addr: Address) {
        if let Some(host) = self.acceptor_hosts.get_mut(&addr) {
            host.restart();
        }
        if let Some(host) = self.proposer_hosts.get_mut(&addr) {
            host.restart();
        }
        self.fault_history.push(format!("restart: {addr}"));
    }

    /// Set the drop rate for the directed link `from → to`.
    pub fn set_drop_rate(&mut self, from: Address, to: Address, rate: f64) {
        self.network.set_drop_rate(from, to, rate);
    }

    /// Set latency for the directed link `from → to`.
    pub fn set_link_latency(
        &mut self,
        from: Address,
        to: Address,
        latency: LinkLatency,
    ) {
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

    // -- Internal: outbox collection ----------------------------------------

    /// Drain output messages from all hosts into the pending-message queue.
    ///
    /// `ProposeResponse` messages are recorded as client responses immediately.
    /// `BroadcastCommittedValue` is fanned out as individual learn messages
    /// to each other proposer.
    fn collect_outbox_messages(&mut self) {
        let acceptor_addrs: Vec<Address> =
            self.acceptor_hosts.keys().copied().collect();
        let proposer_addrs: Vec<Address> =
            self.proposer_hosts.keys().copied().collect();

        // Drain acceptor outboxes.
        for &addr in &acceptor_addrs {
            let host = self.acceptor_hosts.get_mut(&addr).unwrap();
            for msg in host.drain_outbox() {
                match msg {
                    AcceptorOutputMessage::PreparePromise { to, payload } => {
                        self.pending_messages.push(
                            PendingMessage::PreparePromise {
                                from: addr,
                                to,
                                payload,
                            },
                        );
                    }
                    AcceptorOutputMessage::PrepareRejection { to, payload } => {
                        self.pending_messages.push(
                            PendingMessage::PrepareRejection {
                                from: addr,
                                to,
                                payload,
                            },
                        );
                    }
                    AcceptorOutputMessage::AcceptAccepted { to, payload } => {
                        self.pending_messages.push(
                            PendingMessage::AcceptAccepted {
                                from: addr,
                                to,
                                payload,
                            },
                        );
                    }
                    AcceptorOutputMessage::AcceptRejected { to, payload } => {
                        self.pending_messages.push(
                            PendingMessage::AcceptRejected {
                                from: addr,
                                to,
                                payload,
                            },
                        );
                    }
                }
            }
        }

        // Drain proposer outboxes.
        for &addr in &proposer_addrs {
            let host = self.proposer_hosts.get_mut(&addr).unwrap();
            for msg in host.drain_outbox() {
                match msg {
                    ProposerOutputMessage::SendPrepare(req) => {
                        for &acc in &acceptor_addrs {
                            self.pending_messages.push(
                                PendingMessage::PrepareRequest {
                                    from: addr,
                                    to: acc,
                                    payload: PrepareRequest {
                                        ballot: req.ballot,
                                    },
                                },
                            );
                        }
                    }
                    ProposerOutputMessage::SendAccept(req) => {
                        for &acc in &acceptor_addrs {
                            self.pending_messages.push(
                                PendingMessage::AcceptRequest {
                                    from: addr,
                                    to: acc,
                                    payload: req.clone(),
                                },
                            );
                        }
                    }
                    ProposerOutputMessage::ProposeResponse {
                        to: _,
                        request_id,
                        response,
                    } => {
                        self.client_responses.insert(
                            request_id,
                            ClientResponse {
                                request_id,
                                from: addr,
                                response,
                            },
                        );
                    }
                    ProposerOutputMessage::BroadcastCommittedValue {
                        ballot,
                        value,
                        prepared_ballot,
                    } => {
                        for &other in &proposer_addrs {
                            if other != addr {
                                self.pending_messages.push(
                                    PendingMessage::LearnCommittedValue {
                                        from: addr,
                                        to: other,
                                        ballot,
                                        value: value.clone(),
                                        prepared_ballot,
                                    },
                                );
                            }
                        }
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
                if self.is_delivered(from, to) {
                    if let Some(host) = self.acceptor_hosts.get_mut(&to) {
                        host.deliver_prepare(from, payload);
                    }
                }
            }
            PendingMessage::AcceptRequest { from, to, payload } => {
                if self.is_delivered(from, to) {
                    if let Some(host) = self.acceptor_hosts.get_mut(&to) {
                        host.deliver_accept(from, payload);
                    }
                }
            }
            PendingMessage::PreparePromise { from, to, payload } => {
                if self.is_delivered(from, to) {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_prepare_promise(payload);
                    }
                }
            }
            PendingMessage::PrepareRejection { from, to, payload } => {
                if self.is_delivered(from, to) {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_prepare_rejection(payload);
                    }
                }
            }
            PendingMessage::AcceptAccepted { from, to, payload } => {
                if self.is_delivered(from, to) {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_accept_accepted(payload);
                    }
                }
            }
            PendingMessage::AcceptRejected { from, to, payload } => {
                if self.is_delivered(from, to) {
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
                if self.is_delivered(from, to) {
                    if let Some(host) = self.proposer_hosts.get_mut(&to) {
                        host.deliver_learn_committed_value(
                            ballot,
                            value,
                            prepared_ballot,
                        );
                    }
                }
            }
        }
    }

    /// Check whether a message from `from` to `to` is delivered by the
    /// simulated network.
    fn is_delivered(&mut self, from: Address, to: Address) -> bool {
        self.network.check_delivery(from, to, &mut self.rng)
            == DeliveryStatus::Success
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::operation::{
        FnOperation, OperationResult, OperationStatus,
    };

    fn write_op(value: i32) -> Box<dyn Operation<i32>> {
        Box::new(FnOperation::new("write", move |_: &i32| {
            OperationResult::new(OperationStatus::Success, value)
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
}
