//! Test harness for recording Porcupine linearizability histories.
//!
//! Wraps a [`SimulationCluster`] and a [`PorcupineHistoryBuilder`] so that
//! proposal submission and response collection automatically record call/return
//! events for the Porcupine model checker.

use std::fs;
use std::io::Write;
use std::path::Path;

use fast_caspaxos_core::operation::Operation;

use crate::address::Address;
use crate::cluster::{ClientResponse, ClusterConfig, SimulationCluster};
use crate::history::{OperationInput, PorcupineHistoryBuilder, ValueOutput};
use crate::network::LinkLatency;
use crate::value::StringValue;

/// Test harness that wraps a [`SimulationCluster<StringValue>`] with automatic
/// Porcupine history recording.
pub struct PorcupineTestHarness {
    cluster: SimulationCluster<StringValue>,
    history: PorcupineHistoryBuilder,
}

impl PorcupineTestHarness {
    /// Create a new harness from a cluster config and history builder.
    pub fn new(config: ClusterConfig, history: PorcupineHistoryBuilder) -> Self {
        Self {
            cluster: SimulationCluster::new(config),
            history,
        }
    }

    /// Submit a proposal and record the call event.
    ///
    /// Returns the auto-assigned request ID.
    pub fn send_tracked_proposal(
        &mut self,
        proposer: Address,
        operation: Box<dyn Operation<StringValue>>,
        input: OperationInput,
    ) -> u32 {
        let client = Address::client(proposer.ordinal);
        let req_id = self.cluster.send_proposal(proposer, operation);
        self.history
            .on_proposal_sent(client, proposer, req_id, input);
        req_id
    }

    /// Run until the given request gets a response, then record the return event.
    ///
    /// Returns the response if one was received within `max_steps`.
    pub fn run_until_response_and_record(
        &mut self,
        request_id: u32,
        max_steps: usize,
        output_fn: impl FnOnce(&StringValue) -> ValueOutput,
    ) -> Option<&ClientResponse<StringValue>> {
        let resp = self.cluster.run_until_response(request_id, max_steps);
        if let Some(r) = resp {
            let output = output_fn(&r.response.committed_value);
            self.history
                .on_proposal_completed(request_id, output, r.response.round);
        }
        self.cluster.client_responses().get(&request_id)
    }

    /// Drive the cluster until all `request_ids` have responses, recording
    /// return events as they complete. Uses timeouts to push through conflicts.
    pub fn drive_to_all_responses_and_record(
        &mut self,
        request_ids: &[u32],
        max_rounds: usize,
        output_fn: impl Fn(&StringValue) -> ValueOutput,
    ) {
        // Track which IDs still need recording.
        let mut pending: Vec<u32> = request_ids.to_vec();

        for _ in 0..max_rounds {
            self.cluster.run_to_completion(200);

            pending.retain(|&id| {
                if let Some(resp) = self.cluster.client_responses().get(&id) {
                    let output = output_fn(&resp.response.committed_value);
                    self.history
                        .on_proposal_completed(id, output, resp.response.round);
                    false // remove from pending
                } else {
                    true // keep in pending
                }
            });

            if pending.is_empty() {
                return;
            }

            // Trigger timeouts for all proposers to push through conflicts.
            for p in self.cluster.proposer_addresses() {
                self.cluster.trigger_timeout(p);
            }
        }
    }

    /// Crash a node.
    pub fn crash_node(&mut self, addr: Address) {
        self.cluster.crash_node(addr);
    }

    /// Restart a previously crashed node.
    pub fn restart_node(&mut self, addr: Address) {
        self.cluster.restart_node(addr);
    }

    /// Create a bidirectional partition.
    pub fn create_partition(&mut self, a: Address, b: Address) {
        self.cluster.create_partition(a, b);
    }

    /// Heal a partition.
    pub fn heal_partition(&mut self, a: Address, b: Address) {
        self.cluster.heal_partition(a, b);
    }

    /// Trigger a timeout on a proposer.
    pub fn trigger_timeout(&mut self, addr: Address) {
        self.cluster.trigger_timeout(addr);
    }

    /// Set the drop rate for a directed link.
    pub fn set_drop_rate(&mut self, from: Address, to: Address, rate: f64) {
        self.cluster.set_drop_rate(from, to, rate);
    }

    /// Set latency for a directed link.
    pub fn set_link_latency(&mut self, from: Address, to: Address, latency: LinkLatency) {
        self.cluster.set_link_latency(from, to, latency);
    }

    /// Run the cluster to completion (up to `max_steps`).
    pub fn run_to_completion(&mut self, max_steps: usize) -> usize {
        self.cluster.run_to_completion(max_steps)
    }

    /// Export the recorded history to a JSON file.
    pub fn export_history(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create output directory");
        }
        let json = self.history.to_json();
        fs::write(path, json).expect("failed to write porcupine history");
    }

    /// Append the recorded history as a single JSONL line to a shared file.
    ///
    /// Creates the file (and parent dirs) if it doesn't exist, otherwise
    /// appends. Each call writes exactly one line.
    pub fn append_history_jsonl(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create output directory");
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("failed to open JSONL file for append");
        writeln!(file, "{}", self.history.to_json_line()).expect("failed to append JSONL line");
    }

    /// Build the recorded history.
    pub fn build_history(&self) -> crate::history::PorcupineHistory {
        self.history.build()
    }

    /// Number of events recorded.
    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    /// Access the underlying cluster (read-only).
    pub fn cluster(&self) -> &SimulationCluster<StringValue> {
        &self.cluster
    }
}
