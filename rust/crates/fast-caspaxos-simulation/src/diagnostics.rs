//! Structured tracing for Fast `CASPaxos` simulation.
//!
//! Provides trace events for protocol lifecycle, message delivery,
//! and fault injection. Uses the `tracing` crate.

use crate::address::Address;
use fast_caspaxos_core::ballot::Ballot;

/// Trace a proposal being submitted to a proposer.
pub fn trace_proposal_submitted(proposer: Address, request_id: u32) {
    tracing::info!(
        proposer = %proposer,
        request_id = request_id,
        "proposal submitted"
    );
}

/// Trace a message being delivered.
pub fn trace_message_delivered(from: Address, to: Address, msg_type: &str) {
    tracing::debug!(
        from = %from,
        to = %to,
        msg_type = msg_type,
        "message delivered"
    );
}

/// Trace a message being dropped by the network.
pub fn trace_message_dropped(from: Address, to: Address, msg_type: &str, reason: &str) {
    tracing::debug!(
        from = %from,
        to = %to,
        msg_type = msg_type,
        reason = reason,
        "message dropped"
    );
}

/// Trace a client response being received.
pub fn trace_proposal_completed(request_id: u32, round: u32) {
    tracing::info!(request_id = request_id, round = round, "proposal completed");
}

/// Trace a node crash.
pub fn trace_node_crashed(addr: Address) {
    tracing::warn!(node = %addr, "node crashed");
}

/// Trace a node restart.
pub fn trace_node_restarted(addr: Address) {
    tracing::info!(node = %addr, "node restarted");
}

/// Trace a partition being created.
pub fn trace_partition_created(a: Address, b: Address) {
    tracing::warn!(node_a = %a, node_b = %b, "partition created");
}

/// Trace a partition being healed.
pub fn trace_partition_healed(a: Address, b: Address) {
    tracing::info!(node_a = %a, node_b = %b, "partition healed");
}

/// Trace a committed value inferred by the safety monitor.
pub fn trace_commit_inferred(ballot: Ballot, num_witnesses: usize) {
    tracing::info!(
        ballot = %ballot,
        witnesses = num_witnesses,
        "commit inferred by safety monitor"
    );
}

/// Trace a timeout being triggered.
pub fn trace_timeout_triggered(addr: Address) {
    tracing::debug!(node = %addr, "timeout triggered");
}
