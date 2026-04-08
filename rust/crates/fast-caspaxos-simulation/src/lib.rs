//! # Fast `CASPaxos` Simulation
//!
//! Deterministic simulation harness for testing Fast `CASPaxos` protocol implementations.

pub mod acceptor_host;
pub mod address;
pub mod chaos;
pub mod cluster;
pub mod consensus_register;
pub mod diagnostics;
pub mod history;
pub mod network;
pub mod porcupine_harness;
pub mod porcupine_ops;
pub mod proposer_host;
pub mod safety_checker;
pub mod safety_monitor;
pub mod state_store;
pub mod value;

#[cfg(feature = "turmoil")]
pub mod turmoil_sim;
