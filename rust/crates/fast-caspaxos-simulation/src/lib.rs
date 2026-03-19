//! # Fast CASPaxos Simulation
//!
//! Deterministic simulation harness for testing Fast CASPaxos protocol implementations.

pub mod acceptor_host;
pub mod address;
pub mod cluster;
pub mod network;
pub mod proposer_host;
pub mod safety_checker;
pub mod safety_monitor;
pub mod state_store;
pub mod value;
