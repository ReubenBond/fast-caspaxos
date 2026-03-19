//! # Fast CASPaxos Core
//!
//! Pure protocol implementation for Fast CASPaxos — a blend of Fast Paxos and CASPaxos
//! that supports both leaderless 1-RTT fast commits and leadered classic commits.
//!
//! This crate contains only synchronous, allocation-free state machines with no I/O.
//! All communication is mediated through handler traits that the host layer implements.
//!
//! ## Architecture
//!
//! - [`Ballot`] — Epoch identifier `(round, proposer)` distinguishing fast vs classic rounds
//! - [`AcceptorEngine`] — Acceptor state machine (prepare/accept phases)
//! - [`ProposerEngine`] — Proposer state machine (the core algorithm)
//! - [`OperationDriver`] — Client-side work queue orchestrator
//!
//! ## Handler Pattern
//!
//! Instead of returning effect enums or allocating vectors, engines call methods on
//! a generic handler trait passed by the caller. This is zero-cost (monomorphized)
//! and zero-allocation:
//!
//! ```rust,ignore
//! engine.handle_prepare_promise(&mut my_host, message);
//! // my_host implements ProposerHandler and receives callbacks directly
//! ```

pub mod acceptor;
pub mod ballot;
pub mod driver;
pub mod messages;
pub mod operation;
pub mod proposer;
pub mod quorum;

// Re-export primary types for convenience.
pub use acceptor::{AcceptorEngine, AcceptorHandler, AcceptorState};
pub use ballot::Ballot;
pub use driver::{DriverCompletion, DriverHandler, OperationDriver, ScheduledProposal};
pub use messages::*;
pub use operation::{FnOperation, Operation, OperationResult, OperationStatus, RoutedOperation};
pub use proposer::{ProposerConfig, ProposerEngine, ProposerHandler, ValueStatus};
