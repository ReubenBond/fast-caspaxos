//! Acceptor host adapter for the simulation.
//!
//! Bridges [`AcceptorEngine`] to the simulation transport by collecting
//! protocol responses into an outbox that the cluster drains each tick.
//! State is persisted to an [`AcceptorStateStore`] after every mutation
//! so the acceptor survives crash/restart cycles.

use fast_caspaxos_core::acceptor::{AcceptorEngine, AcceptorHandler, AcceptorState};
use fast_caspaxos_core::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, PreparePromise, PrepareRejection, PrepareRequest,
};

use crate::address::Address;
use crate::state_store::AcceptorStateStore;

// ---------------------------------------------------------------------------
// Output messages
// ---------------------------------------------------------------------------

/// A message produced by an acceptor host in response to a protocol operation.
#[derive(Debug, Clone)]
pub enum AcceptorOutputMessage<V> {
    PreparePromise {
        to: Address,
        payload: PreparePromise<V, Address>,
    },
    PrepareRejection {
        to: Address,
        payload: PrepareRejection<V, Address>,
    },
    AcceptAccepted {
        to: Address,
        payload: AcceptAccepted<Address>,
    },
    AcceptRejected {
        to: Address,
        payload: AcceptRejected<Address>,
    },
}

// ---------------------------------------------------------------------------
// Private handler — routes callbacks into the outbox
// ---------------------------------------------------------------------------

struct HostHandler<'a, V> {
    from: Address,
    outbox: &'a mut Vec<AcceptorOutputMessage<V>>,
}

impl<V: Clone + PartialEq + Default> AcceptorHandler<V, Address> for HostHandler<'_, V> {
    fn on_prepare_promised(&mut self, result: PreparePromise<V, Address>) {
        self.outbox.push(AcceptorOutputMessage::PreparePromise {
            to: self.from,
            payload: result,
        });
    }

    fn on_prepare_rejected(&mut self, result: PrepareRejection<V, Address>) {
        self.outbox.push(AcceptorOutputMessage::PrepareRejection {
            to: self.from,
            payload: result,
        });
    }

    fn on_accept_accepted(&mut self, result: AcceptAccepted<Address>) {
        self.outbox.push(AcceptorOutputMessage::AcceptAccepted {
            to: self.from,
            payload: result,
        });
    }

    fn on_accept_rejected(&mut self, result: AcceptRejected<Address>) {
        self.outbox.push(AcceptorOutputMessage::AcceptRejected {
            to: self.from,
            payload: result,
        });
    }
}

// ---------------------------------------------------------------------------
// AcceptorHost
// ---------------------------------------------------------------------------

/// Wraps an [`AcceptorEngine`] and a durable state store.
///
/// Handles crash/restart with state persistence. Protocol responses are
/// collected in an outbox that the simulation cluster drains after each
/// delivery.
pub struct AcceptorHost<V, S: AcceptorStateStore<V>> {
    address: Address,
    engine: AcceptorEngine<V, Address>,
    state_store: S,
    is_running: bool,
    outbox: Vec<AcceptorOutputMessage<V>>,
}

impl<V, S> AcceptorHost<V, S>
where
    V: Clone + PartialEq + Default + std::fmt::Debug,
    S: AcceptorStateStore<V>,
{
    /// Creates a new host, loading persisted state from the store if available.
    pub fn new(address: Address, state_store: S) -> Self {
        let saved = state_store.load();
        let engine = AcceptorEngine::new(address, saved);
        Self {
            address,
            engine,
            state_store,
            is_running: true,
            outbox: Vec::new(),
        }
    }

    /// Returns the address of this acceptor.
    pub fn address(&self) -> Address {
        self.address
    }

    /// Returns `true` if the host is running (not crashed).
    pub fn is_running(&self) -> bool {
        self.is_running
    }

    /// Returns a snapshot of the current acceptor state.
    pub fn state(&self) -> AcceptorState<V> {
        self.engine.state()
    }

    /// Delivers a prepare request from `from` and collects the response.
    pub fn deliver_prepare(&mut self, from: Address, request: &PrepareRequest) {
        if !self.is_running {
            return;
        }
        let mut handler = HostHandler {
            from,
            outbox: &mut self.outbox,
        };
        self.engine.prepare(&mut handler, request);
        self.save_state();
    }

    /// Delivers an accept request from `from` and collects the response.
    pub fn deliver_accept(&mut self, from: Address, request: &AcceptRequest<V>) {
        if !self.is_running {
            return;
        }
        let mut handler = HostHandler {
            from,
            outbox: &mut self.outbox,
        };
        self.engine.accept(&mut handler, request);
        self.save_state();
    }

    /// Takes all accumulated output messages, leaving the outbox empty.
    pub fn drain_outbox(&mut self) -> Vec<AcceptorOutputMessage<V>> {
        std::mem::take(&mut self.outbox)
    }

    /// Simulates a crash — stops processing and clears the outbox.
    pub fn crash(&mut self) {
        self.is_running = false;
        self.outbox.clear();
    }

    /// Restarts after a crash by reloading state from the store.
    pub fn restart(&mut self) {
        let saved = self.state_store.load();
        self.engine = AcceptorEngine::new(self.address, saved);
        self.is_running = true;
        self.outbox.clear();
    }

    fn save_state(&mut self) {
        self.state_store.save(self.engine.state());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::InMemoryAcceptorStateStore;
    use fast_caspaxos_core::ballot::Ballot;

    type Host = AcceptorHost<Option<i32>, InMemoryAcceptorStateStore<Option<i32>>>;

    fn make_host(ordinal: u32) -> Host {
        let addr = Address::acceptor(ordinal);
        let store = InMemoryAcceptorStateStore::new();
        AcceptorHost::new(addr, store)
    }

    fn proposer_addr() -> Address {
        Address::proposer(1)
    }

    #[test]
    fn new_host_is_running() {
        let host = make_host(0);
        assert!(host.is_running());
    }

    #[test]
    fn prepare_produces_promise_in_outbox() {
        let mut host = make_host(0);
        let request = PrepareRequest {
            ballot: Ballot::initial_classic(1),
        };
        host.deliver_prepare(proposer_addr(), &request);

        let msgs = host.drain_outbox();
        assert_eq!(msgs.len(), 1);
        assert!(matches!(
            &msgs[0],
            AcceptorOutputMessage::PreparePromise { to, .. } if *to == proposer_addr()
        ));
    }

    #[test]
    fn accept_produces_accepted_in_outbox() {
        let mut host = make_host(0);
        // Prepare first to set promised ballot.
        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::initial_classic(1),
            },
        );
        host.drain_outbox();

        // Now accept.
        let request = AcceptRequest {
            ballot: Ballot::initial_classic(1),
            value: Some(42),
            next_ballot_to_prepare: None,
        };
        host.deliver_accept(proposer_addr(), &request);

        let msgs = host.drain_outbox();
        assert_eq!(msgs.len(), 1);
        assert!(matches!(
            &msgs[0],
            AcceptorOutputMessage::AcceptAccepted { to, .. } if *to == proposer_addr()
        ));
    }

    #[test]
    fn state_persisted_after_prepare() {
        let addr = Address::acceptor(0);
        let store: InMemoryAcceptorStateStore<Option<i32>> = InMemoryAcceptorStateStore::new();
        let mut host = AcceptorHost::new(addr, store);

        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::initial_classic(1),
            },
        );

        let state = host.state();
        assert_eq!(state.promised_ballot, Ballot::initial_classic(1));
    }

    #[test]
    fn state_persisted_after_accept() {
        let addr = Address::acceptor(0);
        let store = InMemoryAcceptorStateStore::new();
        let mut host = AcceptorHost::new(addr, store);

        let ballot = Ballot::initial_classic(1);
        host.deliver_prepare(proposer_addr(), &PrepareRequest { ballot });
        host.drain_outbox();

        host.deliver_accept(
            proposer_addr(),
            &AcceptRequest {
                ballot,
                value: Some(99),
                next_ballot_to_prepare: None,
            },
        );

        let state = host.state();
        assert_eq!(state.accepted_ballot, ballot);
        assert_eq!(state.accepted_value, Some(99));
    }

    #[test]
    fn crash_stops_message_processing() {
        let mut host = make_host(0);
        host.crash();
        assert!(!host.is_running());

        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::initial_classic(1),
            },
        );
        assert!(host.drain_outbox().is_empty());
    }

    #[test]
    fn restart_reloads_from_store() {
        let addr = Address::acceptor(0);
        let store = InMemoryAcceptorStateStore::new();
        let mut host = AcceptorHost::new(addr, store);

        let ballot = Ballot::initial_classic(1);
        host.deliver_prepare(proposer_addr(), &PrepareRequest { ballot });
        host.drain_outbox();

        host.deliver_accept(
            proposer_addr(),
            &AcceptRequest {
                ballot,
                value: Some(42),
                next_ballot_to_prepare: None,
            },
        );
        host.drain_outbox();

        // Crash and restart.
        host.crash();
        host.restart();
        assert!(host.is_running());

        // State should be recovered — a new prepare at the same ballot is rejected.
        let state = host.state();
        assert_eq!(state.accepted_ballot, ballot);
        assert_eq!(state.accepted_value, Some(42));
    }

    #[test]
    fn drain_outbox_returns_empty_after_drain() {
        let mut host = make_host(0);
        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::initial_classic(1),
            },
        );
        let first = host.drain_outbox();
        assert_eq!(first.len(), 1);

        let second = host.drain_outbox();
        assert!(second.is_empty());
    }

    #[test]
    fn stale_prepare_produces_rejection() {
        let mut host = make_host(0);
        // Prepare with a higher ballot first.
        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::new(0, 5, 1),
            },
        );
        host.drain_outbox();

        // Now a stale prepare.
        host.deliver_prepare(
            proposer_addr(),
            &PrepareRequest {
                ballot: Ballot::new(0, 2, 1),
            },
        );
        let msgs = host.drain_outbox();
        assert_eq!(msgs.len(), 1);
        assert!(matches!(
            &msgs[0],
            AcceptorOutputMessage::PrepareRejection { .. }
        ));
    }
}
