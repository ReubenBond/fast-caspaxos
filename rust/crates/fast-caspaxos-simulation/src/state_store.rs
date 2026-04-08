//! Acceptor state storage for crash recovery.
//!
//! The [`AcceptorStateStore`] trait abstracts durable storage so the simulation
//! can swap in-memory stores while a real deployment would use disk.

use fast_caspaxos_core::acceptor::AcceptorState;

/// Durable state store for an acceptor.
///
/// Implementations persist [`AcceptorState`] across crashes so that an acceptor
/// can recover its promised/accepted ballot and value on restart.
pub trait AcceptorStateStore<V> {
    /// Load the previously saved state, if any.
    fn load(&self) -> Option<AcceptorState<V>>;

    /// Persist the current acceptor state.
    fn save(&mut self, state: AcceptorState<V>);

    /// Clear all persisted state (used for full reset).
    fn clear(&mut self);
}

/// In-memory implementation of [`AcceptorStateStore`] for simulation/testing.
#[derive(Debug, Clone)]
pub struct InMemoryAcceptorStateStore<V> {
    state: Option<AcceptorState<V>>,
}

impl<V> InMemoryAcceptorStateStore<V> {
    pub fn new() -> Self {
        Self { state: None }
    }
}

impl<V> Default for InMemoryAcceptorStateStore<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Clone> AcceptorStateStore<V> for InMemoryAcceptorStateStore<V> {
    fn load(&self) -> Option<AcceptorState<V>> {
        self.state.clone()
    }

    fn save(&mut self, state: AcceptorState<V>) {
        self.state = Some(state);
    }

    fn clear(&mut self) {
        self.state = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::ballot::Ballot;

    #[test]
    fn empty_store_returns_none() {
        let store: InMemoryAcceptorStateStore<i32> = InMemoryAcceptorStateStore::new();
        assert!(store.load().is_none());
    }

    #[test]
    fn save_and_load_round_trip() {
        let mut store = InMemoryAcceptorStateStore::new();
        let state = AcceptorState {
            promised_ballot: Ballot::initial_fast(),
            accepted_ballot: Ballot::initial_classic(1),
            accepted_value: Some(42),
        };
        store.save(state.clone());
        assert_eq!(store.load(), Some(state));
    }

    #[test]
    fn clear_removes_state() {
        let mut store = InMemoryAcceptorStateStore::new();
        store.save(AcceptorState {
            promised_ballot: Ballot::initial_fast(),
            accepted_ballot: Ballot::ZERO,
            accepted_value: None::<i32>,
        });
        store.clear();
        assert!(store.load().is_none());
    }

    #[test]
    fn save_overwrites_previous() {
        let mut store = InMemoryAcceptorStateStore::new();
        let state1 = AcceptorState {
            promised_ballot: Ballot::initial_fast(),
            accepted_ballot: Ballot::ZERO,
            accepted_value: Some(1),
        };
        let state2 = AcceptorState {
            promised_ballot: Ballot::initial_classic(2),
            accepted_ballot: Ballot::initial_classic(1),
            accepted_value: Some(99),
        };
        store.save(state1);
        store.save(state2.clone());
        assert_eq!(store.load(), Some(state2));
    }

    #[test]
    fn default_is_empty() {
        let store: InMemoryAcceptorStateStore<String> = InMemoryAcceptorStateStore::default();
        assert!(store.load().is_none());
    }
}
