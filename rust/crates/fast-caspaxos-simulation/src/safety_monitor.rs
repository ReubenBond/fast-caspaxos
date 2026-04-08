//! Acceptor state safety monitor for Fast `CASPaxos` simulation.
//!
//! Observes acceptor state snapshots and infers committed values when a quorum
//! of acceptors agree on the same ballot+value. Ported from C#
//! `FastCasAcceptorSafetyMonitor<TValue>`.

use std::collections::BTreeMap;

use fast_caspaxos_core::acceptor::AcceptorState;
use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::quorum;

use crate::address::Address;

/// A committed value inferred from acceptor state quorum agreement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferredCommit<V> {
    pub ballot: Ballot,
    pub value: V,
    pub witnesses: Vec<Address>,
}

/// Errors detected by safety checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafetyViolation<V> {
    /// Two different values were committed at the same ballot.
    DivergentValuesAtBallot {
        ballot: Ballot,
        value_a: V,
        value_b: V,
    },
}

/// Monitors acceptor state snapshots to infer committed values.
///
/// A value is considered committed when enough acceptors have accepted the
/// same ballot+value to form a quorum:
/// - Classic ballots: strict majority
/// - Fast ballots: 3/4 quorum
pub struct AcceptorSafetyMonitor<V> {
    num_acceptors: usize,
    /// Committed values in ballot order.
    commit_history: Vec<InferredCommit<V>>,
}

impl<V: Clone + PartialEq + std::fmt::Debug> AcceptorSafetyMonitor<V> {
    pub fn new(num_acceptors: usize) -> Self {
        Self {
            num_acceptors,
            commit_history: Vec::new(),
        }
    }

    /// Observe the current state of all acceptors and check for new commits.
    ///
    /// Groups acceptors by `(accepted_ballot, accepted_value)` and checks
    /// if any group forms a quorum.
    pub fn observe(&mut self, states: &BTreeMap<Address, AcceptorState<V>>) {
        // Group acceptors by (accepted_ballot, accepted_value).
        // We use a Vec of groups since V may not be Hash.
        let mut groups: Vec<(Ballot, V, Vec<Address>)> = Vec::new();

        for (addr, state) in states {
            if state.accepted_ballot.is_zero() {
                continue;
            }

            let mut found = false;
            for (ballot, value, witnesses) in &mut groups {
                if *ballot == state.accepted_ballot && *value == state.accepted_value {
                    witnesses.push(*addr);
                    found = true;
                    break;
                }
            }
            if !found {
                groups.push((
                    state.accepted_ballot,
                    state.accepted_value.clone(),
                    vec![*addr],
                ));
            }
        }

        for (ballot, value, witnesses) in groups {
            if !quorum::has_quorum(ballot, witnesses.len(), self.num_acceptors) {
                continue;
            }

            // Check if this commit is already recorded.
            let already_recorded = self
                .commit_history
                .iter()
                .any(|c| c.ballot == ballot && c.value == value);
            if already_recorded {
                continue;
            }

            self.commit_history.push(InferredCommit {
                ballot,
                value,
                witnesses,
            });
        }
    }

    /// Get the full commit history.
    pub fn commit_history(&self) -> &[InferredCommit<V>] {
        &self.commit_history
    }

    /// Get the latest committed value, if any.
    pub fn latest_commit(&self) -> Option<&InferredCommit<V>> {
        self.commit_history.last()
    }

    /// Reset the monitor for a new simulation run.
    pub fn reset(&mut self) {
        self.commit_history.clear();
    }

    /// Check safety: no two different values committed at the same ballot.
    pub fn check_safety(&self) -> Result<(), SafetyViolation<V>> {
        for i in 0..self.commit_history.len() {
            for j in (i + 1)..self.commit_history.len() {
                let a = &self.commit_history[i];
                let b = &self.commit_history[j];
                if a.ballot == b.ballot && a.value != b.value {
                    return Err(SafetyViolation::DivergentValuesAtBallot {
                        ballot: a.ballot,
                        value_a: a.value.clone(),
                        value_b: b.value.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_acceptor_state<V: Default>(ballot: Ballot, value: V) -> AcceptorState<V> {
        AcceptorState {
            promised_ballot: ballot,
            accepted_ballot: ballot,
            accepted_value: value,
        }
    }

    fn default_acceptor_state<V: Default>() -> AcceptorState<V> {
        AcceptorState::default()
    }

    fn acceptor_addr(ordinal: u32) -> Address {
        Address::acceptor(ordinal)
    }

    // ── Tests ─────────────────────────────────────────────────────────

    #[test]
    fn empty_observe_no_commits() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let states: BTreeMap<Address, AcceptorState<i32>> = (0..5)
            .map(|i| (acceptor_addr(i), default_acceptor_state()))
            .collect();

        monitor.observe(&states);
        assert!(monitor.commit_history().is_empty());
        assert!(monitor.latest_commit().is_none());
    }

    #[test]
    fn classic_quorum_infers_commit() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot = Ballot::initial_classic(1);

        let mut states: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        // 3 of 5 acceptors accept the same classic ballot+value
        for i in 0..3 {
            states.insert(acceptor_addr(i), make_acceptor_state(ballot, 42));
        }
        // 2 acceptors haven't accepted anything
        for i in 3..5 {
            states.insert(acceptor_addr(i), default_acceptor_state());
        }

        monitor.observe(&states);
        assert_eq!(monitor.commit_history().len(), 1);

        let commit = monitor.latest_commit().unwrap();
        assert_eq!(commit.ballot, ballot);
        assert_eq!(commit.value, 42);
        assert_eq!(commit.witnesses.len(), 3);
        assert!(monitor.check_safety().is_ok());
    }

    #[test]
    fn fast_quorum_infers_commit() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot = Ballot::initial_fast();

        let mut states: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        // 4 of 5 acceptors accept the same fast ballot+value (need ⌈3*5/4⌉ = 4)
        for i in 0..4 {
            states.insert(acceptor_addr(i), make_acceptor_state(ballot, 99));
        }
        states.insert(acceptor_addr(4), default_acceptor_state());

        monitor.observe(&states);
        assert_eq!(monitor.commit_history().len(), 1);

        let commit = monitor.latest_commit().unwrap();
        assert_eq!(commit.ballot, ballot);
        assert_eq!(commit.value, 99);
        assert_eq!(commit.witnesses.len(), 4);
        assert!(monitor.check_safety().is_ok());
    }

    #[test]
    fn below_quorum_no_commit() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot = Ballot::initial_classic(1);

        let mut states: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        // Only 2 of 5 accept — not enough for classic quorum (need 3)
        for i in 0..2 {
            states.insert(acceptor_addr(i), make_acceptor_state(ballot, 42));
        }
        for i in 2..5 {
            states.insert(acceptor_addr(i), default_acceptor_state());
        }

        monitor.observe(&states);
        assert!(monitor.commit_history().is_empty());
    }

    #[test]
    fn divergent_values_detected() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot = Ballot::initial_classic(1);

        // Manually inject two divergent commits at the same ballot
        // to verify check_safety catches the violation.
        monitor.commit_history.push(InferredCommit {
            ballot,
            value: 1,
            witnesses: vec![acceptor_addr(0), acceptor_addr(1), acceptor_addr(2)],
        });
        monitor.commit_history.push(InferredCommit {
            ballot,
            value: 2,
            witnesses: vec![acceptor_addr(3), acceptor_addr(4)],
        });

        let result = monitor.check_safety();
        assert!(result.is_err());
        match result.unwrap_err() {
            SafetyViolation::DivergentValuesAtBallot {
                ballot: b,
                value_a,
                value_b,
            } => {
                assert_eq!(b, ballot);
                assert_eq!(value_a, 1);
                assert_eq!(value_b, 2);
            }
        }
    }

    #[test]
    fn multiple_commits_tracked() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot1 = Ballot::initial_classic(1);
        let ballot2 = ballot1.next_round(1);

        // First commit
        let mut states1: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        for i in 0..3 {
            states1.insert(acceptor_addr(i), make_acceptor_state(ballot1, 10));
        }
        for i in 3..5 {
            states1.insert(acceptor_addr(i), default_acceptor_state());
        }
        monitor.observe(&states1);

        // Second commit at a higher ballot
        let mut states2: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        for i in 0..3 {
            states2.insert(acceptor_addr(i), make_acceptor_state(ballot2, 20));
        }
        for i in 3..5 {
            states2.insert(acceptor_addr(i), default_acceptor_state());
        }
        monitor.observe(&states2);

        assert_eq!(monitor.commit_history().len(), 2);
        assert_eq!(monitor.commit_history()[0].ballot, ballot1);
        assert_eq!(monitor.commit_history()[0].value, 10);
        assert_eq!(monitor.commit_history()[1].ballot, ballot2);
        assert_eq!(monitor.commit_history()[1].value, 20);
        assert!(monitor.check_safety().is_ok());
    }

    #[test]
    fn reset_clears_history() {
        let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
        let ballot = Ballot::initial_classic(1);

        let mut states: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
        for i in 0..3 {
            states.insert(acceptor_addr(i), make_acceptor_state(ballot, 42));
        }
        for i in 3..5 {
            states.insert(acceptor_addr(i), default_acceptor_state());
        }
        monitor.observe(&states);
        assert_eq!(monitor.commit_history().len(), 1);

        monitor.reset();
        assert!(monitor.commit_history().is_empty());
        assert!(monitor.latest_commit().is_none());
    }
}
