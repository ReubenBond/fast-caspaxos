use std::cmp::Ordering;
use std::fmt;

/// Epoch identifier `(round, proposer)` used to distinguish fast vs classic rounds.
///
/// A ballot with `proposer == 0` represents a **fast round** (leaderless, 1-RTT).
/// A ballot with `proposer != 0` represents a **classic round** (leadered).
///
/// Ordering is lexicographic: first by `round`, then by `proposer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Ballot {
    pub round: u32,
    pub proposer: u32,
}

impl Ballot {
    /// The zero ballot — the default, representing "no ballot seen yet".
    pub const ZERO: Ballot = Ballot {
        round: 0,
        proposer: 0,
    };

    /// Creates a new ballot with the given round and proposer.
    #[must_use]
    pub const fn new(round: u32, proposer: u32) -> Self {
        Self { round, proposer }
    }

    /// The initial fast-round ballot `(1, 0)`.
    #[must_use]
    pub const fn initial_fast() -> Self {
        Self {
            round: 1,
            proposer: 0,
        }
    }

    /// The initial classic-round ballot `(1, proposer)`.
    ///
    /// # Panics
    ///
    /// Panics if `proposer` is 0 — classic rounds require a non-zero proposer.
    #[must_use]
    pub const fn initial_classic(proposer: u32) -> Self {
        assert!(proposer > 0, "classic round proposer must be > 0");
        Self {
            round: 1,
            proposer,
        }
    }

    /// Returns a new ballot in the next round for the given proposer.
    #[must_use]
    pub const fn next_round(self, proposer: u32) -> Self {
        Self {
            round: self.round + 1,
            proposer,
        }
    }

    /// Returns `true` if this is the zero (default) ballot.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.round == 0 && self.proposer == 0
    }

    /// Returns `true` if this ballot represents a fast round (`proposer == 0`).
    #[must_use]
    pub const fn is_fast_round(self) -> bool {
        self.proposer == 0
    }

    /// Returns `true` if this ballot represents a classic round (`proposer != 0`).
    #[must_use]
    pub const fn is_classic_round(self) -> bool {
        self.proposer != 0
    }

    /// Returns the greater of two ballots.
    #[must_use]
    pub fn max(left: Ballot, right: Ballot) -> Ballot {
        if left >= right {
            left
        } else {
            right
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.round
            .cmp(&other.round)
            .then(self.proposer.cmp(&other.proposer))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_zero() {
            write!(f, "Ballot(ø)")
        } else {
            write!(f, "Ballot(r{}, p{})", self.round, self.proposer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ── Unit tests ──────────────────────────────────────────────────

    #[test]
    fn zero_ballot_is_default() {
        let z = Ballot::ZERO;
        assert_eq!(z, Ballot::default());
        assert_eq!(z.round, 0);
        assert_eq!(z.proposer, 0);
        assert!(z.is_zero());
    }

    #[test]
    fn initial_fast_is_round_1_proposer_0() {
        let b = Ballot::initial_fast();
        assert_eq!(b.round, 1);
        assert_eq!(b.proposer, 0);
        assert!(b.is_fast_round());
        assert!(!b.is_classic_round());
        assert!(!b.is_zero());
    }

    #[test]
    fn initial_classic_is_round_1_with_proposer() {
        let b = Ballot::initial_classic(5);
        assert_eq!(b.round, 1);
        assert_eq!(b.proposer, 5);
        assert!(b.is_classic_round());
        assert!(!b.is_fast_round());
        assert!(!b.is_zero());
    }

    #[test]
    #[should_panic(expected = "classic round proposer must be > 0")]
    fn initial_classic_panics_on_zero_proposer() {
        let _ = Ballot::initial_classic(0);
    }

    #[test]
    fn next_round_increments_round() {
        let b = Ballot::new(3, 7);
        let next = b.next_round(2);
        assert_eq!(next.round, 4);
        assert_eq!(next.proposer, 2);
    }

    #[test]
    fn ordering_by_round_first() {
        let low = Ballot::new(1, 99);
        let high = Ballot::new(2, 1);
        assert!(low < high);
    }

    #[test]
    fn ordering_by_proposer_within_same_round() {
        let a = Ballot::new(5, 1);
        let b = Ballot::new(5, 2);
        assert!(a < b);
    }

    #[test]
    fn equal_ballots_compare_equal() {
        let a = Ballot::new(3, 4);
        let b = Ballot::new(3, 4);
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn display_zero() {
        assert_eq!(Ballot::ZERO.to_string(), "Ballot(ø)");
    }

    #[test]
    fn display_non_zero() {
        assert_eq!(Ballot::new(2, 3).to_string(), "Ballot(r2, p3)");
    }

    #[test]
    fn display_fast_round() {
        assert_eq!(Ballot::initial_fast().to_string(), "Ballot(r1, p0)");
    }

    #[test]
    fn max_returns_greater() {
        let a = Ballot::new(1, 5);
        let b = Ballot::new(2, 1);
        assert_eq!(Ballot::max(a, b), b);
        assert_eq!(Ballot::max(b, a), b);
    }

    #[test]
    fn max_of_equal_returns_left() {
        let a = Ballot::new(3, 3);
        let b = Ballot::new(3, 3);
        assert_eq!(Ballot::max(a, b), a);
    }

    #[test]
    fn zero_is_le_all() {
        assert!(Ballot::ZERO < Ballot::new(0, 1));
        assert!(Ballot::ZERO < Ballot::new(1, 0));
        assert!(Ballot::ZERO < Ballot::new(1, 1));
    }

    #[test]
    fn is_fast_round_and_is_classic_round_are_exclusive_for_non_zero() {
        let fast = Ballot::new(1, 0);
        assert!(fast.is_fast_round());
        assert!(!fast.is_classic_round());

        let classic = Ballot::new(1, 1);
        assert!(classic.is_classic_round());
        assert!(!classic.is_fast_round());
    }

    // ── Property tests ──────────────────────────────────────────────

    fn arb_ballot() -> impl Strategy<Value = Ballot> {
        (0u32..1000, 0u32..100).prop_map(|(round, proposer)| Ballot::new(round, proposer))
    }

    proptest! {
        #[test]
        fn ordering_is_total(a in arb_ballot(), b in arb_ballot()) {
            // Exactly one of <, ==, > holds
            let ord = a.cmp(&b);
            let rev = b.cmp(&a);
            prop_assert_eq!(ord, rev.reverse());
        }

        #[test]
        fn ordering_is_antisymmetric(a in arb_ballot(), b in arb_ballot()) {
            if a <= b && b <= a {
                prop_assert_eq!(a, b);
            }
        }

        #[test]
        fn ordering_is_transitive(
            a in arb_ballot(),
            b in arb_ballot(),
            c in arb_ballot(),
        ) {
            if a <= b && b <= c {
                prop_assert!(a <= c);
            }
            if a >= b && b >= c {
                prop_assert!(a >= c);
            }
        }

        #[test]
        fn max_is_gte_both(a in arb_ballot(), b in arb_ballot()) {
            let m = Ballot::max(a, b);
            prop_assert!(m >= a);
            prop_assert!(m >= b);
        }

        #[test]
        fn max_is_one_of_inputs(a in arb_ballot(), b in arb_ballot()) {
            let m = Ballot::max(a, b);
            prop_assert!(m == a || m == b);
        }

        #[test]
        fn zero_is_minimum(b in arb_ballot()) {
            prop_assert!(Ballot::ZERO <= b);
        }

        #[test]
        fn next_round_is_strictly_greater(b in arb_ballot(), proposer in 0u32..100) {
            let next = b.next_round(proposer);
            prop_assert!(next > b);
        }

        #[test]
        fn is_fast_and_classic_are_mutually_exclusive(b in arb_ballot()) {
            prop_assert!(b.is_fast_round() != b.is_classic_round());
        }

        #[test]
        fn display_roundtrip_zero_or_non_zero(b in arb_ballot()) {
            let s = b.to_string();
            if b.is_zero() {
                prop_assert_eq!(s, "Ballot(ø)");
            } else {
                prop_assert!(s.starts_with("Ballot(r"));
                prop_assert!(s.contains(", p"));
                prop_assert!(s.ends_with(')'));
            }
        }
    }
}
