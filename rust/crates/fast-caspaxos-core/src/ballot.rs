use std::cmp::Ordering;
use std::fmt;

// ---------------------------------------------------------------------------
// Ballot — the unified (epoch, round, proposer) ballot
// ---------------------------------------------------------------------------

/// Ballot identifier `(epoch, round, proposer)` with epoch fencing.
///
/// A ballot with `proposer == 0` represents a **fast round** (leaderless, 1-RTT).
/// A ballot with `proposer != 0` represents a **classic round** (leadered).
///
/// Ordering is lexicographic on `(round, proposer)` only — the `epoch` field
/// is used for fencing but does **not** participate in ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Ballot {
    pub epoch: u32,
    pub round: u32,
    pub proposer: u32,
}

/// Error returned when a retry is blocked by an epoch mismatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochMismatch {
    /// The epoch observed in the conflicting ballot.
    pub required_epoch: u32,
}

impl fmt::Display for EpochMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "epoch mismatch: required >= {}", self.required_epoch)
    }
}

impl Ballot {
    /// The zero ballot — the default, representing "no ballot seen yet".
    pub const ZERO: Ballot = Ballot {
        epoch: 0,
        round: 0,
        proposer: 0,
    };

    /// Creates a new ballot with the given epoch, round, and proposer.
    #[must_use]
    pub const fn new(epoch: u32, round: u32, proposer: u32) -> Self {
        Self {
            epoch,
            round,
            proposer,
        }
    }

    /// Create a seed ballot for the given epoch. Use `seed.next_round(0)` for
    /// initial fast, `seed.next_round(pid)` for initial classic.
    #[must_use]
    pub const fn seed(epoch: u32) -> Self {
        Self {
            epoch,
            round: 0,
            proposer: 0,
        }
    }

    /// The initial fast-round ballot `(1, 0)` with epoch 0.
    #[must_use]
    pub const fn initial_fast() -> Self {
        Self {
            epoch: 0,
            round: 1,
            proposer: 0,
        }
    }

    /// The initial classic-round ballot `(1, proposer)` with epoch 0.
    ///
    /// # Panics
    ///
    /// Panics if `proposer` is 0 — classic rounds require a non-zero proposer.
    #[must_use]
    pub const fn initial_classic(proposer: u32) -> Self {
        assert!(proposer > 0, "classic round proposer must be > 0");
        Self {
            epoch: 0,
            round: 1,
            proposer,
        }
    }

    /// Create the next-round ballot for the given proposer.
    ///
    /// Preserves the epoch.
    #[must_use]
    pub const fn next_round(self, proposer: u32) -> Self {
        Self {
            epoch: self.epoch,
            round: self.round + 1,
            proposer,
        }
    }

    /// Returns `true` if this is the zero (default) ballot.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.epoch == 0 && self.round == 0 && self.proposer == 0
    }

    /// Returns `true` if this is a fast round (`proposer == 0`).
    #[must_use]
    pub const fn is_fast_round(self) -> bool {
        self.proposer == 0
    }

    /// Returns `true` if this is a classic round (`proposer != 0`).
    #[must_use]
    pub const fn is_classic_round(self) -> bool {
        self.proposer != 0
    }

    /// Returns the highest epoch seen across two ballots.
    #[must_use]
    pub fn max_epoch(self, other: Self) -> u32 {
        self.epoch.max(other.epoch)
    }

    /// Validate that retrying from `self` (the seed ballot / current config)
    /// against a `conflict` ballot is possible. Returns Err if the conflict
    /// has a higher epoch requiring external action (config refresh).
    pub fn validate_retry(&self, conflict: &Self) -> Result<(), EpochMismatch> {
        if conflict.epoch > self.epoch {
            Err(EpochMismatch {
                required_epoch: conflict.epoch,
            })
        } else {
            Ok(())
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
        if self.epoch == 0 && self.is_zero() {
            write!(f, "Ballot(ø)")
        } else if self.epoch == 0 {
            write!(f, "Ballot(r{}, p{})", self.round, self.proposer)
        } else {
            write!(
                f,
                "Ballot(e{}, r{}, p{})",
                self.epoch, self.round, self.proposer
            )
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ── Ballot unit tests ─────────────────────────────────────────────

    #[test]
    fn zero_ballot_is_default() {
        let z = Ballot::ZERO;
        assert_eq!(z, Ballot::default());
        assert_eq!(z.epoch, 0);
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
        let b = Ballot::new(0, 3, 7);
        let next = b.next_round(2);
        assert_eq!(next.round, 4);
        assert_eq!(next.proposer, 2);
    }

    #[test]
    fn ordering_by_round_first() {
        let low = Ballot::new(0, 1, 99);
        let high = Ballot::new(0, 2, 1);
        assert!(low < high);
    }

    #[test]
    fn ordering_by_proposer_within_same_round() {
        let a = Ballot::new(0, 5, 1);
        let b = Ballot::new(0, 5, 2);
        assert!(a < b);
    }

    #[test]
    fn equal_ballots_compare_equal() {
        let a = Ballot::new(0, 3, 4);
        let b = Ballot::new(0, 3, 4);
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn display_zero() {
        assert_eq!(Ballot::ZERO.to_string(), "Ballot(ø)");
    }

    #[test]
    fn display_non_zero() {
        assert_eq!(Ballot::new(0, 2, 3).to_string(), "Ballot(r2, p3)");
    }

    #[test]
    fn display_fast_round() {
        assert_eq!(Ballot::initial_fast().to_string(), "Ballot(r1, p0)");
    }

    #[test]
    fn max_returns_greater() {
        let a = Ballot::new(0, 1, 5);
        let b = Ballot::new(0, 2, 1);
        assert_eq!(a.max(b), b);
        assert_eq!(b.max(a), b);
    }

    #[test]
    fn max_of_equal_returns_self() {
        let a = Ballot::new(0, 3, 3);
        assert_eq!(a.max(Ballot::new(0, 3, 3)), a);
    }

    #[test]
    fn zero_is_le_all() {
        assert!(Ballot::ZERO < Ballot::new(0, 0, 1));
        assert!(Ballot::ZERO < Ballot::new(0, 1, 0));
        assert!(Ballot::ZERO < Ballot::new(0, 1, 1));
    }

    #[test]
    fn is_fast_round_and_is_classic_round_are_exclusive_for_non_zero() {
        let fast = Ballot::new(0, 1, 0);
        assert!(fast.is_fast_round());
        assert!(!fast.is_classic_round());

        let classic = Ballot::new(0, 1, 1);
        assert!(classic.is_classic_round());
        assert!(!classic.is_fast_round());
    }

    #[test]
    fn next_round_from_zero_is_initial_fast() {
        assert_eq!(Ballot::ZERO.next_round(0), Ballot::initial_fast());
    }

    #[test]
    fn next_round_from_zero_is_initial_classic() {
        assert_eq!(Ballot::ZERO.next_round(5), Ballot::initial_classic(5));
    }

    // ── Epoch-specific unit tests ─────────────────────────────────────

    #[test]
    fn epoch_not_in_ordering() {
        let a = Ballot::new(5, 1, 0);
        let b = Ballot::new(1, 1, 0);
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }

    #[test]
    fn epoch_in_equality() {
        let a = Ballot::new(5, 1, 0);
        let b = Ballot::new(1, 1, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn next_round_preserves_epoch() {
        let b = Ballot::new(5, 3, 1);
        let next = b.next_round(2);
        assert_eq!(next.epoch, 5);
        assert_eq!(next.round, 4);
        assert_eq!(next.proposer, 2);
    }

    #[test]
    fn seed_creates_epoch_base() {
        let s = Ballot::seed(5);
        assert_eq!(s.epoch, 5);
        assert_eq!(s.round, 0);
        assert_eq!(s.proposer, 0);
    }

    #[test]
    fn validate_retry_ok_same_epoch() {
        let seed = Ballot::seed(5);
        let conflict = Ballot::new(5, 10, 2);
        assert!(seed.validate_retry(&conflict).is_ok());

        let lower = Ballot::new(3, 10, 2);
        assert!(seed.validate_retry(&lower).is_ok());
    }

    #[test]
    fn validate_retry_err_higher_epoch() {
        let seed = Ballot::seed(5);
        let conflict = Ballot::new(8, 1, 1);
        let err = seed.validate_retry(&conflict).unwrap_err();
        assert_eq!(err.required_epoch, 8);
    }

    #[test]
    fn max_epoch_returns_higher() {
        let a = Ballot::new(3, 1, 0);
        let b = Ballot::new(7, 2, 0);
        assert_eq!(a.max_epoch(b), 7);
        assert_eq!(b.max_epoch(a), 7);
    }

    #[test]
    fn display_with_epoch() {
        assert_eq!(Ballot::new(5, 1, 0).to_string(), "Ballot(e5, r1, p0)");
    }

    #[test]
    fn display_without_epoch() {
        assert_eq!(Ballot::new(0, 1, 0).to_string(), "Ballot(r1, p0)");
    }

    #[test]
    fn new_with_epoch() {
        let b = Ballot::new(10, 3, 7);
        assert_eq!(b.epoch, 10);
        assert_eq!(b.round, 3);
        assert_eq!(b.proposer, 7);
    }

    #[test]
    fn epoch_mismatch_display() {
        let e = EpochMismatch { required_epoch: 42 };
        assert_eq!(e.to_string(), "epoch mismatch: required >= 42");
    }

    // ── Property tests ────────────────────────────────────────────────

    mod proptests {
        use super::*;

        fn arb_ballot() -> impl Strategy<Value = Ballot> {
            (0u32..1000, 0u32..100).prop_map(|(round, proposer)| Ballot::new(0, round, proposer))
        }

        fn arb_ballot_with_epoch() -> impl Strategy<Value = Ballot> {
            (0u32..100, 0u32..1000, 0u32..100)
                .prop_map(|(epoch, round, proposer)| Ballot::new(epoch, round, proposer))
        }

        proptest! {
            #[test]
            fn ordering_is_total(a in arb_ballot(), b in arb_ballot()) {
                let ord = a.cmp(&b);
                let rev = b.cmp(&a);
                prop_assert_eq!(ord, rev.reverse());
            }

            #[test]
            fn ordering_is_antisymmetric(a in arb_ballot(), b in arb_ballot()) {
                if a <= b && b <= a {
                    // Same Ord ordering means same (round, proposer); epoch=0 for both.
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
                let m = a.max(b);
                prop_assert!(m >= a);
                prop_assert!(m >= b);
            }

            #[test]
            fn max_is_one_of_inputs(a in arb_ballot(), b in arb_ballot()) {
                let m = a.max(b);
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

            #[test]
            fn epoch_does_not_affect_ordering(
                round in 0u32..1000,
                proposer in 0u32..100,
                epoch1 in 0u32..100,
                epoch2 in 0u32..100,
            ) {
                let a = Ballot::new(epoch1, round, proposer);
                let b = Ballot::new(epoch2, round, proposer);
                prop_assert_eq!(a.cmp(&b), Ordering::Equal);
            }

            #[test]
            fn next_round_preserves_epoch_prop(b in arb_ballot_with_epoch(), proposer in 0u32..100) {
                let next = b.next_round(proposer);
                prop_assert_eq!(next.epoch, b.epoch);
            }
        }
    }
}
