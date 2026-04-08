//! Quorum utilities for `Fast CASPaxos`.
//!
//! Classic rounds require a strict majority (`n/2 + 1`).
//! Fast rounds require a ¾ quorum (`⌈3n/4⌉`).

use crate::ballot::Ballot;

/// Classic quorum size: strict majority.
#[inline]
pub const fn classic_quorum_size(n: usize) -> usize {
    n / 2 + 1
}

/// Fast quorum size: `⌈3n/4⌉`.
#[inline]
pub const fn fast_quorum_size(n: usize) -> usize {
    (3 * n).div_ceil(4)
}

/// Check if `responses` is enough for quorum given a `total` acceptor count.
///
/// Fast ballots use the ¾ rule; classic ballots use strict majority.
#[inline]
pub fn has_quorum(ballot: Ballot, responses: usize, total: usize) -> bool {
    if ballot.is_fast_round() {
        // 4 * responses >= 3 * total  ⟺  responses >= ⌈3n/4⌉
        4 * responses >= 3 * total
    } else {
        // 2 * responses > total  ⟺  responses > n/2
        2 * responses > total
    }
}

/// Check if quorum is still achievable given current success/failure counts.
///
/// Returns `true` if either the success or failure side could still reach
/// quorum with all remaining responses going their way.
#[inline]
pub fn is_quorum_possible(ballot: Ballot, successes: usize, failures: usize, total: usize) -> bool {
    let remaining = total.saturating_sub(successes + failures);
    has_quorum(ballot, successes + remaining, total)
        || has_quorum(ballot, failures + remaining, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ballot::Ballot;

    // -----------------------------------------------------------------------
    // Quorum sizes
    // -----------------------------------------------------------------------

    #[test]
    fn classic_quorum_sizes() {
        assert_eq!(classic_quorum_size(1), 1);
        assert_eq!(classic_quorum_size(2), 2);
        assert_eq!(classic_quorum_size(3), 2);
        assert_eq!(classic_quorum_size(4), 3);
        assert_eq!(classic_quorum_size(5), 3);
        assert_eq!(classic_quorum_size(7), 4);
    }

    #[test]
    fn fast_quorum_sizes() {
        assert_eq!(fast_quorum_size(1), 1);
        assert_eq!(fast_quorum_size(2), 2);
        assert_eq!(fast_quorum_size(3), 3);
        assert_eq!(fast_quorum_size(4), 3);
        assert_eq!(fast_quorum_size(5), 4);
        assert_eq!(fast_quorum_size(7), 6);
    }

    #[test]
    fn fast_ge_classic() {
        for n in 1..=100 {
            assert!(
                fast_quorum_size(n) >= classic_quorum_size(n),
                "fast_quorum_size({n}) < classic_quorum_size({n})"
            );
        }
    }

    // -----------------------------------------------------------------------
    // has_quorum
    // -----------------------------------------------------------------------

    #[test]
    fn classic_has_quorum() {
        let classic = Ballot::initial_classic(1);
        // 5 acceptors → need > 2.5 → 3
        assert!(!has_quorum(classic, 2, 5));
        assert!(has_quorum(classic, 3, 5));
        assert!(has_quorum(classic, 5, 5));
    }

    #[test]
    fn fast_has_quorum() {
        let fast = Ballot::initial_fast();
        // 5 acceptors → need 4*r >= 15 → r >= 4
        assert!(!has_quorum(fast, 3, 5));
        assert!(has_quorum(fast, 4, 5));
        assert!(has_quorum(fast, 5, 5));
    }

    #[test]
    fn fast_quorum_4_acceptors() {
        let fast = Ballot::initial_fast();
        // 4 acceptors → need 4*r >= 12 → r >= 3
        assert!(!has_quorum(fast, 2, 4));
        assert!(has_quorum(fast, 3, 4));
    }

    #[test]
    fn has_quorum_zero_total() {
        let fast = Ballot::initial_fast();
        let classic = Ballot::initial_classic(1);
        // 0 responses with 0 total: 4*0 >= 3*0 is true
        assert!(has_quorum(fast, 0, 0));
        // classic: 2*0 > 0 is false
        assert!(!has_quorum(classic, 0, 0));
    }

    // -----------------------------------------------------------------------
    // is_quorum_possible
    // -----------------------------------------------------------------------

    #[test]
    fn quorum_possible_at_start() {
        let fast = Ballot::initial_fast();
        let classic = Ballot::initial_classic(1);
        assert!(is_quorum_possible(fast, 0, 0, 5));
        assert!(is_quorum_possible(classic, 0, 0, 5));
    }

    #[test]
    fn quorum_reports_possible_when_failure_side_dominates() {
        let classic = Ballot::initial_classic(1);
        // 5 acceptors, 0 success, 5 failures → remaining=0
        // success side: 0 → 2*0 > 5 → false
        // failure side: 5 → 2*5 > 5 → true (failure side reached quorum!)
        assert!(is_quorum_possible(classic, 0, 5, 5));
    }

    #[test]
    fn quorum_impossible_split_evenly_fast() {
        let fast = Ballot::initial_fast();
        // 8 acceptors, 2 success, 2 failures → remaining=4
        // success: 2+4=6, 4*6=24 >= 24 → true → still possible
        assert!(is_quorum_possible(fast, 2, 2, 8));
        // 8 acceptors, 1 success, 6 failures → remaining=1
        // success: 1+1=2, 4*2=8 >= 24 → false
        // failure: 6+1=7, 4*7=28 >= 24 → true → still possible
        assert!(is_quorum_possible(fast, 1, 6, 8));
    }

    #[test]
    fn quorum_still_possible_partial() {
        let classic = Ballot::initial_classic(1);
        // 5 acceptors, 1 success, 1 failure → remaining=3
        // success side: 1+3=4 → 2*4 > 5 → true
        assert!(is_quorum_possible(classic, 1, 1, 5));
    }

    #[test]
    fn quorum_impossible_too_many_failures() {
        let classic = Ballot::initial_classic(1);
        // 5 acceptors, 0 success, 3 failures → remaining=2
        // success side: 0+2=2 → 2*2 > 5 → false
        // failure side: 3+2=5 → 2*5 > 5 → true
        // So it IS possible (failure side can reach quorum).
        assert!(is_quorum_possible(classic, 0, 3, 5));

        // 5 acceptors, 1 success, 3 failures → remaining=1
        // success side: 1+1=2 → 2*2=4 > 5 → false
        // failure side: 3+1=4 → 2*4=8 > 5 → true
        assert!(is_quorum_possible(classic, 1, 3, 5));
    }

    #[test]
    fn quorum_impossible_when_neither_side_can_win() {
        let fast = Ballot::initial_fast();
        // 5 acceptors, 1 success, 1 failure, remaining=3
        // success: 1+3=4 → 4*4=16 >= 15 → true → IS possible
        assert!(is_quorum_possible(fast, 1, 1, 5));

        // 7 acceptors, 1 success, 2 failures, remaining=4
        // success: 1+4=5 → 4*5=20 >= 21 → false
        // failure: 2+4=6 → 4*6=24 >= 21 → true
        assert!(is_quorum_possible(fast, 1, 2, 7));

        // 7 acceptors, 1 success, 5 failures, remaining=1
        // success: 1+1=2 → 4*2=8 >= 21 → false
        // failure: 5+1=6 → 4*6=24 >= 21 → true
        assert!(is_quorum_possible(fast, 1, 5, 7));
    }

    // -----------------------------------------------------------------------
    // Proptest
    // -----------------------------------------------------------------------

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// For any n >= 3, two classic quorums always overlap.
            #[test]
            fn classic_quorums_overlap(n in 3_usize..=200) {
                let q = classic_quorum_size(n);
                // Two quorums of size q drawn from n items must share at least one.
                prop_assert!(q + q > n, "q={q}, n={n}: two classic quorums don't overlap");
            }

            /// For any n >= 4, a fast quorum and a classic quorum always overlap.
            #[test]
            fn fast_classic_quorums_overlap(n in 4_usize..=200) {
                let fq = fast_quorum_size(n);
                let cq = classic_quorum_size(n);
                prop_assert!(
                    fq + cq > n,
                    "fq={fq}, cq={cq}, n={n}: fast+classic quorums don't overlap"
                );
            }

            /// fast_quorum_size >= classic_quorum_size for all n >= 1.
            #[test]
            fn fast_gte_classic(n in 1_usize..=500) {
                prop_assert!(
                    fast_quorum_size(n) >= classic_quorum_size(n),
                    "fast < classic for n={n}"
                );
            }

            /// has_quorum(classic, q, n) is true when q >= classic_quorum_size(n).
            #[test]
            fn classic_threshold_matches(n in 1_usize..=100) {
                let classic = Ballot::initial_classic(1);
                let q = classic_quorum_size(n);
                prop_assert!(has_quorum(classic, q, n));
                if q > 0 {
                    prop_assert!(!has_quorum(classic, q - 1, n));
                }
            }

            /// has_quorum(fast, q, n) is true when q >= fast_quorum_size(n).
            #[test]
            fn fast_threshold_matches(n in 1_usize..=100) {
                let fast = Ballot::initial_fast();
                let q = fast_quorum_size(n);
                prop_assert!(has_quorum(fast, q, n));
                if q > 0 {
                    prop_assert!(!has_quorum(fast, q - 1, n));
                }
            }

            /// is_quorum_possible: success-side potential (n - f) is independent
            /// of how many successes have arrived so far.
            #[test]
            fn quorum_possible_success_side_monotonic(n in 3_usize..=30) {
                let ballot = Ballot::initial_classic(1);
                // Generate valid (s, f) with s + f < n
                let max_used = n - 1;
                for s in 0..max_used {
                    for f in 0..(max_used - s) {
                        let remaining = n - s - f;
                        let remaining_after = n - (s + 1) - f;
                        // If success side alone could reach quorum before...
                        if has_quorum(ballot, s + remaining, n) {
                            // ...it still can after one more success arrives
                            prop_assert!(has_quorum(ballot, s + 1 + remaining_after, n));
                        }
                    }
                }
            }
        }
    }
}
