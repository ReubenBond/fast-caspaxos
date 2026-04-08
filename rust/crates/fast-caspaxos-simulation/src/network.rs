//! Deterministic, in-memory network simulation with fault injection.
//!
//! Manages partitions, message drop rates, and link latencies between
//! [`Address`]es. All state is deterministic when used with a seeded RNG.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use rand::Rng;

use crate::address::Address;

/// What happened when a message was sent through the simulated network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// Message delivered successfully.
    Success,
    /// Message was randomly dropped based on the configured drop rate.
    Dropped,
    /// Message could not be delivered because the link is partitioned.
    Partitioned,
}

/// Per-link latency configuration.
#[derive(Debug, Clone, Copy)]
pub struct LinkLatency {
    /// Fixed component of the delay.
    pub base_delay: Duration,
    /// Maximum random jitter added on top of `base_delay`.
    pub max_jitter: Duration,
}

impl LinkLatency {
    pub fn new(base_delay: Duration, max_jitter: Duration) -> Self {
        Self {
            base_delay,
            max_jitter,
        }
    }

    /// Zero-latency link (useful for tests that don't care about timing).
    pub fn zero() -> Self {
        Self {
            base_delay: Duration::ZERO,
            max_jitter: Duration::ZERO,
        }
    }

    /// Sample a concrete latency value using the given RNG.
    pub fn sample(&self, rng: &mut impl Rng) -> Duration {
        if self.max_jitter.is_zero() {
            return self.base_delay;
        }
        #[allow(clippy::cast_possible_truncation)]
        let max_jitter_nanos = self.max_jitter.as_nanos() as u64;
        let jitter_nanos = rng.gen_range(0..=max_jitter_nanos);
        self.base_delay + Duration::from_nanos(jitter_nanos)
    }
}

/// Normalize a pair of addresses into `(min, max)` order so that
/// partitions are always stored bidirectionally.
fn normalize_pair(a: Address, b: Address) -> (Address, Address) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Deterministic network simulation with fault injection.
///
/// Manages partitions, message drop rates, and link latencies between
/// addresses. All state is deterministic when used with a seeded RNG.
///
/// # Determinism
///
/// [`BTreeMap`] and [`BTreeSet`] are used throughout so that iteration
/// order is independent of hash seeds or allocation layout.
pub struct SimulationNetwork {
    /// Bidirectional partitions stored as normalized `(min, max)` pairs.
    partitions: BTreeSet<(Address, Address)>,
    /// Per-directed-link drop rates: `(from, to) -> probability [0.0, 1.0]`.
    drop_rates: BTreeMap<(Address, Address), f64>,
    /// Per-directed-link latency overrides.
    link_latencies: BTreeMap<(Address, Address), LinkLatency>,
    /// Default latency applied when no per-link override exists.
    default_latency: LinkLatency,
}

impl SimulationNetwork {
    pub fn new(default_latency: LinkLatency) -> Self {
        Self {
            partitions: BTreeSet::new(),
            drop_rates: BTreeMap::new(),
            link_latencies: BTreeMap::new(),
            default_latency,
        }
    }

    // --- Partition management ---

    /// Create a bidirectional partition between `a` and `b`.
    ///
    /// Messages in both directions will be blocked.
    pub fn create_partition(&mut self, a: Address, b: Address) {
        self.partitions.insert(normalize_pair(a, b));
    }

    /// Heal a previously created partition between `a` and `b`.
    pub fn heal_partition(&mut self, a: Address, b: Address) {
        self.partitions.remove(&normalize_pair(a, b));
    }

    /// Returns `true` if traffic between `from` and `to` is blocked.
    pub fn is_partitioned(&self, from: Address, to: Address) -> bool {
        self.partitions.contains(&normalize_pair(from, to))
    }

    /// Remove all partitions.
    pub fn clear_all_partitions(&mut self) {
        self.partitions.clear();
    }

    // --- Drop rates ---

    /// Set the probability that a message from `from` to `to` is dropped.
    ///
    /// `rate` must be in `[0.0, 1.0]`.
    ///
    /// # Panics
    ///
    /// Panics if `rate` is not in `[0.0, 1.0]`.
    pub fn set_drop_rate(&mut self, from: Address, to: Address, rate: f64) {
        assert!(
            (0.0..=1.0).contains(&rate),
            "drop rate must be in [0.0, 1.0], got {rate}"
        );
        if rate == 0.0 {
            self.drop_rates.remove(&(from, to));
        } else {
            self.drop_rates.insert((from, to), rate);
        }
    }

    /// Remove any configured drop rate for the directed link `from → to`.
    pub fn clear_drop_rate(&mut self, from: Address, to: Address) {
        self.drop_rates.remove(&(from, to));
    }

    // --- Latency ---

    /// Set a latency override for the directed link `from → to`.
    pub fn set_link_latency(&mut self, from: Address, to: Address, latency: LinkLatency) {
        self.link_latencies.insert((from, to), latency);
    }

    /// Get the latency configuration for `from → to`, falling back to the
    /// default if no override has been set.
    pub fn get_link_latency(&self, from: Address, to: Address) -> LinkLatency {
        self.link_latencies
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_latency)
    }

    /// Sample a concrete latency value for `from → to` using the given RNG.
    pub fn sample_latency(&self, from: Address, to: Address, rng: &mut impl Rng) -> Duration {
        self.get_link_latency(from, to).sample(rng)
    }

    // --- Delivery check ---

    /// Determine whether a message from `from` to `to` would be delivered.
    ///
    /// Checks partitions first, then probabilistic drops. Returns
    /// [`DeliveryStatus::Success`] if neither condition fires.
    pub fn check_delivery(&self, from: Address, to: Address, rng: &mut impl Rng) -> DeliveryStatus {
        if self.is_partitioned(from, to) {
            return DeliveryStatus::Partitioned;
        }
        if let Some(&rate) = self.drop_rates.get(&(from, to)) {
            if rng.gen::<f64>() < rate {
                return DeliveryStatus::Dropped;
            }
        }
        DeliveryStatus::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn make_rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    fn p(n: u32) -> Address {
        Address::proposer(n)
    }

    fn a(n: u32) -> Address {
        Address::acceptor(n)
    }

    // --- Partition tests ---

    #[test]
    fn new_network_has_no_partitions() {
        let net = SimulationNetwork::new(LinkLatency::zero());
        assert!(!net.is_partitioned(p(0), a(0)));
        assert!(!net.is_partitioned(a(0), p(0)));
    }

    #[test]
    fn create_partition_blocks_both_directions() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.create_partition(p(0), a(1));

        assert!(net.is_partitioned(p(0), a(1)));
        assert!(net.is_partitioned(a(1), p(0)));
    }

    #[test]
    fn heal_partition_unblocks_both_directions() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.create_partition(p(0), a(1));
        net.heal_partition(p(0), a(1));

        assert!(!net.is_partitioned(p(0), a(1)));
        assert!(!net.is_partitioned(a(1), p(0)));
    }

    #[test]
    fn heal_reversed_argument_order() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.create_partition(p(0), a(1));
        // Heal with reversed order — should still work due to normalization.
        net.heal_partition(a(1), p(0));

        assert!(!net.is_partitioned(p(0), a(1)));
    }

    #[test]
    fn multiple_partitions_are_independent() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.create_partition(p(0), a(0));
        net.create_partition(p(1), a(1));

        // Healing one should not affect the other.
        net.heal_partition(p(0), a(0));

        assert!(!net.is_partitioned(p(0), a(0)));
        assert!(net.is_partitioned(p(1), a(1)));
    }

    #[test]
    fn clear_all_partitions() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.create_partition(p(0), a(0));
        net.create_partition(p(1), a(1));
        net.create_partition(a(0), a(1));

        net.clear_all_partitions();

        assert!(!net.is_partitioned(p(0), a(0)));
        assert!(!net.is_partitioned(p(1), a(1)));
        assert!(!net.is_partitioned(a(0), a(1)));
    }

    #[test]
    fn self_loop_is_never_partitioned() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        // A node partitioned with itself is an odd edge case — we store (a, a)
        // which normalize_pair produces, but semantically it shouldn't happen.
        // Verify default: no partition on self.
        assert!(!net.is_partitioned(p(0), p(0)));

        // Even if someone explicitly partitions a node with itself, is_partitioned
        // correctly reflects the stored state.
        net.create_partition(p(0), p(0));
        assert!(net.is_partitioned(p(0), p(0)));
        net.heal_partition(p(0), p(0));
        assert!(!net.is_partitioned(p(0), p(0)));
    }

    // --- Drop rate tests ---

    #[test]
    fn zero_drop_rate_never_drops() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 0.0);

        for _ in 0..100 {
            assert_eq!(
                net.check_delivery(p(0), a(0), &mut rng),
                DeliveryStatus::Success
            );
        }
    }

    #[test]
    fn full_drop_rate_always_drops() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 1.0);

        for _ in 0..100 {
            assert_eq!(
                net.check_delivery(p(0), a(0), &mut rng),
                DeliveryStatus::Dropped
            );
        }
    }

    #[test]
    fn partial_drop_rate_is_probabilistic() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 0.5);

        let mut dropped = 0u32;
        let trials = 1000;
        for _ in 0..trials {
            if net.check_delivery(p(0), a(0), &mut rng) == DeliveryStatus::Dropped {
                dropped += 1;
            }
        }
        // With 1000 trials at 50%, we expect ~500 drops. Allow wide margin.
        assert!(dropped > 300, "too few drops: {dropped}");
        assert!(dropped < 700, "too many drops: {dropped}");
    }

    #[test]
    fn drop_rate_is_directional() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 1.0);

        // Reverse direction should still succeed (no drop rate set).
        assert_eq!(
            net.check_delivery(a(0), p(0), &mut rng),
            DeliveryStatus::Success
        );
    }

    #[test]
    fn clear_drop_rate() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 1.0);
        net.clear_drop_rate(p(0), a(0));

        assert_eq!(
            net.check_delivery(p(0), a(0), &mut rng),
            DeliveryStatus::Success
        );
    }

    #[test]
    #[should_panic(expected = "drop rate must be in [0.0, 1.0]")]
    fn invalid_drop_rate_panics() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.set_drop_rate(p(0), a(0), 1.5);
    }

    // --- Latency tests ---

    #[test]
    fn default_latency_used_when_no_override() {
        let default = LinkLatency::new(Duration::from_millis(10), Duration::from_millis(5));
        let net = SimulationNetwork::new(default);

        let lat = net.get_link_latency(p(0), a(0));
        assert_eq!(lat.base_delay, Duration::from_millis(10));
        assert_eq!(lat.max_jitter, Duration::from_millis(5));
    }

    #[test]
    fn per_link_latency_override() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let custom = LinkLatency::new(Duration::from_millis(50), Duration::from_millis(20));
        net.set_link_latency(p(0), a(0), custom);

        let lat = net.get_link_latency(p(0), a(0));
        assert_eq!(lat.base_delay, Duration::from_millis(50));
        assert_eq!(lat.max_jitter, Duration::from_millis(20));

        // Other links still use default.
        let default_lat = net.get_link_latency(p(1), a(1));
        assert_eq!(default_lat.base_delay, Duration::ZERO);
    }

    #[test]
    fn zero_jitter_returns_base_delay() {
        let lat = LinkLatency::new(Duration::from_millis(10), Duration::ZERO);
        let mut rng = make_rng();

        for _ in 0..100 {
            assert_eq!(lat.sample(&mut rng), Duration::from_millis(10));
        }
    }

    #[test]
    fn sample_latency_within_bounds() {
        let base = Duration::from_millis(10);
        let jitter = Duration::from_millis(5);
        let lat = LinkLatency::new(base, jitter);
        let mut rng = make_rng();

        for _ in 0..1000 {
            let sample = lat.sample(&mut rng);
            assert!(sample >= base, "sample {sample:?} < base {base:?}");
            assert!(
                sample <= base + jitter,
                "sample {sample:?} > base+jitter {:?}",
                base + jitter
            );
        }
    }

    #[test]
    fn sample_latency_via_network() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let custom = LinkLatency::new(Duration::from_millis(10), Duration::from_millis(5));
        net.set_link_latency(p(0), a(0), custom);
        let mut rng = make_rng();

        let sample = net.sample_latency(p(0), a(0), &mut rng);
        assert!(sample >= Duration::from_millis(10));
        assert!(sample <= Duration::from_millis(15));
    }

    // --- Delivery check tests ---

    #[test]
    fn check_delivery_success_on_clean_network() {
        let net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        assert_eq!(
            net.check_delivery(p(0), a(0), &mut rng),
            DeliveryStatus::Success
        );
    }

    #[test]
    fn check_delivery_partitioned() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.create_partition(p(0), a(0));

        assert_eq!(
            net.check_delivery(p(0), a(0), &mut rng),
            DeliveryStatus::Partitioned
        );
        assert_eq!(
            net.check_delivery(a(0), p(0), &mut rng),
            DeliveryStatus::Partitioned
        );
    }

    #[test]
    fn partition_takes_precedence_over_drop_rate() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        let mut rng = make_rng();
        net.set_drop_rate(p(0), a(0), 1.0);
        net.create_partition(p(0), a(0));

        // Should report Partitioned, not Dropped.
        assert_eq!(
            net.check_delivery(p(0), a(0), &mut rng),
            DeliveryStatus::Partitioned
        );
    }

    #[test]
    fn deterministic_with_same_seed() {
        let mut net = SimulationNetwork::new(LinkLatency::zero());
        net.set_drop_rate(p(0), a(0), 0.5);

        let run = |seed: u64| -> Vec<DeliveryStatus> {
            let mut rng = StdRng::seed_from_u64(seed);
            (0..100)
                .map(|_| net.check_delivery(p(0), a(0), &mut rng))
                .collect()
        };

        let run1 = run(123);
        let run2 = run(123);
        assert_eq!(run1, run2, "same seed must produce identical results");

        // Different seed should (very likely) differ.
        let run3 = run(999);
        assert_ne!(
            run1, run3,
            "different seeds should produce different results"
        );
    }
}
