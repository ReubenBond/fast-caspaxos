//! Chaos/fuzz tests for Fast CASPaxos simulation.
//!
//! Each test runs the [`ChaosRunner`] with various configurations and seeds,
//! asserting that the safety monitor never detects a divergent commit.

use fast_caspaxos_simulation::chaos::{ChaosConfig, ChaosRunner, StringChaosRunner};
use fast_caspaxos_simulation::cluster::ClusterConfig;
use fast_caspaxos_simulation::history::EventKind;
use fast_caspaxos_simulation::network::LinkLatency;

fn default_cluster() -> ClusterConfig {
    ClusterConfig {
        num_acceptors: 5,
        num_proposers: 3,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 42,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    }
}

/// Returns a base seed for high-iteration sweeps.
///
/// Reads `PORCUPINE_SEED` if set (for reproducibility), otherwise derives
/// one from the current time so every run explores a new seed region.
fn base_seed() -> u64 {
    if let Ok(val) = std::env::var("PORCUPINE_SEED") {
        val.parse::<u64>()
            .expect("PORCUPINE_SEED must be a u64 integer")
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

#[test]
fn chaos_single_seed_safety() {
    let result = ChaosRunner::new(default_cluster(), ChaosConfig::default(), 42).run();
    assert!(result.safety_ok, "Safety violated: {:?}", result.error);
    assert!(result.proposals_submitted > 0);
}

#[test]
fn chaos_50_seeds() {
    for seed in 0..50 {
        let result = ChaosRunner::new(default_cluster(), ChaosConfig::default(), seed).run();
        assert!(
            result.safety_ok,
            "Seed {seed} violated safety: {:?}",
            result.error
        );
    }
}

#[test]
fn chaos_classic_mode() {
    let mut config = default_cluster();
    config.enable_fast_commit = false;
    for seed in 0..20 {
        let result = ChaosRunner::new(config.clone(), ChaosConfig::default(), seed).run();
        assert!(result.safety_ok, "Classic seed {seed}: {:?}", result.error);
    }
}

#[test]
fn chaos_leader_mode() {
    let mut config = default_cluster();
    config.enable_fast_commit = false;
    config.enable_distinguished_leader = true;
    for seed in 0..20 {
        let result = ChaosRunner::new(config.clone(), ChaosConfig::default(), seed).run();
        assert!(result.safety_ok, "Leader seed {seed}: {:?}", result.error);
    }
}

#[test]
fn chaos_aggressive_partitions() {
    let chaos = ChaosConfig {
        partition_probability: 0.15,
        heal_probability: 0.15,
        ..ChaosConfig::default()
    };
    for seed in 0..20 {
        let result = ChaosRunner::new(default_cluster(), chaos.clone(), seed).run();
        assert!(
            result.safety_ok,
            "Partition seed {seed}: {:?}",
            result.error
        );
    }
}

#[test]
fn chaos_aggressive_crashes() {
    let chaos = ChaosConfig {
        crash_probability: 0.12,
        restart_probability: 0.15,
        ..ChaosConfig::default()
    };
    for seed in 0..20 {
        let result = ChaosRunner::new(default_cluster(), chaos.clone(), seed).run();
        assert!(result.safety_ok, "Crash seed {seed}: {:?}", result.error);
    }
}

#[test]
fn chaos_high_proposal_rate() {
    let chaos = ChaosConfig {
        proposal_probability: 0.6,
        max_steps: 200,
        ..ChaosConfig::default()
    };
    for seed in 0..10 {
        let result = ChaosRunner::new(default_cluster(), chaos.clone(), seed).run();
        assert!(
            result.safety_ok,
            "High-rate seed {seed}: {:?}",
            result.error
        );
    }
}

#[test]
fn chaos_all_modes_sweep() {
    let modes = [
        (true, false),  // fast
        (false, false), // classic
        (false, true),  // leader
        (true, true),   // fast+leader
    ];
    for (fast, leader) in modes {
        let config = ClusterConfig {
            enable_fast_commit: fast,
            enable_distinguished_leader: leader,
            ..default_cluster()
        };
        for seed in 0..10 {
            let result = ChaosRunner::new(config.clone(), ChaosConfig::default(), seed).run();
            assert!(
                result.safety_ok,
                "Mode ({fast},{leader}) seed {seed}: {:?}",
                result.error
            );
        }
    }
}

// =========================================================================
// StringChaosRunner — chaos with Porcupine history recording
// =========================================================================

#[test]
fn string_chaos_records_history() {
    let result = StringChaosRunner::new(default_cluster(), ChaosConfig::default(), 42).run();
    assert!(
        result.inner.safety_ok,
        "Safety violated: {:?}",
        result.inner.error
    );
    assert!(result.inner.proposals_submitted > 0);

    let history = result.history.build();
    assert_eq!(history.model_kind, "string");

    // Every completed proposal should have a call+return pair.
    let calls = history
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Call)
        .count();
    let returns = history
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Return)
        .count();
    assert!(calls > 0, "should have recorded call events");
    // Returns may be fewer than calls (some proposals may not complete under chaos).
    assert!(returns <= calls);
    assert_eq!(returns, result.inner.proposals_completed);
}

#[test]
fn string_chaos_1k_seeds_all_safe() {
    let base = base_seed();
    for i in 0..1_000u64 {
        let seed = base.wrapping_add(i);
        let result = StringChaosRunner::new(default_cluster(), ChaosConfig::default(), seed).run();
        assert!(
            result.inner.safety_ok,
            "base={base} seed={seed}: {:?}",
            result.inner.error
        );

        let history = result.history.build();
        assert_eq!(history.model_kind, "string");
    }
}

#[test]
fn string_chaos_all_modes_1k_seeds() {
    let base = base_seed();
    let modes = [
        (true, false),  // fast
        (false, false), // classic
        (false, true),  // leader
        (true, true),   // fast+leader
    ];
    for (fast, leader) in modes {
        let config = ClusterConfig {
            enable_fast_commit: fast,
            enable_distinguished_leader: leader,
            ..default_cluster()
        };
        for i in 0..250u64 {
            let seed = base.wrapping_add(i);
            let result = StringChaosRunner::new(config.clone(), ChaosConfig::default(), seed).run();
            assert!(
                result.inner.safety_ok,
                "base={base} mode=({fast},{leader}) seed={seed}: {:?}",
                result.inner.error
            );
        }
    }
}
