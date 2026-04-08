//! Linearizability tests for Fast CASPaxos simulation.
//!
//! Verifies that simulation runs produce linearizable histories using
//! the acceptor safety monitor infrastructure.

use std::collections::BTreeMap;

use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult};
use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::{ClusterConfig, SimulationCluster};
use fast_caspaxos_simulation::network::LinkLatency;
use fast_caspaxos_simulation::safety_monitor::AcceptorSafetyMonitor;

fn write_op(value: i32) -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("write", move |_: &i32| {
        OperationResult::Success(value)
    }))
}

fn increment_op() -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("increment", |v: &i32| {
        OperationResult::Success(v + 1)
    }))
}

/// Helper: observe all acceptor states into the safety monitor.
fn observe_acceptors(
    cluster: &SimulationCluster<i32>,
    monitor: &mut AcceptorSafetyMonitor<i32>,
    num_acceptors: u32,
) {
    let mut states = BTreeMap::new();
    for i in 1..=num_acceptors {
        let addr = Address::acceptor(i);
        states.insert(addr, cluster.acceptor_state(addr));
    }
    monitor.observe(&states);
}

#[test]
fn single_proposer_commits_are_linearizable() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 1001,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    };
    let mut cluster = SimulationCluster::new(config);
    let mut monitor = AcceptorSafetyMonitor::new(5);
    let p1 = Address::proposer(1);

    for i in 1..=10 {
        let id = cluster.send_proposal(p1, write_op(i));
        let resp = cluster.run_until_response(id, 500);
        assert!(resp.is_some(), "write({i}) should complete");
        assert_eq!(resp.unwrap().response.committed_value, i);
        observe_acceptors(&cluster, &mut monitor, 5);
    }

    assert!(
        monitor.check_safety().is_ok(),
        "safety violated after 10 sequential writes"
    );
    assert!(
        !monitor.commit_history().is_empty(),
        "should have observed commits"
    );
}

#[test]
fn two_proposer_commits_are_linearizable() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 2,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 2002,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    };
    let mut cluster = SimulationCluster::new(config);
    let mut monitor = AcceptorSafetyMonitor::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    for i in 1..=6 {
        let proposer = if i % 2 == 1 { p1 } else { p2 };
        let id = cluster.send_proposal(proposer, write_op(i * 10));
        let resp = cluster.run_until_response(id, 500);
        assert!(resp.is_some(), "write({}) should complete", i * 10);
        observe_acceptors(&cluster, &mut monitor, 5);
    }

    assert!(
        monitor.check_safety().is_ok(),
        "safety violated with alternating proposers"
    );
}

#[test]
fn safety_monitor_detects_all_commits() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 3003,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    };
    let mut cluster = SimulationCluster::new(config);
    let mut monitor = AcceptorSafetyMonitor::new(5);
    let p1 = Address::proposer(1);

    for i in 1..=5 {
        let id = cluster.send_proposal(p1, write_op(i * 100));
        cluster.run_until_response(id, 500);
        observe_acceptors(&cluster, &mut monitor, 5);
    }

    assert!(
        monitor.commit_history().len() >= 5,
        "expected at least 5 commits, got {}",
        monitor.commit_history().len()
    );
    assert!(monitor.check_safety().is_ok());
}

#[test]
fn crash_restart_preserves_safety() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: false,
        enable_distinguished_leader: false,
        seed: 4004,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    };
    let mut cluster = SimulationCluster::new(config);
    let mut monitor = AcceptorSafetyMonitor::new(5);
    let p1 = Address::proposer(1);

    // Write initial value
    let id = cluster.send_proposal(p1, write_op(1));
    let resp = cluster.run_until_response(id, 500);
    assert!(resp.is_some(), "initial write should complete");
    observe_acceptors(&cluster, &mut monitor, 5);

    // Crash one acceptor
    cluster.crash_node(Address::acceptor(3));

    // Write again with one acceptor down (still have quorum of 4/5)
    let id = cluster.send_proposal(p1, write_op(2));
    let resp = cluster.run_until_response(id, 500);
    assert!(resp.is_some(), "write after crash should complete");
    observe_acceptors(&cluster, &mut monitor, 5);

    // Restart the crashed acceptor
    cluster.restart_node(Address::acceptor(3));

    // Write again after restart
    let id = cluster.send_proposal(p1, write_op(3));
    let resp = cluster.run_until_response(id, 500);
    assert!(resp.is_some(), "write after restart should complete");
    observe_acceptors(&cluster, &mut monitor, 5);

    assert!(
        monitor.check_safety().is_ok(),
        "safety violated after crash/restart"
    );
}

#[test]
fn mode_matrix_all_safe() {
    for mode in [(true, false), (false, false), (false, true), (true, true)] {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 2,
            enable_fast_commit: mode.0,
            enable_distinguished_leader: mode.1,
            seed: 12345,
            default_latency: LinkLatency::zero(),
            adaptive_policy: None,
        };
        let mut cluster = SimulationCluster::new(config);
        let mut monitor = AcceptorSafetyMonitor::new(5);
        let p1 = Address::proposer(1);

        for i in 1..=5 {
            let id = cluster.send_proposal(p1, write_op(i));
            cluster.run_until_response(id, 500);
            observe_acceptors(&cluster, &mut monitor, 5);
        }

        assert!(
            monitor.check_safety().is_ok(),
            "Safety violated in mode {:?}",
            mode
        );
    }
}

#[test]
fn increment_sequence_is_linearizable() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 5005,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    };
    let mut cluster = SimulationCluster::new(config);
    let mut monitor = AcceptorSafetyMonitor::new(5);
    let p1 = Address::proposer(1);

    for expected in 1..=5 {
        let id = cluster.send_proposal(p1, increment_op());
        let resp = cluster.run_until_response(id, 500);
        assert!(resp.is_some(), "increment #{expected} should complete");
        assert_eq!(
            resp.unwrap().response.committed_value,
            expected,
            "increment #{expected} should produce {expected}"
        );
        observe_acceptors(&cluster, &mut monitor, 5);
    }

    assert!(
        monitor.check_safety().is_ok(),
        "safety violated during increment sequence"
    );
}
