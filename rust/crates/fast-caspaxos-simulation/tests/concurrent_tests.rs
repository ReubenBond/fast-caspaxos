//! Concurrent proposer test scenarios for Fast CASPaxos.
//!
//! These tests exercise true concurrent in-flight proposals by calling
//! `send_proposal()` multiple times *before* `run_to_completion()`, so
//! messages from all proposers are interleaved during delivery.

use std::collections::BTreeMap;

use fast_caspaxos_core::acceptor::AcceptorState;
use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult};
use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::{ClusterConfig, SimulationCluster};
use fast_caspaxos_simulation::network::LinkLatency;
use fast_caspaxos_simulation::safety_monitor::AcceptorSafetyMonitor;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn read_op() -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("read", |_: &i32| {
        OperationResult::NotApplicable(0)
    }))
}

fn config(acceptors: u32, proposers: u32, fast: bool, leader: bool, seed: u64) -> ClusterConfig {
    ClusterConfig {
        num_acceptors: acceptors,
        num_proposers: proposers,
        enable_fast_commit: fast,
        enable_distinguished_leader: leader,
        seed,
        default_latency: LinkLatency::zero(),
        adaptive_policy: None,
    }
}

/// Snapshot all acceptor states and feed them into the safety monitor.
fn observe_acceptors(
    cluster: &SimulationCluster<i32>,
    monitor: &mut AcceptorSafetyMonitor<i32>,
    num_acceptors: u32,
) {
    let mut states: BTreeMap<Address, AcceptorState<i32>> = BTreeMap::new();
    for i in 1..=num_acceptors {
        let addr = Address::acceptor(i);
        states.insert(addr, cluster.acceptor_state(addr));
    }
    monitor.observe(&states);
}

/// Drive the cluster until all `request_ids` have responses, using timeouts
/// to resolve conflicts. Returns once all have completed or after `max_rounds`
/// retry rounds.
fn drive_to_all_responses(
    cluster: &mut SimulationCluster<i32>,
    request_ids: &[u32],
    max_rounds: usize,
) {
    for _ in 0..max_rounds {
        cluster.run_to_completion(200);
        let all_done = request_ids
            .iter()
            .all(|id| cluster.client_responses().contains_key(id));
        if all_done {
            return;
        }
        // Trigger timeouts for all proposers to push through conflicts.
        for p in cluster.proposer_addresses() {
            cluster.trigger_timeout(p);
        }
    }
}

// =========================================================================
// 1. Two proposers concurrent fast conflict
// =========================================================================

#[test]
fn two_proposers_concurrent_fast_conflict() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, false, 2000));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Submit both proposals *before* running the simulation.
    let req1 = cluster.send_proposal(p1, write_op(10));
    let req2 = cluster.send_proposal(p2, write_op(20));

    drive_to_all_responses(&mut cluster, &[req1, req2], 50);

    // Both should have a response.
    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should eventually get a response"
    );
    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should eventually get a response"
    );

    // The committed values must be consistent: the last writer wins.
    let v1 = cluster.client_responses()[&req1].response.committed_value;
    let v2 = cluster.client_responses()[&req2].response.committed_value;
    assert!(
        v1 == 10 || v1 == 20,
        "P1 committed value should be one of the proposed values, got {v1}"
    );
    assert!(
        v2 == 10 || v2 == 20,
        "P2 committed value should be one of the proposed values, got {v2}"
    );

    // Safety: no divergent values at the same ballot.
    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(monitor.check_safety().is_ok(), "safety violation detected");
}

// =========================================================================
// 2. Three-way concurrent conflict
// =========================================================================

#[test]
fn three_way_concurrent_conflict() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, true, false, 2001));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);
    let p3 = Address::proposer(3);

    let req1 = cluster.send_proposal(p1, write_op(100));
    let req2 = cluster.send_proposal(p2, write_op(200));
    let req3 = cluster.send_proposal(p3, write_op(300));

    drive_to_all_responses(&mut cluster, &[req1, req2, req3], 80);

    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should respond"
    );
    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should respond"
    );
    assert!(
        cluster.client_responses().contains_key(&req3),
        "P3 should respond"
    );

    // All three must have committed a value from the proposed set.
    let vals: Vec<i32> = [req1, req2, req3]
        .iter()
        .map(|id| cluster.client_responses()[id].response.committed_value)
        .collect();
    for v in &vals {
        assert!(
            *v == 100 || *v == 200 || *v == 300,
            "unexpected committed value {v}"
        );
    }

    // The last committed value should be consistent across acceptors.
    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(monitor.check_safety().is_ok(), "safety violation detected");
}

// =========================================================================
// 3. Concurrent fast falls back to classic
// =========================================================================

#[test]
fn concurrent_fast_falls_back_to_classic() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, false, 2002));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Both submit conflicting values in fast mode.
    let req1 = cluster.send_proposal(p1, write_op(11));
    let req2 = cluster.send_proposal(p2, write_op(22));

    // Run initial messages — fast path likely conflicts.
    cluster.run_to_completion(200);

    // Drive through timeouts to force classic recovery.
    drive_to_all_responses(&mut cluster, &[req1, req2], 60);

    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should complete via classic fallback"
    );
    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should complete via classic fallback"
    );

    let v1 = cluster.client_responses()[&req1].response.committed_value;
    let v2 = cluster.client_responses()[&req2].response.committed_value;
    assert!(v1 == 11 || v1 == 22, "P1 committed unexpected value {v1}");
    assert!(v2 == 11 || v2 == 22, "P2 committed unexpected value {v2}");
}

// =========================================================================
// 4. Leader crash, peer recovery
// =========================================================================

#[test]
fn leader_crash_peer_recovery() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, false, false, 2003));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // P1 commits a value.
    let req1 = cluster.send_proposal(p1, write_op(42));
    let resp1 = cluster.run_until_response(req1, 1000);
    assert!(resp1.is_some(), "P1 initial write should succeed");
    assert_eq!(resp1.unwrap().response.committed_value, 42);

    // Crash P1.
    cluster.crash_node(p1);

    // P2 submits a new proposal and should complete on its own.
    let req2 = cluster.send_proposal(p2, write_op(99));
    drive_to_all_responses(&mut cluster, &[req2], 50);

    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should recover and complete after P1 crash"
    );
    let v2 = cluster.client_responses()[&req2].response.committed_value;
    assert_eq!(v2, 99, "P2 should commit its proposed value");
}

// =========================================================================
// 5. Concurrent writes with safety monitor
// =========================================================================

#[test]
fn concurrent_writes_with_safety_monitor() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, true, false, 2004));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let proposers = [
        Address::proposer(1),
        Address::proposer(2),
        Address::proposer(3),
    ];

    for round in 0..3 {
        let base = (round + 1) * 100;
        let mut reqs = Vec::new();
        for (i, &p) in proposers.iter().enumerate() {
            reqs.push(cluster.send_proposal(p, write_op(base + i as i32)));
        }

        drive_to_all_responses(&mut cluster, &reqs, 60);

        // After each round, observe and check safety.
        observe_acceptors(&cluster, &mut monitor, 5);
        assert!(
            monitor.check_safety().is_ok(),
            "safety violation after round {round}"
        );
    }

    // All three rounds completed with no violations.
    assert!(
        monitor.commit_history().len() >= 1,
        "should have at least one inferred commit"
    );
}

// =========================================================================
// 6. Concurrent with partition then heal
// =========================================================================

#[test]
fn concurrent_with_partition_then_heal() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, false, false, 2005));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // P1 can reach acceptors 1-3, P2 can reach acceptors 3-5.
    // Partition P1 from acceptors 4,5 and P2 from acceptors 1,2.
    cluster.create_partition(p1, Address::acceptor(4));
    cluster.create_partition(p1, Address::acceptor(5));
    cluster.create_partition(p2, Address::acceptor(1));
    cluster.create_partition(p2, Address::acceptor(2));

    // Both submit concurrently.
    let req1 = cluster.send_proposal(p1, write_op(50));
    let req2 = cluster.send_proposal(p2, write_op(60));

    // P1 has classic quorum (3 of 5), P2 also has classic quorum (3 of 5).
    // Let them run and resolve via timeouts.
    drive_to_all_responses(&mut cluster, &[req1, req2], 60);

    // P1 should complete since it has a classic quorum.
    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should complete with quorum from acceptors 1-3"
    );

    // Heal partitions so P2 can complete if it hasn't yet.
    cluster.heal_partition(p1, Address::acceptor(4));
    cluster.heal_partition(p1, Address::acceptor(5));
    cluster.heal_partition(p2, Address::acceptor(1));
    cluster.heal_partition(p2, Address::acceptor(2));

    drive_to_all_responses(&mut cluster, &[req1, req2], 60);

    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should complete after partition heals"
    );

    // Values must be consistent.
    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(
        monitor.check_safety().is_ok(),
        "safety violation detected after heal"
    );
}

// =========================================================================
// 7. Many proposers stress
// =========================================================================

#[test]
fn many_proposers_stress() {
    let mut cluster = SimulationCluster::<i32>::new(config(7, 5, true, false, 2006));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(7);
    let proposers: Vec<Address> = (1..=5).map(Address::proposer).collect();

    let mut all_reqs = Vec::new();

    // Each proposer submits 3 operations = 15 total, all concurrent.
    for (pi, &p) in proposers.iter().enumerate() {
        for op_idx in 0..3 {
            let value = (pi as i32 + 1) * 100 + op_idx;
            all_reqs.push(cluster.send_proposal(p, write_op(value)));
        }
    }

    drive_to_all_responses(&mut cluster, &all_reqs, 150);

    let completed = all_reqs
        .iter()
        .filter(|id| cluster.client_responses().contains_key(id))
        .count();

    // All 15 should eventually complete.
    assert_eq!(
        completed,
        all_reqs.len(),
        "all {len} proposals should complete, only {completed} did",
        len = all_reqs.len()
    );

    observe_acceptors(&cluster, &mut monitor, 7);
    assert!(
        monitor.check_safety().is_ok(),
        "safety violation in stress test"
    );
}

// =========================================================================
// 8. Concurrent reads and writes
// =========================================================================

#[test]
fn concurrent_reads_and_writes() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, false, false, 2007));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);
    let p3 = Address::proposer(3);

    // P1 and P2 write, P3 reads — all submitted before running.
    let write_req1 = cluster.send_proposal(p1, write_op(10));
    let write_req2 = cluster.send_proposal(p2, write_op(20));
    let read_req = cluster.send_proposal(p3, read_op());

    drive_to_all_responses(&mut cluster, &[write_req1, write_req2, read_req], 60);

    // All should complete.
    assert!(
        cluster.client_responses().contains_key(&write_req1),
        "write from P1 should complete"
    );
    assert!(
        cluster.client_responses().contains_key(&write_req2),
        "write from P2 should complete"
    );
    assert!(
        cluster.client_responses().contains_key(&read_req),
        "read from P3 should complete"
    );

    // Write values should be from the proposed set.
    let wv1 = cluster.client_responses()[&write_req1]
        .response
        .committed_value;
    let wv2 = cluster.client_responses()[&write_req2]
        .response
        .committed_value;
    assert!(
        wv1 == 10 || wv1 == 20,
        "write from P1 committed unexpected value {wv1}"
    );
    assert!(
        wv2 == 10 || wv2 == 20,
        "write from P2 committed unexpected value {wv2}"
    );
}

// =========================================================================
// 9. Repeated concurrent rounds
// =========================================================================

#[test]
fn repeated_concurrent_rounds() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, true, false, 2008));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let proposers = [
        Address::proposer(1),
        Address::proposer(2),
        Address::proposer(3),
    ];

    let mut previous_max_committed: i32 = i32::MIN;

    for round in 0..5 {
        let base = (round + 1) * 1000;
        let mut reqs = Vec::new();
        for (i, &p) in proposers.iter().enumerate() {
            reqs.push(cluster.send_proposal(p, write_op(base + i as i32)));
        }

        drive_to_all_responses(&mut cluster, &reqs, 60);

        // All proposals in this round should have completed.
        for (idx, &req_id) in reqs.iter().enumerate() {
            assert!(
                cluster.client_responses().contains_key(&req_id),
                "round {round}, proposer {idx} should complete"
            );
        }

        // Committed values in this round must be >= previous round's max.
        let round_committed: Vec<i32> = reqs
            .iter()
            .map(|id| cluster.client_responses()[id].response.committed_value)
            .collect();
        let round_max = *round_committed.iter().max().unwrap();
        assert!(
            round_max >= previous_max_committed,
            "round {round}: committed max {round_max} should be >= previous {previous_max_committed}"
        );
        previous_max_committed = round_max;

        observe_acceptors(&cluster, &mut monitor, 5);
        assert!(
            monitor.check_safety().is_ok(),
            "safety violation after round {round}"
        );
    }
}

// =========================================================================
// 10. Concurrent under drops
// =========================================================================

#[test]
fn concurrent_under_drops() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, false, false, 2009));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Set 20% drop rate on all links.
    for pi in 1..=2 {
        let p = Address::proposer(pi);
        for ai in 1..=5 {
            let a = Address::acceptor(ai);
            cluster.set_drop_rate(p, a, 0.2);
            cluster.set_drop_rate(a, p, 0.2);
        }
    }

    let req1 = cluster.send_proposal(p1, write_op(77));
    let req2 = cluster.send_proposal(p2, write_op(88));

    drive_to_all_responses(&mut cluster, &[req1, req2], 150);

    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should eventually complete despite drops"
    );
    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should eventually complete despite drops"
    );

    let v1 = cluster.client_responses()[&req1].response.committed_value;
    let v2 = cluster.client_responses()[&req2].response.committed_value;
    assert!(v1 == 77 || v1 == 88, "P1 committed unexpected value {v1}");
    assert!(v2 == 77 || v2 == 88, "P2 committed unexpected value {v2}");

    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(
        monitor.check_safety().is_ok(),
        "safety violation under drops"
    );
}

// =========================================================================
// Bonus: Concurrent increment operations
// =========================================================================

#[test]
fn concurrent_increments_are_linearizable() {
    // Three proposers each do one increment concurrently. The final value
    // must be exactly 3 (each increment adds 1, all three must serialize).
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, false, false, 2010));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let proposers = [
        Address::proposer(1),
        Address::proposer(2),
        Address::proposer(3),
    ];

    let mut reqs = Vec::new();
    for &p in &proposers {
        reqs.push(cluster.send_proposal(p, increment_op()));
    }

    drive_to_all_responses(&mut cluster, &reqs, 80);

    for (i, &req_id) in reqs.iter().enumerate() {
        assert!(
            cluster.client_responses().contains_key(&req_id),
            "increment from proposer {i} should complete"
        );
    }

    // Collect all committed values from the three increments.
    let mut committed_vals: Vec<i32> = reqs
        .iter()
        .map(|id| cluster.client_responses()[id].response.committed_value)
        .collect();
    committed_vals.sort();

    // Each increment should produce a unique value in {1, 2, 3}.
    assert_eq!(
        committed_vals,
        vec![1, 2, 3],
        "increments should serialize to 1, 2, 3"
    );

    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(
        monitor.check_safety().is_ok(),
        "safety violation in concurrent increments"
    );
}

// =========================================================================
// Bonus: Concurrent proposals with distinguished leader
// =========================================================================

#[test]
fn concurrent_with_distinguished_leader() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, true, 2011));
    let mut monitor = AcceptorSafetyMonitor::<i32>::new(5);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    let req1 = cluster.send_proposal(p1, write_op(111));
    let req2 = cluster.send_proposal(p2, write_op(222));

    drive_to_all_responses(&mut cluster, &[req1, req2], 60);

    assert!(
        cluster.client_responses().contains_key(&req1),
        "P1 should respond"
    );
    assert!(
        cluster.client_responses().contains_key(&req2),
        "P2 should respond"
    );

    let v1 = cluster.client_responses()[&req1].response.committed_value;
    let v2 = cluster.client_responses()[&req2].response.committed_value;
    assert!(v1 == 111 || v1 == 222, "P1 committed unexpected {v1}");
    assert!(v2 == 111 || v2 == 222, "P2 committed unexpected {v2}");

    observe_acceptors(&cluster, &mut monitor, 5);
    assert!(
        monitor.check_safety().is_ok(),
        "safety violation with leader mode"
    );
}
