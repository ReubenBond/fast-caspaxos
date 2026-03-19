//! Deterministic simulation integration tests for Fast CASPaxos.
//!
//! These tests exercise the full [`SimulationCluster`] with various fault
//! scenarios: partitions, crashes, restarts, quorum edge cases, and protocol
//! mode combinations.

use fast_caspaxos_core::ballot::Ballot;
use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult, OperationStatus};
use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::{ClusterConfig, SimulationCluster};
use fast_caspaxos_simulation::network::LinkLatency;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a write operation that sets the value to the given integer.
fn write_op(value: i32) -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("write", move |_: &i32| {
        OperationResult::new(OperationStatus::Success, value)
    }))
}

/// Create an increment operation.
fn increment_op() -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("increment", |v: &i32| {
        OperationResult::new(OperationStatus::Success, v + 1)
    }))
}

/// Create a read operation (NotApplicable — returns current value without modifying).
fn read_op() -> Box<dyn Operation<i32>> {
    Box::new(FnOperation::new("read", |_: &i32| {
        OperationResult::new(OperationStatus::NotApplicable, 0)
    }))
}

/// Helper: create a cluster config with given parameters.
fn config(acceptors: u32, proposers: u32, fast: bool, leader: bool, seed: u64) -> ClusterConfig {
    ClusterConfig {
        num_acceptors: acceptors,
        num_proposers: proposers,
        enable_fast_commit: fast,
        enable_distinguished_leader: leader,
        seed,
        default_latency: LinkLatency::zero(),
    }
}

/// Submit a write, run to completion, and assert the committed value.
fn write_and_assert(
    cluster: &mut SimulationCluster<i32>,
    proposer: Address,
    value: i32,
    max_steps: usize,
) -> u32 {
    let req_id = cluster.send_proposal(proposer, write_op(value));
    let resp = cluster.run_until_response(req_id, max_steps);
    assert!(resp.is_some(), "expected response for write({value})");
    assert_eq!(
        resp.unwrap().response.committed_value, value,
        "committed value should be {value}"
    );
    req_id
}

// =========================================================================
// Basic Protocol Tests
// =========================================================================

#[test]
fn single_proposer_single_write() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 100));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, write_op(42));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "expected a response");
    assert_eq!(resp.unwrap().response.committed_value, 42);
}

#[test]
fn single_proposer_sequential_writes() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 101));
    let proposer = Address::proposer(1);

    for expected in 1..=3 {
        let req_id = cluster.send_proposal(proposer, increment_op());
        let resp = cluster.run_until_response(req_id, 1000);
        assert!(resp.is_some(), "increment #{expected} should succeed");
        assert_eq!(
            resp.unwrap().response.committed_value, expected,
            "increment #{expected} should produce {expected}"
        );
    }
}

#[test]
fn two_proposers_sequential_writes() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, false, 102));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // P1 writes 10
    let req1 = cluster.send_proposal(p1, write_op(10));
    let resp1 = cluster.run_until_response(req1, 1000);
    assert!(resp1.is_some(), "P1 write should succeed");
    assert_eq!(resp1.unwrap().response.committed_value, 10);

    // P2 writes 20 — should see the updated state
    let req2 = cluster.send_proposal(p2, write_op(20));
    let resp2 = cluster.run_until_response(req2, 1000);
    assert!(resp2.is_some(), "P2 write should succeed");
    assert_eq!(resp2.unwrap().response.committed_value, 20);
}

#[test]
fn three_proposers_sequential() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 3, true, false, 103));

    for i in 1..=3 {
        let proposer = Address::proposer(i);
        let value = (i * 10) as i32;
        write_and_assert(&mut cluster, proposer, value, 1000);
    }
}

// =========================================================================
// Protocol Mode Matrix Tests
// =========================================================================

#[test]
fn classic_mode_write() {
    // fast_commit=false, leader=false → prepare + accept path
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 200));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, write_op(42));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "classic mode should complete");
    assert_eq!(resp.unwrap().response.committed_value, 42);
}

#[test]
fn fast_mode_write() {
    // fast_commit=true, leader=false → skip prepare on first write
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 201));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, write_op(99));
    let fast_steps = cluster.run_to_completion(1000);

    assert!(
        cluster.client_responses().contains_key(&req_id),
        "fast mode should complete"
    );
    assert_eq!(cluster.client_responses()[&req_id].response.committed_value, 99);
    // Fast path should complete in fewer steps than classic
    assert!(fast_steps <= 20, "fast mode took {fast_steps} steps (expected ≤ 20)");
}

#[test]
fn leader_mode_write() {
    // fast_commit=false, leader=true → piggybacked ballot for next request
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, true, 202));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, write_op(7));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "leader mode should complete");
    assert_eq!(resp.unwrap().response.committed_value, 7);
}

#[test]
fn fast_plus_leader_write() {
    // fast_commit=true, leader=true → combined mode
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, true, 203));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, write_op(55));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "fast+leader mode should complete");
    assert_eq!(resp.unwrap().response.committed_value, 55);
}

#[test]
fn all_four_mode_combinations_produce_same_value() {
    let modes = [
        (false, false, "classic"),
        (true, false, "fast"),
        (false, true, "leader"),
        (true, true, "fast+leader"),
    ];

    for (fast, leader, label) in modes {
        let mut cluster = SimulationCluster::<i32>::new(config(5, 1, fast, leader, 204));
        let proposer = Address::proposer(1);

        let req_id = cluster.send_proposal(proposer, write_op(42));
        let resp = cluster.run_until_response(req_id, 1000);
        assert!(resp.is_some(), "{label} mode should complete");
        assert_eq!(
            resp.unwrap().response.committed_value, 42,
            "{label} mode should produce 42"
        );
    }
}

#[test]
fn classic_takes_more_steps_than_fast() {
    // Fast path should be shorter than classic path
    let mut cluster_fast = SimulationCluster::<i32>::new(config(5, 1, true, false, 205));
    let req_fast = cluster_fast.send_proposal(Address::proposer(1), write_op(1));
    let fast_steps = cluster_fast.run_to_completion(1000);
    assert!(cluster_fast.client_responses().contains_key(&req_fast));

    let mut cluster_classic = SimulationCluster::<i32>::new(config(5, 1, false, false, 205));
    let req_classic = cluster_classic.send_proposal(Address::proposer(1), write_op(1));
    let classic_steps = cluster_classic.run_to_completion(1000);
    assert!(cluster_classic.client_responses().contains_key(&req_classic));

    assert!(
        fast_steps < classic_steps,
        "fast ({fast_steps}) should take fewer steps than classic ({classic_steps})"
    );
}

// =========================================================================
// Network Fault Tests
// =========================================================================

#[test]
fn partition_blocks_then_heals() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 300));
    let proposer = Address::proposer(1);

    // Partition proposer from all acceptors
    for i in 1..=5 {
        cluster.create_partition(proposer, Address::acceptor(i));
    }

    let req_id = cluster.send_proposal(proposer, write_op(42));
    cluster.run_to_completion(100);
    assert!(
        !cluster.client_responses().contains_key(&req_id),
        "should not respond while fully partitioned"
    );

    // Heal partition
    for i in 1..=5 {
        cluster.heal_partition(proposer, Address::acceptor(i));
    }
    cluster.trigger_timeout(proposer);

    let resp = cluster.run_until_response(req_id, 1000);
    assert!(resp.is_some(), "should respond after healing partition");
    assert_eq!(resp.unwrap().response.committed_value, 42);
}

#[test]
fn minority_partition_still_completes() {
    // Partition 2 of 5 acceptors — 3 remaining can form classic quorum
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 301));
    let proposer = Address::proposer(1);

    cluster.create_partition(proposer, Address::acceptor(4));
    cluster.create_partition(proposer, Address::acceptor(5));

    let req_id = cluster.send_proposal(proposer, write_op(77));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "should complete with 3/5 acceptors reachable");
    assert_eq!(resp.unwrap().response.committed_value, 77);
}

#[test]
fn majority_partition_blocks() {
    // Partition 3 of 5 acceptors — cannot form quorum
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 302));
    let proposer = Address::proposer(1);

    for i in 3..=5 {
        cluster.create_partition(proposer, Address::acceptor(i));
    }

    let req_id = cluster.send_proposal(proposer, write_op(99));
    cluster.run_to_completion(200);

    assert!(
        !cluster.client_responses().contains_key(&req_id),
        "should not complete when majority is partitioned"
    );
}

#[test]
fn partition_one_acceptor_fast_mode_needs_fallback() {
    // Fast quorum is ceil((3N/4)+1) for N=5 → need 4. Partition 2 → only 3 → no fast quorum.
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 303));
    let proposer = Address::proposer(1);

    cluster.create_partition(proposer, Address::acceptor(4));
    cluster.create_partition(proposer, Address::acceptor(5));

    let req_id = cluster.send_proposal(proposer, write_op(88));

    // Try to complete; may need timeout to fall back to classic path
    for _ in 0..50 {
        if cluster.client_responses().contains_key(&req_id) {
            break;
        }
        cluster.run_to_completion(50);
        if !cluster.client_responses().contains_key(&req_id) {
            cluster.trigger_timeout(proposer);
        }
    }

    assert!(
        cluster.client_responses().contains_key(&req_id),
        "should eventually complete via classic fallback"
    );
    assert_eq!(cluster.client_responses()[&req_id].response.committed_value, 88);
}

// =========================================================================
// Crash / Restart Tests
// =========================================================================

#[test]
fn crash_one_acceptor_still_completes() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 400));
    let proposer = Address::proposer(1);

    cluster.crash_node(Address::acceptor(1));

    let req_id = cluster.send_proposal(proposer, write_op(42));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "should complete with 4/5 acceptors");
    assert_eq!(resp.unwrap().response.committed_value, 42);
}

#[test]
fn crash_two_acceptors_still_completes_classic() {
    // Classic quorum for 5 acceptors is 3; crashing 2 leaves exactly 3
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 401));
    let proposer = Address::proposer(1);

    cluster.crash_node(Address::acceptor(1));
    cluster.crash_node(Address::acceptor(2));

    let req_id = cluster.send_proposal(proposer, write_op(10));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "should complete with 3/5 acceptors (classic quorum)");
    assert_eq!(resp.unwrap().response.committed_value, 10);
}

#[test]
fn crash_and_restart_acceptor() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 402));
    let proposer = Address::proposer(1);
    let crashed_acceptor = Address::acceptor(1);

    // Write a value first so the acceptor has persisted state
    write_and_assert(&mut cluster, proposer, 42, 1000);

    // Capture state before crash
    let state_before = cluster.acceptor_state(crashed_acceptor);

    // Crash and restart
    cluster.crash_node(crashed_acceptor);
    cluster.restart_node(crashed_acceptor);

    // Restarted acceptor should have durable state from the state store
    let state_after = cluster.acceptor_state(crashed_acceptor);
    assert_eq!(
        state_before.accepted_ballot, state_after.accepted_ballot,
        "accepted_ballot should be preserved across restart"
    );
    assert_eq!(
        state_before.promised_ballot, state_after.promised_ballot,
        "promised_ballot should be preserved across restart"
    );

    // Further writes should still succeed
    write_and_assert(&mut cluster, proposer, 100, 1000);
}

#[test]
fn crash_proposer_clears_state() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, false, 403));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // P1 writes successfully
    write_and_assert(&mut cluster, p1, 42, 1000);

    // Crash P1
    cluster.crash_node(p1);

    // Restart P1 — ballot should be reset
    cluster.restart_node(p1);
    assert_eq!(
        cluster.proposer_ballot(p1),
        Ballot::ZERO,
        "restarted proposer should have zero ballot"
    );

    // P2 can still write (uses a different proposer)
    write_and_assert(&mut cluster, p2, 99, 1000);
}

#[test]
fn crash_three_acceptors_blocks() {
    // Crashing 3 of 5 means only 2 left — below any quorum
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 404));
    let proposer = Address::proposer(1);

    for i in 1..=3 {
        cluster.crash_node(Address::acceptor(i));
    }

    let req_id = cluster.send_proposal(proposer, write_op(7));
    cluster.run_to_completion(200);

    assert!(
        !cluster.client_responses().contains_key(&req_id),
        "should not complete with only 2/5 acceptors"
    );
}

#[test]
fn crash_and_restart_multiple_acceptors_then_write() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 405));
    let proposer = Address::proposer(1);

    // Write initial value
    write_and_assert(&mut cluster, proposer, 1, 1000);

    // Crash 2 acceptors, write again (should work with 3)
    cluster.crash_node(Address::acceptor(1));
    cluster.crash_node(Address::acceptor(2));
    write_and_assert(&mut cluster, proposer, 2, 1000);

    // Restart both acceptors
    cluster.restart_node(Address::acceptor(1));
    cluster.restart_node(Address::acceptor(2));

    // Write again — all 5 acceptors available
    write_and_assert(&mut cluster, proposer, 3, 1000);
}

// =========================================================================
// Determinism Tests
// =========================================================================

#[test]
fn same_seed_same_result() {
    // Run the same scenario twice with the same seed — results must be identical
    let run = |seed: u64| -> (i32, usize) {
        let mut cluster = SimulationCluster::<i32>::new(config(5, 2, true, false, seed));
        let p1 = Address::proposer(1);
        let p2 = Address::proposer(2);

        let req1 = cluster.send_proposal(p1, write_op(10));
        cluster.run_until_response(req1, 1000);

        let req2 = cluster.send_proposal(p2, write_op(20));
        let steps = cluster.run_to_completion(1000);

        let val = cluster.client_responses()[&req2].response.committed_value;
        (val, steps)
    };

    let (val_a, steps_a) = run(500);
    let (val_b, steps_b) = run(500);

    assert_eq!(val_a, val_b, "same seed should produce same value");
    assert_eq!(steps_a, steps_b, "same seed should produce same step count");
}

#[test]
fn different_seed_may_differ() {
    // With different seeds, both should complete (values may or may not differ)
    let run = |seed: u64| -> i32 {
        let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, seed));
        let proposer = Address::proposer(1);
        let req_id = cluster.send_proposal(proposer, write_op(42));
        let resp = cluster.run_until_response(req_id, 1000);
        assert!(resp.is_some(), "should complete with seed {seed}");
        resp.unwrap().response.committed_value
    };

    let val_a = run(600);
    let val_b = run(601);

    // Both should produce the correct value (deterministic write, no contention)
    assert_eq!(val_a, 42);
    assert_eq!(val_b, 42);
}

#[test]
fn deterministic_with_drops() {
    // Even with message drops, same seed should produce same outcome
    let run = |seed: u64| -> bool {
        let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, seed));
        let proposer = Address::proposer(1);

        for i in 1..=5 {
            let acc = Address::acceptor(i);
            cluster.set_drop_rate(proposer, acc, 0.3);
            cluster.set_drop_rate(acc, proposer, 0.3);
        }

        let req_id = cluster.send_proposal(proposer, write_op(7));

        for _ in 0..100 {
            if cluster.client_responses().contains_key(&req_id) {
                break;
            }
            cluster.run_to_completion(50);
            if !cluster.client_responses().contains_key(&req_id) {
                cluster.trigger_timeout(proposer);
            }
        }

        cluster.client_responses().contains_key(&req_id)
    };

    let result_a = run(700);
    let result_b = run(700);
    assert_eq!(result_a, result_b, "same seed should produce same drop behavior");
}

// =========================================================================
// Stress / Chaos Tests
// =========================================================================

#[test]
fn many_sequential_writes_no_faults() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 800));
    let proposer = Address::proposer(1);

    for i in 1..=20 {
        let req_id = cluster.send_proposal(proposer, increment_op());
        let resp = cluster.run_until_response(req_id, 1000);
        assert!(resp.is_some(), "increment #{i} should succeed");
        assert_eq!(
            resp.unwrap().response.committed_value, i,
            "after {i} increments value should be {i}"
        );
    }
}

#[test]
fn writes_with_random_crash_restart() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 801));
    let proposer = Address::proposer(1);
    let mut rng = StdRng::seed_from_u64(801);

    let mut successful_writes = 0;

    for round in 1..=10 {
        // Randomly crash or restart one acceptor
        let acc_id = rng.gen_range(1..=5);
        let acc = Address::acceptor(acc_id);

        if rng.gen_bool(0.5) {
            cluster.crash_node(acc);
        } else {
            cluster.restart_node(acc);
        }

        let req_id = cluster.send_proposal(proposer, write_op(round));

        // Retry with timeouts
        for _ in 0..20 {
            if cluster.client_responses().contains_key(&req_id) {
                break;
            }
            cluster.run_to_completion(50);
            if !cluster.client_responses().contains_key(&req_id) {
                cluster.trigger_timeout(proposer);
            }
        }

        if cluster.client_responses().contains_key(&req_id) {
            let val = cluster.client_responses()[&req_id].response.committed_value;
            assert_eq!(val, round, "write that succeeds must have correct value");
            successful_writes += 1;
        }
    }

    // At least some writes should succeed (with random crashes, not all may)
    assert!(
        successful_writes > 0,
        "at least some writes should succeed despite random crashes"
    );
}

#[test]
fn alternating_proposers_with_partitions() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, false, false, 802));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    let mut last_committed = 0;

    for round in 1..=6 {
        let proposer = if round % 2 == 1 { p1 } else { p2 };

        // Create a partition on one acceptor for odd rounds
        if round % 2 == 1 {
            cluster.create_partition(proposer, Address::acceptor(5));
        } else {
            cluster.heal_partition(p1, Address::acceptor(5));
            cluster.heal_partition(p2, Address::acceptor(5));
        }

        let req_id = cluster.send_proposal(proposer, write_op(round as i32));

        for _ in 0..30 {
            if cluster.client_responses().contains_key(&req_id) {
                break;
            }
            cluster.run_to_completion(50);
            if !cluster.client_responses().contains_key(&req_id) {
                cluster.trigger_timeout(proposer);
            }
        }

        if let Some(resp) = cluster.client_responses().get(&req_id) {
            let val = resp.response.committed_value;
            assert_eq!(val, round as i32, "round {round} should commit {round}");
            // Safety: committed values should never go backwards
            assert!(
                val >= last_committed,
                "committed value went backwards: {last_committed} -> {val}"
            );
            last_committed = val;
        }
    }
}

// =========================================================================
// Quorum Verification Tests
// =========================================================================

#[test]
fn fast_quorum_4_of_5_acceptors() {
    // Fast quorum needs ≥ 4 of 5. Crash 2 → 3 left → below fast quorum.
    // Should fall back to classic path after timeout.
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 900));
    let proposer = Address::proposer(1);

    cluster.crash_node(Address::acceptor(1));
    cluster.crash_node(Address::acceptor(2));

    let req_id = cluster.send_proposal(proposer, write_op(42));

    // Fast path won't complete with only 3 acceptors; use timeout to retry via classic
    for _ in 0..50 {
        if cluster.client_responses().contains_key(&req_id) {
            break;
        }
        cluster.run_to_completion(50);
        if !cluster.client_responses().contains_key(&req_id) {
            cluster.trigger_timeout(proposer);
        }
    }

    assert!(
        cluster.client_responses().contains_key(&req_id),
        "should complete via classic fallback after fast quorum failure"
    );
    assert_eq!(cluster.client_responses()[&req_id].response.committed_value, 42);
}

#[test]
fn classic_quorum_3_of_5_acceptors() {
    // Classic quorum for 5 is 3. Crash 2 → 3 left → exactly quorum.
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 901));
    let proposer = Address::proposer(1);

    cluster.crash_node(Address::acceptor(4));
    cluster.crash_node(Address::acceptor(5));

    let req_id = cluster.send_proposal(proposer, write_op(77));
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "3/5 should form classic quorum");
    assert_eq!(resp.unwrap().response.committed_value, 77);
}

#[test]
fn fast_quorum_with_7_acceptors() {
    // 7 acceptors: fast quorum = 6. Crash 2 → 5 left → below fast quorum.
    let mut cluster = SimulationCluster::<i32>::new(config(7, 1, true, false, 902));
    let proposer = Address::proposer(1);

    cluster.crash_node(Address::acceptor(1));
    cluster.crash_node(Address::acceptor(2));

    let req_id = cluster.send_proposal(proposer, write_op(33));

    for _ in 0..50 {
        if cluster.client_responses().contains_key(&req_id) {
            break;
        }
        cluster.run_to_completion(50);
        if !cluster.client_responses().contains_key(&req_id) {
            cluster.trigger_timeout(proposer);
        }
    }

    assert!(
        cluster.client_responses().contains_key(&req_id),
        "should complete via classic fallback with 5/7 acceptors"
    );
    assert_eq!(cluster.client_responses()[&req_id].response.committed_value, 33);
}

// =========================================================================
// Read Operation Tests
// =========================================================================

#[test]
fn read_returns_default_on_empty_register() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1000));
    let proposer = Address::proposer(1);

    let req_id = cluster.send_proposal(proposer, read_op());
    let resp = cluster.run_until_response(req_id, 1000);

    assert!(resp.is_some(), "read should complete");
    // Default i32 is 0
    assert_eq!(resp.unwrap().response.committed_value, 0);
}

#[test]
fn read_after_write_returns_written_value() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1001));
    let proposer = Address::proposer(1);

    write_and_assert(&mut cluster, proposer, 42, 1000);

    // Now read — should see 42 committed
    let req_id = cluster.send_proposal(proposer, read_op());
    let resp = cluster.run_until_response(req_id, 1000);
    assert!(resp.is_some(), "read should complete");
    // Read op returns 0 as its value, but committed_value reflects the register state
    // (the operation returns NotApplicable, so committed_value should be the current state)
    let committed = resp.unwrap().response.committed_value;
    assert!(
        committed == 42 || committed == 0,
        "read committed value should reflect current state, got {committed}"
    );
}

// =========================================================================
// Edge Case & Misc Tests
// =========================================================================

#[test]
fn empty_cluster_run_to_completion() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 1100));
    let steps = cluster.run_to_completion(100);
    assert_eq!(steps, 0, "no messages → zero steps");
}

#[test]
fn num_pending_messages_after_proposal() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 1101));
    let proposer = Address::proposer(1);

    assert_eq!(cluster.num_pending_messages(), 0);
    cluster.send_proposal(proposer, write_op(1));
    assert!(
        cluster.num_pending_messages() > 0,
        "should have pending messages after proposal"
    );
}

#[test]
fn acceptor_addresses_match_config() {
    let cluster = SimulationCluster::<i32>::new(config(7, 3, true, false, 1102));
    assert_eq!(cluster.acceptor_addresses().len(), 7);
    assert_eq!(cluster.proposer_addresses().len(), 3);
}

#[test]
fn fault_history_records_all_events() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 1103));
    let p = Address::proposer(1);
    let a1 = Address::acceptor(1);
    let a2 = Address::acceptor(2);

    cluster.create_partition(p, a1);
    cluster.create_partition(p, a2);
    cluster.crash_node(a1);
    cluster.restart_node(a1);
    cluster.heal_partition(p, a1);
    cluster.heal_partition(p, a2);

    assert_eq!(cluster.fault_history().len(), 6);
}

#[test]
fn trigger_timeout_on_idle_proposer_is_harmless() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, true, false, 1104));
    let proposer = Address::proposer(1);

    // No pending proposal — timeout should be a no-op
    cluster.trigger_timeout(proposer);
    assert_eq!(cluster.num_pending_messages(), 0);
}

#[test]
fn write_after_full_partition_and_heal_cycle() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1105));
    let proposer = Address::proposer(1);

    // Write succeeds
    write_and_assert(&mut cluster, proposer, 1, 1000);

    // Partition everything
    for i in 1..=5 {
        cluster.create_partition(proposer, Address::acceptor(i));
    }

    // This write will not complete
    let blocked_id = cluster.send_proposal(proposer, write_op(2));
    cluster.run_to_completion(200);
    assert!(!cluster.client_responses().contains_key(&blocked_id));

    // Heal and retry
    for i in 1..=5 {
        cluster.heal_partition(proposer, Address::acceptor(i));
    }
    cluster.trigger_timeout(proposer);
    let resp = cluster.run_until_response(blocked_id, 1000);
    assert!(resp.is_some(), "should complete after heal");
    assert_eq!(resp.unwrap().response.committed_value, 2);

    // Subsequent write should work fine
    write_and_assert(&mut cluster, proposer, 3, 1000);
}

#[test]
fn multiple_proposers_increment_safety() {
    // Both proposers increment. Final value should be 2 (each increment adds 1).
    let mut cluster = SimulationCluster::<i32>::new(config(5, 2, false, false, 1106));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    let req1 = cluster.send_proposal(p1, increment_op());
    let resp1 = cluster.run_until_response(req1, 1000);
    assert!(resp1.is_some());
    assert_eq!(resp1.unwrap().response.committed_value, 1);

    let req2 = cluster.send_proposal(p2, increment_op());
    let resp2 = cluster.run_until_response(req2, 1000);
    assert!(resp2.is_some());
    assert_eq!(resp2.unwrap().response.committed_value, 2);
}

#[test]
fn proposer_ballot_advances_after_write() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1107));
    let proposer = Address::proposer(1);

    let ballot_before = cluster.proposer_ballot(proposer);
    write_and_assert(&mut cluster, proposer, 42, 1000);
    let ballot_after = cluster.proposer_ballot(proposer);

    assert!(
        ballot_after > ballot_before,
        "ballot should advance after a write: {ballot_before:?} -> {ballot_after:?}"
    );
}

#[test]
fn crash_restart_then_new_proposal_succeeds() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1108));
    let proposer = Address::proposer(1);

    // Crash proposer before any work
    cluster.crash_node(proposer);
    cluster.restart_node(proposer);

    // New proposal on restarted proposer should succeed
    write_and_assert(&mut cluster, proposer, 42, 1000);
}

#[test]
fn sequential_writes_with_leader_mode() {
    // Leader mode piggybacks prepared ballot for the next request
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, true, 1109));
    let proposer = Address::proposer(1);

    for i in 1..=5 {
        write_and_assert(&mut cluster, proposer, i * 10, 1000);
    }
}

#[test]
fn drop_rate_100_percent_blocks_like_partition() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1110));
    let proposer = Address::proposer(1);

    // 100% drop rate on all links
    for i in 1..=5 {
        let acc = Address::acceptor(i);
        cluster.set_drop_rate(proposer, acc, 1.0);
        cluster.set_drop_rate(acc, proposer, 1.0);
    }

    let req_id = cluster.send_proposal(proposer, write_op(42));
    cluster.run_to_completion(200);

    assert!(
        !cluster.client_responses().contains_key(&req_id),
        "100% drop rate should prevent completion"
    );
}

#[test]
fn message_drop_eventually_succeeds_with_retries() {
    let mut cluster = SimulationCluster::<i32>::new(config(5, 1, false, false, 1111));
    let proposer = Address::proposer(1);

    // 50% drop rate
    for i in 1..=5 {
        let acc = Address::acceptor(i);
        cluster.set_drop_rate(proposer, acc, 0.5);
        cluster.set_drop_rate(acc, proposer, 0.5);
    }

    let req_id = cluster.send_proposal(proposer, write_op(42));

    for _ in 0..500 {
        if cluster.client_responses().contains_key(&req_id) {
            break;
        }
        cluster.run_to_completion(50);
        if !cluster.client_responses().contains_key(&req_id) {
            cluster.trigger_timeout(proposer);
        }
    }

    assert!(
        cluster.client_responses().contains_key(&req_id),
        "should eventually succeed despite 50% drops"
    );
    assert_eq!(cluster.client_responses()[&req_id].response.committed_value, 42);
}
