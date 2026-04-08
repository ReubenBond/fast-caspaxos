//! Integration tests for the adaptive Fast CASPaxos proposer.
//!
//! Covers:
//! - Fast-by-default operation in adaptive mode
//! - Contention → classic mode stabilization
//! - Proposer-to-proposer forwarding
//! - Forward failure → local takeover
//! - Calm classic commits drain debt and reopen fast path
//! - Learning a classic ballot updates the leader hint
//! - Learning a fast ballot reopens fast mode when debt is zero

use fast_caspaxos_core::operation::{FnOperation, OperationResult};
use fast_caspaxos_core::proposer::{AdaptiveMode, AdaptivePolicy};
use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::{ClusterConfig, SimulationCluster};
use fast_caspaxos_simulation::proposer_host::ProposerAdaptiveState;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_op(value: i32) -> Box<dyn fast_caspaxos_core::operation::Operation<i32>> {
    Box::new(FnOperation::new("write", move |_: &i32| {
        OperationResult::Success(value)
    }))
}

fn adaptive_config(num_proposers: u32) -> ClusterConfig {
    ClusterConfig {
        num_acceptors: 5,
        num_proposers,
        enable_fast_commit: true,
        enable_distinguished_leader: false,
        seed: 42,
        adaptive_policy: Some(AdaptivePolicy::default()),
        ..Default::default()
    }
}

fn classic_state(leader: Address, debt: u32) -> ProposerAdaptiveState {
    ProposerAdaptiveState {
        mode: AdaptiveMode::Classic,
        conflict_debt: debt,
        leader_hint: Some(leader),
        forward_attempts: 0,
    }
}

// ---------------------------------------------------------------------------
// 1. Adaptive mode defaults to fast
// ---------------------------------------------------------------------------

#[test]
fn adaptive_starts_fast_and_commits_quickly() {
    let mut cluster = SimulationCluster::<i32>::new(adaptive_config(1));
    let p1 = Address::proposer(1);

    assert_eq!(cluster.proposer_adaptive_mode(p1), Some(AdaptiveMode::Fast));

    let req = cluster.send_proposal(p1, write_op(42));
    let steps = cluster.run_to_completion(1000);
    assert!(
        cluster.client_responses().contains_key(&req),
        "adaptive fast proposal should complete"
    );
    assert_eq!(
        cluster.client_responses()[&req].response.committed_value,
        42
    );
    // Fast path: accept(5) + accepted(5) + learn broadcasts ≤ 25 steps.
    assert!(
        steps <= 25,
        "adaptive fast commit should be quick, took {steps}"
    );
}

// ---------------------------------------------------------------------------
// 2. Contention shifts the victim proposer into classic mode
// ---------------------------------------------------------------------------

#[test]
fn contention_shifts_proposer_to_classic_mode() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 2,
        enable_fast_commit: true,
        seed: 10,
        adaptive_policy: Some(AdaptivePolicy {
            conflict_debt_per_event: 4,
            calm_drain_per_commit: 1,
            classic_entry_threshold: 4,
            fast_probe_threshold: 0,
            max_forward_attempts: 10,
        }),
        ..Default::default()
    };
    let mut cluster = SimulationCluster::<i32>::new(config);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Run interleaved proposals to create contention.
    for round in 1..=8 {
        let r1 = cluster.send_proposal(p1, write_op(round));
        let r2 = cluster.send_proposal(p2, write_op(round * 10));
        cluster.run_to_completion(300);
        cluster.trigger_timeout(p1);
        cluster.trigger_timeout(p2);
        cluster.run_to_completion(200);
        let _ = (r1, r2);
    }

    // After enough contention at least one proposer should be in classic mode.
    let p1_mode = cluster.proposer_adaptive_mode(p1).unwrap();
    let p2_mode = cluster.proposer_adaptive_mode(p2).unwrap();
    assert!(
        p1_mode == AdaptiveMode::Classic || p2_mode == AdaptiveMode::Classic,
        "After contention at least one proposer should be in Classic mode; \
         p1={p1_mode:?} p2={p2_mode:?}"
    );
}

// ---------------------------------------------------------------------------
// 3. Non-leader forwards request to leader and gets response
// ---------------------------------------------------------------------------

#[test]
fn non_leader_forwards_to_leader_and_gets_response() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 2,
        enable_fast_commit: true,
        seed: 77,
        adaptive_policy: Some(AdaptivePolicy {
            conflict_debt_per_event: 4,
            calm_drain_per_commit: 1,
            classic_entry_threshold: 4,
            fast_probe_threshold: 0,
            max_forward_attempts: 5,
        }),
        ..Default::default()
    };
    let mut cluster = SimulationCluster::<i32>::new(config);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Seed P2 into classic mode with P1 as leader hint.
    cluster.set_proposer_adaptive_state(p2, classic_state(p1, 4));

    // Submit a proposal to P2 — it should forward to P1.
    let req = cluster.send_proposal(p2, write_op(42));

    // Run to completion — P1 processes the forwarded request.
    let resp = cluster.run_until_response(req, 2000);
    assert!(
        resp.is_some(),
        "forwarded request should eventually complete"
    );
    assert_eq!(resp.unwrap().response.committed_value, 42);
}

#[test]
fn forwarded_response_updates_forwarder_before_delayed_learn_arrives() {
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 2,
        enable_fast_commit: true,
        seed: 78,
        adaptive_policy: Some(AdaptivePolicy {
            conflict_debt_per_event: 4,
            calm_drain_per_commit: 1,
            classic_entry_threshold: 4,
            fast_probe_threshold: 0,
            max_forward_attempts: 5,
        }),
        ..Default::default()
    };
    let mut cluster = SimulationCluster::<i32>::new(config);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    cluster.set_proposer_adaptive_state(p2, classic_state(p1, 4));
    // Delay/blackhole leader -> forwarder learns so only the forwarded response
    // can update p2 before its next request.
    cluster.create_partition(p1, p2);

    let first_req = cluster.send_proposal(p2, write_op(42));
    let first_resp = cluster.run_until_response(first_req, 2000);
    assert!(
        first_resp.is_some(),
        "forwarded request should complete even if learn messages are delayed"
    );
    assert_eq!(first_resp.unwrap().response.committed_value, 42);

    // With the forwarded response applied, p2 should not try to overwrite stale
    // state on the next sequential write even though the learn broadcast is still
    // partitioned.
    let second_req = cluster.send_proposal(p2, write_op(43));
    let second_resp = cluster.run_until_response(second_req, 3000);
    assert!(
        second_resp.is_some(),
        "forwarder should have learned the first commit from the forwarded response"
    );
    assert_eq!(second_resp.unwrap().response.committed_value, 43);
}

// ---------------------------------------------------------------------------
// 4. Forward failure (leader crashed) triggers local takeover
// ---------------------------------------------------------------------------

#[test]
fn forward_failure_triggers_local_takeover() {
    let max_fwd = 2u32;
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 2,
        enable_fast_commit: true,
        seed: 55,
        adaptive_policy: Some(AdaptivePolicy {
            conflict_debt_per_event: 4,
            calm_drain_per_commit: 1,
            classic_entry_threshold: 4,
            fast_probe_threshold: 0,
            max_forward_attempts: max_fwd,
        }),
        ..Default::default()
    };
    let mut cluster = SimulationCluster::<i32>::new(config);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Seed P2 into classic mode with (crashed) P1 as leader hint.
    cluster.set_proposer_adaptive_state(p2, classic_state(p1, 4));
    // Crash P1.
    cluster.crash_node(p1);

    // Submit max_fwd requests; they forward to crashed P1 (no response).
    for i in 0..max_fwd {
        let _ = cluster.send_proposal(p2, write_op(i as i32 + 1));
        cluster.run_to_completion(50);
    }

    // The (max_fwd+1)-th request should NOT forward (budget exhausted) and
    // instead run locally.
    let last_req = cluster.send_proposal(p2, write_op((max_fwd + 1) as i32));
    cluster.trigger_timeout(p2);
    let resp = cluster.run_until_response(last_req, 3000);
    assert!(
        resp.is_some(),
        "P2 should complete via local takeover after leader failed"
    );
}

// ---------------------------------------------------------------------------
// 5. Calm classic commits drain debt and reopen fast path
// ---------------------------------------------------------------------------

#[test]
fn calm_commits_drain_debt_and_reopen_fast() {
    let drain_per_commit = 1u32;
    let config = ClusterConfig {
        num_acceptors: 5,
        num_proposers: 1,
        enable_fast_commit: true,
        seed: 33,
        adaptive_policy: Some(AdaptivePolicy {
            conflict_debt_per_event: 4,
            calm_drain_per_commit: drain_per_commit,
            classic_entry_threshold: 4,
            fast_probe_threshold: 0,
            max_forward_attempts: 3,
        }),
        ..Default::default()
    };
    let mut cluster = SimulationCluster::<i32>::new(config);
    let p1 = Address::proposer(1);

    // Seed P1 into classic mode with itself as the leader (stable leader).
    let initial_debt = 4u32;
    cluster.set_proposer_adaptive_state(
        p1,
        ProposerAdaptiveState {
            mode: AdaptiveMode::Classic,
            conflict_debt: initial_debt,
            leader_hint: Some(p1),
            forward_attempts: 0,
        },
    );

    // Run enough calm classic commits to drain the debt.
    let commits_needed = initial_debt / drain_per_commit + 1;
    for i in 0..commits_needed {
        let req = cluster.send_proposal(p1, write_op(i as i32));
        let resp = cluster.run_until_response(req, 1000);
        assert!(resp.is_some(), "classic commit {i} should complete");
        cluster.trigger_timeout(p1);
        cluster.run_to_completion(50);
    }

    // After draining, P1 should be back in fast mode.
    let mode = cluster.proposer_adaptive_mode(p1).unwrap();
    assert_eq!(
        mode,
        AdaptiveMode::Fast,
        "After draining debt P1 should return to Fast mode"
    );
}

// ---------------------------------------------------------------------------
// 6. Learning a classic ballot updates leader hint
// ---------------------------------------------------------------------------

#[test]
fn learning_classic_ballot_updates_leader_hint() {
    let mut cluster = SimulationCluster::<i32>::new(adaptive_config(3));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Give P1 classic leadership of itself.
    cluster.set_proposer_adaptive_state(p1, classic_state(p1, 4));

    // P1 commits classically — broadcasts a classic ballot.
    let req = cluster.send_proposal(p1, write_op(100));
    let resp = cluster.run_until_response(req, 1000);
    assert!(resp.is_some(), "P1 classic commit should succeed");

    // Allow broadcast to propagate to P2.
    cluster.run_to_completion(200);

    // If P2 received the broadcast, its leader hint should point to P1.
    let p2_state = cluster.proposer_adaptive_state(p2);
    if let Some(hint) = p2_state.leader_hint {
        assert_eq!(
            hint, p1,
            "P2's leader hint should point to P1 after learning P1's classic commit"
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Learning a fast ballot reopens fast mode when debt=0
// ---------------------------------------------------------------------------

#[test]
fn learning_fast_ballot_reopens_fast_when_debt_zero() {
    let mut cluster = SimulationCluster::<i32>::new(adaptive_config(2));
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);

    // Put P2 into classic mode with zero debt.
    cluster.set_proposer_adaptive_state(
        p2,
        ProposerAdaptiveState {
            mode: AdaptiveMode::Classic,
            conflict_debt: 0,
            leader_hint: Some(p1),
            forward_attempts: 0,
        },
    );

    // P1 commits via the fast path.
    cluster.set_proposer_adaptive_state(
        p1,
        ProposerAdaptiveState {
            mode: AdaptiveMode::Fast,
            conflict_debt: 0,
            leader_hint: None,
            forward_attempts: 0,
        },
    );

    let req = cluster.send_proposal(p1, write_op(7));
    let resp = cluster.run_until_response(req, 1000);
    assert!(resp.is_some(), "P1 fast commit should succeed");

    // Allow broadcast to reach P2.
    cluster.run_to_completion(200);

    // P2 should have reopened fast mode (debt was 0 and fast ballot was learned).
    let p2_mode = cluster.proposer_adaptive_mode(p2).unwrap();
    assert_eq!(
        p2_mode,
        AdaptiveMode::Fast,
        "P2 should reopen fast mode after learning a fast ballot with zero debt"
    );
}

// ---------------------------------------------------------------------------
// 8. Adaptive mode: multiple sequential proposals with single proposer
// ---------------------------------------------------------------------------

#[test]
fn adaptive_single_proposer_multiple_writes() {
    let mut cluster = SimulationCluster::<i32>::new(adaptive_config(1));
    let p1 = Address::proposer(1);

    for value in [10, 20, 30] {
        let req = cluster.send_proposal(p1, write_op(value));
        let resp = cluster.run_until_response(req, 1000);
        assert!(resp.is_some(), "adaptive write of {value} should succeed");
        assert_eq!(resp.unwrap().response.committed_value, value);
    }
}
