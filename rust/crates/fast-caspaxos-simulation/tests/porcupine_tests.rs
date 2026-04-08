//! Porcupine linearizability history export tests.
//!
//! These tests exercise the Porcupine history recording pipeline by running
//! simulation scenarios with `StringValue` and exporting JSON artifacts
//! compatible with the Go-based `porcupine-checker` tool.
//!
//! High-iteration seed sweeps use a random base seed from the
//! `PORCUPINE_SEED` environment variable (set by `just porcupine-check`).
//! Each run explores a different region of the seed space. The base seed
//! is printed on failure for reproducibility.

use std::path::PathBuf;

use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::ClusterConfig;
use fast_caspaxos_simulation::history::{
    EventKind, PorcupineHistory, PorcupineHistoryBuilder, ValueOutput,
};
use fast_caspaxos_simulation::network::LinkLatency;
use fast_caspaxos_simulation::porcupine_harness::PorcupineTestHarness;
use fast_caspaxos_simulation::porcupine_ops::{AppendCharacterOp, ReadStringOp};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("porcupine-histories")
}

fn jsonl_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("porcupine-histories")
        .join(format!("{name}.jsonl"))
}

fn string_output(v: &fast_caspaxos_simulation::value::StringValue) -> ValueOutput {
    ValueOutput::from_string(v)
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

fn assert_well_formed(history: &PorcupineHistory, expected_model: &str) {
    assert_eq!(history.schema_version, "fast-caspaxos-porcupine-history-v1");
    assert_eq!(history.model_kind, expected_model);
    assert!(!history.events.is_empty(), "history should have events");

    // Every call should have a matching return.
    let calls: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Call)
        .collect();
    let returns: Vec<_> = history
        .events
        .iter()
        .filter(|e| e.kind == EventKind::Return)
        .collect();
    assert_eq!(
        calls.len(),
        returns.len(),
        "call/return count mismatch: {} calls, {} returns",
        calls.len(),
        returns.len()
    );

    // Sequences should be monotonically increasing.
    for window in history.events.windows(2) {
        assert!(
            window[1].sequence > window[0].sequence,
            "sequences not monotonically increasing"
        );
    }

    // Calls have input, returns have output.
    for call in &calls {
        assert!(call.input.is_some(), "call event missing input");
        assert!(call.output.is_none(), "call event should not have output");
    }
    for ret in &returns {
        assert!(ret.input.is_none(), "return event should not have input");
        assert!(ret.output.is_some(), "return event missing output");
        assert!(
            ret.protocol_round.is_some(),
            "return event missing protocol_round"
        );
    }
}

fn assert_json_round_trips(history: &PorcupineHistory) {
    let json = serde_json::to_string_pretty(history).unwrap();
    let deserialized: PorcupineHistory = serde_json::from_str(&json).unwrap();
    assert_eq!(history, &deserialized, "JSON round-trip mismatch");
}

// =========================================================================
// Test scenarios — export JSON for Go porcupine-checker
//
// These use `base_seed()` so each `just porcupine-check` invocation
// verifies a different random seed via the Go linearizability checker.
// =========================================================================

#[test]
fn string_sequential_appends_exports_history() {
    let seed = base_seed();
    let scenario = "string-sequential-appends";
    let builder = PorcupineHistoryBuilder::new("string")
        .with_scenario(scenario)
        .with_round(1)
        .with_seed(seed);

    let mut harness = PorcupineTestHarness::new(config(5, 1, true, false, seed), builder);
    let p1 = Address::proposer(1);

    for ch in ['A', 'B', 'C', 'D', 'E'] {
        let op = AppendCharacterOp::new(ch);
        let input = op.porcupine_input();
        let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
        let resp = harness.run_until_response_and_record(req_id, 500, string_output);
        assert!(resp.is_some(), "append '{ch}' should complete");
    }

    let history = harness.build_history();
    assert_well_formed(&history, "string");
    assert_eq!(history.events.len(), 10); // 5 calls + 5 returns
    assert_json_round_trips(&history);

    // Export to file.
    let path = output_dir().join(scenario).join("porcupine-history.json");
    harness.export_history(&path);
    assert!(path.exists(), "exported JSON file should exist");

    // Re-read and validate camelCase keys.
    let json_str = std::fs::read_to_string(&path).unwrap();
    assert!(json_str.contains("\"schemaVersion\""));
    assert!(json_str.contains("\"modelKind\""));
    assert!(json_str.contains("\"append_character\""));
}

#[test]
fn string_two_proposer_contention_exports_history() {
    let seed = base_seed().wrapping_add(100_000);
    let scenario = "string-two-proposer-contention";
    let builder = PorcupineHistoryBuilder::new("string")
        .with_scenario(scenario)
        .with_round(1)
        .with_seed(seed);

    let mut harness = PorcupineTestHarness::new(config(5, 2, true, false, seed), builder);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);
    let chars = ['A', 'B', 'C', 'D', 'E', 'F'];

    // Submit proposals from alternating proposers — concurrent contention.
    let mut req_ids = Vec::new();
    for (i, &ch) in chars.iter().enumerate() {
        let proposer = if i % 2 == 0 { p1 } else { p2 };
        let op = AppendCharacterOp::new(ch);
        let input = op.porcupine_input();
        let req_id = harness.send_tracked_proposal(proposer, Box::new(op), input);
        req_ids.push(req_id);
    }

    harness.drive_to_all_responses_and_record(&req_ids, 100, string_output);

    let history = harness.build_history();
    assert_well_formed(&history, "string");
    assert_eq!(history.events.len(), 12); // 6 calls + 6 returns
    assert_json_round_trips(&history);

    let path = output_dir().join(scenario).join("porcupine-history.json");
    harness.export_history(&path);
    assert!(path.exists());
}

#[test]
fn string_crash_restart_exports_history() {
    let seed = base_seed().wrapping_add(200_000);
    let scenario = "string-crash-restart";
    let builder = PorcupineHistoryBuilder::new("string")
        .with_scenario(scenario)
        .with_round(1)
        .with_seed(seed);

    let mut harness = PorcupineTestHarness::new(config(5, 1, false, false, seed), builder);
    let p1 = Address::proposer(1);

    // Append A.
    let op = AppendCharacterOp::new('A');
    let input = op.porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
    let resp = harness.run_until_response_and_record(req_id, 500, string_output);
    assert!(resp.is_some());

    // Crash one acceptor.
    harness.crash_node(Address::acceptor(3));

    // Append B — should still complete with 4/5 alive.
    let op = AppendCharacterOp::new('B');
    let input = op.porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
    let resp = harness.run_until_response_and_record(req_id, 500, string_output);
    assert!(resp.is_some());

    // Restart the crashed acceptor.
    harness.restart_node(Address::acceptor(3));

    // Append C — should complete with all 5 alive.
    let op = AppendCharacterOp::new('C');
    let input = op.porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
    let resp = harness.run_until_response_and_record(req_id, 500, string_output);
    assert!(resp.is_some());

    let history = harness.build_history();
    assert_well_formed(&history, "string");
    assert_eq!(history.events.len(), 6); // 3 calls + 3 returns
    assert_json_round_trips(&history);

    let path = output_dir().join(scenario).join("porcupine-history.json");
    harness.export_history(&path);
    assert!(path.exists());
}

#[test]
fn string_mode_matrix_exports_history() {
    let base = base_seed();
    let modes = [
        (true, false, "fast"),
        (false, false, "classic"),
        (false, true, "leader"),
        (true, true, "fast-leader"),
    ];

    for (idx, (fast, leader, mode_name)) in modes.iter().enumerate() {
        let seed = base.wrapping_add(300_000 + idx as u64);
        let scenario = format!("string-mode-{mode_name}");
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario(&scenario)
            .with_round(1)
            .with_seed(seed);

        let mut harness = PorcupineTestHarness::new(config(5, 2, *fast, *leader, seed), builder);
        let p1 = Address::proposer(1);

        for ch in ['A', 'B', 'C'] {
            let op = AppendCharacterOp::new(ch);
            let input = op.porcupine_input();
            let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
            let resp = harness.run_until_response_and_record(req_id, 500, string_output);
            assert!(
                resp.is_some(),
                "mode {mode_name}: append '{ch}' should complete"
            );
        }

        let history = harness.build_history();
        assert_well_formed(&history, "string");
        assert_eq!(history.events.len(), 6);
        assert_json_round_trips(&history);

        let path = output_dir().join(&scenario).join("porcupine-history.json");
        harness.export_history(&path);
        assert!(path.exists());
    }
}

#[test]
fn string_read_operation_exports_history() {
    let seed = base_seed().wrapping_add(400_000);
    let scenario = "string-read-ops";
    let builder = PorcupineHistoryBuilder::new("string")
        .with_scenario(scenario)
        .with_round(1)
        .with_seed(seed);

    let mut harness = PorcupineTestHarness::new(config(5, 1, true, false, seed), builder);
    let p1 = Address::proposer(1);

    // Append A.
    let op = AppendCharacterOp::new('A');
    let input = op.porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
    harness.run_until_response_and_record(req_id, 500, string_output);

    // Read.
    let input = ReadStringOp::porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(ReadStringOp), input);
    let resp = harness.run_until_response_and_record(req_id, 500, string_output);
    assert!(resp.is_some());

    // Append B.
    let op = AppendCharacterOp::new('B');
    let input = op.porcupine_input();
    let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
    harness.run_until_response_and_record(req_id, 500, string_output);

    let history = harness.build_history();
    assert_well_formed(&history, "string");
    assert_eq!(history.events.len(), 6); // 3 calls + 3 returns
    assert_json_round_trips(&history);

    // Verify the read event has the right operation kind.
    let read_call = &history.events[2]; // third event (call #2)
    assert_eq!(read_call.input.as_ref().unwrap().kind, "read");

    let path = output_dir().join(scenario).join("porcupine-history.json");
    harness.export_history(&path);
    assert!(path.exists());
}

#[test]
fn string_concurrent_fast_conflict_exports_history() {
    let seed = base_seed().wrapping_add(500_000);
    let scenario = "string-concurrent-fast-conflict";
    let builder = PorcupineHistoryBuilder::new("string")
        .with_scenario(scenario)
        .with_round(1)
        .with_seed(seed);

    let mut harness = PorcupineTestHarness::new(config(5, 3, true, false, seed), builder);
    let p1 = Address::proposer(1);
    let p2 = Address::proposer(2);
    let p3 = Address::proposer(3);

    // Submit all 3 proposals before running — true concurrency.
    let op1 = AppendCharacterOp::new('X');
    let in1 = op1.porcupine_input();
    let id1 = harness.send_tracked_proposal(p1, Box::new(op1), in1);

    let op2 = AppendCharacterOp::new('Y');
    let in2 = op2.porcupine_input();
    let id2 = harness.send_tracked_proposal(p2, Box::new(op2), in2);

    let op3 = AppendCharacterOp::new('Z');
    let in3 = op3.porcupine_input();
    let id3 = harness.send_tracked_proposal(p3, Box::new(op3), in3);

    harness.drive_to_all_responses_and_record(&[id1, id2, id3], 100, string_output);

    let history = harness.build_history();
    assert_well_formed(&history, "string");
    assert_eq!(history.events.len(), 6);
    assert_json_round_trips(&history);

    let path = output_dir().join(scenario).join("porcupine-history.json");
    harness.export_history(&path);
    assert!(path.exists());
}

// =========================================================================

// =========================================================================
// High-iteration seed sweeps
//
// Each sweep derives seeds from `base_seed()`, which reads the
// PORCUPINE_SEED env var (set by `just porcupine-check`) or falls back
// to a time-based value. Every invocation explores a different region
// of the seed space. On failure the base seed is included in the assert
// message for reproducibility.
//
// All histories are streamed as JSONL into a single file so the Go
// porcupine-checker can verify every run.
// =========================================================================

#[test]
fn string_sequential_appends_1k_seeds() {
    let base = base_seed();
    let jpath = jsonl_path("sequential-appends");
    let _ = std::fs::remove_file(&jpath);
    for i in 0..1_000u64 {
        let seed = base.wrapping_add(i);
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("sequential-appends")
            .with_seed(seed);
        let mut harness = PorcupineTestHarness::new(config(5, 1, true, false, seed), builder);
        let p1 = Address::proposer(1);

        for ch in ['A', 'B', 'C'] {
            let op = AppendCharacterOp::new(ch);
            let input = op.porcupine_input();
            let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
            let resp = harness.run_until_response_and_record(req_id, 500, string_output);
            assert!(
                resp.is_some(),
                "base={base} seed={seed}: append '{ch}' should complete"
            );
        }

        let history = harness.build_history();
        assert_well_formed(&history, "string");
        harness.append_history_jsonl(&jpath);
    }
}

#[test]
fn string_two_proposer_contention_1k_seeds() {
    let base = base_seed();
    let jpath = jsonl_path("two-proposer-contention");
    let _ = std::fs::remove_file(&jpath);
    for i in 0..1_000u64 {
        let seed = base.wrapping_add(i);
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("two-proposer-contention")
            .with_seed(seed);
        let mut harness = PorcupineTestHarness::new(config(5, 2, true, false, seed), builder);
        let p1 = Address::proposer(1);
        let p2 = Address::proposer(2);

        let mut req_ids = Vec::new();
        for (j, ch) in ['A', 'B', 'C', 'D'].iter().enumerate() {
            let proposer = if j % 2 == 0 { p1 } else { p2 };
            let op = AppendCharacterOp::new(*ch);
            let input = op.porcupine_input();
            let req_id = harness.send_tracked_proposal(proposer, Box::new(op), input);
            req_ids.push(req_id);
        }

        harness.drive_to_all_responses_and_record(&req_ids, 100, string_output);

        let history = harness.build_history();
        assert_well_formed(&history, "string");
        harness.append_history_jsonl(&jpath);
    }
}

#[test]
fn string_three_proposer_concurrent_1k_seeds() {
    let base = base_seed();
    let jpath = jsonl_path("three-proposer-concurrent");
    let _ = std::fs::remove_file(&jpath);
    for i in 0..1_000u64 {
        let seed = base.wrapping_add(i);
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("three-proposer-concurrent")
            .with_seed(seed);
        let mut harness = PorcupineTestHarness::new(config(5, 3, true, false, seed), builder);
        let proposers = [
            Address::proposer(1),
            Address::proposer(2),
            Address::proposer(3),
        ];

        let mut req_ids = Vec::new();
        for (j, ch) in ['X', 'Y', 'Z'].iter().enumerate() {
            let op = AppendCharacterOp::new(*ch);
            let input = op.porcupine_input();
            let req_id = harness.send_tracked_proposal(proposers[j], Box::new(op), input);
            req_ids.push(req_id);
        }

        harness.drive_to_all_responses_and_record(&req_ids, 100, string_output);

        let history = harness.build_history();
        assert_well_formed(&history, "string");
        harness.append_history_jsonl(&jpath);
    }
}

#[test]
fn string_mode_matrix_1k_seeds() {
    let base = base_seed();
    let jpath = jsonl_path("mode-matrix");
    let _ = std::fs::remove_file(&jpath);
    let modes = [
        (true, false),  // fast
        (false, false), // classic
        (false, true),  // leader
        (true, true),   // fast+leader
    ];

    for (fast, leader) in modes {
        for i in 0..250u64 {
            let seed = base.wrapping_add(i);
            let builder = PorcupineHistoryBuilder::new("string")
                .with_scenario("mode-matrix")
                .with_seed(seed);
            let mut harness = PorcupineTestHarness::new(config(5, 2, fast, leader, seed), builder);
            let p1 = Address::proposer(1);
            let p2 = Address::proposer(2);

            let mut req_ids = Vec::new();
            for (j, ch) in ['A', 'B', 'C', 'D'].iter().enumerate() {
                let proposer = if j % 2 == 0 { p1 } else { p2 };
                let op = AppendCharacterOp::new(*ch);
                let input = op.porcupine_input();
                let req_id = harness.send_tracked_proposal(proposer, Box::new(op), input);
                req_ids.push(req_id);
            }

            harness.drive_to_all_responses_and_record(&req_ids, 100, string_output);

            let history = harness.build_history();
            assert_well_formed(&history, "string");
            harness.append_history_jsonl(&jpath);
        }
    }
}

#[test]
fn string_crash_restart_1k_seeds() {
    let base = base_seed();
    let jpath = jsonl_path("crash-restart");
    let _ = std::fs::remove_file(&jpath);
    for i in 0..1_000u64 {
        let seed = base.wrapping_add(i);
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("crash-restart")
            .with_seed(seed);
        let mut harness = PorcupineTestHarness::new(config(5, 1, false, false, seed), builder);
        let p1 = Address::proposer(1);

        let op = AppendCharacterOp::new('A');
        let input = op.porcupine_input();
        let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
        let resp = harness.run_until_response_and_record(req_id, 500, string_output);
        assert!(
            resp.is_some(),
            "base={base} seed={seed}: first append should complete"
        );

        harness.crash_node(Address::acceptor(((seed % 5) + 1) as u32));

        let op = AppendCharacterOp::new('B');
        let input = op.porcupine_input();
        let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
        let resp = harness.run_until_response_and_record(req_id, 500, string_output);
        assert!(
            resp.is_some(),
            "base={base} seed={seed}: append after crash should complete"
        );

        harness.restart_node(Address::acceptor(((seed % 5) + 1) as u32));

        let op = AppendCharacterOp::new('C');
        let input = op.porcupine_input();
        let req_id = harness.send_tracked_proposal(p1, Box::new(op), input);
        let resp = harness.run_until_response_and_record(req_id, 500, string_output);
        assert!(
            resp.is_some(),
            "base={base} seed={seed}: append after restart should complete"
        );

        let history = harness.build_history();
        assert_well_formed(&history, "string");
        harness.append_history_jsonl(&jpath);
    }
}
