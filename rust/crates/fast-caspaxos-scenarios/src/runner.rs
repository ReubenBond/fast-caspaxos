//! Scenario batch runner for Fast CASPaxos simulation testing.
//!
//! Provides a simplified runner that can execute named scenarios with seeds,
//! and a protocol mode matrix for parameterized testing.

use fast_caspaxos_core::operation::Operation;
use fast_caspaxos_simulation::address::Address;
use fast_caspaxos_simulation::cluster::{ClusterConfig, SimulationCluster};

/// Result of running a single scenario.
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub seed: u64,
    pub success: bool,
    pub message: String,
    pub steps: usize,
    pub proposals_completed: usize,
}

/// Defines a protocol option matrix for parameterized testing.
#[derive(Debug, Clone, Copy)]
pub struct ProtocolMode {
    pub name: &'static str,
    pub enable_fast_commit: bool,
    pub enable_distinguished_leader: bool,
}

impl ProtocolMode {
    pub const CLASSIC: Self = Self {
        name: "classic",
        enable_fast_commit: false,
        enable_distinguished_leader: false,
    };
    pub const FAST: Self = Self {
        name: "fast",
        enable_fast_commit: true,
        enable_distinguished_leader: false,
    };
    pub const LEADER: Self = Self {
        name: "leader",
        enable_fast_commit: false,
        enable_distinguished_leader: true,
    };
    pub const FAST_LEADER: Self = Self {
        name: "fast+leader",
        enable_fast_commit: true,
        enable_distinguished_leader: true,
    };

    /// All four protocol modes.
    pub fn all() -> [Self; 4] {
        [Self::CLASSIC, Self::FAST, Self::LEADER, Self::FAST_LEADER]
    }
}

/// Run a simple sequential scenario: submit operations one at a time to the
/// given proposer, waiting for each to complete before submitting the next.
pub fn run_sequential_scenario(
    config: ClusterConfig,
    proposer: Address,
    operations: Vec<Box<dyn Operation<i32>>>,
    max_steps_per_op: usize,
) -> ScenarioResult {
    let seed = config.seed;
    let mut cluster = SimulationCluster::new(config);
    let mut total_steps = 0;
    let mut completed = 0;

    for op in operations {
        let id = cluster.send_proposal(proposer, op);
        let succeeded = cluster.run_until_response(id, max_steps_per_op).is_some();
        total_steps += cluster.num_pending_messages();
        if succeeded {
            completed += 1;
        } else {
            return ScenarioResult {
                scenario_name: "sequential".into(),
                seed,
                success: false,
                message: format!("Proposal {id} did not complete within {max_steps_per_op} steps"),
                steps: total_steps,
                proposals_completed: completed,
            };
        }
    }

    ScenarioResult {
        scenario_name: "sequential".into(),
        seed,
        success: true,
        message: format!("All {completed} proposals completed"),
        steps: total_steps,
        proposals_completed: completed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_caspaxos_core::operation::{FnOperation, OperationResult, OperationStatus};
    use fast_caspaxos_simulation::network::LinkLatency;

    #[test]
    fn protocol_mode_all_returns_four() {
        let modes = ProtocolMode::all();
        assert_eq!(modes.len(), 4);
        assert_eq!(modes[0].name, "classic");
        assert_eq!(modes[1].name, "fast");
        assert_eq!(modes[2].name, "leader");
        assert_eq!(modes[3].name, "fast+leader");
    }

    #[test]
    fn protocol_mode_classic_flags() {
        let m = ProtocolMode::CLASSIC;
        assert!(!m.enable_fast_commit);
        assert!(!m.enable_distinguished_leader);
    }

    #[test]
    fn protocol_mode_fast_leader_flags() {
        let m = ProtocolMode::FAST_LEADER;
        assert!(m.enable_fast_commit);
        assert!(m.enable_distinguished_leader);
    }

    #[test]
    fn run_sequential_scenario_completes() {
        let config = ClusterConfig {
            num_acceptors: 5,
            num_proposers: 1,
            enable_fast_commit: true,
            enable_distinguished_leader: false,
            seed: 42,
            default_latency: LinkLatency::zero(),
        };
        let proposer = Address::proposer(1);
        let ops: Vec<Box<dyn Operation<i32>>> = (1..=3)
            .map(|v| -> Box<dyn Operation<i32>> {
                Box::new(FnOperation::new("write", move |_: &i32| {
                    OperationResult::new(OperationStatus::Success, v)
                }))
            })
            .collect();

        let result = run_sequential_scenario(config, proposer, ops, 1000);
        assert!(result.success, "scenario should succeed: {}", result.message);
        assert_eq!(result.proposals_completed, 3);
    }

    #[test]
    fn run_sequential_scenario_across_modes() {
        for mode in ProtocolMode::all() {
            let config = ClusterConfig {
                num_acceptors: 5,
                num_proposers: 1,
                enable_fast_commit: mode.enable_fast_commit,
                enable_distinguished_leader: mode.enable_distinguished_leader,
                seed: 99,
                default_latency: LinkLatency::zero(),
            };
            let proposer = Address::proposer(1);
            let ops: Vec<Box<dyn Operation<i32>>> = (1..=2)
                .map(|v| -> Box<dyn Operation<i32>> {
                    Box::new(FnOperation::new("write", move |_: &i32| {
                        OperationResult::new(OperationStatus::Success, v)
                    }))
                })
                .collect();

            let result = run_sequential_scenario(config, proposer, ops, 1000);
            assert!(
                result.success,
                "mode '{}' should succeed: {}",
                mode.name, result.message
            );
        }
    }
}
