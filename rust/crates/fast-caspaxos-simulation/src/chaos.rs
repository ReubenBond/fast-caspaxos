//! Automated chaos injection for stress testing Fast `CASPaxos`.
//!
//! [`ChaosRunner`] drives a [`SimulationCluster`] through randomized fault
//! injection steps — crashes, restarts, partitions, heals, timeouts — while
//! submitting proposals and continuously verifying safety via
//! [`AcceptorSafetyMonitor`].
//!
//! [`StringChaosRunner`] extends this with [`StringValue`]-based proposals and
//! Porcupine history recording for linearizability verification.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use fast_caspaxos_core::operation::{FnOperation, OperationResult};

use crate::address::Address;
use crate::cluster::{ClusterConfig, SimulationCluster};
use crate::history::{PorcupineHistoryBuilder, ValueOutput};
use crate::porcupine_ops::AppendCharacterOp;
use crate::safety_monitor::AcceptorSafetyMonitor;
use crate::value::StringValue;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for chaos testing.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of submitting a new proposal each step.
    pub proposal_probability: f64,
    /// Probability of crashing a random alive node.
    pub crash_probability: f64,
    /// Probability of restarting a random crashed node.
    pub restart_probability: f64,
    /// Probability of creating a random partition.
    pub partition_probability: f64,
    /// Probability of healing a random partition.
    pub heal_probability: f64,
    /// Probability of triggering a timeout on a random proposer.
    pub timeout_probability: f64,
    /// Minimum number of alive acceptors (preserve quorum possibility).
    pub min_alive_acceptors: u32,
    /// Maximum chaos steps to run.
    pub max_steps: usize,
    /// Steps of simulation to run between chaos actions.
    pub sim_steps_per_chaos_step: usize,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            proposal_probability: 0.3,
            crash_probability: 0.05,
            restart_probability: 0.08,
            partition_probability: 0.05,
            heal_probability: 0.08,
            timeout_probability: 0.15,
            min_alive_acceptors: 3,
            max_steps: 500,
            sim_steps_per_chaos_step: 20,
        }
    }
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

/// Result of a chaos run.
#[derive(Debug)]
pub struct ChaosResult {
    pub steps_executed: usize,
    pub proposals_submitted: usize,
    pub proposals_completed: usize,
    pub crashes: usize,
    pub restarts: usize,
    pub partitions_created: usize,
    pub partitions_healed: usize,
    pub timeouts_triggered: usize,
    pub safety_ok: bool,
    pub error: Option<String>,
}

impl ChaosResult {
    fn new() -> Self {
        Self {
            steps_executed: 0,
            proposals_submitted: 0,
            proposals_completed: 0,
            crashes: 0,
            restarts: 0,
            partitions_created: 0,
            partitions_healed: 0,
            timeouts_triggered: 0,
            safety_ok: true,
            error: None,
        }
    }
}

type ActivePartition = (Address, Address);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChaosAction {
    SubmitProposal,
    CrashNode,
    RestartNode,
    CreatePartition,
    HealPartition,
    TriggerTimeout,
}

fn pick_action(chaos_config: &ChaosConfig, roll: f64) -> Option<ChaosAction> {
    let mut threshold = chaos_config.proposal_probability;
    if roll < threshold {
        return Some(ChaosAction::SubmitProposal);
    }

    threshold += chaos_config.crash_probability;
    if roll < threshold {
        return Some(ChaosAction::CrashNode);
    }

    threshold += chaos_config.restart_probability;
    if roll < threshold {
        return Some(ChaosAction::RestartNode);
    }

    threshold += chaos_config.partition_probability;
    if roll < threshold {
        return Some(ChaosAction::CreatePartition);
    }

    threshold += chaos_config.heal_probability;
    if roll < threshold {
        return Some(ChaosAction::HealPartition);
    }

    threshold += chaos_config.timeout_probability;
    if roll < threshold {
        return Some(ChaosAction::TriggerTimeout);
    }

    None
}

fn apply_shared_action<V>(
    action: ChaosAction,
    chaos_config: &ChaosConfig,
    cluster: &mut SimulationCluster<V>,
    rng: &mut StdRng,
    crashed_acceptors: &mut Vec<Address>,
    active_partitions: &mut Vec<ActivePartition>,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    match action {
        ChaosAction::SubmitProposal => {}
        ChaosAction::CrashNode => {
            crash_random_acceptor(chaos_config, cluster, rng, crashed_acceptors, result);
        }
        ChaosAction::RestartNode => {
            restart_random_acceptor(cluster, crashed_acceptors, rng, result);
        }
        ChaosAction::CreatePartition => {
            create_random_partition(cluster, rng, active_partitions, result);
        }
        ChaosAction::HealPartition => {
            heal_random_partition(cluster, active_partitions, rng, result);
        }
        ChaosAction::TriggerTimeout => trigger_random_timeout(cluster, rng, result),
    }
}

fn crash_random_acceptor<V>(
    chaos_config: &ChaosConfig,
    cluster: &mut SimulationCluster<V>,
    rng: &mut StdRng,
    crashed_acceptors: &mut Vec<Address>,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    let all = cluster.acceptor_addresses();
    let alive: Vec<Address> = all
        .iter()
        .filter(|address| !crashed_acceptors.contains(address))
        .copied()
        .collect();
    #[allow(clippy::cast_possible_truncation)]
    if alive.len() as u32 <= chaos_config.min_alive_acceptors {
        return;
    }
    let target = alive[rng.gen_range(0..alive.len())];
    cluster.crash_node(target);
    crashed_acceptors.push(target);
    result.crashes += 1;
}

fn restart_random_acceptor<V>(
    cluster: &mut SimulationCluster<V>,
    crashed_acceptors: &mut Vec<Address>,
    rng: &mut StdRng,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    if crashed_acceptors.is_empty() {
        return;
    }
    let idx = rng.gen_range(0..crashed_acceptors.len());
    let target = crashed_acceptors.swap_remove(idx);
    cluster.restart_node(target);
    result.restarts += 1;
}

fn create_random_partition<V>(
    cluster: &mut SimulationCluster<V>,
    rng: &mut StdRng,
    active_partitions: &mut Vec<ActivePartition>,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    let all_addrs = {
        let mut addrs = cluster.acceptor_addresses();
        addrs.extend(cluster.proposer_addresses());
        addrs
    };
    if all_addrs.len() < 2 {
        return;
    }
    let a = all_addrs[rng.gen_range(0..all_addrs.len())];
    let b = loop {
        let candidate = all_addrs[rng.gen_range(0..all_addrs.len())];
        if candidate != a {
            break candidate;
        }
    };
    let pair = if a < b { (a, b) } else { (b, a) };
    if active_partitions.contains(&pair) {
        return;
    }
    cluster.create_partition(a, b);
    active_partitions.push(pair);
    result.partitions_created += 1;
}

fn heal_random_partition<V>(
    cluster: &mut SimulationCluster<V>,
    active_partitions: &mut Vec<ActivePartition>,
    rng: &mut StdRng,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    if active_partitions.is_empty() {
        return;
    }
    let idx = rng.gen_range(0..active_partitions.len());
    let (a, b) = active_partitions.swap_remove(idx);
    cluster.heal_partition(a, b);
    result.partitions_healed += 1;
}

fn trigger_random_timeout<V>(
    cluster: &mut SimulationCluster<V>,
    rng: &mut StdRng,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    let proposers = cluster.proposer_addresses();
    let target = proposers[rng.gen_range(0..proposers.len())];
    cluster.trigger_timeout(target);
    result.timeouts_triggered += 1;
}

fn maybe_check_safety<V>(
    step: usize,
    cluster: &SimulationCluster<V>,
    monitor: &mut AcceptorSafetyMonitor<V>,
    num_acceptors: u32,
    result: &mut ChaosResult,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    if !step.is_multiple_of(10) {
        return;
    }
    observe_all_acceptors(cluster, monitor, num_acceptors);
    if let Err(violation) = monitor.check_safety() {
        result.safety_ok = false;
        result.error = Some(format!("Safety violation at step {step}: {violation:?}"));
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Runs chaos testing against a simulation cluster.
pub struct ChaosRunner {
    chaos_config: ChaosConfig,
    cluster_config: ClusterConfig,
    seed: u64,
}

#[allow(clippy::unused_self)]
impl ChaosRunner {
    pub fn new(cluster_config: ClusterConfig, chaos_config: ChaosConfig, seed: u64) -> Self {
        Self {
            chaos_config,
            cluster_config,
            seed,
        }
    }

    /// Run the chaos test. Returns the result.
    pub fn run(&self) -> ChaosResult {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut cluster = SimulationCluster::<i32>::new(self.cluster_config.clone());
        let mut monitor = AcceptorSafetyMonitor::new(self.cluster_config.num_acceptors as usize);
        let mut result = ChaosResult::new();

        let mut next_value = 1i32;
        let mut crashed_acceptors: Vec<Address> = Vec::new();
        let mut active_partitions: Vec<ActivePartition> = Vec::new();

        for step in 0..self.chaos_config.max_steps {
            cluster.run_to_completion(self.chaos_config.sim_steps_per_chaos_step);
            match pick_action(&self.chaos_config, rng.gen()) {
                Some(ChaosAction::SubmitProposal) => {
                    self.submit_proposal(&mut cluster, &mut rng, &mut next_value, &mut result);
                }
                Some(action) => apply_shared_action(
                    action,
                    &self.chaos_config,
                    &mut cluster,
                    &mut rng,
                    &mut crashed_acceptors,
                    &mut active_partitions,
                    &mut result,
                ),
                None => {}
            }

            maybe_check_safety(
                step,
                &cluster,
                &mut monitor,
                self.cluster_config.num_acceptors,
                &mut result,
            );

            if !result.safety_ok {
                result.steps_executed = step + 1;
                return result;
            }
        }

        // Final safety check.
        observe_all_acceptors(&cluster, &mut monitor, self.cluster_config.num_acceptors);
        if let Err(violation) = monitor.check_safety() {
            result.safety_ok = false;
            result.error = Some(format!("Final safety violation: {violation:?}"));
        }

        result.proposals_completed = cluster.client_responses().len();
        result.steps_executed = self.chaos_config.max_steps;
        result
    }

    // -- Chaos actions ------------------------------------------------------

    fn submit_proposal(
        &self,
        cluster: &mut SimulationCluster<i32>,
        rng: &mut StdRng,
        next_value: &mut i32,
        result: &mut ChaosResult,
    ) {
        let proposers = cluster.proposer_addresses();
        let proposer = proposers[rng.gen_range(0..proposers.len())];
        let value = *next_value;
        *next_value += 1;
        let op = Box::new(FnOperation::new("chaos-write", move |_: &i32| {
            OperationResult::Success(value)
        }));
        cluster.send_proposal(proposer, op);
        result.proposals_submitted += 1;
    }
}

/// Snapshot all acceptor states and feed them into the safety monitor.
fn observe_all_acceptors<V>(
    cluster: &SimulationCluster<V>,
    monitor: &mut AcceptorSafetyMonitor<V>,
    num_acceptors: u32,
) where
    V: Clone + PartialEq + Default + std::fmt::Debug,
{
    let mut states = BTreeMap::new();
    for i in 1..=num_acceptors {
        let addr = Address::acceptor(i);
        states.insert(addr, cluster.acceptor_state(addr));
    }
    monitor.observe(&states);
}

// ---------------------------------------------------------------------------
// StringChaosRunner — chaos with Porcupine history recording
// ---------------------------------------------------------------------------

/// Result of a [`StringChaosRunner`] run, including the Porcupine history.
#[derive(Debug)]
pub struct StringChaosResult {
    pub inner: ChaosResult,
    pub history: PorcupineHistoryBuilder,
}

/// Chaos runner using [`StringValue`] with Porcupine history recording.
///
/// Runs the same chaos injection pattern as [`ChaosRunner`] but uses
/// `StringValue` operations (character appends) and records call/return
/// events for linearizability verification.
pub struct StringChaosRunner {
    chaos_config: ChaosConfig,
    cluster_config: ClusterConfig,
    seed: u64,
}

impl StringChaosRunner {
    pub fn new(cluster_config: ClusterConfig, chaos_config: ChaosConfig, seed: u64) -> Self {
        Self {
            chaos_config,
            cluster_config,
            seed,
        }
    }

    /// Run the chaos test with history recording. Returns the result
    /// including a [`PorcupineHistoryBuilder`] that can be exported.
    #[allow(clippy::too_many_lines)]
    pub fn run(&self) -> StringChaosResult {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut cluster = SimulationCluster::<StringValue>::new(self.cluster_config.clone());
        let mut monitor = AcceptorSafetyMonitor::new(self.cluster_config.num_acceptors as usize);
        let mut result = ChaosResult::new();
        let mut history = PorcupineHistoryBuilder::new("string")
            .with_scenario("chaos")
            .with_seed(self.seed);

        let mut next_char = b'A';
        let mut crashed_acceptors: Vec<Address> = Vec::new();
        let mut active_partitions: Vec<ActivePartition> = Vec::new();
        // Track pending request IDs that haven't been recorded as returns yet.
        let mut pending_req_ids: Vec<u32> = Vec::new();

        for step in 0..self.chaos_config.max_steps {
            cluster.run_to_completion(self.chaos_config.sim_steps_per_chaos_step);

            // Record return events for any newly completed proposals.
            Self::record_completed(&cluster, &mut history, &mut pending_req_ids, &mut result);

            match pick_action(&self.chaos_config, rng.gen()) {
                Some(ChaosAction::SubmitProposal) => Self::submit_proposal(
                    &mut cluster,
                    &mut rng,
                    &mut next_char,
                    &mut result,
                    &mut history,
                    &mut pending_req_ids,
                ),
                Some(action) => apply_shared_action(
                    action,
                    &self.chaos_config,
                    &mut cluster,
                    &mut rng,
                    &mut crashed_acceptors,
                    &mut active_partitions,
                    &mut result,
                ),
                None => {}
            }

            maybe_check_safety(
                step,
                &cluster,
                &mut monitor,
                self.cluster_config.num_acceptors,
                &mut result,
            );

            if !result.safety_ok {
                result.steps_executed = step + 1;
                return StringChaosResult {
                    inner: result,
                    history,
                };
            }
        }

        // Final: drain any remaining completions.
        cluster.run_to_completion(200);
        Self::record_completed(&cluster, &mut history, &mut pending_req_ids, &mut result);

        observe_all_acceptors(&cluster, &mut monitor, self.cluster_config.num_acceptors);
        if let Err(violation) = monitor.check_safety() {
            result.safety_ok = false;
            result.error = Some(format!("Final safety violation: {violation:?}"));
        }

        result.proposals_completed = cluster.client_responses().len();
        result.steps_executed = self.chaos_config.max_steps;
        StringChaosResult {
            inner: result,
            history,
        }
    }

    fn submit_proposal(
        cluster: &mut SimulationCluster<StringValue>,
        rng: &mut StdRng,
        next_char: &mut u8,
        result: &mut ChaosResult,
        history: &mut PorcupineHistoryBuilder,
        pending_req_ids: &mut Vec<u32>,
    ) {
        let proposers = cluster.proposer_addresses();
        let proposer = proposers[rng.gen_range(0..proposers.len())];
        let ch = *next_char as char;
        // Wrap around after 'Z'.
        *next_char = if *next_char >= b'Z' {
            b'A'
        } else {
            *next_char + 1
        };
        let op = AppendCharacterOp::new(ch);
        let input = op.porcupine_input();
        let client = Address::client(proposer.ordinal);
        let req_id = cluster.send_proposal(proposer, Box::new(op));
        history.on_proposal_sent(client, proposer, req_id, input);
        pending_req_ids.push(req_id);
        result.proposals_submitted += 1;
    }

    fn record_completed(
        cluster: &SimulationCluster<StringValue>,
        history: &mut PorcupineHistoryBuilder,
        pending_req_ids: &mut Vec<u32>,
        result: &mut ChaosResult,
    ) {
        pending_req_ids.retain(|&id| {
            if let Some(resp) = cluster.client_responses().get(&id) {
                let output = ValueOutput::from_string(&resp.response.committed_value);
                history.on_proposal_completed(id, output, resp.response.round);
                result.proposals_completed += 1;
                false
            } else {
                true
            }
        });
    }
}
