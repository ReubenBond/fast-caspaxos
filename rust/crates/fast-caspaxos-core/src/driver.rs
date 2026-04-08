//! Client-side operation driver for Fast `CASPaxos`.
//!
//! [`OperationDriver`] maintains a per-proposer work queue and ensures at most
//! one in-flight request per proposer.  Callbacks flow through the
//! [`DriverHandler`] trait so the host can decide how proposals and completions
//! are delivered.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use crate::messages::RoutedProposeResponse;
use crate::operation::{Operation, RoutedOperation};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A scheduled proposal ready to send to a proposer.
#[derive(Debug)]
pub struct ScheduledProposal<V, A> {
    pub proposer: A,
    pub operation: Box<dyn Operation<V>>,
}

/// Summary produced when all pending work has been drained.
#[derive(Debug, Clone)]
pub struct DriverCompletion<V, A: Ord> {
    pub final_values: BTreeMap<A, V>,
    pub latest_value: V,
    pub final_values_agree: bool,
}

impl<V: PartialEq + fmt::Debug, A: Ord> DriverCompletion<V, A> {
    /// Panics if the latest committed value does not match `expected`.
    pub fn ensure_expected_value(&self, expected: &V) {
        assert_eq!(
            &self.latest_value, expected,
            "final value does not match expected"
        );
    }
}

/// Handler trait for driver callbacks.
pub trait DriverHandler<V, A: Ord> {
    /// Called when the driver has a proposal ready to send.
    fn on_send_proposal(&mut self, proposal: ScheduledProposal<V, A>);

    /// Called when all proposers have finished their work.
    fn on_completed(&mut self, completion: DriverCompletion<V, A>);
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Client-side work queue that maintains at-most-one in-flight request per
/// proposer.
///
/// Generic over `V` (the replicated value type) and `A` (the address /
/// route type used to identify proposers).
pub struct OperationDriver<V, A: Ord> {
    caller: A,
    proposer_order: Vec<A>,
    proposers: BTreeMap<A, ProposerWork<V>>,
    latest_responses: BTreeMap<A, LatestResponse<V>>,
}

struct ProposerWork<V> {
    pending: VecDeque<Box<dyn Operation<V>>>,
    has_scheduled_request: bool,
}

struct LatestResponse<V> {
    round: u32,
    committed_value: V,
}

impl<V: 'static, A: Ord + Clone + fmt::Debug + Send + Sync + 'static> OperationDriver<V, A> {
    /// Creates a new driver for the given `caller` address and set of
    /// proposers.
    ///
    /// # Panics
    ///
    /// Panics if a duplicate proposer is supplied.
    pub fn new(caller: A, proposers: impl IntoIterator<Item = A>) -> Self {
        let mut order = Vec::new();
        let mut map = BTreeMap::new();
        for p in proposers {
            assert!(
                map.insert(
                    p.clone(),
                    ProposerWork {
                        pending: VecDeque::new(),
                        has_scheduled_request: false,
                    }
                )
                .is_none(),
                "duplicate proposer"
            );
            order.push(p);
        }
        Self {
            caller,
            proposer_order: order,
            proposers: map,
            latest_responses: BTreeMap::new(),
        }
    }

    /// Ordered list of proposers.
    pub fn proposers(&self) -> &[A] {
        &self.proposer_order
    }

    /// Returns `true` if any proposer has pending operations or an in-flight
    /// request.
    pub fn has_outstanding_work(&self) -> bool {
        self.proposers
            .values()
            .any(|w| !w.pending.is_empty() || w.has_scheduled_request)
    }

    /// Enqueues a single operation for the given proposer.
    ///
    /// # Panics
    ///
    /// Panics if `proposer` was not supplied at construction time.
    pub fn enqueue(&mut self, proposer: &A, operation: Box<dyn Operation<V>>) {
        self.get_work_mut(proposer).pending.push_back(operation);
    }

    /// Enqueues multiple operations for the given proposer.
    pub fn enqueue_range(
        &mut self,
        proposer: &A,
        operations: impl IntoIterator<Item = Box<dyn Operation<V>>>,
    ) {
        let work = self.get_work_mut(proposer);
        for op in operations {
            work.pending.push_back(op);
        }
    }

    /// Returns a snapshot of the pending operations for a proposer (without
    /// removing them).
    pub fn pending_operations(&self, proposer: &A) -> Vec<&dyn Operation<V>> {
        self.proposers
            .get(proposer)
            .unwrap_or_else(|| panic!("unknown proposer: {proposer:?}"))
            .pending
            .iter()
            .map(|op| &**op as &dyn Operation<V>)
            .collect()
    }

    /// Starts all ready requests (one per proposer that has no in-flight
    /// request and at least one pending operation).
    pub fn start_ready_requests(&mut self, handler: &mut impl DriverHandler<V, A>) {
        for proposal in self.start_ready_requests_core() {
            handler.on_send_proposal(proposal);
        }
    }

    /// Handles a proposer response, schedules the next request for that
    /// proposer if possible, and fires the completion callback when all
    /// work is done.
    pub fn handle_response(
        &mut self,
        response: &RoutedProposeResponse<V, A>,
        handler: &mut impl DriverHandler<V, A>,
    ) where
        V: Clone + PartialEq + Default,
    {
        {
            let work = self.get_work_mut(&response.proposer);
            assert!(
                work.has_scheduled_request,
                "received response for proposer {:?} but no request was in-flight",
                response.proposer
            );
            work.has_scheduled_request = false;
        }
        self.record_latest_response(response);
        for proposal in self.start_ready_requests_core() {
            handler.on_send_proposal(proposal);
        }
        if !self.has_outstanding_work() {
            let completion = self.create_completion();
            handler.on_completed(completion);
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl<V: 'static, A: Ord + Clone + fmt::Debug + Send + Sync + 'static> OperationDriver<V, A> {
    fn get_work_mut(&mut self, proposer: &A) -> &mut ProposerWork<V> {
        self.proposers
            .get_mut(proposer)
            .unwrap_or_else(|| panic!("unknown proposer: {proposer:?}"))
    }

    fn start_ready_requests_core(&mut self) -> Vec<ScheduledProposal<V, A>> {
        // Collect the (proposer, operation) pairs first to avoid borrowing
        // self.proposer_order and self.proposers simultaneously as mutable.
        let dequeued: Vec<(A, Box<dyn Operation<V>>)> = self
            .proposer_order
            .iter()
            .filter_map(|proposer| {
                let work = self.proposers.get_mut(proposer).unwrap();
                if work.has_scheduled_request {
                    return None;
                }
                work.pending.pop_front().map(|op| (proposer.clone(), op))
            })
            .collect();

        let mut requests = Vec::with_capacity(dequeued.len());
        for (proposer, operation) in dequeued {
            let wrapped = self.create_request_operation(operation);
            self.proposers
                .get_mut(&proposer)
                .unwrap()
                .has_scheduled_request = true;
            requests.push(ScheduledProposal {
                proposer,
                operation: wrapped,
            });
        }
        requests
    }

    fn create_request_operation(&self, operation: Box<dyn Operation<V>>) -> Box<dyn Operation<V>> {
        // Wrap the operation with caller routing information so responses
        // can be routed back.  If the operation is already a RoutedOperation
        // for *this* caller we leave it as-is; checking that at runtime
        // would require downcasting which is not ergonomic with trait
        // objects in Rust, so we always wrap.
        Box::new(RoutedOperation::new(self.caller.clone(), operation))
    }

    fn record_latest_response(&mut self, response: &RoutedProposeResponse<V, A>)
    where
        V: Clone,
    {
        let round = response.round();
        let should_insert = match self.latest_responses.get(&response.proposer) {
            Some(existing) => existing.round < round,
            None => true,
        };
        if should_insert {
            self.latest_responses.insert(
                response.proposer.clone(),
                LatestResponse {
                    round,
                    committed_value: response.committed_value().clone(),
                },
            );
        }
    }

    fn create_completion(&self) -> DriverCompletion<V, A>
    where
        V: Clone + PartialEq + Default,
    {
        let mut final_values = BTreeMap::new();
        let mut latest_value = V::default();
        let mut first_value: Option<V> = None;
        let mut agree = true;

        for (proposer, resp) in &self.latest_responses {
            let val = resp.committed_value.clone();
            final_values.insert(proposer.clone(), val.clone());

            match &first_value {
                Some(fv) => {
                    if fv != &val {
                        agree = false;
                    }
                }
                None => {
                    first_value = Some(val.clone());
                }
            }

            latest_value = val;
        }

        DriverCompletion {
            final_values,
            latest_value,
            final_values_agree: agree,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ProposeResponse;
    use crate::operation::{FnOperation, OperationResult};

    // -- Test helpers -------------------------------------------------------

    struct RecordingHandler {
        proposals: Vec<(u32, String)>,
        completions: Vec<DriverCompletion<i32, u32>>,
    }

    impl RecordingHandler {
        fn new() -> Self {
            Self {
                proposals: Vec::new(),
                completions: Vec::new(),
            }
        }
    }

    impl DriverHandler<i32, u32> for RecordingHandler {
        fn on_send_proposal(&mut self, proposal: ScheduledProposal<i32, u32>) {
            self.proposals
                .push((proposal.proposer, format!("{:?}", proposal.operation)));
        }

        fn on_completed(&mut self, completion: DriverCompletion<i32, u32>) {
            self.completions.push(completion);
        }
    }

    fn make_response(proposer: u32, round: u32, value: i32) -> RoutedProposeResponse<i32, u32> {
        RoutedProposeResponse {
            proposer,
            response: ProposeResponse {
                round,
                committed_value: value,
            },
        }
    }

    fn add_op(value: i32) -> Box<dyn Operation<i32>> {
        Box::new(FnOperation::new(format!("add({value})"), move |v: &i32| {
            OperationResult::Success(v + value)
        }))
    }

    // -- Tests --------------------------------------------------------------

    #[test]
    fn no_work_initially() {
        let driver = OperationDriver::<i32, u32>::new(0, [1, 2, 3]);
        assert!(!driver.has_outstanding_work());
    }

    #[test]
    fn single_proposer_single_op() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue(&1, add_op(10));
        assert!(driver.has_outstanding_work());

        driver.start_ready_requests(&mut handler);
        assert_eq!(handler.proposals.len(), 1);
        assert_eq!(handler.proposals[0].0, 1);

        // Still outstanding because a request is in-flight
        assert!(driver.has_outstanding_work());

        driver.handle_response(&make_response(1, 1, 42), &mut handler);
        assert!(!driver.has_outstanding_work());

        assert_eq!(handler.completions.len(), 1);
        let c = &handler.completions[0];
        assert_eq!(c.latest_value, 42);
        assert!(c.final_values_agree);
        assert_eq!(c.final_values[&1], 42);
    }

    #[test]
    fn single_proposer_multiple_ops_serialized() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue(&1, add_op(1));
        driver.enqueue(&1, add_op(2));
        driver.enqueue(&1, add_op(3));

        driver.start_ready_requests(&mut handler);
        // Only one in-flight at a time
        assert_eq!(handler.proposals.len(), 1);

        driver.handle_response(&make_response(1, 1, 10), &mut handler);
        // Second op dispatched
        assert_eq!(handler.proposals.len(), 2);
        assert!(driver.has_outstanding_work());

        driver.handle_response(&make_response(1, 2, 20), &mut handler);
        // Third op dispatched
        assert_eq!(handler.proposals.len(), 3);
        assert!(driver.has_outstanding_work());

        driver.handle_response(&make_response(1, 3, 30), &mut handler);
        assert!(!driver.has_outstanding_work());
        assert_eq!(handler.completions.len(), 1);
        assert_eq!(handler.completions[0].latest_value, 30);
    }

    #[test]
    fn multiple_proposers_parallel() {
        let mut driver = OperationDriver::new(0u32, [1u32, 2u32, 3u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue(&1, add_op(10));
        driver.enqueue(&2, add_op(20));
        driver.enqueue(&3, add_op(30));

        driver.start_ready_requests(&mut handler);
        // All three get their first op
        assert_eq!(handler.proposals.len(), 3);
        let sent_proposers: Vec<u32> = handler.proposals.iter().map(|(p, _)| *p).collect();
        assert_eq!(sent_proposers, vec![1, 2, 3]);

        driver.handle_response(&make_response(1, 1, 100), &mut handler);
        assert!(driver.has_outstanding_work()); // 2, 3 still in-flight

        driver.handle_response(&make_response(2, 1, 200), &mut handler);
        assert!(driver.has_outstanding_work()); // 3 still in-flight

        driver.handle_response(&make_response(3, 1, 300), &mut handler);
        assert!(!driver.has_outstanding_work());
        assert_eq!(handler.completions.len(), 1);
    }

    #[test]
    fn completion_values_agree() {
        let mut driver = OperationDriver::new(0u32, [1u32, 2u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue(&1, add_op(1));
        driver.enqueue(&2, add_op(1));
        driver.start_ready_requests(&mut handler);

        driver.handle_response(&make_response(1, 1, 42), &mut handler);
        driver.handle_response(&make_response(2, 1, 42), &mut handler);

        let c = &handler.completions[0];
        assert!(c.final_values_agree);
        assert_eq!(c.latest_value, 42);
    }

    #[test]
    fn completion_values_disagree() {
        let mut driver = OperationDriver::new(0u32, [1u32, 2u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue(&1, add_op(1));
        driver.enqueue(&2, add_op(1));
        driver.start_ready_requests(&mut handler);

        driver.handle_response(&make_response(1, 1, 42), &mut handler);
        driver.handle_response(&make_response(2, 1, 99), &mut handler);

        let c = &handler.completions[0];
        assert!(!c.final_values_agree);
    }

    #[test]
    fn ensure_expected_value_passes() {
        let c = DriverCompletion {
            final_values: BTreeMap::from([(1u32, 42)]),
            latest_value: 42,
            final_values_agree: true,
        };
        c.ensure_expected_value(&42); // should not panic
    }

    #[test]
    #[should_panic(expected = "final value does not match expected")]
    fn ensure_expected_value_fails() {
        let c = DriverCompletion {
            final_values: BTreeMap::from([(1u32, 42)]),
            latest_value: 42,
            final_values_agree: true,
        };
        c.ensure_expected_value(&99);
    }

    #[test]
    fn enqueue_range_works() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        let mut handler = RecordingHandler::new();

        driver.enqueue_range(&1, vec![add_op(1), add_op(2), add_op(3)]);
        assert_eq!(driver.pending_operations(&1).len(), 3);

        driver.start_ready_requests(&mut handler);
        assert_eq!(handler.proposals.len(), 1);
        // Two still pending after one dispatched
        assert_eq!(driver.pending_operations(&1).len(), 2);
    }

    #[test]
    fn proposers_returns_ordered_list() {
        let driver = OperationDriver::<i32, u32>::new(0, [3, 1, 2]);
        assert_eq!(driver.proposers(), &[3, 1, 2]);
    }

    #[test]
    #[should_panic(expected = "duplicate proposer")]
    fn duplicate_proposer_panics() {
        let _driver = OperationDriver::<i32, u32>::new(0, [1, 2, 1]);
    }

    #[test]
    #[should_panic(expected = "unknown proposer")]
    fn enqueue_unknown_proposer_panics() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        driver.enqueue(&99, add_op(1));
    }

    #[test]
    #[should_panic(expected = "no request was in-flight")]
    fn response_without_scheduled_request_panics() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        let mut handler = RecordingHandler::new();
        driver.handle_response(&make_response(1, 1, 42), &mut handler);
    }

    #[test]
    fn latest_response_keeps_highest_round() {
        let mut driver = OperationDriver::new(0u32, [1u32]);
        let mut handler = RecordingHandler::new();

        // Send two ops sequentially
        driver.enqueue(&1, add_op(1));
        driver.enqueue(&1, add_op(2));
        driver.start_ready_requests(&mut handler);

        // First response at round 5
        driver.handle_response(&make_response(1, 5, 100), &mut handler);
        // Second response at round 3 (lower round — should not replace)
        driver.handle_response(&make_response(1, 3, 200), &mut handler);

        let c = &handler.completions[0];
        // Latest response should still be the round-5 value
        assert_eq!(c.final_values[&1], 100);
        assert_eq!(c.latest_value, 100);
    }

    #[test]
    fn multiple_proposers_with_queued_ops() {
        let mut driver = OperationDriver::new(0u32, [1u32, 2u32]);
        let mut handler = RecordingHandler::new();

        // Queue 2 ops for proposer 1, 1 for proposer 2
        driver.enqueue(&1, add_op(10));
        driver.enqueue(&1, add_op(20));
        driver.enqueue(&2, add_op(30));

        driver.start_ready_requests(&mut handler);
        assert_eq!(handler.proposals.len(), 2); // one per proposer

        // Complete proposer 1's first op → its second should start
        driver.handle_response(&make_response(1, 1, 10), &mut handler);
        assert_eq!(handler.proposals.len(), 3);

        // Complete both remaining
        driver.handle_response(&make_response(2, 1, 30), &mut handler);
        assert!(driver.has_outstanding_work()); // proposer 1 still has one in-flight

        driver.handle_response(&make_response(1, 2, 20), &mut handler);
        assert!(!driver.has_outstanding_work());
        assert_eq!(handler.completions.len(), 1);
    }
}
