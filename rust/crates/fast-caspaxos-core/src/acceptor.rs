//! Acceptor state machine for Fast `CASPaxos`.
//!
//! The [`AcceptorEngine`] is a pure, synchronous state machine that implements
//! the acceptor role in both fast (1-RTT) and classic (2-RTT) rounds. It
//! communicates with the host layer exclusively through the [`AcceptorHandler`]
//! trait — no I/O, no allocations on the hot path.

use crate::ballot::Ballot;
use crate::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, PreparePromise, PrepareRejection, PrepareRequest,
};

// ---------------------------------------------------------------------------
// Persisted state snapshot
// ---------------------------------------------------------------------------

/// Snapshot of acceptor state for persistence/recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorState<V> {
    pub promised_ballot: Ballot,
    pub accepted_ballot: Ballot,
    pub accepted_value: V,
}

impl<V: Default> Default for AcceptorState<V> {
    fn default() -> Self {
        Self {
            promised_ballot: Ballot::initial_fast(),
            accepted_ballot: Ballot::ZERO,
            accepted_value: V::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler trait
// ---------------------------------------------------------------------------

/// Handler trait — the host implements this to receive acceptor callbacks.
///
/// This is the zero-cost, zero-alloc equivalent of C#'s abstract methods.
pub trait AcceptorHandler<V, A> {
    fn on_prepare_promised(&mut self, result: PreparePromise<V, A>);
    fn on_prepare_rejected(&mut self, result: PrepareRejection<V, A>);
    fn on_accept_accepted(&mut self, result: AcceptAccepted<A>);
    fn on_accept_rejected(&mut self, result: AcceptRejected<A>);
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Pure acceptor state machine. No I/O, no async.
///
/// `V` must be `Clone + PartialEq + Default` so we can snapshot values, check
/// idempotency, and initialise with a sensible zero value (matching C#'s
/// `TValue AcceptedValue = default!`).
///
/// `A` must be `Clone` so we can include the acceptor identity in responses.
pub struct AcceptorEngine<V, A> {
    acceptor: A,
    promised_ballot: Ballot,
    accepted_ballot: Ballot,
    accepted_value: V,
}

impl<V: Clone + PartialEq + Default, A: Clone> AcceptorEngine<V, A> {
    /// Create a new acceptor, optionally restoring from persisted state.
    pub fn new(acceptor: A, state: Option<AcceptorState<V>>) -> Self {
        let state = state.unwrap_or_default();
        Self {
            acceptor,
            promised_ballot: state.promised_ballot,
            accepted_ballot: state.accepted_ballot,
            accepted_value: state.accepted_value,
        }
    }

    // -- Accessors ----------------------------------------------------------

    pub fn promised_ballot(&self) -> Ballot {
        self.promised_ballot
    }

    pub fn accepted_ballot(&self) -> Ballot {
        self.accepted_ballot
    }

    pub fn accepted_value(&self) -> &V {
        &self.accepted_value
    }

    fn max_ballot(&self) -> Ballot {
        self.promised_ballot.max(self.accepted_ballot)
    }

    /// Returns the highest epoch seen by this acceptor.
    fn max_epoch(&self) -> u32 {
        self.promised_ballot.max_epoch(self.accepted_ballot)
    }

    /// Returns a snapshot of the current state for persistence.
    pub fn state(&self) -> AcceptorState<V> {
        AcceptorState {
            promised_ballot: self.promised_ballot,
            accepted_ballot: self.accepted_ballot,
            accepted_value: self.accepted_value.clone(),
        }
    }

    // -- Prepare (Phase 1) --------------------------------------------------

    pub fn prepare(&mut self, handler: &mut impl AcceptorHandler<V, A>, request: &PrepareRequest) {
        // Epoch fencing: reject requests from stale epochs.
        if request.ballot.epoch < self.max_epoch() {
            handler.on_prepare_rejected(PrepareRejection {
                round: request.ballot.round,
                acceptor: self.acceptor.clone(),
                accepted_ballot: self.accepted_ballot,
                accepted_value: self.accepted_value.clone(),
                conflict_ballot: self.max_ballot(),
            });
            return;
        }

        if request.ballot < self.max_ballot() {
            handler.on_prepare_rejected(PrepareRejection {
                round: request.ballot.round,
                acceptor: self.acceptor.clone(),
                accepted_ballot: self.accepted_ballot,
                accepted_value: self.accepted_value.clone(),
                conflict_ballot: self.max_ballot(),
            });
            return;
        }
        self.promised_ballot = request.ballot;
        handler.on_prepare_promised(PreparePromise {
            round: request.ballot.round,
            acceptor: self.acceptor.clone(),
            accepted_ballot: self.accepted_ballot,
            accepted_value: self.accepted_value.clone(),
        });
    }

    // -- Accept (Phase 2) ---------------------------------------------------

    pub fn accept(&mut self, handler: &mut impl AcceptorHandler<V, A>, request: &AcceptRequest<V>) {
        // Idempotent: same ballot+value succeeds read-only.
        if request.ballot == self.accepted_ballot && request.value == self.accepted_value {
            self.emit_accepted(handler, request);
            return;
        }

        // Epoch fencing: reject requests from stale epochs.
        if request.ballot.epoch < self.max_epoch() {
            self.emit_accept_rejected(handler, request);
            return;
        }

        // Stale ballot rejected.
        if request.ballot < self.max_ballot() {
            self.emit_accept_rejected(handler, request);
            return;
        }

        // Ballot reuse with different value rejected.
        if request.ballot == self.accepted_ballot {
            self.emit_accept_rejected(handler, request);
            return;
        }

        // Happy path — accept the proposal.
        self.accepted_ballot = request.ballot;
        self.accepted_value = request.value.clone();

        if let Some(next) = request.next_ballot_to_prepare {
            self.promised_ballot = self.promised_ballot.max(next);
        }

        self.emit_accepted(handler, request);
    }

    fn emit_accepted(&self, handler: &mut impl AcceptorHandler<V, A>, request: &AcceptRequest<V>) {
        handler.on_accept_accepted(AcceptAccepted {
            round: request.ballot.round,
            acceptor: self.acceptor.clone(),
            promised_ballot: self.promised_ballot,
        });
    }

    fn emit_accept_rejected(
        &self,
        handler: &mut impl AcceptorHandler<V, A>,
        request: &AcceptRequest<V>,
    ) {
        handler.on_accept_rejected(AcceptRejected {
            round: request.ballot.round,
            acceptor: self.acceptor.clone(),
            conflicting_ballot: self.max_ballot(),
        });
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ballot::Ballot;
    use proptest::prelude::*;

    // -- Recording handler --------------------------------------------------

    #[derive(Debug, Clone)]
    enum Callback<V: Clone, A: Clone> {
        Promised(PreparePromise<V, A>),
        Rejected(PrepareRejection<V, A>),
        Accepted(AcceptAccepted<A>),
        AcceptRejected(AcceptRejected<A>),
    }

    struct RecordingHandler<V: Clone, A: Clone> {
        calls: Vec<Callback<V, A>>,
    }

    impl<V: Clone, A: Clone> RecordingHandler<V, A> {
        fn new() -> Self {
            Self { calls: Vec::new() }
        }

        fn last(&self) -> &Callback<V, A> {
            self.calls.last().expect("no callbacks recorded")
        }
    }

    impl<V: Clone, A: Clone> AcceptorHandler<V, A> for RecordingHandler<V, A> {
        fn on_prepare_promised(&mut self, result: PreparePromise<V, A>) {
            self.calls.push(Callback::Promised(result));
        }
        fn on_prepare_rejected(&mut self, result: PrepareRejection<V, A>) {
            self.calls.push(Callback::Rejected(result));
        }
        fn on_accept_accepted(&mut self, result: AcceptAccepted<A>) {
            self.calls.push(Callback::Accepted(result));
        }
        fn on_accept_rejected(&mut self, result: AcceptRejected<A>) {
            self.calls.push(Callback::AcceptRejected(result));
        }
    }

    // -- Helpers ------------------------------------------------------------

    type Engine = AcceptorEngine<i32, &'static str>;
    type Handler = RecordingHandler<i32, &'static str>;

    fn engine() -> Engine {
        AcceptorEngine::new("acc-1", None)
    }

    fn handler() -> Handler {
        RecordingHandler::new()
    }

    // -- 1. New acceptor defaults -------------------------------------------

    #[test]
    fn new_acceptor_has_initial_fast_promised_ballot() {
        let e = engine();
        assert_eq!(e.promised_ballot(), Ballot::initial_fast());
    }

    #[test]
    fn new_acceptor_has_zero_accepted_ballot() {
        let e = engine();
        assert_eq!(e.accepted_ballot(), Ballot::ZERO);
    }

    #[test]
    fn new_acceptor_has_default_value() {
        let e = engine();
        assert_eq!(*e.accepted_value(), i32::default());
    }

    // -- 2. Prepare promise -------------------------------------------------

    #[test]
    fn prepare_with_ballot_gte_max_promises() {
        let mut e = engine();
        let mut h = handler();

        let ballot = Ballot::new(0, 2, 1);
        e.prepare(&mut h, &PrepareRequest { ballot });

        match h.last() {
            Callback::Promised(p) => {
                assert_eq!(p.round, 2);
                assert_eq!(p.acceptor, "acc-1");
                assert_eq!(p.accepted_ballot, Ballot::ZERO);
                assert_eq!(p.accepted_value, 0);
            }
            other => panic!("expected Promised, got {:?}", other),
        }
        assert_eq!(e.promised_ballot(), ballot);
    }

    // -- 3. Prepare rejection -----------------------------------------------

    #[test]
    fn prepare_with_ballot_lt_max_rejects() {
        let mut e = engine();
        let mut h = handler();

        // First prepare with high ballot to raise the bar
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 5, 1),
            },
        );
        // Now try a lower one
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 3, 1),
            },
        );

        match h.last() {
            Callback::Rejected(r) => {
                assert_eq!(r.round, 3);
                assert_eq!(r.conflict_ballot, Ballot::new(0, 5, 1));
            }
            other => panic!("expected Rejected, got {:?}", other),
        }
    }

    // -- 4. Sequential prepares ---------------------------------------------

    #[test]
    fn increasing_ballots_all_promise() {
        let mut e = engine();
        let mut h = handler();

        for round in 2..=5 {
            e.prepare(
                &mut h,
                &PrepareRequest {
                    ballot: Ballot::new(0, round, 1),
                },
            );
            assert!(matches!(h.last(), Callback::Promised(_)));
        }
        assert_eq!(e.promised_ballot(), Ballot::new(0, 5, 1));
    }

    // -- 5. Accept success --------------------------------------------------

    #[test]
    fn accept_with_new_ballot_succeeds() {
        let mut e = engine();
        let mut h = handler();

        let ballot = Ballot::initial_fast();
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot,
                value: 42,
                next_ballot_to_prepare: None,
            },
        );

        match h.last() {
            Callback::Accepted(a) => {
                assert_eq!(a.round, 1);
                assert_eq!(a.acceptor, "acc-1");
            }
            other => panic!("expected Accepted, got {:?}", other),
        }
        assert_eq!(e.accepted_ballot(), ballot);
        assert_eq!(*e.accepted_value(), 42);
    }

    // -- 6. Accept idempotent -----------------------------------------------

    #[test]
    fn accept_same_ballot_and_value_is_idempotent() {
        let mut e = engine();
        let mut h = handler();

        let ballot = Ballot::initial_fast();
        let req = AcceptRequest {
            ballot,
            value: 42,
            next_ballot_to_prepare: None,
        };

        e.accept(&mut h, &req);
        let state_after_first = e.state();

        e.accept(&mut h, &req);
        assert_eq!(e.state(), state_after_first);
        assert!(matches!(h.last(), Callback::Accepted(_)));
    }

    // -- 7. Accept rejection — stale ballot ---------------------------------

    #[test]
    fn accept_stale_ballot_rejected() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at round 5 to raise max_ballot
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 5, 1),
            },
        );

        // Accept at round 2 must fail
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(0, 2, 1),
                value: 99,
                next_ballot_to_prepare: None,
            },
        );

        match h.last() {
            Callback::AcceptRejected(r) => {
                assert_eq!(r.round, 2);
                assert_eq!(r.conflicting_ballot, Ballot::new(0, 5, 1));
            }
            other => panic!("expected AcceptRejected, got {:?}", other),
        }
    }

    // -- 8. Accept rejection — ballot reuse ---------------------------------

    #[test]
    fn accept_same_ballot_different_value_rejected() {
        let mut e = engine();
        let mut h = handler();

        let ballot = Ballot::initial_fast();
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot,
                value: 42,
                next_ballot_to_prepare: None,
            },
        );

        // Same ballot, different value
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot,
                value: 99,
                next_ballot_to_prepare: None,
            },
        );

        assert!(matches!(h.last(), Callback::AcceptRejected(_)));
        // Original value preserved
        assert_eq!(*e.accepted_value(), 42);
    }

    // -- 9. Piggybacked prepare ---------------------------------------------

    #[test]
    fn piggybacked_prepare_folds_into_promised() {
        let mut e = engine();
        let mut h = handler();

        let accept_ballot = Ballot::initial_fast();
        let next = Ballot::new(0, 3, 0);
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: accept_ballot,
                value: 10,
                next_ballot_to_prepare: Some(next),
            },
        );

        assert_eq!(e.promised_ballot(), next);
        match h.last() {
            Callback::Accepted(a) => assert_eq!(a.promised_ballot, next),
            other => panic!("expected Accepted, got {:?}", other),
        }
    }

    // -- 10. Piggybacked prepare — no downgrade -----------------------------

    #[test]
    fn piggybacked_prepare_does_not_downgrade() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at a high ballot
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 10, 1),
            },
        );

        // Accept at that ballot with a lower next_ballot_to_prepare
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(0, 10, 1),
                value: 55,
                next_ballot_to_prepare: Some(Ballot::new(0, 5, 0)),
            },
        );

        // Should keep the higher promised ballot
        assert_eq!(e.promised_ballot(), Ballot::new(0, 10, 1));
    }

    // -- 11. State round-trip -----------------------------------------------

    #[test]
    fn state_round_trip() {
        let mut e = engine();
        let mut h = handler();

        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 3, 1),
            },
        );
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(0, 3, 1),
                value: 77,
                next_ballot_to_prepare: None,
            },
        );

        let snapshot = e.state();
        let restored: Engine = AcceptorEngine::new("acc-1", Some(snapshot.clone()));
        assert_eq!(restored.state(), snapshot);
    }

    // -- 12. Invariant: max_ballot never decreases --------------------------

    #[test]
    fn invariant_max_ballot_never_decreases() {
        let mut e = engine();
        let mut h = handler();
        let mut prev_max = e.promised_ballot().max(e.accepted_ballot());

        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 2, 1),
            },
        );
        let cur = e.promised_ballot().max(e.accepted_ballot());
        assert!(cur >= prev_max);
        prev_max = cur;

        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(0, 2, 1),
                value: 5,
                next_ballot_to_prepare: None,
            },
        );
        let cur = e.promised_ballot().max(e.accepted_ballot());
        assert!(cur >= prev_max);
        prev_max = cur;

        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(0, 4, 2),
            },
        );
        let cur = e.promised_ballot().max(e.accepted_ballot());
        assert!(cur >= prev_max);
        let _ = prev_max; // suppress unused warning
    }

    // -- Property tests -----------------------------------------------------

    fn arb_ballot() -> impl Strategy<Value = Ballot> {
        (1u32..20, 0u32..5).prop_map(|(round, proposer)| Ballot::new(0, round, proposer))
    }

    #[derive(Debug, Clone)]
    enum Op {
        Prepare(Ballot),
        Accept(Ballot, i32, Option<Ballot>),
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            arb_ballot().prop_map(Op::Prepare),
            (
                arb_ballot(),
                -100i32..100,
                proptest::option::of(arb_ballot())
            )
                .prop_map(|(b, v, n)| Op::Accept(b, v, n)),
        ]
    }

    proptest! {
        #[test]
        fn max_ballot_never_decreases(ops in proptest::collection::vec(arb_op(), 1..50)) {
            let mut e = engine();
            let mut h = handler();
            let mut prev_max = e.promised_ballot().max(e.accepted_ballot());

            for op in ops {
                match op {
                    Op::Prepare(ballot) => {
                        e.prepare(&mut h, &PrepareRequest { ballot });
                    }
                    Op::Accept(ballot, value, next) => {
                        e.accept(
                            &mut h,
                            &AcceptRequest { ballot, value, next_ballot_to_prepare: next },
                        );
                    }
                }
                let cur_max = e.promised_ballot().max(e.accepted_ballot());
                prop_assert!(
                    cur_max >= prev_max,
                    "max_ballot decreased: {:?} < {:?}",
                    cur_max,
                    prev_max,
                );
                prev_max = cur_max;
            }
        }

        #[test]
        fn idempotent_accept_does_not_change_state(
            ballot in arb_ballot(),
            value in -100i32..100,
        ) {
            let mut e = engine();
            let mut h = handler();

            // Prepare so the ballot is accepted
            e.prepare(&mut h, &PrepareRequest { ballot });
            e.accept(
                &mut h,
                &AcceptRequest { ballot, value, next_ballot_to_prepare: None },
            );

            let state_before = e.state();

            // Idempotent replay
            e.accept(
                &mut h,
                &AcceptRequest { ballot, value, next_ballot_to_prepare: None },
            );

            prop_assert_eq!(e.state(), state_before);
            match h.last() {
                Callback::Accepted(_) => {}
                other => prop_assert!(false, "expected Accepted, got {:?}", other),
            }
        }

        /// Accepted value only changes when accepted ballot changes.
        #[test]
        fn accepted_value_changes_only_with_ballot(ops in proptest::collection::vec(arb_op(), 0..50)) {
            let mut e = engine();
            let mut h = handler();
            let mut prev_ballot = e.accepted_ballot();
            let mut prev_value = e.accepted_value().clone();
            for op in ops {
                match op {
                    Op::Prepare(b) => e.prepare(&mut h, &PrepareRequest { ballot: b }),
                    Op::Accept(b, v, nb) => e.accept(&mut h, &AcceptRequest { ballot: b, value: v, next_ballot_to_prepare: nb }),
                }
                let cur_ballot = e.accepted_ballot();
                let cur_value = e.accepted_value().clone();
                if cur_value != prev_value {
                    prop_assert!(cur_ballot != prev_ballot,
                        "value changed from {:?} to {:?} without ballot change (ballot {:?})",
                        prev_value, cur_value, cur_ballot);
                }
                prev_ballot = cur_ballot;
                prev_value = cur_value;
            }
        }

        /// Idempotent accepts with arbitrary ballot/value preserve state.
        #[test]
        fn idempotent_accept_preserves_state(
            ballot in arb_ballot(),
            value in -100i32..100,
        ) {
            let mut e = engine();
            let mut h = handler();
            // First prepare to allow the ballot
            e.prepare(&mut h, &PrepareRequest { ballot });
            // First accept
            e.accept(&mut h, &AcceptRequest { ballot, value, next_ballot_to_prepare: None });
            let state_after_first = e.state();
            h.calls.clear();
            // Second accept (idempotent)
            e.accept(&mut h, &AcceptRequest { ballot, value, next_ballot_to_prepare: None });
            let state_after_second = e.state();
            prop_assert_eq!(state_after_first, state_after_second);
        }

        /// Prepare with strictly increasing ballots always succeeds.
        #[test]
        fn increasing_prepares_always_succeed(rounds in proptest::collection::vec(1u32..100, 1..20)) {
            let mut e = engine();
            let mut h = handler();
            let mut prev_round = 0u32;
            for r in rounds {
                let ballot_round = prev_round.saturating_add(r);
                prev_round = ballot_round;
                let ballot = Ballot::new(0, ballot_round, 1);
                e.prepare(&mut h, &PrepareRequest { ballot });
                match h.last() {
                    Callback::Promised(_) => {},
                    other => prop_assert!(false, "Expected promise, got {:?}", other),
                }
            }
        }
    }

    // -- Epoch fencing tests ------------------------------------------------

    #[test]
    fn prepare_stale_epoch_rejected() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at epoch 3, round 5
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(3, 5, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));

        // Prepare at epoch 2, round 10 — higher round but stale epoch → reject
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(2, 10, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Rejected(_)));
    }

    #[test]
    fn prepare_higher_epoch_lower_round_rejected() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at epoch 1, round 10
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(1, 10, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));

        // Prepare at epoch 3, round 1 — higher epoch but lower round → reject
        // (epoch passes fencing, but round fails normal Paxos check)
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(3, 1, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Rejected(_)));
    }

    #[test]
    fn prepare_higher_epoch_higher_round_accepted() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at epoch 1, round 5
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(1, 5, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));

        // Prepare at epoch 3, round 11 — higher epoch AND higher round → accept
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(3, 11, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));
    }

    #[test]
    fn accept_stale_epoch_rejected() {
        let mut e = engine();
        let mut h = handler();

        // Prepare + accept at epoch 3, round 5
        let ballot = Ballot::new(3, 5, 1);
        e.prepare(&mut h, &PrepareRequest { ballot });
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot,
                value: 42,
                next_ballot_to_prepare: None,
            },
        );
        assert!(matches!(h.last(), Callback::Accepted(_)));

        // Accept at epoch 2, round 10 — higher round but stale epoch → reject
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(2, 10, 1),
                value: 99,
                next_ballot_to_prepare: None,
            },
        );
        assert!(matches!(h.last(), Callback::AcceptRejected(_)));
    }

    #[test]
    fn config_bump_between_prepare_and_accept_succeeds() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at epoch 1, round 5, proposer 1
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(1, 5, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));

        // Accept at epoch 2, round 5, proposer 1 — config bumped, same round
        // Epoch fencing passes (2 >= 1). Ballot (5,1) >= max_ballot (5,1). ✓
        // PartialEq: (e2,r5,p1) != (e1,r5,p1) → not idempotent, not reuse → new accept
        e.accept(
            &mut h,
            &AcceptRequest {
                ballot: Ballot::new(2, 5, 1),
                value: 77,
                next_ballot_to_prepare: None,
            },
        );
        assert!(matches!(h.last(), Callback::Accepted(_)));
        assert_eq!(*e.accepted_value(), 77);
    }

    #[test]
    fn same_epoch_normal_paxos_ordering() {
        let mut e = engine();
        let mut h = handler();

        // Prepare at epoch 5, round 3
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(5, 3, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));

        // Lower round in same epoch → rejected (normal Paxos)
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(5, 2, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Rejected(_)));

        // Higher round in same epoch → accepted
        e.prepare(
            &mut h,
            &PrepareRequest {
                ballot: Ballot::new(5, 4, 1),
            },
        );
        assert!(matches!(h.last(), Callback::Promised(_)));
    }
}
