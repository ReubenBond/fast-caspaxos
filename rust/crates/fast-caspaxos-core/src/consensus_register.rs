//! Combined Fast `CASPaxos` register.
//!
//! [`ConsensusRegister`] composes the existing proposer and acceptor engines into
//! a single per-register abstraction that owns:
//!
//! - coordinator/request-queue behavior
//! - proposer and acceptor protocol state
//! - local proposer->acceptor short-circuiting
//! - deferred learned-commit application while busy

use std::collections::VecDeque;
use std::fmt;

use crate::acceptor::{AcceptorEngine, AcceptorHandler, AcceptorState};
use crate::ballot::Ballot;
use crate::messages::{
    AcceptAccepted, AcceptRejected, AcceptRequest, CommittedValue, PreparePromise,
    PrepareRejection, PrepareRequest, ProposeResponse,
};
use crate::operation::Operation;
use crate::proposer::{
    ProposerConfig, ProposerEngine, ProposerHandler, ValueStatus,
};

/// Client proposal queued for execution by a [`ConsensusRegister`].
pub struct ClientRequest<V, A> {
    pub from: A,
    pub request_id: u32,
    pub operation: Box<dyn Operation<V>>,
}

impl<V, A: fmt::Debug> fmt::Debug for ClientRequest<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientRequest")
            .field("from", &self.from)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

/// A request that was already forwarded by another proposer.
pub struct ForwardedRequest<V, A> {
    pub from: A,
    pub original_caller: A,
    pub request_id: u32,
    pub operation: Box<dyn Operation<V>>,
}

impl<V, A: fmt::Debug> fmt::Debug for ForwardedRequest<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForwardedRequest")
            .field("from", &self.from)
            .field("original_caller", &self.original_caller)
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

/// Messages emitted by a [`ConsensusRegister`].
#[derive(Debug)]
pub enum ConsensusRegisterOutput<V, A> {
    PrepareRequest {
        from: A,
        to: A,
        payload: PrepareRequest,
    },
    AcceptRequest {
        from: A,
        to: A,
        payload: AcceptRequest<V>,
    },
    PreparePromise {
        from: A,
        to: A,
        payload: PreparePromise<V, A>,
    },
    PrepareRejection {
        from: A,
        to: A,
        payload: PrepareRejection<V, A>,
    },
    AcceptAccepted {
        from: A,
        to: A,
        payload: AcceptAccepted<A>,
    },
    AcceptRejected {
        from: A,
        to: A,
        payload: AcceptRejected<A>,
    },
    ProposeResponse {
        to: A,
        request_id: u32,
        response: ProposeResponse<V>,
    },
    ForwardedResponse {
        to: A,
        original_caller: A,
        request_id: u32,
        accepted_ballot: Ballot,
        prepared_ballot: Ballot,
        response: ProposeResponse<V>,
    },
    LearnCommittedValue {
        accepted_ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    },
}

/// Combined register that owns proposer, acceptor, and request coordination for one replica.
pub struct ConsensusRegister<V, A> {
    proposer: A,
    acceptor: A,
    acceptors: Vec<A>,
    proposer_engine: ProposerEngine<V, A>,
    acceptor_engine: AcceptorEngine<V, A>,
    request_queue: VecDeque<PendingRequest<V, A>>,
    active_request: Option<ActiveRequest<A>>,
    pending_learned_commit: Option<LearnedCommit<V>>,
    local_messages: VecDeque<LocalMessage<V, A>>,
    outbox: Vec<ConsensusRegisterOutput<V, A>>,
    draining: bool,
}

impl<V, A> ConsensusRegister<V, A>
where
    V: Clone + PartialEq + Default,
    A: Ord + Clone,
{
    /// Create a new combined register.
    pub fn new(
        proposer: A,
        acceptor: A,
        config: ProposerConfig,
        mut acceptors: Vec<A>,
        acceptor_state: Option<AcceptorState<V>>,
    ) -> Self {
        if !acceptors.iter().any(|candidate| candidate == &acceptor) {
            acceptors.push(acceptor.clone());
        }

        let proposer_engine = ProposerEngine::new(config, acceptors.clone());
        let acceptor_engine = AcceptorEngine::new(acceptor.clone(), acceptor_state);

        Self {
            proposer,
            acceptor,
            acceptors,
            proposer_engine,
            acceptor_engine,
            request_queue: VecDeque::new(),
            active_request: None,
            pending_learned_commit: None,
            local_messages: VecDeque::new(),
            outbox: Vec::new(),
            draining: false,
        }
    }

    /// The proposer identity owned by this register.
    pub fn proposer(&self) -> &A {
        &self.proposer
    }

    /// The acceptor identity owned by this register.
    pub fn acceptor(&self) -> &A {
        &self.acceptor
    }

    /// Addresses of all acceptors targeted by this register's proposer.
    pub fn acceptors(&self) -> &[A] {
        &self.acceptors
    }

    /// Current proposer ballot.
    pub fn proposer_ballot(&self) -> Ballot {
        self.proposer_engine.ballot()
    }

    /// Current cached proposer value.
    pub fn proposer_current_value(&self) -> &V {
        self.proposer_engine.current_value()
    }

    /// Current proposer value status.
    pub fn proposer_value_status(&self) -> ValueStatus {
        self.proposer_engine.value_status()
    }

    /// Prepared ballot cached for reuse, if any.
    pub fn proposer_prepared_ballot(&self) -> Option<Ballot> {
        self.proposer_engine.prepared_ballot()
    }

    /// Snapshot of durable acceptor state.
    pub fn acceptor_state(&self) -> AcceptorState<V> {
        self.acceptor_engine.state()
    }

    /// Returns `true` if the register still has queued or in-flight work.
    pub fn has_pending_requests(&self) -> bool {
        self.active_request.is_some()
            || self.proposer_engine.has_pending_proposal()
            || !self.request_queue.is_empty()
            || !self.local_messages.is_empty()
    }

    /// Submit a client request.
    pub fn submit_client_request(&mut self, request: ClientRequest<V, A>) {
        self.enqueue_or_start(PendingRequest::client(request));
        self.drain_work_loop();
    }

    /// Deliver a request that has already been forwarded by another proposer.
    pub fn deliver_forwarded_request(&mut self, request: ForwardedRequest<V, A>) {
        self.enqueue_or_start(PendingRequest::forwarded(request));
        self.drain_work_loop();
    }

    /// Deliver a prepare request to the local acceptor.
    pub fn deliver_prepare(&mut self, from: A, request: &PrepareRequest) {
        let request = request.clone();
        self.with_acceptor_handler_no_drain(from, |engine, handler| {
            engine.prepare(handler, &request);
        });
        self.drain_work_loop();
    }

    /// Deliver an accept request to the local acceptor.
    pub fn deliver_accept(&mut self, from: A, request: &AcceptRequest<V>) {
        let request = request.clone();
        self.with_acceptor_handler_no_drain(from, |engine, handler| {
            engine.accept(handler, &request);
        });
        self.drain_work_loop();
    }

    /// Deliver a prepare promise to the local proposer.
    pub fn deliver_prepare_promise(&mut self, msg: PreparePromise<V, A>) {
        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.handle_prepare_promised(msg, handler);
        });
        self.drain_work_loop();
    }

    /// Deliver a prepare rejection to the local proposer.
    pub fn deliver_prepare_rejection(&mut self, msg: PrepareRejection<V, A>) {
        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.handle_prepare_rejected(msg, handler);
        });
        self.drain_work_loop();
    }

    /// Deliver an accept-accepted response to the local proposer.
    pub fn deliver_accept_accepted(&mut self, msg: AcceptAccepted<A>) {
        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.handle_accept_accepted(msg, handler);
        });
        self.drain_work_loop();
    }

    /// Deliver an accept-rejected response to the local proposer.
    pub fn deliver_accept_rejected(&mut self, msg: AcceptRejected<A>) {
        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.handle_accept_rejected(msg, handler);
        });
        self.drain_work_loop();
    }

    /// Learn an externally committed value.
    pub fn learn_committed_value(
        &mut self,
        accepted_ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    ) {
        if self.has_pending_requests() {
            self.queue_learned_commit(accepted_ballot, value, prepared_ballot);
        } else {
            self.with_proposer_handler_no_drain(|engine, handler| {
                engine.learn_committed_value(accepted_ballot, value, prepared_ballot, handler);
            });
        }

        self.drain_work_loop();
    }

    /// Handle a proposer response timeout.
    pub fn handle_response_timeout(&mut self, round: u32) -> bool {
        let mut retried = false;
        self.with_proposer_handler_no_drain(|engine, handler| {
            retried = engine.handle_response_timeout(round, handler);
        });
        self.drain_work_loop();
        retried
    }

    /// Take all emitted messages.
    pub fn drain_messages(&mut self) -> Vec<ConsensusRegisterOutput<V, A>> {
        std::mem::take(&mut self.outbox)
    }

    fn enqueue_or_start(&mut self, request: PendingRequest<V, A>) {
        if self.active_request.is_some() || self.proposer_engine.has_pending_proposal() {
            self.request_queue.push_back(request);
        } else {
            self.start_active_request(request);
        }
    }

    fn start_active_request(&mut self, request: PendingRequest<V, A>) {
        let active_request = request.active_request();
        let operation = request.operation;
        self.active_request = Some(active_request);
        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.start_proposal(operation, handler);
        });
    }

    fn queue_learned_commit(
        &mut self,
        accepted_ballot: Ballot,
        value: V,
        prepared_ballot: Ballot,
    ) {
        if accepted_ballot < self.proposer_engine.ballot() {
            return;
        }

        if let Some(pending) = &self.pending_learned_commit {
            if accepted_ballot < pending.accepted_ballot
                || (accepted_ballot == pending.accepted_ballot
                    && prepared_ballot <= pending.prepared_ballot)
            {
                return;
            }
        }

        self.pending_learned_commit = Some(LearnedCommit {
            accepted_ballot,
            value,
            prepared_ballot,
        });
    }

    fn apply_pending_learned_commit_if_idle(&mut self) {
        if self.active_request.is_some()
            || self.proposer_engine.has_pending_proposal()
            || !self.request_queue.is_empty()
        {
            return;
        }

        let Some(learned) = self.pending_learned_commit.take() else {
            return;
        };

        self.with_proposer_handler_no_drain(|engine, handler| {
            engine.learn_committed_value(
                learned.accepted_ballot,
                learned.value,
                learned.prepared_ballot,
                handler,
            );
        });
    }

    fn drain_work_loop(&mut self) {
        if self.draining {
            return;
        }

        self.draining = true;
        loop {
            while let Some(message) = self.local_messages.pop_front() {
                match message {
                    LocalMessage::PrepareRequest { from, payload } => {
                        self.with_acceptor_handler_no_drain(from, |engine, handler| {
                            engine.prepare(handler, &payload);
                        });
                    }
                    LocalMessage::AcceptRequest { from, payload } => {
                        self.with_acceptor_handler_no_drain(from, |engine, handler| {
                            engine.accept(handler, &payload);
                        });
                    }
                    LocalMessage::PreparePromise { payload } => {
                        self.with_proposer_handler_no_drain(|engine, handler| {
                            engine.handle_prepare_promised(payload, handler);
                        });
                    }
                    LocalMessage::PrepareRejection { payload } => {
                        self.with_proposer_handler_no_drain(|engine, handler| {
                            engine.handle_prepare_rejected(payload, handler);
                        });
                    }
                    LocalMessage::AcceptAccepted { payload } => {
                        self.with_proposer_handler_no_drain(|engine, handler| {
                            engine.handle_accept_accepted(payload, handler);
                        });
                    }
                    LocalMessage::AcceptRejected { payload } => {
                        self.with_proposer_handler_no_drain(|engine, handler| {
                            engine.handle_accept_rejected(payload, handler);
                        });
                    }
                }
            }

            let mut progressed = false;
            if self.active_request.is_some() && !self.proposer_engine.has_pending_proposal() {
                self.active_request = None;
                progressed = true;
                self.apply_pending_learned_commit_if_idle();
                if self.active_request.is_none() {
                    if let Some(next) = self.request_queue.pop_front() {
                        self.start_active_request(next);
                    }
                }
            }

            if !progressed && self.local_messages.is_empty() {
                break;
            }
        }

        self.draining = false;
    }

    fn with_proposer_handler_no_drain<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ProposerEngine<V, A>, &mut ProposerCallbacks<'_, V, A>),
    {
        let active_request = self.active_request.clone();
        let proposer = self.proposer.clone();
        let acceptor = self.acceptor.clone();
        let acceptors = &self.acceptors;
        let proposer_engine = &mut self.proposer_engine;
        let local_messages = &mut self.local_messages;
        let outbox = &mut self.outbox;

        let mut completed_response = None;
        let mut committed_value = None;
        {
            let mut handler = ProposerCallbacks {
                proposer,
                acceptor,
                acceptors,
                local_messages,
                outbox,
                completed_response: &mut completed_response,
                committed_value: &mut committed_value,
            };
            f(proposer_engine, &mut handler);
        }

        if let Some(value) = committed_value {
            self.outbox.push(ConsensusRegisterOutput::LearnCommittedValue {
                accepted_ballot: self.proposer_engine.ballot(),
                value,
                prepared_ballot: self.proposer_engine.prepared_ballot().unwrap_or(Ballot::ZERO),
            });
        }

        if let Some(response) = completed_response {
            let active_request = active_request.expect("proposal completed with no active request");
            if active_request.is_forwarded_execution {
                self.outbox.push(ConsensusRegisterOutput::ForwardedResponse {
                    to: active_request.reply_to,
                    original_caller: active_request.original_caller,
                    request_id: active_request.request_id,
                    accepted_ballot: self.proposer_engine.ballot(),
                    prepared_ballot: self.proposer_engine.prepared_ballot().unwrap_or(Ballot::ZERO),
                    response,
                });
            } else {
                self.outbox.push(ConsensusRegisterOutput::ProposeResponse {
                    to: active_request.reply_to,
                    request_id: active_request.request_id,
                    response,
                });
            }
        }
    }

    fn with_acceptor_handler_no_drain<F>(&mut self, response_target: A, f: F)
    where
        F: FnOnce(&mut AcceptorEngine<V, A>, &mut AcceptorCallbacks<'_, V, A>),
    {
        let proposer = self.proposer.clone();
        let acceptor = self.acceptor.clone();
        let acceptor_engine = &mut self.acceptor_engine;
        let local_messages = &mut self.local_messages;
        let outbox = &mut self.outbox;

        let mut handler = AcceptorCallbacks {
            proposer,
            acceptor,
            response_target,
            local_messages,
            outbox,
        };
        f(acceptor_engine, &mut handler);
    }
}

struct PendingRequest<V, A> {
    request_id: u32,
    reply_to: A,
    original_caller: A,
    is_forwarded_execution: bool,
    operation: Box<dyn Operation<V>>,
}

impl<V, A: fmt::Debug> fmt::Debug for PendingRequest<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingRequest")
            .field("request_id", &self.request_id)
            .field("reply_to", &self.reply_to)
            .field("original_caller", &self.original_caller)
            .field("is_forwarded_execution", &self.is_forwarded_execution)
            .finish_non_exhaustive()
    }
}

impl<V, A: Clone> PendingRequest<V, A> {
    fn client(request: ClientRequest<V, A>) -> Self {
        Self {
            request_id: request.request_id,
            reply_to: request.from.clone(),
            original_caller: request.from,
            is_forwarded_execution: false,
            operation: request.operation,
        }
    }

    fn forwarded(request: ForwardedRequest<V, A>) -> Self {
        Self {
            request_id: request.request_id,
            reply_to: request.from,
            original_caller: request.original_caller,
            is_forwarded_execution: true,
            operation: request.operation,
        }
    }

    fn active_request(&self) -> ActiveRequest<A>
    where
        A: Clone,
    {
        ActiveRequest {
            request_id: self.request_id,
            reply_to: self.reply_to.clone(),
            original_caller: self.original_caller.clone(),
            is_forwarded_execution: self.is_forwarded_execution,
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveRequest<A> {
    request_id: u32,
    reply_to: A,
    original_caller: A,
    is_forwarded_execution: bool,
}

#[derive(Debug)]
struct LearnedCommit<V> {
    accepted_ballot: Ballot,
    value: V,
    prepared_ballot: Ballot,
}

#[derive(Debug)]
enum LocalMessage<V, A> {
    PrepareRequest { from: A, payload: PrepareRequest },
    AcceptRequest { from: A, payload: AcceptRequest<V> },
    PreparePromise { payload: PreparePromise<V, A> },
    PrepareRejection { payload: PrepareRejection<V, A> },
    AcceptAccepted { payload: AcceptAccepted<A> },
    AcceptRejected { payload: AcceptRejected<A> },
}

struct ProposerCallbacks<'a, V, A> {
    proposer: A,
    acceptor: A,
    acceptors: &'a [A],
    local_messages: &'a mut VecDeque<LocalMessage<V, A>>,
    outbox: &'a mut Vec<ConsensusRegisterOutput<V, A>>,
    completed_response: &'a mut Option<ProposeResponse<V>>,
    committed_value: &'a mut Option<V>,
}

impl<V, A> ProposerHandler<V, A> for ProposerCallbacks<'_, V, A>
where
    V: Clone + PartialEq + Default,
    A: Ord + Clone,
{
    fn send_prepare(&mut self, request: PrepareRequest) {
        for acceptor in self.acceptors {
            if acceptor == &self.acceptor {
                self.local_messages.push_back(LocalMessage::PrepareRequest {
                    from: self.proposer.clone(),
                    payload: request.clone(),
                });
            } else {
                self.outbox.push(ConsensusRegisterOutput::PrepareRequest {
                    from: self.proposer.clone(),
                    to: acceptor.clone(),
                    payload: request.clone(),
                });
            }
        }
    }

    fn send_accept(&mut self, request: AcceptRequest<V>) {
        for acceptor in self.acceptors {
            if acceptor == &self.acceptor {
                self.local_messages.push_back(LocalMessage::AcceptRequest {
                    from: self.proposer.clone(),
                    payload: request.clone(),
                });
            } else {
                self.outbox.push(ConsensusRegisterOutput::AcceptRequest {
                    from: self.proposer.clone(),
                    to: acceptor.clone(),
                    payload: request.clone(),
                });
            }
        }
    }

    fn proposal_completed(&mut self, response: ProposeResponse<V>) {
        *self.completed_response = Some(response);
    }

    fn value_committed(&mut self, committed: CommittedValue<V>) {
        *self.committed_value = Some(committed.value);
    }
}

struct AcceptorCallbacks<'a, V, A> {
    proposer: A,
    acceptor: A,
    response_target: A,
    local_messages: &'a mut VecDeque<LocalMessage<V, A>>,
    outbox: &'a mut Vec<ConsensusRegisterOutput<V, A>>,
}

impl<V, A> AcceptorHandler<V, A> for AcceptorCallbacks<'_, V, A>
where
    V: Clone + PartialEq + Default,
    A: Ord + Clone,
{
    fn on_prepare_promised(&mut self, result: PreparePromise<V, A>) {
        if self.response_target == self.proposer {
            self.local_messages
                .push_back(LocalMessage::PreparePromise { payload: result });
        } else {
            self.outbox.push(ConsensusRegisterOutput::PreparePromise {
                from: self.acceptor.clone(),
                to: self.response_target.clone(),
                payload: result,
            });
        }
    }

    fn on_prepare_rejected(&mut self, result: PrepareRejection<V, A>) {
        if self.response_target == self.proposer {
            self.local_messages
                .push_back(LocalMessage::PrepareRejection { payload: result });
        } else {
            self.outbox.push(ConsensusRegisterOutput::PrepareRejection {
                from: self.acceptor.clone(),
                to: self.response_target.clone(),
                payload: result,
            });
        }
    }

    fn on_accept_accepted(&mut self, result: AcceptAccepted<A>) {
        if self.response_target == self.proposer {
            self.local_messages
                .push_back(LocalMessage::AcceptAccepted { payload: result });
        } else {
            self.outbox.push(ConsensusRegisterOutput::AcceptAccepted {
                from: self.acceptor.clone(),
                to: self.response_target.clone(),
                payload: result,
            });
        }
    }

    fn on_accept_rejected(&mut self, result: AcceptRejected<A>) {
        if self.response_target == self.proposer {
            self.local_messages
                .push_back(LocalMessage::AcceptRejected { payload: result });
        } else {
            self.outbox.push(ConsensusRegisterOutput::AcceptRejected {
                from: self.acceptor.clone(),
                to: self.response_target.clone(),
                payload: result,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{FnOperation, OperationResult};

    fn write_op(value: i32) -> Box<dyn Operation<i32>> {
        Box::new(FnOperation::new("write", move |_: &i32| {
            OperationResult::Success(value)
        }))
    }

    fn register(acceptors: Vec<&'static str>, local_acceptor: &'static str, fast: bool) -> ConsensusRegister<i32, &'static str> {
        ConsensusRegister::new(
            "p1",
            local_acceptor,
            ProposerConfig {
                proposer_id: 1,
                enable_fast_commit: fast,
                enable_distinguished_leader: false,
                ballot_seed: Ballot::ZERO,
            },
            acceptors,
            None,
        )
    }

    #[test]
    fn local_register_completes_without_remote_messages() {
        let mut register = register(vec!["p1"], "p1", true);

        register.submit_client_request(ClientRequest {
            from: "client",
            request_id: 1,
            operation: write_op(42),
        });

        let messages = register.drain_messages();
        assert_eq!(messages.len(), 2);
        assert!(matches!(
            &messages[0],
            ConsensusRegisterOutput::LearnCommittedValue { value, .. } if *value == 42
        ));
        assert!(matches!(
            &messages[1],
            ConsensusRegisterOutput::ProposeResponse { to, request_id, response }
                if *to == "client" && *request_id == 1 && response.committed_value == 42
        ));
        assert_eq!(*register.proposer_current_value(), 42);
        assert_eq!(register.acceptor_state().accepted_value, 42);
        assert!(!register.has_pending_requests());
    }

    #[test]
    fn remote_register_emits_prepare_accept_and_completion_messages() {
        let mut register = register(vec!["a1", "a2", "a3", "a4", "a5"], "a1", false);

        register.submit_client_request(ClientRequest {
            from: "client",
            request_id: 1,
            operation: write_op(7),
        });

        let first = register.drain_messages();
        assert_eq!(
            first
                .iter()
                .filter(|message| matches!(message, ConsensusRegisterOutput::PrepareRequest { .. }))
                .count(),
            4
        );

        register.deliver_prepare_promise(PreparePromise {
            round: register.proposer_ballot().round,
            acceptor: "a2",
            accepted_ballot: Ballot::ZERO,
            accepted_value: 0,
        });
        assert!(register.drain_messages().is_empty());

        register.deliver_prepare_promise(PreparePromise {
            round: register.proposer_ballot().round,
            acceptor: "a3",
            accepted_ballot: Ballot::ZERO,
            accepted_value: 0,
        });
        let second = register.drain_messages();
        assert_eq!(
            second
                .iter()
                .filter(|message| matches!(message, ConsensusRegisterOutput::AcceptRequest { .. }))
                .count(),
            4
        );

        deliver_accept_quorum(&mut register, ["a2", "a3"]);
        let completion = register.drain_messages();
        assert!(completion.iter().any(|message| matches!(
            message,
            ConsensusRegisterOutput::LearnCommittedValue { value, .. } if *value == 7
        )));
        assert!(completion.iter().any(|message| matches!(
            message,
            ConsensusRegisterOutput::ProposeResponse { to, request_id, response }
                if *to == "client" && *request_id == 1 && response.committed_value == 7
        )));
    }

    #[test]
    fn learned_commit_waits_until_pending_queue_drains() {
        let mut register = register(vec!["a1", "a2", "a3", "a4", "a5"], "a1", false);

        register.submit_client_request(ClientRequest {
            from: "client",
            request_id: 1,
            operation: write_op(1),
        });
        register.submit_client_request(ClientRequest {
            from: "client",
            request_id: 2,
            operation: write_op(2),
        });
        register.learn_committed_value(Ballot::new(0, 3, 4), 99, Ballot::new(0, 4, 4));

        register.drain_messages();
        deliver_prepare_quorum(&mut register, ["a2", "a3"]);
        register.drain_messages();
        deliver_accept_quorum(&mut register, ["a2", "a3"]);
        let after_first = register.drain_messages();
        assert!(after_first.iter().any(|message| matches!(
            message,
            ConsensusRegisterOutput::ProposeResponse { request_id, .. } if *request_id == 1
        )));
        assert_eq!(*register.proposer_current_value(), 1);
        assert_eq!(register.acceptor_state().accepted_value, 1);

        deliver_prepare_quorum(&mut register, ["a2", "a3"]);
        register.drain_messages();
        deliver_accept_quorum(&mut register, ["a2", "a3"]);
        let after_second = register.drain_messages();
        assert!(after_second.iter().any(|message| matches!(
            message,
            ConsensusRegisterOutput::ProposeResponse { request_id, .. } if *request_id == 2
        )));
        assert_eq!(register.proposer_ballot(), Ballot::new(0, 3, 4));
        assert_eq!(*register.proposer_current_value(), 99);
        assert_eq!(register.proposer_prepared_ballot(), Some(Ballot::new(0, 4, 4)));
        assert_eq!(register.acceptor_state().accepted_value, 2);
    }

    #[test]
    fn forwarded_request_emits_forwarded_response() {
        let mut register = register(vec!["p1"], "p1", true);

        register.deliver_forwarded_request(ForwardedRequest {
            from: "forwarder",
            original_caller: "client",
            request_id: 9,
            operation: write_op(11),
        });

        let messages = register.drain_messages();
        assert!(matches!(&messages[0], ConsensusRegisterOutput::LearnCommittedValue { value, .. } if *value == 11));
        assert!(matches!(
            &messages[1],
            ConsensusRegisterOutput::ForwardedResponse {
                to,
                original_caller,
                request_id,
                response,
                ..
            } if *to == "forwarder" && *original_caller == "client" && *request_id == 9 && response.committed_value == 11
        ));
    }

    #[test]
    fn timeout_triggers_retry_messages() {
        let mut register = register(vec!["a1", "a2", "a3", "a4", "a5"], "a1", false);

        register.submit_client_request(ClientRequest {
            from: "client",
            request_id: 1,
            operation: write_op(5),
        });
        let first = register.drain_messages();
        let round = match &first[0] {
            ConsensusRegisterOutput::PrepareRequest { payload, .. } => payload.ballot.round,
            other => panic!("expected prepare request, got {other:?}"),
        };

        assert!(register.handle_response_timeout(round));
        let retry = register.drain_messages();
        assert!(retry.iter().any(|message| matches!(
            message,
            ConsensusRegisterOutput::PrepareRequest { .. }
        )));
    }

    fn deliver_prepare_quorum(
        register: &mut ConsensusRegister<i32, &'static str>,
        acceptors: [&'static str; 2],
    ) {
        for acceptor in acceptors {
            register.deliver_prepare_promise(PreparePromise {
                round: register.proposer_ballot().round,
                acceptor,
                accepted_ballot: Ballot::ZERO,
                accepted_value: 0,
            });
        }
    }

    fn deliver_accept_quorum(
        register: &mut ConsensusRegister<i32, &'static str>,
        acceptors: [&'static str; 2],
    ) {
        for acceptor in acceptors {
            register.deliver_accept_accepted(AcceptAccepted {
                round: register.proposer_ballot().round,
                acceptor,
                promised_ballot: register.proposer_ballot(),
            });
        }
    }
}
