//! Protocol messages for `Fast CASPaxos`.
//!
//! Each message is its own struct (not a single enum) to support the handler
//! pattern where each message type maps to a distinct handler method.

use std::fmt;

use crate::ballot::Ballot;

// ---------------------------------------------------------------------------
// Prepare phase
// ---------------------------------------------------------------------------

/// Phase-1a: proposer asks acceptors to promise not to accept older ballots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareRequest {
    pub ballot: Ballot,
}

/// Phase-1b success: acceptor promises and returns its last accepted state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparePromise<V, A> {
    pub round: u32,
    pub acceptor: A,
    pub accepted_ballot: Ballot,
    pub accepted_value: V,
}

/// Phase-1b rejection: acceptor already promised a higher ballot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareRejection<V, A> {
    pub round: u32,
    pub acceptor: A,
    pub accepted_ballot: Ballot,
    pub accepted_value: V,
    pub conflict_ballot: Ballot,
}

// ---------------------------------------------------------------------------
// Accept phase
// ---------------------------------------------------------------------------

/// Phase-2a: proposer asks acceptors to accept a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptRequest<V> {
    pub ballot: Ballot,
    pub value: V,
    /// If set, tells the acceptor to prepare this ballot after accepting.
    pub next_ballot_to_prepare: Option<Ballot>,
}

/// Phase-2b success: acceptor accepted the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptAccepted<A> {
    pub round: u32,
    pub acceptor: A,
    pub promised_ballot: Ballot,
}

/// Phase-2b rejection: acceptor already promised a conflicting ballot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptRejected<A> {
    pub round: u32,
    pub acceptor: A,
    pub conflicting_ballot: Ballot,
}

// ---------------------------------------------------------------------------
// Client-facing
// ---------------------------------------------------------------------------

/// Final response returned to the client after consensus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeResponse<V> {
    pub round: u32,
    pub committed_value: V,
}

/// Wrapper for a value that has been committed through consensus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedValue<V> {
    pub value: V,
}

/// A [`ProposeResponse`] annotated with the proposer that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedProposeResponse<V, A> {
    pub proposer: A,
    pub response: ProposeResponse<V>,
}

impl<V, A> RoutedProposeResponse<V, A> {
    pub fn round(&self) -> u32 {
        self.response.round
    }

    pub fn committed_value(&self) -> &V {
        &self.response.committed_value
    }
}

// ---------------------------------------------------------------------------
// Display impls — match C# ToString patterns
// ---------------------------------------------------------------------------

impl fmt::Display for PrepareRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrepareRequest({})", self.ballot)
    }
}

impl<V: fmt::Display, A: fmt::Display> fmt::Display for PreparePromise<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PreparePromise(r{}, {}, {}, {})",
            self.round, self.acceptor, self.accepted_ballot, self.accepted_value
        )
    }
}

impl<V: fmt::Display, A: fmt::Display> fmt::Display for PrepareRejection<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PrepareRejection(r{}, {}, {}, {}, {})",
            self.round,
            self.acceptor,
            self.accepted_ballot,
            self.accepted_value,
            self.conflict_ballot
        )
    }
}

impl<V: fmt::Display> fmt::Display for AcceptRequest<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.next_ballot_to_prepare {
            Some(next) => write!(
                f,
                "AcceptRequest({}, {}, next: {})",
                self.ballot, self.value, next
            ),
            None => write!(f, "AcceptRequest({}, {})", self.ballot, self.value),
        }
    }
}

impl<A: fmt::Display> fmt::Display for AcceptAccepted<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.promised_ballot.round > self.round {
            write!(
                f,
                "AcceptAccepted(r{}, {}, Promised: {})",
                self.round, self.acceptor, self.promised_ballot
            )
        } else {
            write!(f, "AcceptAccepted(r{}, {})", self.round, self.acceptor)
        }
    }
}

impl<A: fmt::Display> fmt::Display for AcceptRejected<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AcceptRejected(r{}, {}, {})",
            self.round, self.acceptor, self.conflicting_ballot
        )
    }
}

impl<V: fmt::Display> fmt::Display for ProposeResponse<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProposeResponse(r{}, {})",
            self.round, self.committed_value
        )
    }
}

impl<V: fmt::Display> fmt::Display for CommittedValue<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommittedValue({})", self.value)
    }
}

impl<V: fmt::Display, A: fmt::Display> fmt::Display for RoutedProposeResponse<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RoutedProposeResponse({}, {})",
            self.proposer, self.response
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ballot::Ballot;

    #[test]
    fn prepare_request_display() {
        let msg = PrepareRequest {
            ballot: Ballot::initial_fast(),
        };
        assert_eq!(msg.to_string(), "PrepareRequest(Ballot(r1, p0))");
    }

    #[test]
    fn prepare_promise_display() {
        let msg = PreparePromise {
            round: 1,
            acceptor: "node-A",
            accepted_ballot: Ballot::ZERO,
            accepted_value: 42,
        };
        assert_eq!(msg.to_string(), "PreparePromise(r1, node-A, Ballot(ø), 42)");
    }

    #[test]
    fn prepare_rejection_display() {
        let msg = PrepareRejection {
            round: 2,
            acceptor: "node-B",
            accepted_ballot: Ballot::new(0, 1, 0),
            accepted_value: 10,
            conflict_ballot: Ballot::new(0, 2, 3),
        };
        assert_eq!(
            msg.to_string(),
            "PrepareRejection(r2, node-B, Ballot(r1, p0), 10, Ballot(r2, p3))"
        );
    }

    #[test]
    fn accept_request_display_without_next() {
        let msg = AcceptRequest {
            ballot: Ballot::new(0, 1, 0),
            value: "hello",
            next_ballot_to_prepare: None,
        };
        assert_eq!(msg.to_string(), "AcceptRequest(Ballot(r1, p0), hello)");
    }

    #[test]
    fn accept_request_display_with_next() {
        let msg = AcceptRequest {
            ballot: Ballot::new(0, 1, 0),
            value: "hello",
            next_ballot_to_prepare: Some(Ballot::new(0, 2, 1)),
        };
        assert_eq!(
            msg.to_string(),
            "AcceptRequest(Ballot(r1, p0), hello, next: Ballot(r2, p1))"
        );
    }

    #[test]
    fn accept_accepted_display_simple() {
        let msg = AcceptAccepted {
            round: 3,
            acceptor: "node-C",
            promised_ballot: Ballot::new(0, 3, 0),
        };
        assert_eq!(msg.to_string(), "AcceptAccepted(r3, node-C)");
    }

    #[test]
    fn accept_accepted_display_with_promise() {
        let msg = AcceptAccepted {
            round: 3,
            acceptor: "node-C",
            promised_ballot: Ballot::new(0, 4, 1),
        };
        assert_eq!(
            msg.to_string(),
            "AcceptAccepted(r3, node-C, Promised: Ballot(r4, p1))"
        );
    }

    #[test]
    fn accept_rejected_display() {
        let msg = AcceptRejected {
            round: 1,
            acceptor: "node-D",
            conflicting_ballot: Ballot::new(0, 2, 5),
        };
        assert_eq!(
            msg.to_string(),
            "AcceptRejected(r1, node-D, Ballot(r2, p5))"
        );
    }

    #[test]
    fn propose_response_display() {
        let msg = ProposeResponse {
            round: 1,
            committed_value: 99,
        };
        assert_eq!(msg.to_string(), "ProposeResponse(r1, 99)");
    }

    #[test]
    fn committed_value_display() {
        let cv = CommittedValue { value: "done" };
        assert_eq!(cv.to_string(), "CommittedValue(done)");
    }

    #[test]
    fn routed_propose_response() {
        let rpr = RoutedProposeResponse {
            proposer: "node-P",
            response: ProposeResponse {
                round: 5,
                committed_value: 42,
            },
        };
        assert_eq!(rpr.round(), 5);
        assert_eq!(*rpr.committed_value(), 42);
        assert_eq!(
            rpr.to_string(),
            "RoutedProposeResponse(node-P, ProposeResponse(r5, 42))"
        );
    }

    #[test]
    fn struct_equality() {
        let a = PrepareRequest {
            ballot: Ballot::new(0, 1, 0),
        };
        let b = PrepareRequest {
            ballot: Ballot::new(0, 1, 0),
        };
        let c = PrepareRequest {
            ballot: Ballot::new(0, 2, 0),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn clone_independence() {
        let original = ProposeResponse {
            round: 1,
            committed_value: String::from("value"),
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }
}
