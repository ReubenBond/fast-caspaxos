//! Porcupine linearizability history export.
//!
//! Produces a JSON history of call/return events that can be fed into the
//! [Porcupine](https://github.com/anishathalye/porcupine) model checker to
//! verify linearizability of Fast `CASPaxos` simulation runs.
//!
//! The JSON schema uses camelCase field names and matches the format expected
//! by the Go-based `porcupine-checker` tool in `tools/porcupine-checker/`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::address::Address;
use crate::value::{SetValue, StringValue};

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// Top-level Porcupine history document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PorcupineHistory {
    pub schema_version: String,
    pub model_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub round: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    pub events: Vec<PorcupineEvent>,
}

/// A single call or return event in the history.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PorcupineEvent {
    pub sequence: u64,
    pub kind: EventKind,
    pub operation_id: u32,
    pub request_id: u32,
    pub client_id: u32,
    pub client: String,
    pub proposer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<OperationInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<ValueOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_round: Option<u32>,
}

/// Whether the event is a call (request sent) or return (response received).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Call,
    Return,
}

/// Structured description of the operation being invoked.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OperationInput {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// Structured description of the committed value returned from a proposal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ValueOutput {
    pub version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_value: Option<Vec<String>>,
}

impl ValueOutput {
    /// Create a [`ValueOutput`] from a [`StringValue`].
    pub fn from_string(v: &StringValue) -> Self {
        Self {
            version: v.version,
            string_value: Some(v.value.clone()),
            set_value: None,
        }
    }

    /// Create a [`ValueOutput`] from a [`SetValue`].
    pub fn from_set(v: &SetValue) -> Self {
        let sorted: Vec<String> = v.value.iter().cloned().collect();
        Self {
            version: v.version,
            string_value: None,
            set_value: Some(sorted),
        }
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Metadata stashed when a call event is recorded, so the corresponding
/// return event can reference the same client/proposer.
#[derive(Debug)]
struct PendingCall {
    client_id: u32,
    client: String,
    proposer: String,
}

/// Incrementally builds a [`PorcupineHistory`] from simulation events.
#[derive(Debug)]
pub struct PorcupineHistoryBuilder {
    model_kind: String,
    events: Vec<PorcupineEvent>,
    next_sequence: u64,
    pending: HashMap<u32, PendingCall>,
    scenario_name: Option<String>,
    round: Option<i32>,
    seed: Option<u64>,
}

impl Default for PorcupineHistoryBuilder {
    fn default() -> Self {
        Self::new("register")
    }
}

impl PorcupineHistoryBuilder {
    pub fn new(model_kind: &str) -> Self {
        Self {
            model_kind: model_kind.to_owned(),
            events: Vec::new(),
            next_sequence: 0,
            pending: HashMap::new(),
            scenario_name: None,
            round: None,
            seed: None,
        }
    }

    pub fn with_scenario(mut self, name: impl Into<String>) -> Self {
        self.scenario_name = Some(name.into());
        self
    }

    pub fn with_round(mut self, round: i32) -> Self {
        self.round = Some(round);
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Record a proposal being sent (call event).
    pub fn on_proposal_sent(
        &mut self,
        client: Address,
        proposer: Address,
        request_id: u32,
        input: OperationInput,
    ) {
        let client_id = proposer.ordinal;
        let client_str = client.network_address();
        let proposer_str = proposer.network_address();

        self.events.push(PorcupineEvent {
            sequence: self.next_sequence,
            kind: EventKind::Call,
            operation_id: request_id,
            request_id,
            client_id,
            client: client_str.clone(),
            proposer: proposer_str.clone(),
            input: Some(input),
            output: None,
            protocol_round: None,
        });
        self.next_sequence += 1;

        self.pending.insert(
            request_id,
            PendingCall {
                client_id,
                client: client_str,
                proposer: proposer_str,
            },
        );
    }

    /// Record a proposal completing (return event).
    ///
    /// # Panics
    ///
    /// Panics if `request_id` does not match a prior call event.
    pub fn on_proposal_completed(
        &mut self,
        request_id: u32,
        output: ValueOutput,
        protocol_round: u32,
    ) {
        let call = self
            .pending
            .remove(&request_id)
            .unwrap_or_else(|| panic!("no matching call for request_id {request_id}"));

        self.events.push(PorcupineEvent {
            sequence: self.next_sequence,
            kind: EventKind::Return,
            operation_id: request_id,
            request_id,
            client_id: call.client_id,
            client: call.client,
            proposer: call.proposer,
            input: None,
            output: Some(output),
            protocol_round: Some(protocol_round),
        });
        self.next_sequence += 1;
    }

    /// Build the final history document.
    pub fn build(&self) -> PorcupineHistory {
        PorcupineHistory {
            schema_version: "fast-caspaxos-porcupine-history-v1".to_owned(),
            model_kind: self.model_kind.clone(),
            scenario_name: self.scenario_name.clone(),
            round: self.round,
            seed: self.seed,
            events: self.events.clone(),
        }
    }

    /// Serialize the history to a pretty-printed JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self.build()).expect("history serialization should not fail")
    }

    /// Serialize the history to a compact single-line JSON string (for JSONL).
    pub fn to_json_line(&self) -> String {
        serde_json::to_string(&self.build()).expect("history serialization should not fail")
    }

    /// Number of events recorded so far.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::Address;

    fn append_input(ch: &str) -> OperationInput {
        OperationInput {
            kind: "append_character".to_owned(),
            expected_version: None,
            value: Some(ch.to_owned()),
        }
    }

    fn read_input() -> OperationInput {
        OperationInput {
            kind: "read".to_owned(),
            expected_version: None,
            value: None,
        }
    }

    fn string_output(version: u32, value: &str) -> ValueOutput {
        ValueOutput {
            version,
            string_value: Some(value.to_owned()),
            set_value: None,
        }
    }

    #[test]
    fn empty_history() {
        let builder = PorcupineHistoryBuilder::new("string");
        let history = builder.build();

        assert!(history.events.is_empty());
        assert_eq!(history.schema_version, "fast-caspaxos-porcupine-history-v1");
        assert_eq!(history.model_kind, "string");
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn single_call_return() {
        let mut builder = PorcupineHistoryBuilder::new("string");

        builder.on_proposal_sent(
            Address::client(0),
            Address::proposer(1),
            1,
            append_input("A"),
        );
        builder.on_proposal_completed(1, string_output(1, "A"), 1);

        let history = builder.build();
        assert_eq!(history.events.len(), 2);

        let call = &history.events[0];
        assert_eq!(call.sequence, 0);
        assert_eq!(call.kind, EventKind::Call);
        assert_eq!(call.operation_id, 1);
        assert_eq!(call.request_id, 1);
        assert_eq!(call.client_id, 1); // proposer ordinal
        assert_eq!(call.client, "client-0");
        assert_eq!(call.proposer, "proposer-1");
        assert!(call.input.is_some());
        assert!(call.output.is_none());

        let ret = &history.events[1];
        assert_eq!(ret.sequence, 1);
        assert_eq!(ret.kind, EventKind::Return);
        assert_eq!(ret.operation_id, 1);
        assert!(ret.input.is_none());
        assert!(ret.output.is_some());
        assert_eq!(ret.protocol_round, Some(1));
    }

    #[test]
    fn multiple_operations() {
        let mut builder = PorcupineHistoryBuilder::new("string");

        for i in 0..3u32 {
            let ch = (b'A' + i as u8) as char;
            builder.on_proposal_sent(
                Address::client(i),
                Address::proposer(0),
                i + 1,
                append_input(&ch.to_string()),
            );
            builder.on_proposal_completed(i + 1, string_output(i + 1, "x"), i + 1);
        }

        let history = builder.build();
        assert_eq!(history.events.len(), 6);

        for (idx, event) in history.events.iter().enumerate() {
            assert_eq!(event.sequence, idx as u64);
        }
    }

    #[test]
    fn json_round_trip() {
        let mut builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("round-trip-test")
            .with_seed(99);

        builder.on_proposal_sent(
            Address::client(0),
            Address::proposer(1),
            1,
            append_input("X"),
        );
        builder.on_proposal_completed(1, string_output(1, "X"), 1);

        let history = builder.build();
        let json = serde_json::to_string(&history).unwrap();
        let deserialized: PorcupineHistory = serde_json::from_str(&json).unwrap();

        assert_eq!(history, deserialized);
    }

    #[test]
    fn json_uses_camel_case() {
        let mut builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("test")
            .with_round(1)
            .with_seed(42);

        builder.on_proposal_sent(
            Address::client(0),
            Address::proposer(1),
            1,
            append_input("A"),
        );
        builder.on_proposal_completed(1, string_output(1, "A"), 1);

        let json = builder.to_json();

        // Top-level fields
        assert!(json.contains("\"schemaVersion\""));
        assert!(json.contains("\"modelKind\""));
        assert!(json.contains("\"scenarioName\""));
        assert!(json.contains("\"events\""));

        // Event fields
        assert!(json.contains("\"operationId\""));
        assert!(json.contains("\"requestId\""));
        assert!(json.contains("\"clientId\""));
        assert!(json.contains("\"protocolRound\""));

        // Input fields
        assert!(json.contains("\"kind\""));

        // Output fields
        assert!(json.contains("\"stringValue\""));

        // Should NOT contain snake_case or PascalCase
        assert!(!json.contains("\"schema_version\""));
        assert!(!json.contains("\"SchemaVersion\""));
        assert!(!json.contains("\"model_kind\""));
        assert!(!json.contains("\"ModelKind\""));
    }

    #[test]
    fn call_has_input_no_output() {
        let mut builder = PorcupineHistoryBuilder::new("string");
        builder.on_proposal_sent(Address::client(0), Address::proposer(0), 1, read_input());

        let call = &builder.build().events[0];
        assert_eq!(call.input.as_ref().unwrap().kind, "read");
        assert!(call.output.is_none());
        assert!(call.protocol_round.is_none());
    }

    #[test]
    fn return_has_output_and_round() {
        let mut builder = PorcupineHistoryBuilder::new("string");
        builder.on_proposal_sent(
            Address::client(0),
            Address::proposer(0),
            1,
            append_input("Z"),
        );
        builder.on_proposal_completed(1, string_output(1, "Z"), 3);

        let ret = &builder.build().events[1];
        assert!(ret.input.is_none());
        let output = ret.output.as_ref().unwrap();
        assert_eq!(output.version, 1);
        assert_eq!(output.string_value.as_deref(), Some("Z"));
        assert_eq!(ret.protocol_round, Some(3));
    }

    #[test]
    fn scenario_metadata() {
        let builder = PorcupineHistoryBuilder::new("string")
            .with_scenario("my-scenario")
            .with_round(5)
            .with_seed(42);

        let history = builder.build();
        assert_eq!(history.scenario_name.as_deref(), Some("my-scenario"));
        assert_eq!(history.round, Some(5));
        assert_eq!(history.seed, Some(42));
    }

    #[test]
    fn concurrent_operations() {
        let mut builder = PorcupineHistoryBuilder::new("string");

        // Two calls issued before either returns.
        builder.on_proposal_sent(
            Address::client(0),
            Address::proposer(0),
            10,
            append_input("A"),
        );
        builder.on_proposal_sent(
            Address::client(1),
            Address::proposer(1),
            20,
            append_input("B"),
        );

        // Returns arrive in a different order than calls.
        builder.on_proposal_completed(20, string_output(2, "AB"), 2);
        builder.on_proposal_completed(10, string_output(1, "A"), 1);

        let history = builder.build();
        assert_eq!(history.events.len(), 4);

        // Sequences are monotonically increasing regardless of interleaving.
        let seqs: Vec<u64> = history.events.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3]);

        // First return matches the second call (request_id 20).
        assert_eq!(history.events[2].kind, EventKind::Return);
        assert_eq!(history.events[2].operation_id, 20);
        assert_eq!(history.events[2].client, "client-1");

        // Second return matches the first call (request_id 10).
        assert_eq!(history.events[3].kind, EventKind::Return);
        assert_eq!(history.events[3].operation_id, 10);
        assert_eq!(history.events[3].client, "client-0");
    }

    #[test]
    fn value_output_from_string() {
        let sv = StringValue::new(3, "hello");
        let output = ValueOutput::from_string(&sv);
        assert_eq!(output.version, 3);
        assert_eq!(output.string_value.as_deref(), Some("hello"));
        assert!(output.set_value.is_none());
    }

    #[test]
    fn value_output_from_set() {
        use std::collections::BTreeSet;
        let items: BTreeSet<String> = ["b", "a", "c"].iter().map(|s| s.to_string()).collect();
        let sv = SetValue::new(2, items);
        let output = ValueOutput::from_set(&sv);
        assert_eq!(output.version, 2);
        assert!(output.string_value.is_none());
        // BTreeSet is sorted, so output preserves alphabetical order
        assert_eq!(
            output.set_value.as_deref(),
            Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()].as_slice())
        );
    }
}
