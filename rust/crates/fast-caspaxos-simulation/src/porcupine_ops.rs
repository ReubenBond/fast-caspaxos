//! Porcupine-aware operations for linearizability history recording.
//!
//! Each operation type implements [`Operation<V>`] for protocol execution
//! and provides a [`porcupine_input`](AppendCharacterOp::porcupine_input)
//! method that returns the structured [`OperationInput`] descriptor for
//! Porcupine history recording.

use std::fmt;

use fast_caspaxos_core::operation::{Operation, OperationResult};

use crate::history::OperationInput;
use crate::value::StringValue;

// ---------------------------------------------------------------------------
// String model operations
// ---------------------------------------------------------------------------

/// Appends a single character to the string register, incrementing the version.
pub struct AppendCharacterOp {
    pub character: char,
}

impl AppendCharacterOp {
    pub fn new(character: char) -> Self {
        Self { character }
    }

    pub fn porcupine_input(&self) -> OperationInput {
        OperationInput {
            kind: "append_character".to_owned(),
            expected_version: None,
            value: Some(self.character.to_string()),
        }
    }
}

impl Operation<StringValue> for AppendCharacterOp {
    fn apply(&self, current: &StringValue) -> OperationResult<StringValue> {
        OperationResult::Success(StringValue::new(
            current.version + 1,
            format!("{}{}", current.value, self.character),
        ))
    }
}

impl fmt::Debug for AppendCharacterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AppendCharacter('{}')", self.character)
    }
}

/// Reads the current string register value without modification.
pub struct ReadStringOp;

impl ReadStringOp {
    pub fn porcupine_input() -> OperationInput {
        OperationInput {
            kind: "read".to_owned(),
            expected_version: None,
            value: None,
        }
    }
}

impl Operation<StringValue> for ReadStringOp {
    fn apply(&self, current: &StringValue) -> OperationResult<StringValue> {
        OperationResult::NotApplicable(current.clone())
    }
}

impl fmt::Debug for ReadStringOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReadString")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_character_applies() {
        let op = AppendCharacterOp::new('A');
        let current = StringValue::default();
        let result = op.apply(&current);
        assert!(result.is_success());
        assert_eq!(result.into_value(), StringValue::new(1, "A"));
    }

    #[test]
    fn append_character_chains() {
        let op_a = AppendCharacterOp::new('A');
        let op_b = AppendCharacterOp::new('B');
        let v0 = StringValue::default();
        let v1 = op_a.apply(&v0).into_value();
        let v2 = op_b.apply(&v1).into_value();
        assert_eq!(v2, StringValue::new(2, "AB"));
    }

    #[test]
    fn append_character_porcupine_input() {
        let op = AppendCharacterOp::new('X');
        let input = op.porcupine_input();
        assert_eq!(input.kind, "append_character");
        assert_eq!(input.value.as_deref(), Some("X"));
        assert!(input.expected_version.is_none());
    }

    #[test]
    fn read_string_returns_current() {
        let op = ReadStringOp;
        let current = StringValue::new(3, "abc");
        let result = op.apply(&current);
        assert!(!result.is_success()); // NotApplicable
        assert_eq!(result.into_value(), StringValue::new(3, "abc"));
    }

    #[test]
    fn read_string_porcupine_input() {
        let input = ReadStringOp::porcupine_input();
        assert_eq!(input.kind, "read");
        assert!(input.value.is_none());
        assert!(input.expected_version.is_none());
    }
}
