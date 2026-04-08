//! Pre-built operation sequences for regression testing.
//!
//! Ported from C# `TestCorpus`. Provides deterministic operation sequences
//! that exercise the protocol in known-good patterns.

use fast_caspaxos_core::operation::{FnOperation, Operation, OperationResult};

/// Pre-built operation sequences for regression testing.
pub struct TestCorpus;

impl TestCorpus {
    /// "Hello, World!" character-by-character append operations.
    /// Each operation appends one character to the string.
    pub fn hello_world_string_ops() -> Vec<Box<dyn Operation<String>>> {
        "Hello, World!"
            .chars()
            .map(|c| -> Box<dyn Operation<String>> {
                Box::new(FnOperation::new(
                    format!("append '{c}'"),
                    move |current: &String| {
                        let mut next = current.clone();
                        next.push(c);
                        OperationResult::Success(next)
                    },
                ))
            })
            .collect()
    }

    /// Integer increment operations (n times).
    pub fn increment_ops(n: usize) -> Vec<Box<dyn Operation<i32>>> {
        (0..n)
            .map(|_| -> Box<dyn Operation<i32>> {
                Box::new(FnOperation::new("increment", |v: &i32| {
                    OperationResult::Success(v + 1)
                }))
            })
            .collect()
    }

    /// Write a specific value.
    pub fn write_op<V: Clone + std::fmt::Debug + 'static>(value: V) -> Box<dyn Operation<V>> {
        Box::new(FnOperation::new(
            format!("write({value:?})"),
            move |_: &V| OperationResult::Success(value.clone()),
        ))
    }

    /// Read (`NotApplicable`) operation.
    pub fn read_op<V: Clone + Default + std::fmt::Debug + 'static>() -> Box<dyn Operation<V>> {
        Box::new(FnOperation::new("read", |_: &V| {
            OperationResult::NotApplicable(V::default())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_world_produces_13_ops() {
        let ops = TestCorpus::hello_world_string_ops();
        assert_eq!(ops.len(), 13);
    }

    #[test]
    fn hello_world_ops_build_expected_string() {
        let ops = TestCorpus::hello_world_string_ops();
        let mut value = String::new();
        for op in &ops {
            let result = op.apply(&value);
            value = result.into_value();
        }
        assert_eq!(value, "Hello, World!");
    }

    #[test]
    fn increment_ops_produces_correct_count() {
        let ops = TestCorpus::increment_ops(5);
        assert_eq!(ops.len(), 5);
    }

    #[test]
    fn increment_ops_apply_correctly() {
        let ops = TestCorpus::increment_ops(3);
        let mut value = 0;
        for op in &ops {
            let result = op.apply(&value);
            value = result.into_value();
        }
        assert_eq!(value, 3);
    }

    #[test]
    fn write_op_sets_value() {
        let op = TestCorpus::write_op(42);
        let result = op.apply(&0);
        assert_eq!(result, OperationResult::Success(42));
    }

    #[test]
    fn read_op_returns_not_applicable() {
        let op = TestCorpus::read_op::<i32>();
        let result = op.apply(&99);
        assert!(matches!(result, OperationResult::NotApplicable(_)));
    }
}
