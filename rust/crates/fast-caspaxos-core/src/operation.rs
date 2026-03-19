//! Operations that can be applied to replicated state.
//!
//! An [`Operation`] transforms a current value into a new value, returning an
//! [`OperationResult`] containing the [`OperationStatus`] and the resulting value.
//!
//! [`RoutedOperation`] wraps any operation with the caller's address so the
//! protocol layer can route responses back to the originator.
//!
//! [`FnOperation`] provides a convenient way to create operations from closures.

use core::fmt;

/// Status of an operation application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationStatus {
    /// The operation failed to apply.
    Failed,
    /// The operation was not applicable to the current value.
    NotApplicable,
    /// The operation was applied successfully.
    Success,
}

/// Result of applying an operation to a value.
#[derive(Debug, Clone)]
pub struct OperationResult<V> {
    pub status: OperationStatus,
    pub value: V,
}

impl<V> OperationResult<V> {
    /// Creates a new `OperationResult`.
    pub fn new(status: OperationStatus, value: V) -> Self {
        Self { status, value }
    }
}

/// An operation that can be applied to a value to produce a new value.
///
/// This is the Rust equivalent of the C# `IOperation<TValue>` interface.
/// Implementations must also implement [`Debug`] for diagnostics.
pub trait Operation<V>: fmt::Debug {
    /// Applies this operation to the current value, returning the new value
    /// and the status of the application.
    fn apply(&self, current: &V) -> OperationResult<V>;
}

/// An operation tagged with the caller's address for routing.
///
/// Wraps any [`Operation<V>`] with the caller's address so the protocol layer
/// can route responses back to the originator. Delegates [`Operation::apply`]
/// to the inner operation.
pub struct RoutedOperation<V, A> {
    /// The address of the caller that submitted this operation.
    pub caller: A,
    operation: Box<dyn Operation<V>>,
}

impl<V, A> RoutedOperation<V, A> {
    /// Creates a new `RoutedOperation` wrapping the given operation with a caller address.
    pub fn new(caller: A, operation: Box<dyn Operation<V>>) -> Self {
        Self { caller, operation }
    }

    /// Returns a reference to the inner operation.
    pub fn operation(&self) -> &dyn Operation<V> {
        &*self.operation
    }
}

impl<V, A> Operation<V> for RoutedOperation<V, A>
where
    A: fmt::Debug,
{
    fn apply(&self, current: &V) -> OperationResult<V> {
        self.operation.apply(current)
    }
}

impl<V, A: fmt::Debug> fmt::Debug for RoutedOperation<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutedOperation")
            .field("caller", &self.caller)
            .field("operation", &self.operation)
            .finish()
    }
}

impl<V, A: fmt::Display> fmt::Display for RoutedOperation<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoutedOperation({}, {:?})", self.caller, self.operation)
    }
}

type ApplyFn<V> = Box<dyn Fn(&V) -> OperationResult<V>>;

/// An operation built from a closure.
///
/// This is a convenience type that wraps a `Fn(&V) -> OperationResult<V>` closure,
/// analogous to the C# `Operation<TInput, TValue>` class which wraps a delegate.
pub struct FnOperation<V> {
    name: String,
    apply_fn: ApplyFn<V>,
}

impl<V> FnOperation<V> {
    /// Creates a new `FnOperation` from a name and a closure.
    pub fn new<F>(name: impl Into<String>, apply_fn: F) -> Self
    where
        F: Fn(&V) -> OperationResult<V> + 'static,
    {
        Self {
            name: name.into(),
            apply_fn: Box::new(apply_fn),
        }
    }

    /// Returns the name of this operation.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<V> Operation<V> for FnOperation<V> {
    fn apply(&self, current: &V) -> OperationResult<V> {
        (self.apply_fn)(current)
    }
}

impl<V> fmt::Debug for FnOperation<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FnOperation")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<V> fmt::Display for FnOperation<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple test operation that increments an i32.
    #[derive(Debug)]
    struct IncrementOp;

    impl Operation<i32> for IncrementOp {
        fn apply(&self, current: &i32) -> OperationResult<i32> {
            OperationResult::new(OperationStatus::Success, current + 1)
        }
    }

    #[test]
    fn apply_simple_operation() {
        let op = IncrementOp;
        let result = op.apply(&10);

        assert_eq!(result.status, OperationStatus::Success);
        assert_eq!(result.value, 11);
    }

    #[test]
    fn operation_status_equality() {
        assert_eq!(OperationStatus::Failed, OperationStatus::Failed);
        assert_ne!(OperationStatus::Failed, OperationStatus::Success);
        assert_ne!(OperationStatus::NotApplicable, OperationStatus::Success);
    }

    #[test]
    fn routed_operation_delegates_apply() {
        let inner = IncrementOp;
        let routed = RoutedOperation::new("node-1", Box::new(inner));

        assert_eq!(routed.caller, "node-1");

        let result = routed.apply(&42);
        assert_eq!(result.status, OperationStatus::Success);
        assert_eq!(result.value, 43);
    }

    #[test]
    fn routed_operation_debug_includes_caller() {
        let routed = RoutedOperation::new("node-2", Box::new(IncrementOp));
        let debug = format!("{routed:?}");

        assert!(debug.contains("node-2"));
        assert!(debug.contains("RoutedOperation"));
    }

    #[test]
    fn fn_operation_with_closure() {
        let op = FnOperation::new("double", |v: &i32| {
            OperationResult::new(OperationStatus::Success, v * 2)
        });

        assert_eq!(op.name(), "double");

        let result = op.apply(&5);
        assert_eq!(result.status, OperationStatus::Success);
        assert_eq!(result.value, 10);
    }

    #[test]
    fn fn_operation_returning_not_applicable() {
        let op = FnOperation::new("positive_only", |v: &i32| {
            if *v < 0 {
                OperationResult::new(OperationStatus::NotApplicable, *v)
            } else {
                OperationResult::new(OperationStatus::Success, v + 1)
            }
        });

        let success = op.apply(&5);
        assert_eq!(success.status, OperationStatus::Success);
        assert_eq!(success.value, 6);

        let not_applicable = op.apply(&-3);
        assert_eq!(not_applicable.status, OperationStatus::NotApplicable);
        assert_eq!(not_applicable.value, -3);
    }

    #[test]
    fn fn_operation_display_shows_name() {
        let op = FnOperation::<i32>::new("my_op", |v| {
            OperationResult::new(OperationStatus::Success, *v)
        });
        assert_eq!(format!("{op}"), "my_op");
    }

    #[test]
    fn fn_operation_as_trait_object() {
        let op: Box<dyn Operation<i32>> = Box::new(FnOperation::new("boxed", |v: &i32| {
            OperationResult::new(OperationStatus::Success, v + 100)
        }));

        let result = op.apply(&1);
        assert_eq!(result.status, OperationStatus::Success);
        assert_eq!(result.value, 101);
    }

    #[test]
    fn routed_fn_operation_end_to_end() {
        let op = FnOperation::new("triple", |v: &i32| {
            OperationResult::new(OperationStatus::Success, v * 3)
        });
        let routed = RoutedOperation::new(42_u64, Box::new(op));

        assert_eq!(routed.caller, 42_u64);

        let result = routed.apply(&7);
        assert_eq!(result.status, OperationStatus::Success);
        assert_eq!(result.value, 21);
    }
}
