//! Safety invariant checker for Fast CASPaxos simulation.
//!
//! Validates linearizable commit histories and client response monotonicity.
//! Ported from C# `FastCasSafetyInvariantChecker`.

use std::collections::BTreeMap;

use crate::address::Address;
use crate::value::VersionedValue;

/// Errors from safety invariant checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafetyError {
    /// Two different values committed at the same version.
    DivergentValues {
        version: u32,
        value_a: String,
        value_b: String,
    },
    /// A committed value is not a valid successor to its predecessor.
    InvalidSuccessor {
        version: u32,
        value: String,
        predecessor: String,
    },
    /// Client saw a version regression (non-monotonic).
    VersionRegression {
        client: Address,
        saw_version: u32,
        then_version: u32,
    },
    /// Duplicate version in commit history.
    DuplicateVersion { version: u32 },
}

/// Validates safety invariants across an entire simulation run.
pub struct SafetyInvariantChecker;

impl SafetyInvariantChecker {
    /// Check that commit histories from all proposers are consistent.
    ///
    /// Invariants checked:
    /// 1. No duplicate versions in any single history
    /// 2. All proposers agree on the value at each version
    /// 3. Each committed value is a valid successor to its predecessor
    pub fn check_commit_histories<V: VersionedValue + std::fmt::Display>(
        histories: &[Vec<V>],
    ) -> Result<(), SafetyError> {
        // Build version → value map across all histories, checking for conflicts.
        let mut version_map: BTreeMap<u32, V> = BTreeMap::new();

        for history in histories {
            // Check for duplicate versions within a single history.
            let mut seen_versions: BTreeMap<u32, ()> = BTreeMap::new();
            for val in history {
                let v = val.version();
                if seen_versions.contains_key(&v) {
                    return Err(SafetyError::DuplicateVersion { version: v });
                }
                seen_versions.insert(v, ());

                // Check cross-history agreement.
                if let Some(existing) = version_map.get(&v) {
                    if existing != val {
                        return Err(SafetyError::DivergentValues {
                            version: v,
                            value_a: existing.to_string(),
                            value_b: val.to_string(),
                        });
                    }
                } else {
                    version_map.insert(v, val.clone());
                }
            }
        }

        // Check valid successor chains across the merged, ordered history.
        let ordered: Vec<(&u32, &V)> = version_map.iter().collect();
        for window in ordered.windows(2) {
            let (_, pred) = window[0];
            let (_, succ) = window[1];
            if !succ.is_valid_successor_to(pred) {
                return Err(SafetyError::InvalidSuccessor {
                    version: succ.version(),
                    value: succ.to_string(),
                    predecessor: pred.to_string(),
                });
            }
        }

        Ok(())
    }

    /// Check that client responses are monotonically increasing in version.
    pub fn check_client_response_monotonicity<V: VersionedValue>(
        responses: &[(Address, V)],
    ) -> Result<(), SafetyError> {
        let mut latest_by_client: BTreeMap<Address, u32> = BTreeMap::new();
        for (client, value) in responses {
            let version = value.version();
            if let Some(&prev) = latest_by_client.get(client) {
                if version < prev {
                    return Err(SafetyError::VersionRegression {
                        client: *client,
                        saw_version: prev,
                        then_version: version,
                    });
                }
            }
            latest_by_client.insert(*client, version);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::StringValue;

    #[test]
    fn valid_history_passes() {
        let history = vec![
            StringValue::new(1, "a"),
            StringValue::new(2, "ab"),
            StringValue::new(3, "abc"),
        ];
        let result = SafetyInvariantChecker::check_commit_histories(&[history]);
        assert!(result.is_ok());
    }

    #[test]
    fn divergent_values_fails() {
        let h1 = vec![StringValue::new(1, "a")];
        let h2 = vec![StringValue::new(1, "b")];
        let result = SafetyInvariantChecker::check_commit_histories(&[h1, h2]);
        assert!(result.is_err());
        match result.unwrap_err() {
            SafetyError::DivergentValues {
                version,
                value_a,
                value_b,
            } => {
                assert_eq!(version, 1);
                assert_eq!(value_a, "v1:a");
                assert_eq!(value_b, "v1:b");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn invalid_successor_fails() {
        // "b" is not a valid successor to "a" (doesn't start with "a")
        let history = vec![StringValue::new(1, "a"), StringValue::new(2, "b")];
        let result = SafetyInvariantChecker::check_commit_histories(&[history]);
        assert!(result.is_err());
        match result.unwrap_err() {
            SafetyError::InvalidSuccessor {
                version,
                value,
                predecessor,
            } => {
                assert_eq!(version, 2);
                assert_eq!(value, "v2:b");
                assert_eq!(predecessor, "v1:a");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn client_monotonicity_passes() {
        let client = Address::client(0);
        let responses: Vec<(Address, StringValue)> = vec![
            (client, StringValue::new(1, "a")),
            (client, StringValue::new(2, "ab")),
            (client, StringValue::new(3, "abc")),
        ];
        let result = SafetyInvariantChecker::check_client_response_monotonicity(&responses);
        assert!(result.is_ok());
    }

    #[test]
    fn client_regression_fails() {
        let client = Address::client(0);
        let responses: Vec<(Address, StringValue)> = vec![
            (client, StringValue::new(3, "abc")),
            (client, StringValue::new(1, "a")),
        ];
        let result = SafetyInvariantChecker::check_client_response_monotonicity(&responses);
        assert!(result.is_err());
        match result.unwrap_err() {
            SafetyError::VersionRegression {
                client: c,
                saw_version,
                then_version,
            } => {
                assert_eq!(c, client);
                assert_eq!(saw_version, 3);
                assert_eq!(then_version, 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_history_passes() {
        let result =
            SafetyInvariantChecker::check_commit_histories::<StringValue>(&[]);
        assert!(result.is_ok());

        let result = SafetyInvariantChecker::check_commit_histories::<StringValue>(&[vec![]]);
        assert!(result.is_ok());

        let result =
            SafetyInvariantChecker::check_client_response_monotonicity::<StringValue>(&[]);
        assert!(result.is_ok());
    }
}
