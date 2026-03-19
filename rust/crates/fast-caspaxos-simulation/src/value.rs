use std::collections::BTreeSet;
use std::fmt;

/// A value that can be versioned and validated for successor chains.
pub trait VersionedValue: Clone + PartialEq + Eq + fmt::Debug + Default {
    /// Returns the version number of this value.
    fn version(&self) -> u32;

    /// Returns `true` if `self` is a valid successor to `predecessor`.
    fn is_valid_successor_to(&self, predecessor: &Self) -> bool;
}

/// A string value where each successor must append to the predecessor's string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct StringValue {
    pub version: u32,
    pub value: String,
}

impl StringValue {
    pub fn new(version: u32, value: impl Into<String>) -> Self {
        Self {
            version,
            value: value.into(),
        }
    }
}

impl VersionedValue for StringValue {
    fn version(&self) -> u32 {
        self.version
    }

    fn is_valid_successor_to(&self, predecessor: &Self) -> bool {
        self.version > predecessor.version && self.value.starts_with(&predecessor.value)
    }
}

impl fmt::Display for StringValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}:{}", self.version, self.value)
    }
}

/// A set value where each successor must be a superset of the predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetValue {
    pub version: u32,
    pub value: BTreeSet<String>,
}

impl SetValue {
    pub fn new(version: u32, value: BTreeSet<String>) -> Self {
        Self { version, value }
    }
}

impl VersionedValue for SetValue {
    fn version(&self) -> u32 {
        self.version
    }

    fn is_valid_successor_to(&self, predecessor: &Self) -> bool {
        self.version > predecessor.version && predecessor.value.is_subset(&self.value)
    }
}

impl fmt::Display for SetValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let items: Vec<&str> = self.value.iter().map(String::as_str).collect();
        write!(f, "v{}:{{{}}}", self.version, items.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- StringValue tests ---

    #[test]
    fn string_value_construction() {
        let sv = StringValue::new(1, "hello");
        assert_eq!(sv.version, 1);
        assert_eq!(sv.value, "hello");
    }

    #[test]
    fn string_value_default() {
        let sv = StringValue::default();
        assert_eq!(sv.version, 0);
        assert_eq!(sv.value, "");
    }

    #[test]
    fn string_value_display() {
        let sv = StringValue::new(3, "abc");
        assert_eq!(sv.to_string(), "v3:abc");
    }

    #[test]
    fn string_value_version() {
        let sv = StringValue::new(42, "x");
        assert_eq!(VersionedValue::version(&sv), 42);
    }

    #[test]
    fn string_value_valid_successor_appends() {
        let v1 = StringValue::new(1, "hello");
        let v2 = StringValue::new(2, "hello world");
        assert!(v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn string_value_valid_successor_identical_string_higher_version() {
        let v1 = StringValue::new(1, "hello");
        let v2 = StringValue::new(2, "hello");
        assert!(v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn string_value_invalid_successor_no_prefix() {
        let v1 = StringValue::new(1, "hello");
        let v2 = StringValue::new(2, "world");
        assert!(!v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn string_value_invalid_successor_same_version() {
        let v1 = StringValue::new(1, "hello");
        let v2 = StringValue::new(1, "hello world");
        assert!(!v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn string_value_invalid_successor_lower_version() {
        let v1 = StringValue::new(2, "hello");
        let v2 = StringValue::new(1, "hello world");
        assert!(!v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn string_value_successor_from_default() {
        let v0 = StringValue::default();
        let v1 = StringValue::new(1, "start");
        assert!(v1.is_valid_successor_to(&v0));
    }

    // --- SetValue tests ---

    #[test]
    fn set_value_construction() {
        let items: BTreeSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let sv = SetValue::new(1, items.clone());
        assert_eq!(sv.version, 1);
        assert_eq!(sv.value, items);
    }

    #[test]
    fn set_value_default() {
        let sv = SetValue::default();
        assert_eq!(sv.version, 0);
        assert!(sv.value.is_empty());
    }

    #[test]
    fn set_value_display() {
        let items: BTreeSet<String> = ["b", "a", "c"].iter().map(|s| s.to_string()).collect();
        let sv = SetValue::new(2, items);
        // BTreeSet is sorted, so output should be alphabetical
        assert_eq!(sv.to_string(), "v2:{a, b, c}");
    }

    #[test]
    fn set_value_display_empty() {
        let sv = SetValue::new(0, BTreeSet::new());
        assert_eq!(sv.to_string(), "v0:{}");
    }

    #[test]
    fn set_value_version() {
        let sv = SetValue::new(7, BTreeSet::new());
        assert_eq!(VersionedValue::version(&sv), 7);
    }

    #[test]
    fn set_value_valid_successor_adds_elements() {
        let v1_items: BTreeSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let v2_items: BTreeSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let v1 = SetValue::new(1, v1_items);
        let v2 = SetValue::new(2, v2_items);
        assert!(v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn set_value_valid_successor_same_elements_higher_version() {
        let items: BTreeSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let v1 = SetValue::new(1, items.clone());
        let v2 = SetValue::new(2, items);
        assert!(v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn set_value_invalid_successor_removes_elements() {
        let v1_items: BTreeSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let v2_items: BTreeSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
        let v1 = SetValue::new(1, v1_items);
        let v2 = SetValue::new(2, v2_items);
        assert!(!v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn set_value_invalid_successor_same_version() {
        let v1_items: BTreeSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
        let v2_items: BTreeSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let v1 = SetValue::new(1, v1_items);
        let v2 = SetValue::new(1, v2_items);
        assert!(!v2.is_valid_successor_to(&v1));
    }

    #[test]
    fn set_value_successor_from_default() {
        let v0 = SetValue::default();
        let items: BTreeSet<String> = ["x"].iter().map(|s| s.to_string()).collect();
        let v1 = SetValue::new(1, items);
        assert!(v1.is_valid_successor_to(&v0));
    }
}
