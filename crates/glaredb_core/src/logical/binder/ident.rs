use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};

use glaredb_parser::ast;
use serde::{Deserialize, Serialize};

/// An indentifier that tracks both its raw (display) string, and its normalized
/// form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinderIdent {
    /// The raw string as entered by the user.
    raw: String,
    /// The normalized string used for comparisons and hashing.
    ///
    /// If quoted, this will be the same as the raw string.
    normalized: String,
    /// If this string was quoted by the user.
    quoted: bool,
}

impl BinderIdent {
    pub fn new(raw: impl Into<String>, quoted: bool) -> Self {
        let raw = raw.into();
        let normalized = if quoted {
            raw.clone()
        } else {
            raw.to_ascii_lowercase()
        };

        BinderIdent {
            raw,
            normalized,
            quoted,
        }
    }

    pub fn is_quoted(&self) -> bool {
        self.quoted
    }

    /// Get the normalized string.
    pub fn as_normalized_str(&self) -> &str {
        &self.normalized
    }

    /// Get the original string.
    ///
    /// Should not be used for comparisons.
    pub fn as_raw_str(&self) -> &str {
        &self.raw
    }
}

impl From<String> for BinderIdent {
    fn from(value: String) -> Self {
        Self::new(value, false)
    }
}

impl From<&str> for BinderIdent {
    fn from(value: &str) -> Self {
        Self::new(value, false)
    }
}

impl From<ast::Ident> for BinderIdent {
    fn from(value: ast::Ident) -> Self {
        Self::new(value.value, value.quoted)
    }
}

impl Borrow<str> for BinderIdent {
    fn borrow(&self) -> &str {
        &self.normalized
    }
}

impl PartialEq for BinderIdent {
    fn eq(&self, other: &Self) -> bool {
        self.normalized == other.normalized
    }
}

impl Eq for BinderIdent {}

impl Hash for BinderIdent {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.normalized.hash(state);
    }
}

impl fmt::Display for BinderIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn basic_hash_map_unquoted_insert() {
        let mut cols: HashMap<BinderIdent, usize> = HashMap::new();

        // Unquoted "EmployeeName" normalizes to "employeename".
        cols.insert(BinderIdent::new("EmployeeName", false), 0);

        // Get normalized
        assert_eq!(cols.get("employeename"), Some(&0));
        assert_eq!(cols.get("EmployeeName"), None);

        // Get using another ident.
        assert_eq!(cols.get(&BinderIdent::new("employeename", false)), Some(&0));
        assert_eq!(cols.get(&BinderIdent::new("EmployeeName", false)), Some(&0));
        assert_eq!(cols.get(&BinderIdent::new("EMPLOYEENAME", false)), Some(&0));

        // Doesn't normalize to the same thing.
        assert_eq!(cols.get(&BinderIdent::new("EmployeeName", true)), None);
    }

    #[test]
    fn basic_hash_map_quoted_insert() {
        let mut cols: HashMap<BinderIdent, usize> = HashMap::new();

        // Quoted "EmployeeName" normalizes to "EmployeeName".
        cols.insert(BinderIdent::new("EmployeeName", true), 0);

        // Get normalized
        assert_eq!(cols.get("EmployeeName"), Some(&0));
        assert_eq!(cols.get("employeename"), None);

        // Get using another ident.
        assert_eq!(cols.get(&BinderIdent::new("EmployeeName", true)), Some(&0));

        // None of these normalize to the same thing.
        assert_eq!(cols.get(&BinderIdent::new("employeename", false)), None);
        assert_eq!(cols.get(&BinderIdent::new("EmployeeName", false)), None);
        assert_eq!(cols.get(&BinderIdent::new("EMPLOYEENAME", false)), None);
    }
}
