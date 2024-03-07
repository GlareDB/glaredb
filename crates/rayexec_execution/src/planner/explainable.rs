use std::collections::BTreeMap;
use std::fmt;

/// An entry in an output for explaining a query.
// TODO: Maybe allow serializing this with serde to enable json output of
// explain queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainEntry {
    /// Name of the node.
    pub name: String,
    /// Items to display in the explain entry.
    ///
    /// Using a btree to ensure consistent ordering (mostly for test ouput).
    pub items: BTreeMap<String, ExplainValue>,
}

impl ExplainEntry {
    /// Create a new explain entry for a query node.
    pub fn new(name: impl Into<String>) -> Self {
        ExplainEntry {
            name: name.into(),
            items: BTreeMap::new(),
        }
    }

    /// Put a value in the explain entry.
    pub fn with_value(mut self, key: impl Into<String>, value: impl fmt::Display) -> Self {
        let key = key.into();
        let val = ExplainValue::Value(value.to_string());
        self.items.insert(key, val);
        self
    }

    /// Put a list of values in the explain entry.
    pub fn with_values<S: fmt::Display>(
        mut self,
        key: impl Into<String>,
        values: impl IntoIterator<Item = S>,
    ) -> Self {
        let key = key.into();
        let vals = ExplainValue::Values(values.into_iter().map(|s| s.to_string()).collect());
        self.items.insert(key, vals);
        self
    }
}

impl fmt::Display for ExplainEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.items.is_empty() {
            write!(f, "(")?;
            for (idx, (k, v)) in self.items.iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{k} = {v}")?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainValue {
    Value(String),
    Values(Vec<String>),
}

impl fmt::Display for ExplainValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{v}"),
            Self::Values(v) => write!(f, "[{}]", v.join(", ")),
        }
    }
}

/// Configuration for producing an ExplainEntry for a node in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplainConfig {
    pub verbose: bool,
}

/// Trait for explaining nodes in a query tree.
pub trait Explainable {
    /// Create an ExplainEntry for this node.
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry;
}

/// Wrapper around column indexes to provide consistent formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnIndexes<'a>(pub &'a [usize]);

impl<'a> fmt::Display for ColumnIndexes<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, col) in self.0.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "#{col}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_entry_display_no_values() {
        let ent = ExplainEntry::new("DummyNode");

        let out = ent.to_string();
        assert_eq!("DummyNode", out);
    }

    #[test]
    fn explain_entry_display_with_values() {
        let ent = ExplainEntry::new("DummyNode")
            .with_value("k1", "v1")
            .with_values("k2", ["vs1", "vs2", "vs3"]);

        let out = ent.to_string();
        assert_eq!("DummyNode(k1 = v1, k2 = [vs1, vs2, vs3])", out);
    }
}
