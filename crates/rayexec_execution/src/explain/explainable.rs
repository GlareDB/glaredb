use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};

/// An entry in an output for explaining a query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn with_value_context(
        self,
        key: impl Into<String>,
        conf: ExplainConfig,
        value: impl ContextDisplay,
    ) -> Self {
        let key = key.into();

        let mut ent = self.with_value(
            key.clone(),
            ContextDisplayWrapper::with_mode(&value, conf.context_mode),
        );

        // If we're printing out a verbose plan, go ahead and print out the raw
        // form of the value.
        if conf.verbose && matches!(conf.context_mode, ContextDisplayMode::Enriched(_)) {
            ent = ent.with_value(
                format!("{key}_raw"),
                ContextDisplayWrapper::with_mode(value, ContextDisplayMode::Raw),
            )
        }

        ent
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

    pub fn with_values_context<S: ContextDisplay>(
        self,
        key: impl Into<String>,
        conf: ExplainConfig,
        values: impl IntoIterator<Item = S>,
    ) -> Self {
        let key = key.into();
        let values: Vec<_> = values.into_iter().collect();

        let mut ent = self.with_values(
            key.clone(),
            values
                .iter()
                .map(|v| ContextDisplayWrapper::with_mode(v, conf.context_mode)),
        );

        // If we're printing out a verbose plan, go ahead and print out the raw
        // form of the values.
        if conf.verbose && matches!(conf.context_mode, ContextDisplayMode::Enriched(_)) {
            ent = ent.with_values(
                format!("{key}_raw"),
                values
                    .into_iter()
                    .map(|v| ContextDisplayWrapper::with_mode(v, ContextDisplayMode::Raw)),
            )
        }

        ent
    }

    pub fn with_named_map<S1: fmt::Display, S2: fmt::Display>(
        mut self,
        key: impl Into<String>,
        map_name: impl Into<String>,
        map: impl IntoIterator<Item = (S1, S2)>,
    ) -> Self {
        let key = key.into();
        let vals = ExplainValue::NamedMap(
            map_name.into(),
            map.into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        self.items.insert(key, vals);
        self
    }
}

impl fmt::Display for ExplainEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.items.is_empty() {
            write!(f, " (")?;
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

// TODO: `AsExplainValue` trait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplainValue {
    Value(String),
    Values(Vec<String>),
    NamedMap(String, Vec<(String, String)>),
}

impl fmt::Display for ExplainValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{v}"),
            Self::Values(v) => write!(f, "[{}]", v.join(", ")),
            Self::NamedMap(name, map) => {
                let s = map
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                // "{k1: v1, k2: v2 ... }"
                write!(f, "{} {{{}}}", name, s)
            }
        }
    }
}

/// Configuration for producing an ExplainEntry for a node in a query.
#[derive(Debug, Clone, Copy)]
pub struct ExplainConfig<'a> {
    pub context_mode: ContextDisplayMode<'a>,
    pub verbose: bool,
}

impl<'a> ExplainConfig<'a> {
    pub const RAW_VERBOSE: Self = Self {
        context_mode: ContextDisplayMode::Raw,
        verbose: true,
    };
}

/// Trait for explaining a single node in the query tree.
pub trait Explainable {
    /// Create an ExplainEntry for this node.
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry;
}

/// Wrapper around column indexes to provide consistent formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnIndexes<'a>(pub &'a [usize]);

impl fmt::Display for ColumnIndexes<'_> {
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
        assert_eq!("DummyNode (k1 = v1, k2 = [vs1, vs2, vs3])", out);
    }

    #[test]
    fn explain_entry_display_with_map_value() {
        let ent = ExplainEntry::new("DummyNode")
            .with_value("k1", "v1")
            .with_named_map("k2", "my_map", [("m1", "v1"), ("m2", "v2")]);

        let out = ent.to_string();
        assert_eq!("DummyNode (k1 = v1, k2 = my_map {m1: v1, m2: v2})", out);
    }
}
