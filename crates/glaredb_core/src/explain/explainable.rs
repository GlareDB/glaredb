use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::util::fmt::displayable::IntoDisplayableSlice;

/// An entry in an output for explaining a query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainEntry {
    /// Name of the node.
    pub name: String,
    /// Items to display in the explain entry.
    ///
    /// Using a btree to ensure consistent ordering.
    pub items: BTreeMap<String, ExplainValue>,
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

/// Builder for explain entries.
#[derive(Debug, Clone)]
pub struct EntryBuilder<'a> {
    conf: ExplainConfig<'a>,
    name: String,
    items: BTreeMap<String, ExplainValue>,
}

impl<'a> EntryBuilder<'a> {
    pub fn new(name: impl Into<String>, conf: ExplainConfig<'a>) -> Self {
        EntryBuilder {
            conf,
            name: name.into(),
            items: BTreeMap::new(),
        }
    }

    /// Simple key value pair.
    pub fn with_value(mut self, key: impl Into<String>, value: impl fmt::Display) -> Self {
        let key = key.into();
        let val = ExplainValue::Value(value.to_string());
        self.items.insert(key, val);
        self
    }

    pub fn with_value_opt(self, key: impl Into<String>, value: Option<impl fmt::Display>) -> Self {
        match value {
            Some(value) => self.with_value(key, value),
            None => self,
        }
    }

    pub fn with_value_opt_if_verbose(
        self,
        key: impl Into<String>,
        value: Option<impl fmt::Display>,
    ) -> Self {
        if self.conf.verbose {
            self.with_value_opt(key, value)
        } else {
            self
        }
    }

    pub fn with_value_if_verbose(self, key: impl Into<String>, value: impl fmt::Display) -> Self {
        if self.conf.verbose {
            self.with_value(key, value)
        } else {
            self
        }
    }

    /// Key value pair where the value can enrich itself from the context in the
    /// explain config.
    ///
    /// For example, this should be used for column expressions to display the
    /// pretty name in the explain plan.
    ///
    /// If the config is set to verbose, this will alway append a "raw" key
    /// value pair.
    pub fn with_contextual_value(
        mut self,
        key: impl Into<String>,
        value: impl ContextDisplay,
    ) -> Self {
        let key = key.into();
        let mode = self.conf.context_mode;
        self = self.with_value(key.clone(), ContextDisplayWrapper::with_mode(&value, mode));

        // If we're printing out a verbose plan, go ahead and print out the raw
        // form of the value.
        if self.conf.verbose && matches!(self.conf.context_mode, ContextDisplayMode::Enriched(_)) {
            self = self.with_value(
                format!("{key}_raw"),
                ContextDisplayWrapper::with_mode(value, ContextDisplayMode::Raw),
            )
        }

        self
    }

    /// Put a list of values associated with a single key in the explain entry.
    pub fn with_values<S>(
        mut self,
        key: impl Into<String>,
        values: impl IntoIterator<Item = S>,
    ) -> Self
    where
        S: fmt::Display,
    {
        let key = key.into();
        let vals = ExplainValue::Values(values.into_iter().map(|s| s.to_string()).collect());
        self.items.insert(key, vals);
        self
    }

    pub fn with_values_if_verbose<S>(
        self,
        key: impl Into<String>,
        values: impl IntoIterator<Item = S>,
    ) -> Self
    where
        S: fmt::Display,
    {
        if self.conf.verbose {
            self.with_values(key, values)
        } else {
            self
        }
    }

    /// Put a list of contextual values associated with a single key in the entry.
    pub fn with_contextual_values<S>(
        mut self,
        key: impl Into<String>,
        values: impl IntoIterator<Item = S>,
    ) -> Self
    where
        S: ContextDisplay,
    {
        let key = key.into();
        let values: Vec<_> = values.into_iter().collect();
        let mode = self.conf.context_mode;

        self = self.with_values(
            key.clone(),
            values
                .iter()
                .map(|v| ContextDisplayWrapper::with_mode(v, mode)),
        );

        // If we're printing out a verbose plan, go ahead and print out the raw
        // form of the values.
        if self.conf.verbose && matches!(self.conf.context_mode, ContextDisplayMode::Enriched(_)) {
            self = self.with_values(
                format!("{key}_raw"),
                values
                    .into_iter()
                    .map(|v| ContextDisplayWrapper::with_mode(v, ContextDisplayMode::Raw)),
            )
        }

        self
    }

    pub fn with_contextual_values_opt<S>(
        self,
        key: impl Into<String>,
        values: Option<impl IntoIterator<Item = S>>,
    ) -> Self
    where
        S: ContextDisplay,
    {
        match values {
            Some(values) => self.with_contextual_values(key, values),
            None => self,
        }
    }

    pub fn with_contextual_values_opt_if_verbose<S>(
        self,
        key: impl Into<String>,
        values: Option<impl IntoIterator<Item = S>>,
    ) -> Self
    where
        S: ContextDisplay,
    {
        if self.conf.verbose {
            self.with_contextual_values_opt(key, values)
        } else {
            self
        }
    }

    pub fn build(self) -> ExplainEntry {
        ExplainEntry {
            name: self.name,
            items: self.items,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplainValue {
    Value(String),
    Values(Vec<String>),
}

impl fmt::Display for ExplainValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{v}"),
            Self::Values(v) => write!(f, "{}", v.display_with_brackets()),
        }
    }
}

/// Configuration for producing an ExplainEntry for a node in a query.
#[derive(Debug, Clone, Copy)]
pub struct ExplainConfig<'a> {
    pub context_mode: ContextDisplayMode<'a>,
    pub verbose: bool,
}

impl ExplainConfig<'_> {
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
        let ent = EntryBuilder::new("DummyNode", ExplainConfig::RAW_VERBOSE).build();

        let out = ent.to_string();
        assert_eq!("DummyNode", out);
    }

    #[test]
    fn explain_entry_display_with_values() {
        let ent = EntryBuilder::new("DummyNode", ExplainConfig::RAW_VERBOSE)
            .with_value("k1", "v1")
            .with_values("k2", ["vs1", "vs2", "vs3"])
            .build();

        let out = ent.to_string();
        assert_eq!("DummyNode (k1 = v1, k2 = [vs1, vs2, vs3])", out);
    }
}
