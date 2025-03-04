use rayexec_error::{Result, ResultExt};
use serde::{Deserialize, Serialize};
use tracing::error;

use super::explainable::{ExplainConfig, ExplainEntry};
use crate::execution::intermediate::pipeline::IntermediatePipeline;
use crate::explain::explainable::Explainable;
use crate::explain::node::ExplainNode;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::logical_explain::ExplainFormat;
use crate::logical::operator::LogicalOperator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExplainChars {
    item_left_border: char,
    item_left_border_last: char,
}

const DEFAULT_EXPLAIN_CHARS: ExplainChars = ExplainChars {
    item_left_border: '├',
    item_left_border_last: '└',
};

/// Formats explain output for various plan stages.
#[derive(Debug)]
pub struct ExplainFormatter {
    format: ExplainFormat,
}

impl ExplainFormatter {
    pub fn new(format: ExplainFormat) -> Self {
        ExplainFormatter { format }
    }

    pub fn format(&self, node: &ExplainNode) -> Result<String> {
        match self.format {
            ExplainFormat::Text => {
                fn fmt(node: &ExplainNode, indent: usize, buf: &mut String) -> Result<()> {
                    use std::fmt::Write as _;

                    writeln!(buf, "{}{}", " ".repeat(indent), node.entry.name)?;

                    for (idx, (item_name, item)) in node.entry.items.iter().enumerate() {
                        let border = if idx == node.entry.items.len() - 1 {
                            DEFAULT_EXPLAIN_CHARS.item_left_border_last
                        } else {
                            DEFAULT_EXPLAIN_CHARS.item_left_border
                        };

                        writeln!(
                            buf,
                            "{}  {} {}: {}",
                            " ".repeat(indent),
                            border,
                            item_name,
                            item
                        )?;
                    }

                    for child in &node.children {
                        fmt(child, indent + 2, buf)?;
                    }

                    Ok(())
                }

                let mut buf = String::new();
                fmt(node, 0, &mut buf)?;

                Ok(buf)
            }
            ExplainFormat::Json => {
                serde_json::to_string(&node).context("failed to serialize to json")
            }
        }
    }
}
