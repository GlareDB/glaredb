use glaredb_error::{Result, ResultExt};

use super::node::ExplainedPlan;
use crate::explain::node::ExplainNode;
use crate::logical::logical_explain::ExplainFormat;

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

    pub fn format(&self, plan: &ExplainedPlan) -> Result<String> {
        match self.format {
            ExplainFormat::Text => {
                use std::fmt::Write as _;

                fn fmt(node: &ExplainNode, indent: usize, buf: &mut String) -> Result<()> {
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
                // Base
                writeln!(buf, "Base")?;
                fmt(&plan.base, 2, &mut buf)?;

                // Materializations.
                for (mat_ref, plan) in &plan.materializations {
                    writeln!(buf, "{mat_ref}")?;
                    fmt(plan, 2, &mut buf)?;
                }

                // Remove trailing newline.
                if buf.ends_with('\n') {
                    buf.pop();
                }

                Ok(buf)
            }
            ExplainFormat::Json => {
                serde_json::to_string_pretty(&plan).context("failed to serialize to json")
            }
        }
    }
}
