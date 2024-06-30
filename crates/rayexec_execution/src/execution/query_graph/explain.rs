use crate::{
    execution::pipeline::Pipeline,
    logical::{
        context::QueryContext,
        explainable::{ExplainConfig, ExplainEntry, Explainable},
        operator::{ExplainFormat, LogicalOperator},
    },
};
use rayexec_error::{Result, ResultExt};

/// Formats a logical plan into explain output.
pub fn format_logical_plan_for_explain(
    context: Option<&QueryContext>,
    plan: &LogicalOperator,
    format: ExplainFormat,
    verbose: bool,
) -> Result<String> {
    let conf = ExplainConfig { verbose };
    match format {
        ExplainFormat::Text => {
            ExplainNode::walk_logical(context, plan, conf).format_text(0, String::new())
        }
        ExplainFormat::Json => unimplemented!(),
    }
}

/// Formats pipelines into explain output.
pub fn format_pipelines_for_explain<'a>(
    pipelines: impl Iterator<Item = &'a Pipeline>,
    format: ExplainFormat,
    verbose: bool,
) -> Result<String> {
    let conf = ExplainConfig { verbose };

    let mut nodes: Vec<_> = pipelines
        .map(|p| ExplainNode::walk_pipeline(p, conf))
        .collect();
    // Flip so that the "output" pipeline is at the top of the explain.
    nodes.reverse();

    match format {
        ExplainFormat::Text => {
            let mut buf = String::new();
            for node in nodes {
                buf = node.format_text(0, buf)?;
            }
            Ok(buf)
        }
        ExplainFormat::Json => unimplemented!(),
    }
}

#[derive(Debug)]
struct ExplainNode {
    entry: ExplainEntry,
    children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn walk_pipeline(pipeline: &Pipeline, conf: ExplainConfig) -> ExplainNode {
        let mut children: Vec<_> = pipeline
            .iter_operators()
            .map(|op| ExplainNode {
                entry: op.explain_entry(conf),
                children: Vec::new(),
            })
            .collect();

        // Flip the order so that the "sink" operator is on top for consistency
        // with the logical explain output.
        children.reverse();

        ExplainNode {
            entry: pipeline.explain_entry(conf),
            children,
        }
    }

    fn walk_logical(
        context: Option<&QueryContext>,
        plan: &LogicalOperator,
        conf: ExplainConfig,
    ) -> ExplainNode {
        let children = match plan {
            LogicalOperator::Projection(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::Filter(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::Aggregate(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::Order(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::AnyJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.left, conf),
                    Self::walk_logical(context, &p.right, conf),
                ]
            }
            LogicalOperator::EqualityJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.left, conf),
                    Self::walk_logical(context, &p.right, conf),
                ]
            }
            LogicalOperator::CrossJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.left, conf),
                    Self::walk_logical(context, &p.right, conf),
                ]
            }
            LogicalOperator::DependentJoin(p) => {
                vec![
                    Self::walk_logical(context, &p.left, conf),
                    Self::walk_logical(context, &p.right, conf),
                ]
            }
            LogicalOperator::Limit(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::CreateTableAs(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::Insert(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::Explain(p) => vec![Self::walk_logical(context, &p.input, conf)],
            LogicalOperator::MaterializedScan(scan) => {
                if let Some(inner) = context {
                    let plan = &inner.materialized[scan.idx].root;
                    vec![Self::walk_logical(context, plan, conf)]
                } else {
                    Vec::new()
                }
            }
            LogicalOperator::Empty
            | LogicalOperator::ExpressionList(_)
            | LogicalOperator::SetVar(_)
            | LogicalOperator::ShowVar(_)
            | LogicalOperator::ResetVar(_)
            | LogicalOperator::Scan(_)
            | LogicalOperator::TableFunction(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::Describe(_)
            | LogicalOperator::AttachDatabase(_)
            | LogicalOperator::DetachDatabase(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateTable(_) => Vec::new(),
        };

        ExplainNode {
            entry: plan.explain_entry(conf),
            children,
        }
    }

    fn format_text(&self, indent: usize, mut buf: String) -> Result<String> {
        use std::fmt::Write as _;
        writeln!(buf, "{}{}", " ".repeat(indent), self.entry)
            .context("failed to write to explain buffer")?;

        for child in &self.children {
            buf = child.format_text(indent + 2, buf)?;
        }

        Ok(buf)
    }
}
