use crate::{
    execution::intermediate::{
        IntermediatePipeline, IntermediatePipelineGroup, PipelineSink, PipelineSource,
    },
    explain::explainable::Explainable,
    logical::{
        binder::bind_context::BindContext, logical_explain::ExplainFormat,
        operator::LogicalOperator,
    },
};
use rayexec_error::{Result, ResultExt};
use serde::{Deserialize, Serialize};

use super::explainable::{ExplainConfig, ExplainEntry};

/// Formats explain output for various plan stages.
#[derive(Debug)]
pub struct ExplainFormatter<'a> {
    bind_context: &'a BindContext,
    config: ExplainConfig,
    format: ExplainFormat,
}

impl<'a> ExplainFormatter<'a> {
    pub fn new(
        bind_context: &'a BindContext,
        config: ExplainConfig,
        format: ExplainFormat,
    ) -> Self {
        ExplainFormatter {
            bind_context,
            config,
            format,
        }
    }

    pub fn format_logical_plan(&self, root: &LogicalOperator) -> Result<String> {
        let node = ExplainNode::walk_logical_plan(self.bind_context, root, self.config);
        self.format(&node)
    }

    pub fn format_intermedate_groups(
        &self,
        groups: &[(&str, &IntermediatePipelineGroup)],
    ) -> Result<String> {
        let node = ExplainNode::from_intermediate_groups(self.bind_context, groups, self.config);
        self.format(&node)
    }

    fn format(&self, node: &ExplainNode) -> Result<String> {
        match self.format {
            ExplainFormat::Text => {
                fn fmt(node: &ExplainNode, indent: usize, buf: &mut String) -> Result<()> {
                    use std::fmt::Write as _;
                    writeln!(buf, "{}{}", " ".repeat(indent), node.entry)
                        .context("failed to write to explain buffer")?;

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

#[derive(Debug, Serialize, Deserialize)]
struct ExplainNode {
    entry: ExplainEntry,
    children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn from_intermediate_groups(
        bind_context: &BindContext,
        groups: &[(&str, &IntermediatePipelineGroup)],
        config: ExplainConfig,
    ) -> ExplainNode {
        let entry = ExplainEntry::new("IntermediatePipelineGroups");
        let children = groups
            .iter()
            .map(|(label, group)| Self::from_intermediate_group(bind_context, group, label, config))
            .collect();

        ExplainNode { entry, children }
    }

    fn from_intermediate_group(
        bind_context: &BindContext,
        group: &IntermediatePipelineGroup,
        label: &str,
        config: ExplainConfig,
    ) -> ExplainNode {
        let entry = ExplainEntry::new(format!("IntermediatePipelineGroup {label}"));

        let children = group
            .pipelines
            .values()
            .map(|pipeline| Self::from_intermedate_pipeline(bind_context, pipeline, config))
            .collect();

        ExplainNode { entry, children }
    }

    fn from_intermedate_pipeline(
        bind_context: &BindContext,
        pipeline: &IntermediatePipeline,
        config: ExplainConfig,
    ) -> ExplainNode {
        let _ = bind_context;

        let mut entry = ExplainEntry::new(format!("IntermediatePipeline {}", pipeline.id.0));
        entry = match pipeline.sink {
            PipelineSink::QueryOutput => entry.with_value("Sink", "QueryOutput"),
            PipelineSink::InPipeline => entry.with_value("Sink", "InPipeline"),
            PipelineSink::InGroup {
                pipeline_id,
                operator_idx,
                input_idx,
            } => entry.with_named_map(
                "Sink",
                "InGroup",
                [
                    ("pipeline_id", pipeline_id.0),
                    ("operator_idx", operator_idx),
                    ("input_idx", input_idx),
                ],
            ),
            PipelineSink::OtherGroup {
                stream_id,
                partitions,
            } => entry.with_named_map(
                "Sink",
                "OtherGroup",
                [
                    ("query_id", stream_id.query_id.to_string()),
                    ("stream_id", stream_id.stream_id.to_string()),
                    ("partitions", partitions.to_string()),
                ],
            ),
        };

        entry = match pipeline.source {
            PipelineSource::InPipeline => entry.with_value("Source", "InPipeline"),
            PipelineSource::OtherGroup {
                stream_id,
                partitions,
            } => entry.with_named_map(
                "Source",
                "OtherGroup",
                [
                    ("query_id", stream_id.query_id.to_string()),
                    ("stream_id", stream_id.stream_id.to_string()),
                    ("partitions", partitions.to_string()),
                ],
            ),
            PipelineSource::OtherPipeline { pipeline } => {
                entry.with_named_map("Source", "OtherPipeline", [("pipeline_id", pipeline.0)])
            }
        };

        let children = pipeline
            .operators
            .iter()
            .map(|op| ExplainNode {
                entry: op.explain_entry(config),
                children: Vec::new(),
            })
            .collect();

        ExplainNode { entry, children }
    }

    fn walk_logical_plan(
        bind_context: &BindContext,
        plan: &LogicalOperator,
        config: ExplainConfig,
    ) -> ExplainNode {
        let (entry, children) = match plan {
            LogicalOperator::Invalid => (ExplainEntry::new("INVALID"), &Vec::new()),
            LogicalOperator::Project(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Filter(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Scan(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Aggregate(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SetOp(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Empty(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Limit(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Order(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SetVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ResetVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ShowVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::AttachDatabase(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::DetachDatabase(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Drop(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Insert(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CreateSchema(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CreateTable(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Describe(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Explain(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CopyTo(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CrossJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ArbitraryJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ComparisonJoin(n) => (n.explain_entry(config), &n.children),
        };

        let children = children
            .iter()
            .map(|c| Self::walk_logical_plan(bind_context, c, config))
            .collect();

        // This will be used at some point.
        let _ = bind_context;

        ExplainNode { entry, children }
    }
}
