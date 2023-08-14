use datafusion_proto::logical_plan::LogicalExtensionCodec;
/// extension implementations for converting our logical plan into datafusion logical plan
use std::sync::Arc;

use super::logical_plan::CreateTable;
use crate::errors::{internal, Result};
use datafusion::logical_expr::{
    Extension as LogicalPlanExtension, LogicalPlan, UserDefinedLogicalNodeCore,
};
use protogen::export::prost::Message;

pub trait ExtensionConversion {
    fn into_extension(self) -> LogicalPlanExtension
    where
        Self: Sized + UserDefinedLogicalNodeCore,
    {
        LogicalPlanExtension {
            node: Arc::new(self),
        }
    }
    fn try_from_extension(extension: &LogicalPlanExtension) -> Result<Self>
    where
        Self: Sized;
    fn encode(&self, buf: &mut Vec<u8>, codec: &dyn LogicalExtensionCodec) -> Result<()>;
}

impl UserDefinedLogicalNodeCore for CreateTable {
    fn name(&self) -> &str {
        "CreateTable"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        match self.source {
            Some(ref src) => vec![src],
            None => vec![],
        }
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateTable")
    }

    fn from_template(&self, _exprs: &[datafusion::prelude::Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}

impl ExtensionConversion for CreateTable {
    fn try_from_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "CreateTable::try_from_extension: unsupported extension",
            )),
        }
    }

    fn encode(&self, buf: &mut Vec<u8>, codec: &dyn LogicalExtensionCodec) -> Result<()> {
        let extension = self.try_to_proto(codec);
        extension.encode(buf);
        Ok(())
    }
}
