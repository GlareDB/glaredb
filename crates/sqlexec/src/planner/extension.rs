use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use protogen::metastore::types::service;
use std::sync::Arc;

use super::logical_plan::{CreateTable, Insert};
use crate::errors::{internal, Result};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::ToDFSchema;
use datafusion::execution::context::{QueryPlanner, SessionState};
///! extension implementations for converting our logical plan into datafusion logical plan
use datafusion::logical_expr::{
    DmlStatement, Extension as LogicalPlanExtension, LogicalPlan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use protogen::export::prost::Message;
use protogen::gen::metastore::service as proto;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum DatafusionExtension {
    CreateTable(CreateTable),
}

impl DatafusionExtension {
    pub fn from_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension
            .node
            .as_any()
            .downcast_ref::<DatafusionExtension>()
        {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "DatafusionExtension::from_extension: unsupported extension",
            )),
        }
    }
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::CreateTable(create_table) => {
                let s = service::CreateTable {
                    schema: create_table.table_name.to_string(),
                    name: create_table.table_name.to_string(),
                    options: create_table.schema.clone().into(),
                    if_not_exists: create_table.if_not_exists,
                };
                let s: proto::CreateTable = s.try_into().unwrap();
                s.encode(buf).unwrap();
            }
        }
        Ok(())
    }

    pub fn try_decode(buf: &[u8]) -> Result<Self> {
        // let s = service::CreateTable::decode(buf).unwrap();
        let s: proto::CreateTable = proto::CreateTable::decode(buf).unwrap();
        let s: service::CreateTable = s.try_into().unwrap();

        println!("s: {:?}", s);
        todo!()
    }
}

pub(super) trait IntoExtension {
    fn into_extension(self) -> LogicalPlanExtension;
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
        todo!()
    }

    fn from_template(&self, exprs: &[datafusion::prelude::Expr], inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }
}

impl UserDefinedLogicalNodeCore for DatafusionExtension {
    fn name(&self) -> &str {
        match self {
            DatafusionExtension::CreateTable(_) => "CreateTable",
        }
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            DatafusionExtension::CreateTable(s) => UserDefinedLogicalNodeCore::inputs(s),
        }
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        match self {
            DatafusionExtension::CreateTable(s) => UserDefinedLogicalNodeCore::schema(s),
        }
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        match self {
            DatafusionExtension::CreateTable(s) => UserDefinedLogicalNodeCore::expressions(s),
        }
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatafusionExtension::CreateTable(s) => {
                UserDefinedLogicalNodeCore::fmt_for_explain(s, f)
            }
        }
    }

    fn from_template(&self, exprs: &[datafusion::prelude::Expr], inputs: &[LogicalPlan]) -> Self {
        match self {
            DatafusionExtension::CreateTable(s) => DatafusionExtension::CreateTable(
                UserDefinedLogicalNodeCore::from_template(s, exprs, inputs),
            ),
        }
    }
}

impl IntoExtension for DatafusionExtension {
    fn into_extension(self) -> LogicalPlanExtension {
        LogicalPlanExtension {
            node: Arc::new(self),
        }
    }
}

impl IntoExtension for CreateTable {
    fn into_extension(self) -> LogicalPlanExtension {
        DatafusionExtension::CreateTable(self).into_extension()
    }
}
