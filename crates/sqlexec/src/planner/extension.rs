use std::str::FromStr;
use std::sync::Arc;

use datafusion::logical_expr::{Extension as LogicalPlanExtension, UserDefinedLogicalNodeCore};

use super::logical_plan::{
    AlterDatabase,
    AlterTable,
    AlterTunnelRotateKeys,
    CopyTo,
    CreateCredentials,
    CreateExternalDatabase,
    CreateExternalTable,
    CreateSchema,
    CreateTable,
    CreateTempTable,
    CreateTunnel,
    CreateView,
    Delete,
    DescribeTable,
    DropCredentials,
    DropDatabase,
    DropSchemas,
    DropTables,
    DropTunnel,
    DropViews,
    Insert,
    SetVariable,
    ShowVariable,
    Update,
};
use crate::errors::{internal, ExecError, Result};
use crate::LogicalPlan;

/// This tracks all of our extensions so that we can ensure an exhaustive match on anywhere that uses the extension
///
/// This should match all of the variants expressed in `protogen::sqlexec::logical_plan::LogicalPlanExtension`
#[derive(Debug)]
pub enum ExtensionType {
    AlterDatabase,
    AlterTable,
    AlterTunnelRotateKeys,
    CreateCredentials,
    CreateExternalDatabase,
    CreateExternalTable,
    CreateSchema,
    CreateTable,
    CreateTempTable,
    CreateTunnel,
    CreateView,
    DescribeTable,
    DropTables,
    DropCredentials,
    DropDatabase,
    DropSchemas,
    DropTunnel,
    DropViews,
    SetVariable,
    ShowVariable,
    CopyTo,
    Update,
    Insert,
    Delete,
    Load,
}

impl FromStr for ExtensionType {
    type Err = ExecError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            AlterDatabase::EXTENSION_NAME => Self::AlterDatabase,
            AlterTable::EXTENSION_NAME => Self::AlterTable,
            AlterTunnelRotateKeys::EXTENSION_NAME => Self::AlterTunnelRotateKeys,
            CreateCredentials::EXTENSION_NAME => Self::CreateCredentials,
            CreateExternalDatabase::EXTENSION_NAME => Self::CreateExternalDatabase,
            CreateExternalTable::EXTENSION_NAME => Self::CreateExternalTable,
            CreateSchema::EXTENSION_NAME => Self::CreateSchema,
            CreateTable::EXTENSION_NAME => Self::CreateTable,
            CreateTempTable::EXTENSION_NAME => Self::CreateTempTable,
            CreateTunnel::EXTENSION_NAME => Self::CreateTunnel,
            CreateView::EXTENSION_NAME => Self::CreateView,
            DescribeTable::EXTENSION_NAME => Self::DescribeTable,
            DropTables::EXTENSION_NAME => Self::DropTables,
            DropCredentials::EXTENSION_NAME => Self::DropCredentials,
            DropDatabase::EXTENSION_NAME => Self::DropDatabase,
            DropSchemas::EXTENSION_NAME => Self::DropSchemas,
            DropTunnel::EXTENSION_NAME => Self::DropTunnel,
            DropViews::EXTENSION_NAME => Self::DropViews,
            SetVariable::EXTENSION_NAME => Self::SetVariable,
            ShowVariable::EXTENSION_NAME => Self::ShowVariable,
            CopyTo::EXTENSION_NAME => Self::CopyTo,
            Update::EXTENSION_NAME => Self::Update,
            Insert::EXTENSION_NAME => Self::Insert,
            Delete::EXTENSION_NAME => Self::Delete,
            crate::planner::logical_plan::Load::EXTENSION_NAME => Self::Load,
            _ => return Err(internal!("unknown extension type: {}", s)),
        })
    }
}

pub trait ExtensionNode: Sized + UserDefinedLogicalNodeCore {
    const EXTENSION_NAME: &'static str;

    fn into_extension(self) -> LogicalPlanExtension {
        LogicalPlanExtension {
            node: Arc::new(self),
        }
    }

    fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Datafusion(datafusion::logical_expr::LogicalPlan::Extension(
            self.into_extension(),
        ))
    }
}
