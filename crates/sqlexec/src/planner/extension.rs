/// extension implementations for converting our logical plan into datafusion logical plan
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::{str::FromStr, sync::Arc};

use crate::{
    errors::{internal, ExecError, Result},
    LogicalPlan,
};
use datafusion::logical_expr::{Extension as LogicalPlanExtension, UserDefinedLogicalNodeCore};

use super::logical_plan::{
    AlterDatabaseRename, AlterTableRename, AlterTunnelRotateKeys, CreateCredentials,
    CreateExternalDatabase, CreateExternalTable, CreateSchema, CreateTable, CreateTunnel,
    DropTables,
};

/// This tracks all of our extensions so that we can ensure an exhaustive match on anywhere that uses the extension
///
/// This should match all of the variants expressed in `protogen::sqlexec::logical_plan::LogicalPlanExtension`
pub enum ExtensionType {
    CreateTable,
    CreateExternalTable,
    CreateSchema,
    DropTables,
    AlterTableRename,
    AlterDatabaseRename,
    AlterTunnelRotateKeys,
    CreateCredentials,
    CreateExternalDatabase,
    CreateTunnel,
}

impl FromStr for ExtensionType {
    type Err = ExecError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            CreateTable::EXTENSION_NAME => Self::CreateTable,
            CreateExternalTable::EXTENSION_NAME => Self::CreateExternalTable,
            CreateSchema::EXTENSION_NAME => Self::CreateSchema,
            DropTables::EXTENSION_NAME => Self::DropTables,
            AlterTableRename::EXTENSION_NAME => Self::AlterTableRename,
            AlterDatabaseRename::EXTENSION_NAME => Self::AlterDatabaseRename,
            AlterTunnelRotateKeys::EXTENSION_NAME => Self::AlterTunnelRotateKeys,
            CreateCredentials::EXTENSION_NAME => Self::CreateCredentials,
            CreateExternalDatabase::EXTENSION_NAME => Self::CreateExternalDatabase,
            CreateTunnel::EXTENSION_NAME => Self::CreateTunnel,
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
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self>;
    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()>;
    fn try_encode_extension(
        extension: &LogicalPlanExtension,
        buf: &mut Vec<u8>,
        codec: &dyn LogicalExtensionCodec,
    ) -> Result<()> {
        let extension = Self::try_decode_extension(extension)?;
        extension.try_encode(buf, codec)
    }
}
