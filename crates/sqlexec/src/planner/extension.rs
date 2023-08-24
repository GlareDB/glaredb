/// extension implementations for converting our logical plan into datafusion logical plan
use datafusion_proto::{
    logical_plan::LogicalExtensionCodec, physical_plan::PhysicalExtensionCodec,
};
use protogen::ProtoConvError;
use std::{str::FromStr, sync::Arc};

use crate::{
    errors::{internal, ExecError, Result},
    LogicalPlan,
};
use datafusion::{
    execution::{runtime_env::RuntimeEnv, FunctionRegistry},
    logical_expr::{Extension as LogicalPlanExtension, UserDefinedLogicalNodeCore},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};

use super::logical_plan::{
    AlterDatabaseRename, AlterTableRename, AlterTunnelRotateKeys, CopyTo, CreateCredentials,
    CreateExternalDatabase, CreateExternalTable, CreateSchema, CreateTable, CreateTempTable,
    CreateTunnel, CreateView, Delete, DropCredentials, DropDatabase, DropSchemas, DropTables,
    DropTunnel, DropViews, Insert, SetVariable, Update,
};

/// This tracks all of our extensions so that we can ensure an exhaustive match on anywhere that uses the extension
///
/// This should match all of the variants expressed in `protogen::sqlexec::logical_plan::LogicalPlanExtension`
#[derive(Debug)]
pub enum ExtensionType {
    AlterDatabaseRename,
    AlterTableRename,
    AlterTunnelRotateKeys,
    CreateCredentials,
    CreateExternalDatabase,
    CreateExternalTable,
    CreateSchema,
    CreateTable,
    CreateTempTable,
    CreateTunnel,
    CreateView,
    DropTables,
    DropCredentials,
    DropDatabase,
    DropSchemas,
    DropTunnel,
    DropViews,
    SetVariable,
    CopyTo,
    Update,
    Insert,
    Delete,
}

impl FromStr for ExtensionType {
    type Err = ExecError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            AlterDatabaseRename::EXTENSION_NAME => Self::AlterDatabaseRename,
            AlterTableRename::EXTENSION_NAME => Self::AlterTableRename,
            AlterTunnelRotateKeys::EXTENSION_NAME => Self::AlterTunnelRotateKeys,
            CreateCredentials::EXTENSION_NAME => Self::CreateCredentials,
            CreateExternalDatabase::EXTENSION_NAME => Self::CreateExternalDatabase,
            CreateExternalTable::EXTENSION_NAME => Self::CreateExternalTable,
            CreateSchema::EXTENSION_NAME => Self::CreateSchema,
            CreateTable::EXTENSION_NAME => Self::CreateTable,
            CreateTempTable::EXTENSION_NAME => Self::CreateTempTable,
            CreateTunnel::EXTENSION_NAME => Self::CreateTunnel,
            CreateView::EXTENSION_NAME => Self::CreateView,
            DropTables::EXTENSION_NAME => Self::DropTables,
            DropCredentials::EXTENSION_NAME => Self::DropCredentials,
            DropDatabase::EXTENSION_NAME => Self::DropDatabase,
            DropSchemas::EXTENSION_NAME => Self::DropSchemas,
            DropTunnel::EXTENSION_NAME => Self::DropTunnel,
            DropViews::EXTENSION_NAME => Self::DropViews,
            SetVariable::EXTENSION_NAME => Self::SetVariable,
            CopyTo::EXTENSION_NAME => Self::CopyTo,
            Update::EXTENSION_NAME => Self::Update,
            Insert::EXTENSION_NAME => Self::Insert,
            Delete::EXTENSION_NAME => Self::Delete,
            _ => return Err(internal!("unknown extension type: {}", s)),
        })
    }
}

pub trait ExtensionNode: Sized + UserDefinedLogicalNodeCore {
    type ProtoRepr;
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

    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self, ProtoConvError>;

    fn try_encode_extension(
        extension: &LogicalPlanExtension,
        buf: &mut Vec<u8>,
        codec: &dyn LogicalExtensionCodec,
    ) -> Result<()> {
        // TODO: ?
        let extension = Self::try_decode_extension(extension)?;
        extension.try_encode(buf, codec)
    }
}

pub trait PhysicalExtensionNode: Sized + ExecutionPlan {
    type ProtoRepr;

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn PhysicalExtensionCodec) -> Result<()>;

    fn try_decode(
        proto: Self::ProtoRepr,
        _registry: &dyn FunctionRegistry,
        _runtime: &RuntimeEnv,
        _extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self, ProtoConvError>;
}
