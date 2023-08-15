use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use uuid::Uuid;

use crate::errors::{internal, ExecError};
use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{self as plan};
use crate::remote::table::RemoteTableProvider;
use protogen::export::prost::Message;

pub struct GlareDBExtensionCodec<'a> {
    table_providers: Option<&'a HashMap<Uuid, Arc<dyn TableProvider>>>,
}

impl<'a> GlareDBExtensionCodec<'a> {
    pub fn new_decoder(table_providers: &'a HashMap<Uuid, Arc<dyn TableProvider>>) -> Self {
        Self {
            table_providers: Some(table_providers),
        }
    }

    pub fn new_encoder() -> Self {
        Self {
            table_providers: None,
        }
    }
}

impl<'a> fmt::Debug for GlareDBExtensionCodec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlareDBExtensionCodec").finish()
    }
}

impl<'a> LogicalExtensionCodec for GlareDBExtensionCodec<'a> {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        use protogen::sqlexec::logical_plan::{
            self as proto_plan, LogicalPlanExtensionType as PlanType,
        };
        let lp_extension = proto_plan::LogicalPlanExtension::decode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if lp_extension.inner.is_none() {
            return Err(DataFusionError::External(Box::new(ExecError::Internal(
                "missing extension type".to_string(),
            ))));
        }

        Ok(match lp_extension.inner.unwrap() {
            PlanType::CreateTable(create_table) => {
                let create_table: plan::CreateTable = create_table
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_table.into_extension()
            }
            PlanType::CreateSchema(create_schema) => {
                let create_schema: plan::CreateSchema = create_schema
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_schema.into_extension()
            }
            PlanType::DropTables(drop_tables) => {
                let drop_tables: plan::DropTables = drop_tables
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_tables.into_extension()
            }
            PlanType::CreateExternalTable(create_external_table) => {
                let create_external_table: plan::CreateExternalTable = create_external_table
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_external_table.into_extension()
            }
            PlanType::AlterTableRename(alter_table_rename) => {
                let alter_table_rename: plan::AlterTableRename = alter_table_rename
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_table_rename.into_extension()
            }
            PlanType::AlterDatabaseRename(alter_database_rename) => {
                let alter_database_rename: plan::AlterDatabaseRename = alter_database_rename
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_database_rename.into_extension()
            }
        })
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        match node.node.name() {
            plan::CreateTable::EXTENSION_NAME => {
                plan::CreateTable::try_encode_extension(node, buf, self)
            }
            plan::CreateExternalTable::EXTENSION_NAME => {
                plan::CreateExternalTable::try_encode_extension(node, buf, self)
            }
            plan::CreateSchema::EXTENSION_NAME => {
                plan::CreateSchema::try_encode_extension(node, buf, self)
            }
            plan::DropTables::EXTENSION_NAME => {
                plan::DropTables::try_encode_extension(node, buf, self)
            }
            plan::AlterTableRename::EXTENSION_NAME => {
                plan::AlterTableRename::try_encode_extension(node, buf, self)
            }
            plan::AlterDatabaseRename::EXTENSION_NAME => {
                plan::AlterDatabaseRename::try_encode_extension(node, buf, self)
            }
            _ => {
                return Err(DataFusionError::External(Box::new(internal!(
                    "cannot encode the extension type {:?}",
                    node.node.name()
                ))))
            }
        }
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let provider_id = Uuid::from_slice(buf).map_err(|e| {
            DataFusionError::External(Box::new(ExecError::InvalidRemoteId("provider", e)))
        })?;

        let provider = self
            .table_providers
            .and_then(|table_providers| table_providers.get(&provider_id))
            .ok_or_else(|| {
                DataFusionError::External(Box::new(ExecError::Internal(format!(
                    "cannot decode the table provider for id: {provider_id}"
                ))))
            })?;

        Ok(Arc::clone(provider))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(remote_provider) = node.as_any().downcast_ref::<RemoteTableProvider>() {
            remote_provider
                .encode(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Err(DataFusionError::External(Box::new(ExecError::Internal(
                "can only encode `RemoteTableProvider`".to_string(),
            ))))
        }
    }
}
