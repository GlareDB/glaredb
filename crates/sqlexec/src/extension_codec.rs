use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use uuid::Uuid;

use crate::errors::ExecError;
use crate::planner::extension::{ExtensionNode, ExtensionType};
use crate::planner::logical_plan as plan;
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
        _inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension> {
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
                let create_table = plan::CreateTable::try_decode(create_table, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_table.into_extension()
            }
            PlanType::CreateSchema(create_schema) => {
                let create_schema = plan::CreateSchema::try_decode(create_schema, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_schema.into_extension()
            }
            PlanType::DropTables(drop_tables) => {
                let drop_tables = plan::DropTables::try_decode(drop_tables, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_tables.into_extension()
            }
            PlanType::CreateExternalTable(create_external_table) => {
                let create_external_table =
                    plan::CreateExternalTable::try_decode(create_external_table, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_external_table.into_extension()
            }
            PlanType::AlterTableRename(alter_table_rename) => {
                let alter_table_rename =
                    plan::AlterTableRename::try_decode(alter_table_rename, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_table_rename.into_extension()
            }
            PlanType::AlterDatabaseRename(alter_database_rename) => {
                let alter_database_rename =
                    plan::AlterDatabaseRename::try_decode(alter_database_rename, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_database_rename.into_extension()
            }
            PlanType::AlterTunnelRotateKeys(alter_tunnel_rotate_keys) => {
                let alter_tunnel_rotate_keys =
                    plan::AlterTunnelRotateKeys::try_decode(alter_tunnel_rotate_keys, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_tunnel_rotate_keys.into_extension()
            }
            PlanType::CreateCredentials(create_credentials) => {
                let create_credentials =
                    plan::CreateCredentials::try_decode(create_credentials, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_credentials.into_extension()
            }
            PlanType::CreateExternalDatabase(create_external_db) => {
                let create_external_db =
                    plan::CreateExternalDatabase::try_decode(create_external_db, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_external_db.into_extension()
            }
            PlanType::CreateTunnel(create_tunnel) => {
                let create_tunnel = plan::CreateTunnel::try_decode(create_tunnel, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_tunnel.into_extension()
            }
            PlanType::CreateTempTable(create_temp_table) => {
                let create_temp_table =
                    plan::CreateTempTable::try_decode(create_temp_table, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_temp_table.into_extension()
            }
            PlanType::CreateView(create_view) => {
                let create_view = plan::CreateView::try_decode(create_view, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_view.into_extension()
            }
            PlanType::DropCredentials(drop_credentials) => {
                let drop_credentials =
                    plan::DropCredentials::try_decode(drop_credentials, ctx, self)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_credentials.into_extension()
            }
            PlanType::DropDatabase(drop_database) => {
                let drop_database = plan::DropDatabase::try_decode(drop_database, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_database.into_extension()
            }
            PlanType::DropSchemas(drop_schemas) => {
                let drop_schemas = plan::DropSchemas::try_decode(drop_schemas, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_schemas.into_extension()
            }
            PlanType::DropTunnel(drop_tunnel) => {
                let drop_tunnel = plan::DropTunnel::try_decode(drop_tunnel, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_tunnel.into_extension()
            }
            PlanType::DropViews(drop_views) => {
                let drop_views = plan::DropViews::try_decode(drop_views, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_views.into_extension()
            }
            PlanType::SetVariable(set_variable) => {
                let set_variable = plan::SetVariable::try_decode(set_variable, ctx, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                set_variable.into_extension()
            }
            PlanType::CopyTo(copy_to) => plan::CopyTo::try_decode(copy_to, ctx, self)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .into_extension(),
        })
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        let extension = node
            .node
            .name()
            .parse::<ExtensionType>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        match extension {
            ExtensionType::CreateTable => plan::CreateTable::try_encode_extension(node, buf, self),
            ExtensionType::CreateExternalTable => {
                plan::CreateExternalTable::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateSchema => {
                plan::CreateSchema::try_encode_extension(node, buf, self)
            }
            ExtensionType::DropTables => plan::DropTables::try_encode_extension(node, buf, self),
            ExtensionType::AlterTableRename => {
                plan::AlterTableRename::try_encode_extension(node, buf, self)
            }
            ExtensionType::AlterDatabaseRename => {
                plan::AlterDatabaseRename::try_encode_extension(node, buf, self)
            }
            ExtensionType::AlterTunnelRotateKeys => {
                plan::AlterTunnelRotateKeys::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateCredentials => {
                plan::CreateCredentials::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateExternalDatabase => {
                plan::CreateExternalDatabase::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateTunnel => {
                plan::CreateTunnel::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateTempTable => {
                plan::CreateTempTable::try_encode_extension(node, buf, self)
            }
            ExtensionType::CreateView => plan::CreateView::try_encode_extension(node, buf, self),
            ExtensionType::DropCredentials => {
                plan::DropCredentials::try_encode_extension(node, buf, self)
            }
            ExtensionType::DropDatabase => {
                plan::DropDatabase::try_encode_extension(node, buf, self)
            }
            ExtensionType::DropSchemas => plan::DropSchemas::try_encode_extension(node, buf, self),
            ExtensionType::DropTunnel => plan::DropTunnel::try_encode_extension(node, buf, self),
            ExtensionType::DropViews => plan::DropViews::try_encode_extension(node, buf, self),
            ExtensionType::SetVariable => plan::SetVariable::try_encode_extension(node, buf, self),
            ExtensionType::CopyTo => plan::CopyTo::try_encode_extension(node, buf, self),
        }
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
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
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
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

impl<'a> PhysicalExtensionCodec for GlareDBExtensionCodec<'a> {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("inputs: {inputs:?}")
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        unimplemented!("node: {node:?}")
    }
}
