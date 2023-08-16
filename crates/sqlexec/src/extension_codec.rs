use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use uuid::Uuid;

use crate::errors::ExecError;
use crate::planner::extension::{ExtensionNode, ExtensionType};
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
            PlanType::AlterTunnelRotateKeys(alter_tunnel_rotate_keys) => {
                let alter_tunnel_rotate_keys: plan::AlterTunnelRotateKeys =
                    alter_tunnel_rotate_keys
                        .try_into()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                alter_tunnel_rotate_keys.into_extension()
            }
            PlanType::CreateCredentials(create_credentials) => {
                let create_credentials: plan::CreateCredentials = create_credentials
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_credentials.into_extension()
            }
            PlanType::CreateExternalDatabase(create_external_db) => {
                let create_external_db: plan::CreateExternalDatabase = create_external_db
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_external_db.into_extension()
            }
            PlanType::CreateTunnel(create_tunnel) => {
                let create_tunnel: plan::CreateTunnel = create_tunnel
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_tunnel.into_extension()
            }
            PlanType::CreateTempTable(create_temp_table) => {
                let create_temp_table: plan::CreateTempTable = create_temp_table
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_temp_table.into_extension()
            }
            PlanType::CreateView(create_view) => {
                let create_view: plan::CreateView = create_view
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                create_view.into_extension()
            }
            PlanType::DropCredentials(drop_credentials) => {
                let drop_credentials: plan::DropCredentials = drop_credentials
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_credentials.into_extension()
            }
            PlanType::DropDatabase(drop_database) => {
                let drop_database: plan::DropDatabase = drop_database
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_database.into_extension()
            }
            PlanType::DropSchemas(drop_schemas) => {
                let drop_schemas: plan::DropSchemas = drop_schemas
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_schemas.into_extension()
            }
            PlanType::DropTunnel(drop_tunnel) => {
                let drop_tunnel: plan::DropTunnel = drop_tunnel
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_tunnel.into_extension()
            }
            PlanType::DropViews(drop_views) => {
                let drop_views: plan::DropViews = drop_views
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                drop_views.into_extension()
            }
            PlanType::ClientExchangeSend(send) => {
                let send: plan::ClientExchangeSend = send
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                send.into_extension()
            }
            PlanType::ClientExchangeRecv(recv) => {
                let recv: plan::ClientExchangeRecv = recv
                    .try_into()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                recv.into_extension()
            }
        })
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
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
            ExtensionType::ClientExchangeSend => {
                plan::ClientExchangeSend::try_encode_extension(node, buf, self)
            }
            ExtensionType::ClientExchangeRecv => {
                plan::ClientExchangeRecv::try_encode_extension(node, buf, self)
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
