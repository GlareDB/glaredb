use core::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use uuid::Uuid;

use crate::errors::ExecError;
use crate::planner::extension::{ExtensionNode, ExtensionType, PhysicalExtensionNode};
use crate::planner::logical_plan as plan;
use crate::planner::physical_plan::create_credentials_exec::CreateCredentialsExec;
use crate::planner::physical_plan::remote_scan::ProviderReference;
use crate::planner::physical_plan::{
    client_recv::ClientExchangeRecvExec, remote_scan::RemoteScanExec,
};
use crate::remote::provider_cache::ProviderCache;
use crate::remote::table::StubRemoteTableProvider;

use protogen::export::prost::Message;

pub struct GlareDBExtensionCodec<'a> {
    table_providers: Option<&'a ProviderCache>,
}

impl<'a> GlareDBExtensionCodec<'a> {
    pub fn new_decoder(table_providers: &'a ProviderCache) -> Self {
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

        Ok(provider)
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(remote_provider) = node.as_any().downcast_ref::<StubRemoteTableProvider>() {
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
        _inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use protogen::sqlexec::physical_plan as proto;

        let ext = proto::ExecutionPlanExtension::decode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let ext = ext
            .inner
            .ok_or_else(|| DataFusionError::Plan("missing execution plan".to_string()))?;

        let plan: Arc<dyn ExecutionPlan> = match ext {
            proto::ExecutionPlanExtensionType::ClientExchangeRecvExec(ext) => {
                let broadcast_id = Uuid::from_slice(&ext.broadcast_id).map_err(|e| {
                    DataFusionError::Plan(format!("failed to decode broadcast id: {e}"))
                })?;
                let schema = ext
                    .schema
                    .ok_or(DataFusionError::Plan("schema is required".to_string()))?;
                // TODO: Upstream `TryFrom` impl that doesn't need a reference.
                let schema: Schema = (&schema).try_into()?;

                Arc::new(ClientExchangeRecvExec {
                    broadcast_id,
                    schema: Arc::new(schema),
                })
            }
            proto::ExecutionPlanExtensionType::RemoteScanExec(ext) => {
                let provider_id = Uuid::from_slice(&ext.provider_id).map_err(|e| {
                    DataFusionError::Plan(format!("failed to decode provider id: {e}"))
                })?;
                let projected_schema = ext
                    .projected_schema
                    .ok_or(DataFusionError::Plan("schema is required".to_string()))?;
                // TODO: Upstream `TryFrom` impl that doesn't need a reference.
                let projected_schema: Schema = (&projected_schema).try_into()?;
                let projection = if ext.projection.is_empty() {
                    None
                } else {
                    Some(ext.projection.into_iter().map(|u| u as usize).collect())
                };

                let filters = ext
                    .filters
                    .iter()
                    .map(|expr| parse_expr(expr, registry))
                    .collect::<Result<Vec<Expr>, _>>()?;

                let limit = ext.limit.map(|l| l as usize);

                // We're on the remote side, get the real table provider from
                // the cache.
                let prov = self
                    .table_providers
                    .expect("remote context should have provider cache")
                    .get(&provider_id)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("Missing proivder for id: {provider_id}"))
                    })?;

                Arc::new(RemoteScanExec {
                    provider: ProviderReference::Provider(prov),
                    projected_schema: Arc::new(projected_schema),
                    projection,
                    filters,
                    limit,
                })
            }
            proto::ExecutionPlanExtensionType::CreateCredentialsExec(create_credentials) => {
                let exec = CreateCredentialsExec::try_decode(create_credentials, self)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Arc::new(exec)
            }
        };

        Ok(plan)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        use protogen::sqlexec::physical_plan as proto;

        let inner = if let Some(exec) = node.as_any().downcast_ref::<ClientExchangeRecvExec>() {
            proto::ExecutionPlanExtensionType::ClientExchangeRecvExec(
                proto::ClientExchangeRecvExec {
                    broadcast_id: exec.broadcast_id.into_bytes().to_vec(),
                    schema: Some(exec.schema.clone().try_into()?),
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<RemoteScanExec>() {
            let id = match exec.provider {
                ProviderReference::RemoteReference(id) => id,
                ProviderReference::Provider(_) => {
                    return Err(DataFusionError::Internal(
                        "Unexpectedly got table provider on client side".to_string(),
                    ))
                }
            };

            proto::ExecutionPlanExtensionType::RemoteScanExec(proto::RemoteScanExec {
                provider_id: id.into_bytes().to_vec(),
                projected_schema: Some(exec.projected_schema.clone().try_into()?),
                projection: exec
                    .projection
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|u| u as u64)
                    .collect(),
                filters: exec
                    .filters
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<_, _>>()?,
                limit: exec.limit.map(|u| u as u64),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateCredentialsExec>() {
            return exec
                .try_encode(buf, self)
                .map_err(|e| DataFusionError::External(Box::new(e)));
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "encoding not implemented for physical plan: {node:?}"
            )));
        };

        let enc = proto::ExecutionPlanExtension { inner: Some(inner) };

        enc.encode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
