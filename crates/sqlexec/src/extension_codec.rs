use core::fmt;
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::logical_expr::{
    AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowFunction, WindowUDF,
};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::expressions::{
    ApproxDistinct, ApproxMedian, ApproxPercentileCont, ApproxPercentileContWithWeight, ArrayAgg,
    Avg, BitAnd, BitOr, BitXor, BoolAnd, BoolOr, Correlation, Count, Covariance, CovariancePop,
    CumeDist, FirstValue, Grouping, LastValue, Literal, Max, Median, Min, NthValue, Ntile, Rank,
    RankType, Regr, RowNumber, Stddev, StddevPop, Sum, Variance, VariancePop, WindowShift,
};
use datafusion::physical_plan::union::InterleaveExec;
use datafusion::physical_plan::values::ValuesExec;
use datafusion::physical_plan::windows::{
    create_window_expr, BoundedWindowAggExec, BuiltInWindowExpr, PartitionSearchMode,
    PlainAggregateWindowExpr, WindowAggExec,
};
use datafusion::physical_plan::{
    displayable, AggregateExpr, ExecutionPlan, PhysicalExpr, WindowExpr,
};
use datafusion::prelude::{Expr, SessionContext};
use datafusion_ext::runtime::runtime_group::RuntimeGroupExec;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::{parse_physical_expr, parse_physical_sort_expr};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::physical_window_expr_node;
use protogen::metastore::types::catalog::RuntimePreference;
use protogen::sqlexec::physical_plan::window_agg_exec_node;
use uuid::Uuid;

use crate::errors::ExecError;
use crate::planner::extension::{ExtensionNode, ExtensionType, PhysicalExtensionNode};
use crate::planner::logical_plan as plan;
use crate::planner::physical_plan::alter_database_rename::AlterDatabaseRenameExec;
use crate::planner::physical_plan::alter_table_rename::AlterTableRenameExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
use crate::planner::physical_plan::copy_to::CopyToExec;
use crate::planner::physical_plan::create_credentials::CreateCredentialsExec;
use crate::planner::physical_plan::create_external_database::CreateExternalDatabaseExec;
use crate::planner::physical_plan::create_external_table::CreateExternalTableExec;
use crate::planner::physical_plan::create_schema::CreateSchemaExec;
use crate::planner::physical_plan::create_table::CreateTableExec;
use crate::planner::physical_plan::create_temp_table::CreateTempTableExec;
use crate::planner::physical_plan::create_tunnel::CreateTunnelExec;
use crate::planner::physical_plan::create_view::CreateViewExec;
use crate::planner::physical_plan::delete::DeleteExec;
use crate::planner::physical_plan::drop_credentials::DropCredentialsExec;
use crate::planner::physical_plan::drop_database::DropDatabaseExec;
use crate::planner::physical_plan::drop_schemas::DropSchemasExec;
use crate::planner::physical_plan::drop_tables::DropTablesExec;
use crate::planner::physical_plan::drop_tunnel::DropTunnelExec;
use crate::planner::physical_plan::drop_views::DropViewsExec;
use crate::planner::physical_plan::insert::InsertExec;
use crate::planner::physical_plan::remote_scan::ProviderReference;
use crate::planner::physical_plan::set_var::SetVarExec;
use crate::planner::physical_plan::show_var::ShowVarExec;
use crate::planner::physical_plan::update::UpdateExec;
use crate::planner::physical_plan::values::ExtValuesExec;
use crate::planner::physical_plan::{
    client_recv::ClientExchangeRecvExec, remote_scan::RemoteScanExec,
};
use crate::remote::provider_cache::ProviderCache;
use crate::remote::table::StubRemoteTableProvider;

use protogen::export::prost::Message;

pub struct GlareDBExtensionCodec<'a> {
    table_providers: Option<&'a ProviderCache>,
    runtime: Option<Arc<RuntimeEnv>>,
}
struct EmptyFunctionRegistry;

impl FunctionRegistry for EmptyFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Function '{name}'"))
        )
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{name}'"))
        )
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Window Function '{name}'"))
        )
    }
}

impl<'a> GlareDBExtensionCodec<'a> {
    pub fn new_decoder(table_providers: &'a ProviderCache, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            table_providers: Some(table_providers),
            runtime: Some(runtime),
        }
    }

    pub fn new_encoder() -> Self {
        Self {
            table_providers: None,
            runtime: None,
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
            ExtensionType::ShowVariable => {
                plan::ShowVariable::try_encode_extension(node, buf, self)
            }
            ExtensionType::CopyTo => plan::CopyTo::try_encode_extension(node, buf, self),
            ExtensionType::Update => plan::Update::try_encode_extension(node, buf, self),
            ExtensionType::Delete => plan::Update::try_encode_extension(node, buf, self),
            ExtensionType::Insert => plan::Insert::try_encode_extension(node, buf, self),
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
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use protogen::sqlexec::physical_plan as proto;

        let ext = proto::ExecutionPlanExtension::decode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let ext = ext
            .inner
            .ok_or_else(|| DataFusionError::Plan("missing execution plan".to_string()))?;

        //TODO! use the `PhysicalExtensionNode` trait to decode the extension instead of hardcoding here.
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
            proto::ExecutionPlanExtensionType::CreateSchema(ext) => Arc::new(CreateSchemaExec {
                catalog_version: ext.catalog_version,
                schema_reference: ext
                    .schema_reference
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing schema references".to_string())
                    })?
                    .into(),
                if_not_exists: ext.if_not_exists,
            }),
            proto::ExecutionPlanExtensionType::CreateCredentialsExec(create_credentials) => {
                let exec = CreateCredentialsExec::try_decode(
                    create_credentials,
                    &EmptyFunctionRegistry,
                    self.runtime
                        .as_ref()
                        .expect("runtime should be set on decoder"),
                    self,
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Arc::new(exec)
            }
            proto::ExecutionPlanExtensionType::AlterDatabaseRenameExec(ext) => {
                Arc::new(AlterDatabaseRenameExec {
                    catalog_version: ext.catalog_version,
                    name: ext.name,
                    new_name: ext.new_name,
                })
            }
            proto::ExecutionPlanExtensionType::AlterTableRenameExec(ext) => {
                Arc::new(AlterTableRenameExec {
                    catalog_version: ext.catalog_version,
                    tbl_reference: ext
                        .tbl_reference
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing table references".to_string())
                        })?
                        .into(),
                    new_tbl_reference: ext
                        .new_tbl_reference
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing new table references".to_string())
                        })?
                        .into(),
                })
            }
            proto::ExecutionPlanExtensionType::AlterTunnelRotateKeysExec(ext) => {
                Arc::new(AlterTunnelRotateKeysExec {
                    catalog_version: ext.catalog_version,
                    name: ext.name,
                    if_exists: ext.if_exists,
                    new_ssh_key: ext.new_ssh_key,
                })
            }
            proto::ExecutionPlanExtensionType::DropDatabaseExec(ext) => {
                Arc::new(DropDatabaseExec {
                    catalog_version: ext.catalog_version,
                    names: ext.names,
                    if_exists: ext.if_exists,
                })
            }
            proto::ExecutionPlanExtensionType::CreateTableExec(ext) => {
                let schema = ext
                    .arrow_schema
                    .ok_or(DataFusionError::Plan("schema is required".to_string()))?;
                let schema: Schema = (&schema).try_into()?;

                Arc::new(CreateTableExec {
                    catalog_version: ext.catalog_version,
                    tbl_reference: ext
                        .tbl_reference
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing table references".to_string())
                        })?
                        .into(),
                    if_not_exists: ext.if_not_exists,
                    arrow_schema: Arc::new(schema),
                    source: inputs.get(0).cloned(),
                })
            }
            proto::ExecutionPlanExtensionType::CreateTempTableExec(ext) => {
                let schema = ext
                    .arrow_schema
                    .ok_or(DataFusionError::Plan("schema is required".to_string()))?;
                let schema: Schema = (&schema).try_into()?;

                Arc::new(CreateTempTableExec {
                    tbl_reference: ext
                        .tbl_reference
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing table references".to_string())
                        })?
                        .into(),
                    if_not_exists: ext.if_not_exists,
                    arrow_schema: Arc::new(schema),
                    source: inputs.get(0).cloned(),
                })
            }
            proto::ExecutionPlanExtensionType::DropSchemasExec(ext) => Arc::new(DropSchemasExec {
                catalog_version: ext.catalog_version,
                schema_references: ext
                    .schema_references
                    .into_iter()
                    .map(|r| r.into())
                    .collect(),
                if_exists: ext.if_exists,
                cascade: ext.cascade,
            }),
            proto::ExecutionPlanExtensionType::DropTunnelExec(ext) => Arc::new(DropTunnelExec {
                catalog_version: ext.catalog_version,
                names: ext.names,
                if_exists: ext.if_exists,
            }),
            proto::ExecutionPlanExtensionType::DropViewsExec(ext) => Arc::new(DropViewsExec {
                catalog_version: ext.catalog_version,
                view_references: ext.view_references.into_iter().map(|r| r.into()).collect(),
                if_exists: ext.if_exists,
            }),
            proto::ExecutionPlanExtensionType::CreateExternalDatabaseExec(ext) => {
                let options = ext.options.ok_or(protogen::ProtoConvError::RequiredField(
                    "options".to_string(),
                ))?;
                Arc::new(CreateExternalDatabaseExec {
                    catalog_version: ext.catalog_version,
                    database_name: ext.database_name,
                    if_not_exists: ext.if_not_exists,
                    options: options.try_into()?,
                    tunnel: ext.tunnel,
                })
            }
            proto::ExecutionPlanExtensionType::CreateExternalTableExec(ext) => {
                let table_options =
                    ext.table_options
                        .ok_or(protogen::ProtoConvError::RequiredField(
                            "table_options".to_string(),
                        ))?;
                Arc::new(CreateExternalTableExec {
                    catalog_version: ext.catalog_version,
                    tbl_reference: ext
                        .tbl_reference
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing table references".to_string())
                        })?
                        .into(),
                    if_not_exists: ext.if_not_exists,
                    table_options: table_options.try_into()?,
                    tunnel: ext.tunnel,
                })
            }
            proto::ExecutionPlanExtensionType::CreateTunnelExec(ext) => {
                let options = ext.options.ok_or(protogen::ProtoConvError::RequiredField(
                    "options".to_string(),
                ))?;
                Arc::new(CreateTunnelExec {
                    catalog_version: ext.catalog_version,
                    name: ext.name,
                    if_not_exists: ext.if_not_exists,
                    options: options.try_into()?,
                })
            }
            proto::ExecutionPlanExtensionType::CreateViewExec(ext) => Arc::new(CreateViewExec {
                catalog_version: ext.catalog_version,
                view_reference: ext
                    .view_reference
                    .ok_or_else(|| DataFusionError::Internal("missing view reference".to_string()))?
                    .into(),
                sql: ext.sql,
                columns: ext.columns,
                or_replace: ext.or_replace,
            }),
            proto::ExecutionPlanExtensionType::DropCredentialsExec(ext) => {
                Arc::new(DropCredentialsExec {
                    catalog_version: ext.catalog_version,
                    names: ext.names,
                    if_exists: ext.if_exists,
                })
            }
            proto::ExecutionPlanExtensionType::DropTablesExec(ext) => Arc::new(DropTablesExec {
                catalog_version: ext.catalog_version,
                tbl_references: ext.tbl_references.into_iter().map(|r| r.into()).collect(),
                if_exists: ext.if_exists,
            }),
            proto::ExecutionPlanExtensionType::SetVarExec(ext) => Arc::new(SetVarExec {
                variable: ext.variable,
                values: ext.values,
            }),
            proto::ExecutionPlanExtensionType::ShowVarExec(ext) => Arc::new(ShowVarExec {
                variable: ext.variable,
            }),
            proto::ExecutionPlanExtensionType::UpdateExec(ext) => {
                let mut updates = Vec::with_capacity(ext.updates.len());
                for update in ext.updates {
                    let expr = update.expr.ok_or_else(|| {
                        DataFusionError::Internal("missing expression".to_string())
                    })?;
                    let expr = parse_expr(&expr, registry)?;
                    updates.push((update.column.clone(), expr));
                }
                let where_expr: Option<Expr> = ext
                    .where_expr
                    .map(|expr| parse_expr(&expr, registry))
                    .transpose()?;
                Arc::new(UpdateExec {
                    table: ext
                        .table
                        .ok_or_else(|| DataFusionError::Internal("missing table".to_string()))?
                        .try_into()?,
                    updates,
                    where_expr,
                })
            }
            proto::ExecutionPlanExtensionType::InsertExec(ext) => Arc::new(InsertExec {
                table: ext
                    .table
                    .ok_or_else(|| DataFusionError::Internal("missing table".to_string()))?
                    .try_into()?,
                source: inputs
                    .get(0)
                    .ok_or_else(|| DataFusionError::Internal("missing input source".to_string()))?
                    .clone(),
            }),
            proto::ExecutionPlanExtensionType::DeleteExec(ext) => {
                let where_expr: Option<Expr> = ext
                    .where_expr
                    .map(|expr| parse_expr(&expr, registry))
                    .transpose()?;
                Arc::new(DeleteExec {
                    table: ext
                        .table
                        .ok_or_else(|| DataFusionError::Internal("missing table".to_string()))?
                        .try_into()?,
                    where_expr,
                })
            }
            proto::ExecutionPlanExtensionType::CopyToExec(ext) => Arc::new(CopyToExec {
                format: ext
                    .format
                    .ok_or_else(|| DataFusionError::Internal("missing format options".to_string()))?
                    .try_into()?,
                dest: ext
                    .dest
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing destination options".to_string())
                    })?
                    .try_into()?,
                source: inputs
                    .get(0)
                    .ok_or_else(|| DataFusionError::Internal("missing input source".to_string()))?
                    .clone(),
            }),
            proto::ExecutionPlanExtensionType::ValuesExec(ext) => {
                let schema = ext
                    .schema
                    .ok_or_else(|| DataFusionError::Internal("missing schema".to_string()))?;
                let reader = IpcFileReader::try_new(Cursor::new(ext.data), None)?;
                Arc::new(ExtValuesExec {
                    schema: (&schema).try_into()?,
                    data: reader.collect::<Result<Vec<_>, ArrowError>>()?,
                })
            }
            proto::ExecutionPlanExtensionType::InterleaveExec(_ext) => {
                Arc::new(InterleaveExec::try_new(inputs.to_vec())?)
            }
            proto::ExecutionPlanExtensionType::RuntimeGroupExec(_ext) => {
                Arc::new(RuntimeGroupExec::new(
                    RuntimePreference::Unspecified,
                    inputs
                        .get(0)
                        .ok_or_else(|| DataFusionError::Internal("missing child".to_string()))?
                        .clone(),
                ))
            }
            proto::ExecutionPlanExtensionType::AnalyzeExec(ext) => {
                let input = inputs
                    .get(0)
                    .ok_or_else(|| DataFusionError::Internal("missing input source".to_string()))?
                    .clone();
                let schema = ext
                    .schema
                    .ok_or_else(|| DataFusionError::Internal("missing schema".to_string()))?;
                Arc::new(AnalyzeExec::new(
                    ext.verbose,
                    ext.show_statistics,
                    input.clone(),
                    Arc::new((&schema).try_into()?),
                ))
            }
            proto::ExecutionPlanExtensionType::WindowFunc(ext) => {
                let input = inputs
                    .get(0)
                    .ok_or_else(|| DataFusionError::Internal("missing input source".to_string()))?
                    .clone();
                let input_schema = ext
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "input_schema in WindowAggrNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let input_schema: SchemaRef = SchemaRef::new((&input_schema).try_into()?);

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = ext
                    .window_expr
                    .iter()
                    .map(|window_expr| {
                        parse_physical_window_expr(window_expr, registry, input_schema.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_keys = ext
                    .partition_keys
                    .iter()
                    .map(|expr| parse_physical_expr(expr, registry, input.schema().as_ref()))
                    .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;

                if let Some(partition_search_mode) = ext.partition_search_mode.as_ref() {
                    let partition_search_mode = match partition_search_mode {
                        window_agg_exec_node::PartitionSearchMode::Linear(_) => {
                            PartitionSearchMode::Linear
                        }
                        window_agg_exec_node::PartitionSearchMode::PartiallySorted(
                            partiall_sorted,
                        ) => PartitionSearchMode::PartiallySorted(
                            partiall_sorted
                                .columns
                                .iter()
                                .map(|c| *c as usize)
                                .collect(),
                        ),
                        window_agg_exec_node::PartitionSearchMode::Sorted(_) => {
                            PartitionSearchMode::Sorted
                        }
                    };

                    Arc::new(BoundedWindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        input_schema,
                        partition_keys,
                        partition_search_mode,
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        input_schema,
                        partition_keys,
                    )?)
                }
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
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateSchemaExec>() {
            proto::ExecutionPlanExtensionType::CreateSchema(proto::CreateSchema {
                catalog_version: exec.catalog_version,
                schema_reference: Some(exec.schema_reference.clone().into()),
                if_not_exists: exec.if_not_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateCredentialsExec>() {
            return exec
                .try_encode(buf, self)
                .map_err(|e| DataFusionError::External(Box::new(e)));
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateTableExec>() {
            proto::ExecutionPlanExtensionType::CreateTableExec(proto::CreateTableExec {
                catalog_version: exec.catalog_version,
                tbl_reference: Some(exec.tbl_reference.clone().into()),
                if_not_exists: exec.if_not_exists,
                arrow_schema: Some(exec.arrow_schema.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateTempTableExec>() {
            proto::ExecutionPlanExtensionType::CreateTempTableExec(proto::CreateTempTableExec {
                tbl_reference: Some(exec.tbl_reference.clone().into()),
                if_not_exists: exec.if_not_exists,
                arrow_schema: Some(exec.arrow_schema.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<AlterDatabaseRenameExec>() {
            proto::ExecutionPlanExtensionType::AlterDatabaseRenameExec(
                proto::AlterDatabaseRenameExec {
                    catalog_version: exec.catalog_version,
                    name: exec.name.clone(),
                    new_name: exec.new_name.clone(),
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<AlterTableRenameExec>() {
            proto::ExecutionPlanExtensionType::AlterTableRenameExec(proto::AlterTableRenameExec {
                catalog_version: exec.catalog_version,
                tbl_reference: Some(exec.tbl_reference.clone().into()),
                new_tbl_reference: Some(exec.new_tbl_reference.clone().into()),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<AlterTunnelRotateKeysExec>() {
            proto::ExecutionPlanExtensionType::AlterTunnelRotateKeysExec(
                proto::AlterTunnelRotateKeysExec {
                    catalog_version: exec.catalog_version,
                    name: exec.name.clone(),
                    if_exists: exec.if_exists,
                    new_ssh_key: exec.new_ssh_key.clone(),
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<DropDatabaseExec>() {
            proto::ExecutionPlanExtensionType::DropDatabaseExec(proto::DropDatabaseExec {
                catalog_version: exec.catalog_version,
                names: exec.names.clone(),
                if_exists: exec.if_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DropSchemasExec>() {
            proto::ExecutionPlanExtensionType::DropSchemasExec(proto::DropSchemasExec {
                catalog_version: exec.catalog_version,
                schema_references: exec
                    .schema_references
                    .clone()
                    .into_iter()
                    .map(|r| r.into())
                    .collect(),
                if_exists: exec.if_exists,
                cascade: exec.cascade,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DropTunnelExec>() {
            proto::ExecutionPlanExtensionType::DropTunnelExec(proto::DropTunnelExec {
                catalog_version: exec.catalog_version,
                names: exec.names.clone(),
                if_exists: exec.if_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DropViewsExec>() {
            proto::ExecutionPlanExtensionType::DropViewsExec(proto::DropViewsExec {
                catalog_version: exec.catalog_version,
                view_references: exec
                    .view_references
                    .clone()
                    .into_iter()
                    .map(|r| r.into())
                    .collect(),
                if_exists: exec.if_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateExternalDatabaseExec>() {
            proto::ExecutionPlanExtensionType::CreateExternalDatabaseExec(
                proto::CreateExternalDatabaseExec {
                    catalog_version: exec.catalog_version,
                    database_name: exec.database_name.clone(),
                    options: Some(exec.options.clone().into()),
                    if_not_exists: exec.if_not_exists,
                    tunnel: exec.tunnel.clone(),
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateExternalTableExec>() {
            proto::ExecutionPlanExtensionType::CreateExternalTableExec(
                proto::CreateExternalTableExec {
                    catalog_version: exec.catalog_version,
                    tbl_reference: Some(exec.tbl_reference.clone().into()),
                    if_not_exists: exec.if_not_exists,
                    table_options: Some(exec.table_options.clone().try_into()?),
                    tunnel: exec.tunnel.clone(),
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateTunnelExec>() {
            proto::ExecutionPlanExtensionType::CreateTunnelExec(proto::CreateTunnelExec {
                catalog_version: exec.catalog_version,
                name: exec.name.clone(),
                options: Some(exec.options.clone().into()),
                if_not_exists: exec.if_not_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateViewExec>() {
            proto::ExecutionPlanExtensionType::CreateViewExec(proto::CreateViewExec {
                catalog_version: exec.catalog_version,
                view_reference: Some(exec.view_reference.clone().into()),
                sql: exec.sql.clone(),
                columns: exec.columns.clone(),
                or_replace: exec.or_replace,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DropCredentialsExec>() {
            proto::ExecutionPlanExtensionType::DropCredentialsExec(proto::DropCredentialsExec {
                catalog_version: exec.catalog_version,
                names: exec.names.clone(),
                if_exists: exec.if_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DropTablesExec>() {
            proto::ExecutionPlanExtensionType::DropTablesExec(proto::DropTablesExec {
                catalog_version: exec.catalog_version,
                tbl_references: exec
                    .tbl_references
                    .clone()
                    .into_iter()
                    .map(|r| r.into())
                    .collect(),
                if_exists: exec.if_exists,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<SetVarExec>() {
            proto::ExecutionPlanExtensionType::SetVarExec(proto::SetVarExec {
                variable: exec.variable.clone(),
                values: exec.values.clone(),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<ShowVarExec>() {
            proto::ExecutionPlanExtensionType::ShowVarExec(proto::ShowVarExec {
                variable: exec.variable.clone(),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<UpdateExec>() {
            let mut updates = Vec::with_capacity(exec.updates.len());
            for (col, expr) in &exec.updates {
                updates.push(proto::UpdateSelector {
                    column: col.clone(),
                    expr: Some(expr.try_into()?),
                });
            }

            proto::ExecutionPlanExtensionType::UpdateExec(proto::UpdateExec {
                table: Some(exec.table.clone().try_into()?),
                updates,
                where_expr: exec
                    .where_expr
                    .as_ref()
                    .map(|expr| expr.try_into())
                    .transpose()?,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<InsertExec>() {
            proto::ExecutionPlanExtensionType::InsertExec(proto::InsertExec {
                table: Some(exec.table.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<DeleteExec>() {
            proto::ExecutionPlanExtensionType::DeleteExec(proto::DeleteExec {
                table: Some(exec.table.clone().try_into()?),
                where_expr: exec
                    .where_expr
                    .as_ref()
                    .map(|expr| expr.try_into())
                    .transpose()?,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CopyToExec>() {
            proto::ExecutionPlanExtensionType::CopyToExec(proto::CopyToExec {
                format: Some(exec.format.clone().try_into()?),
                dest: Some(exec.dest.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<ValuesExec>() {
            // ValuesExec only expects 1 partition.
            let schema = exec.schema();

            // HACK: Currently, we can't collect the data directly from
            // `ValuesExec`. There's an existing PR submitted in the DataFusion
            // repository to make it possible to construct, destruct (and
            // eventually serialize) a values execution plan:
            // https://github.com/apache/arrow-datafusion/pull/7444.
            //
            // For now we simply block on the runtime here and collect the data
            // from the stream.
            let stream = exec.execute(0, Arc::new(TaskContext::default()))?;
            let mut data = Vec::new();
            {
                let mut writer = IpcFileWriter::try_new(&mut data, schema.as_ref())?;
                for batch in futures::executor::block_on_stream(stream) {
                    let batch = batch?;
                    writer.write(&batch)?;
                }
                writer.finish()?;
            }

            proto::ExecutionPlanExtensionType::ValuesExec(proto::ValuesExec {
                schema: Some(schema.as_ref().try_into()?),
                data,
            })
        } else if let Some(_exec) = node.as_any().downcast_ref::<InterleaveExec>() {
            // TODO: Upstream to datafusion

            // Note that InterleaveExec only depends on physical plans which are
            // already encoded. We don't need to store anything extra on the
            // proto message.
            proto::ExecutionPlanExtensionType::InterleaveExec(proto::InterleaveExec {})
        } else if let Some(_exec) = node.as_any().downcast_ref::<RuntimeGroupExec>() {
            proto::ExecutionPlanExtensionType::RuntimeGroupExec(proto::RuntimeGroupExec {})
        } else if let Some(exec) = node.as_any().downcast_ref::<AnalyzeExec>() {
            // verbose is not a pub in datafusion, so we can either set it true or false
            // TODO: update this once verbose is set to pub in datafusion
            proto::ExecutionPlanExtensionType::AnalyzeExec(proto::AnalyzeExec {
                verbose: true,
                show_statistics: true,
                schema: Some(exec.schema().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<WindowAggExec>() {
            let input_schema: datafusion_proto::protobuf::Schema =
                exec.input_schema().as_ref().try_into()?;

            let window_expr = exec
                .window_expr()
                .iter()
                .map(|e| try_from_window_expr(e.clone()))
                .collect::<Result<Vec<protogen::sqlexec::physical_plan::GlarePhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<datafusion_proto::protobuf::PhysicalExprNode>>>()?;
            proto::ExecutionPlanExtensionType::WindowFunc(
                protogen::sqlexec::physical_plan::WindowAggExecNode {
                    // input: Some(Box::new(input)),
                    window_expr,
                    input_schema: Some(input_schema),
                    partition_keys,
                    partition_search_mode: None,
                },
            )
        } else if let Some(exec) = node.as_any().downcast_ref::<BoundedWindowAggExec>() {
            println!("{exec:#?}");

            let input_schema: datafusion_proto::protobuf::Schema =
                exec.input_schema().as_ref().try_into()?;

            let window_expr = exec
                .window_expr()
                .iter()
                .map(|e| try_from_window_expr(e.clone()))
                .collect::<Result<Vec<protogen::sqlexec::physical_plan::GlarePhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<datafusion_proto::protobuf::PhysicalExprNode>>>()?;

            let partition_search_mode = match &exec.partition_search_mode {
                PartitionSearchMode::Linear => window_agg_exec_node::PartitionSearchMode::Linear(
                    datafusion_proto::protobuf::EmptyMessage {},
                ),
                PartitionSearchMode::PartiallySorted(columns) => {
                    window_agg_exec_node::PartitionSearchMode::PartiallySorted(
                        protogen::sqlexec::physical_plan::PartiallySortedPartitionSearchMode {
                            columns: columns.iter().map(|c| *c as u64).collect(),
                        },
                    )
                }
                PartitionSearchMode::Sorted => window_agg_exec_node::PartitionSearchMode::Sorted(
                    datafusion_proto::protobuf::EmptyMessage {},
                ),
            };

            proto::ExecutionPlanExtensionType::WindowFunc(
                protogen::sqlexec::physical_plan::WindowAggExecNode {
                    // input: Some(Box::new(input)),
                    window_expr,
                    input_schema: Some(input_schema),
                    partition_keys,
                    partition_search_mode: Some(partition_search_mode),
                },
            )
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "encoding not implemented for physical plan: {}",
                displayable(node.as_ref()).indent(true),
            )));
        };

        let enc = proto::ExecutionPlanExtension { inner: Some(inner) };

        enc.encode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

fn parse_physical_window_expr(
    proto: &protogen::sqlexec::physical_plan::GlarePhysicalWindowExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr = proto
        .args
        .iter()
        .map(|e| parse_physical_expr(e, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let partition_by = proto
        .partition_by
        .iter()
        .map(|p| parse_physical_expr(p, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let order_by = proto
        .order_by
        .iter()
        .map(|o| parse_physical_sort_expr(o, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let window_frame = proto
        .window_frame
        .as_ref()
        .map(|wf| wf.clone().try_into())
        .transpose()
        .map_err(|e| DataFusionError::Internal(format!("{e}")))?
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Missing required field 'window_frame' in protobuf".to_string(),
            )
        })?;

    let window_function: WindowFunction = proto
        .window_function
        .as_ref()
        .ok_or_else(|| DataFusionError::Internal("missing window function".to_string()))?
        .try_into()?;

    create_window_expr(
        &window_function,
        proto.name.clone(),
        &window_node_expr,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        input_schema,
    )
}

fn try_from_window_expr(
    window_expr: Arc<dyn WindowExpr>,
) -> Result<protogen::sqlexec::physical_plan::GlarePhysicalWindowExprNode> {
    let expr = window_expr.as_any();

    let mut args = window_expr.expressions().to_vec();

    let window_function = if let Some(built_in_window_expr) =
        expr.downcast_ref::<BuiltInWindowExpr>()
    {
        let expr = built_in_window_expr.get_built_in_func_expr();
        let built_in_fn_expr = expr.as_any();

        let builtin_fn = if built_in_fn_expr.downcast_ref::<RowNumber>().is_some() {
            datafusion_proto::protobuf::BuiltInWindowFunction::RowNumber
        } else if let Some(rank_expr) = built_in_fn_expr.downcast_ref::<Rank>() {
            match rank_expr.get_type() {
                RankType::Basic => datafusion_proto::protobuf::BuiltInWindowFunction::Rank,
                RankType::Dense => datafusion_proto::protobuf::BuiltInWindowFunction::DenseRank,
                RankType::Percent => datafusion_proto::protobuf::BuiltInWindowFunction::PercentRank,
            }
        } else if built_in_fn_expr.downcast_ref::<CumeDist>().is_some() {
            datafusion_proto::protobuf::BuiltInWindowFunction::CumeDist
        } else if let Some(_ntile_expr) = built_in_fn_expr.downcast_ref::<Ntile>() {
            args.insert(
                0,
                Arc::new(Literal::new(datafusion::common::ScalarValue::Int64(Some(
                    0, // Can't get "n" without public fn
                )))),
            );
            datafusion_proto::protobuf::BuiltInWindowFunction::Ntile
        } else if let Some(window_shift_expr) = built_in_fn_expr.downcast_ref::<WindowShift>() {
            args.insert(
                1,
                Arc::new(Literal::new(datafusion::common::ScalarValue::Int64(Some(
                    window_shift_expr.get_shift_offset(),
                )))),
            );
            // if let Some(default_value) = window_shift_expr.get_default_value() {
            //     args.insert(2, Arc::new(Literal::new(default_value)));
            // }
            if window_shift_expr.get_shift_offset() >= 0 {
                datafusion_proto::protobuf::BuiltInWindowFunction::Lag
            } else {
                datafusion_proto::protobuf::BuiltInWindowFunction::Lead
            }
        } else if let Some(_nth_value_expr) = built_in_fn_expr.downcast_ref::<NthValue>() {
            // match nth_value_expr.get_kind() {
            //     NthValueKind::First => {
            //         datafusion_proto::protobuf::BuiltInWindowFunction::FirstValue
            //     }
            //     NthValueKind::Last => {
            //         datafusion_proto::protobuf::BuiltInWindowFunction::LastValue
            //     }
            //     NthValueKind::Nth(n) => {
            //         args.insert(
            //             1,
            //             Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
            //                 n as i64,
            //             )))),
            //         );
            //         datafusion_proto::protobuf::BuiltInWindowFunction::NthValue
            //     }
            // }
            args.insert(
                1,
                Arc::new(Literal::new(datafusion::common::ScalarValue::Int64(Some(
                    // Can't get "n" without the kind (which isn't public yet)
                    // Hardcoding for now...
                    1,
                )))),
            );
            datafusion_proto::protobuf::BuiltInWindowFunction::FirstValue
        } else {
            return Err(DataFusionError::Internal(
                "Not Implemented: BuiltIn function not supported: {expr:?}".to_string(),
            ));
        };

        physical_window_expr_node::WindowFunction::BuiltInFunction(builtin_fn as i32)
    } else if let Some(plain_aggr_window_expr) = expr.downcast_ref::<PlainAggregateWindowExpr>() {
        aggr_expr_to_window_fn(plain_aggr_window_expr.get_aggregate_expr().as_ref())?
    } else if let Some(sliding_aggr_window_expr) = expr.downcast_ref::<SlidingAggregateWindowExpr>()
    {
        aggr_expr_to_window_fn(sliding_aggr_window_expr.get_aggregate_expr().as_ref())?
    } else {
        return Err(DataFusionError::Internal(
            "Not Implemented: WindowExpr not supported: {window_expr:?}".to_string(),
        ));
    };

    let args = args
        .into_iter()
        .map(|e| e.try_into())
        .collect::<Result<Vec<datafusion_proto::protobuf::PhysicalExprNode>>>()?;

    let partition_by = window_expr
        .partition_by()
        .iter()
        .map(|p| p.clone().try_into())
        .collect::<Result<Vec<datafusion_proto::protobuf::PhysicalExprNode>>>()?;

    let order_by = window_expr
        .order_by()
        .iter()
        .map(|o| o.clone().try_into())
        .collect::<Result<Vec<datafusion_proto::protobuf::PhysicalSortExprNode>>>()?;

    let window_frame: datafusion_proto::protobuf::WindowFrame = window_expr
        .get_window_frame()
        .as_ref()
        .try_into()
        .map_err(|e| DataFusionError::Internal(format!("{e}")))?;

    let name = window_expr.name().to_string();

    Ok(
        protogen::sqlexec::physical_plan::GlarePhysicalWindowExprNode {
            args,
            partition_by,
            order_by,
            window_frame: Some(window_frame),
            window_function: Some(window_function),
            name,
        },
    )
}

fn aggr_expr_to_window_fn(
    expr: &dyn AggregateExpr,
) -> Result<physical_window_expr_node::WindowFunction> {
    let aggr_expr = expr.as_any();
    // TODO: `DistinctCount`, `DistinctSum`, `DistinctBitXor`,
    // `DistinctArrayAgg`, `OrderSensitiveArrayAgg` ?
    let func = if aggr_expr.downcast_ref::<Count>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Count
    } else if aggr_expr.downcast_ref::<Grouping>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Grouping
    } else if aggr_expr.downcast_ref::<BitAnd>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::BitAnd
    } else if aggr_expr.downcast_ref::<BitOr>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::BitOr
    } else if aggr_expr.downcast_ref::<BitXor>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::BitXor
    } else if aggr_expr.downcast_ref::<BoolAnd>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::BoolAnd
    } else if aggr_expr.downcast_ref::<BoolOr>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::BoolOr
    } else if aggr_expr.downcast_ref::<Sum>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Sum
    } else if aggr_expr.downcast_ref::<ApproxDistinct>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::ApproxDistinct
    } else if aggr_expr.downcast_ref::<ArrayAgg>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<Min>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Min
    } else if aggr_expr.downcast_ref::<Max>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Max
    } else if aggr_expr.downcast_ref::<Avg>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Avg
    } else if aggr_expr.downcast_ref::<Variance>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Variance
    } else if aggr_expr.downcast_ref::<VariancePop>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::VariancePop
    } else if aggr_expr.downcast_ref::<Covariance>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Covariance
    } else if aggr_expr.downcast_ref::<CovariancePop>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::CovariancePop
    } else if aggr_expr.downcast_ref::<Stddev>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Stddev
    } else if aggr_expr.downcast_ref::<StddevPop>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::StddevPop
    } else if aggr_expr.downcast_ref::<Correlation>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Correlation
    } else if let Some(_regr_expr) = aggr_expr.downcast_ref::<Regr>() {
        // Can't get regr_type without public RegrType
        // match regr_expr.get_regr_type() {
        //     RegrType::Slope => datafusion_proto::protobuf::AggregateFunction::RegrSlope,
        //     RegrType::Intercept => datafusion_proto::protobuf::AggregateFunction::RegrIntercept,
        //     RegrType::Count => datafusion_proto::protobuf::AggregateFunction::RegrCount,
        //     RegrType::R2 => datafusion_proto::protobuf::AggregateFunction::RegrR2,
        //     RegrType::AvgX => datafusion_proto::protobuf::AggregateFunction::RegrAvgx,
        //     RegrType::AvgY => datafusion_proto::protobuf::AggregateFunction::RegrAvgy,
        //     RegrType::SXX => datafusion_proto::protobuf::AggregateFunction::RegrSxx,
        //     RegrType::SYY => datafusion_proto::protobuf::AggregateFunction::RegrSyy,
        //     RegrType::SXY => datafusion_proto::protobuf::AggregateFunction::RegrSxy,
        // }
        datafusion_proto::protobuf::AggregateFunction::RegrSlope
    } else if aggr_expr.downcast_ref::<ApproxPercentileCont>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::ApproxPercentileCont
    } else if aggr_expr
        .downcast_ref::<ApproxPercentileContWithWeight>()
        .is_some()
    {
        datafusion_proto::protobuf::AggregateFunction::ApproxPercentileContWithWeight
    } else if aggr_expr.downcast_ref::<ApproxMedian>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::ApproxMedian
    } else if aggr_expr.downcast_ref::<Median>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::Median
    } else if aggr_expr.downcast_ref::<FirstValue>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::FirstValueAgg
    } else if aggr_expr.downcast_ref::<LastValue>().is_some() {
        datafusion_proto::protobuf::AggregateFunction::LastValueAgg
    } else {
        return Err(DataFusionError::Internal(
            "Not Implemented: Aggregate function not supported: {expr:?}".to_string(),
        ));
    };
    Ok(physical_window_expr_node::WindowFunction::AggrFunction(
        func as i32,
    ))
}
