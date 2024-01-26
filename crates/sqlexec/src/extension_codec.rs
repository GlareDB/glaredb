use core::fmt;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::union::InterleaveExec;
use datafusion::physical_plan::values::ValuesExec;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::Expr;
use datafusion_ext::metrics::{
    ReadOnlyDataSourceMetricsExecAdapter,
    WriteOnlyDataSourceMetricsExecAdapter,
};
use datafusion_ext::runtime::runtime_group::RuntimeGroupExec;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use protogen::metastore::types::catalog::RuntimePreference;
use uuid::Uuid;

use crate::planner::physical_plan::alter_database::AlterDatabaseExec;
use crate::planner::physical_plan::alter_table::AlterTableExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
use crate::planner::physical_plan::client_recv::ClientExchangeRecvExec;
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
use crate::planner::physical_plan::describe_table::DescribeTableExec;
use crate::planner::physical_plan::drop_credentials::DropCredentialsExec;
use crate::planner::physical_plan::drop_database::DropDatabaseExec;
use crate::planner::physical_plan::drop_schemas::DropSchemasExec;
use crate::planner::physical_plan::drop_tables::DropTablesExec;
use crate::planner::physical_plan::drop_tunnel::DropTunnelExec;
use crate::planner::physical_plan::drop_views::DropViewsExec;
use crate::planner::physical_plan::insert::InsertExec;
use crate::planner::physical_plan::remote_scan::{ProviderReference, RemoteScanExec};
use crate::planner::physical_plan::set_var::SetVarExec;
use crate::planner::physical_plan::show_var::ShowVarExec;
use crate::planner::physical_plan::update::UpdateExec;
use crate::planner::physical_plan::values::ExtValuesExec;
use crate::remote::provider_cache::ProviderCache;

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
                let work_id = Uuid::from_slice(&ext.work_id)
                    .map_err(|e| DataFusionError::Plan(format!("failed to decode work id: {e}")))?;
                let schema = ext
                    .schema
                    .ok_or(DataFusionError::Plan("schema is required".to_string()))?;
                let schema: Schema = (&schema).try_into()?;

                Arc::new(ClientExchangeRecvExec {
                    work_id,
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

                Arc::new(RemoteScanExec::new(
                    ProviderReference::Provider(prov),
                    Arc::new(projected_schema),
                    projection,
                    filters,
                    limit,
                ))
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
                let options = create_credentials
                    .options
                    .ok_or(DataFusionError::Plan("options is required".to_string()))?;
                Arc::new(CreateCredentialsExec {
                    name: create_credentials.name,
                    catalog_version: create_credentials.catalog_version,
                    options: options.try_into()?,
                    comment: create_credentials.comment,
                    or_replace: create_credentials.or_replace,
                })
            }
            proto::ExecutionPlanExtensionType::DescribeTable(describe_table) => {
                let entry = describe_table
                    .entry
                    .ok_or(DataFusionError::Plan("entry is required".to_string()))?;
                Arc::new(DescribeTableExec {
                    entry: entry.try_into()?,
                })
            }
            proto::ExecutionPlanExtensionType::AlterDatabaseExec(ext) => {
                Arc::new(AlterDatabaseExec {
                    catalog_version: ext.catalog_version,
                    name: ext.name,
                    operation: ext
                        .operation
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "missing alter database operation".to_string(),
                            )
                        })?
                        .try_into()?,
                })
            }
            proto::ExecutionPlanExtensionType::AlterTableExec(ext) => Arc::new(AlterTableExec {
                catalog_version: ext.catalog_version,
                schema: ext.schema,
                name: ext.name,
                operation: ext
                    .operation
                    .ok_or_else(|| {
                        DataFusionError::Internal("missing alter table operation".to_string())
                    })?
                    .try_into()?,
            }),
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
                    or_replace: ext.or_replace,
                    arrow_schema: Arc::new(schema),
                    source: inputs.first().cloned(),
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
                    or_replace: ext.or_replace,
                    arrow_schema: Arc::new(schema),
                    source: inputs.first().cloned(),
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
                    or_replace: ext.or_replace,
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
                tbl_entries: ext
                    .tbl_entries
                    .into_iter()
                    .map(|r| r.try_into())
                    .collect::<Result<_, _>>()
                    .expect("failed to decode table entries"),
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
            proto::ExecutionPlanExtensionType::InsertExec(ext) => {
                let provider_id = Uuid::from_slice(&ext.provider_id).map_err(|e| {
                    DataFusionError::Plan(format!("failed to decode provider id: {e}"))
                })?;

                // We're on the remote side, get the real table provider from
                // the cache.
                let prov = self
                    .table_providers
                    .expect("remote context should have provider cache")
                    .get(&provider_id)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("Missing proivder for id: {provider_id}"))
                    })?;

                Arc::new(InsertExec {
                    provider: ProviderReference::Provider(prov),
                    source: Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(
                        inputs
                            .first()
                            .ok_or_else(|| {
                                DataFusionError::Internal("missing input source".to_string())
                            })?
                            .clone(),
                    )),
                })
            }
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
                source: Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(
                    inputs
                        .first()
                        .ok_or_else(|| {
                            DataFusionError::Internal("missing input source".to_string())
                        })?
                        .clone(),
                )),
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
                        .first()
                        .ok_or_else(|| DataFusionError::Internal("missing child".to_string()))?
                        .clone(),
                ))
            }
            proto::ExecutionPlanExtensionType::AnalyzeExec(ext) => {
                let input = inputs
                    .first()
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
            proto::ExecutionPlanExtensionType::DataSourceMetricsExecAdapter(ext) => {
                let source = inputs
                    .first()
                    .ok_or_else(|| DataFusionError::Internal("missing child".to_string()))?
                    .clone();

                if ext.track_writes {
                    Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(source))
                } else {
                    Arc::new(ReadOnlyDataSourceMetricsExecAdapter::new(source))
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
                    work_id: exec.work_id.into_bytes().to_vec(),
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
            proto::ExecutionPlanExtensionType::CreateCredentialsExec(proto::CreateCredentialsExec {
                name: exec.name.clone(),
                catalog_version: exec.catalog_version,
                options: Some(exec.options.clone().into()),
                comment: exec.comment.clone(),
                or_replace: exec.or_replace,
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateTableExec>() {
            proto::ExecutionPlanExtensionType::CreateTableExec(proto::CreateTableExec {
                catalog_version: exec.catalog_version,
                tbl_reference: Some(exec.tbl_reference.clone().into()),
                if_not_exists: exec.if_not_exists,
                or_replace: exec.or_replace,
                arrow_schema: Some(exec.arrow_schema.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<CreateTempTableExec>() {
            proto::ExecutionPlanExtensionType::CreateTempTableExec(proto::CreateTempTableExec {
                tbl_reference: Some(exec.tbl_reference.clone().into()),
                if_not_exists: exec.if_not_exists,
                or_replace: exec.or_replace,
                arrow_schema: Some(exec.arrow_schema.clone().try_into()?),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<AlterDatabaseExec>() {
            proto::ExecutionPlanExtensionType::AlterDatabaseExec(proto::AlterDatabaseExec {
                catalog_version: exec.catalog_version,
                name: exec.name.clone(),
                operation: Some(exec.operation.clone().into()),
            })
        } else if let Some(exec) = node.as_any().downcast_ref::<AlterTableExec>() {
            proto::ExecutionPlanExtensionType::AlterTableExec(proto::AlterTableExec {
                catalog_version: exec.catalog_version,
                schema: exec.schema.to_owned(),
                name: exec.name.to_owned(),
                operation: Some(exec.operation.clone().into()),
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
                    or_replace: exec.or_replace,
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
        } else if let Some(exec) = node.as_any().downcast_ref::<DescribeTableExec>() {
            proto::ExecutionPlanExtensionType::DescribeTable(proto::DescribeTableExec {
                entry: Some(exec.entry.clone().try_into()?),
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
                tbl_entries: exec
                    .tbl_entries
                    .clone()
                    .into_iter()
                    .map(|r| r.try_into())
                    .collect::<Result<_, _>>()
                    .expect("failed to encode table entries"),
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
            let id = match exec.provider {
                ProviderReference::RemoteReference(id) => id,
                ProviderReference::Provider(_) => {
                    return Err(DataFusionError::Internal(
                        "Unexpectedly got table provider on client side".to_string(),
                    ))
                }
            };

            proto::ExecutionPlanExtensionType::InsertExec(proto::InsertExec {
                provider_id: id.into_bytes().to_vec(),
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
        } else if let Some(_exec) = node
            .as_any()
            .downcast_ref::<ReadOnlyDataSourceMetricsExecAdapter>()
        {
            proto::ExecutionPlanExtensionType::DataSourceMetricsExecAdapter(
                proto::DataSourceMetricsExecAdapter {
                    track_writes: false,
                },
            )
        } else if let Some(_exec) = node
            .as_any()
            .downcast_ref::<WriteOnlyDataSourceMetricsExecAdapter>()
        {
            proto::ExecutionPlanExtensionType::DataSourceMetricsExecAdapter(
                proto::DataSourceMetricsExecAdapter { track_writes: true },
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
