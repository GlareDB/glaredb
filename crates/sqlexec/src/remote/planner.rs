use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;

use std::sync::Arc;

use crate::metastore::catalog::SessionCatalog;
use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{
    AlterDatabaseRename, AlterTableRename, AlterTunnelRotateKeys, CreateCredentials,
    CreateExternalDatabase, CreateExternalTable, CreateSchema, CreateTable, CreateTempTable,
    CreateTunnel, CreateView, Delete, DropCredentials, DropDatabase, DropSchemas, DropTables,
    DropTunnel, DropViews, Insert, Update,
};
use crate::planner::physical_plan::alter_database_rename::AlterDatabaseRenameExec;
use crate::planner::physical_plan::alter_table_rename::AlterTableRenameExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
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
use crate::planner::physical_plan::remote_exec::RemoteExecutionExec;
use crate::planner::physical_plan::send_recv::SendRecvJoinExec;
use crate::planner::physical_plan::update::UpdateExec;

use super::client::RemoteSessionClient;
use super::rewriter::LocalSideTableRewriter;

pub struct DDLExtensionPlanner {
    catalog_version: u64,
}
impl DDLExtensionPlanner {
    pub fn new(catalog_version: u64) -> Self {
        Self { catalog_version }
    }
}
#[async_trait]
impl ExtensionPlanner for DDLExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&DfLogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let extension_type = node.name().parse::<ExtensionType>().unwrap();

        match extension_type {
            ExtensionType::AlterDatabaseRename => {
                let lp = require_downcast_lp::<AlterDatabaseRename>(node);
                let exec = AlterDatabaseRenameExec {
                    catalog_version: self.catalog_version,
                    name: lp.name.to_string(),
                    new_name: lp.new_name.to_string(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::AlterTableRename => {
                let lp = require_downcast_lp::<AlterTableRename>(node);
                let exec = AlterTableRenameExec {
                    catalog_version: self.catalog_version,
                    reference: lp.reference.clone(),
                    new_reference: lp.reference.clone(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::AlterTunnelRotateKeys => {
                let lp = require_downcast_lp::<AlterTunnelRotateKeys>(node);
                let exec = AlterTunnelRotateKeysExec {
                    catalog_version: self.catalog_version,
                    name: lp.name.to_string(),
                    if_exists: lp.if_exists,
                    new_ssh_key: lp.new_ssh_key.clone(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CreateCredentials => {
                let lp = require_downcast_lp::<CreateCredentials>(node);
                let exec = CreateCredentialsExec {
                    catalog_version: self.catalog_version,
                    name: lp.name.clone(),
                    options: lp.options.clone(),
                    comment: lp.comment.clone(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CreateExternalDatabase => {
                let lp = require_downcast_lp::<CreateExternalDatabase>(node);
                Ok(Some(Arc::new(CreateExternalDatabaseExec {
                    catalog_version: self.catalog_version,
                    database_name: lp.database_name.clone(),
                    if_not_exists: lp.if_not_exists,
                    options: lp.options.clone(),
                    tunnel: lp.tunnel.clone(),
                })))
            }
            ExtensionType::CreateExternalTable => {
                let lp = require_downcast_lp::<CreateExternalTable>(node);
                Ok(Some(Arc::new(CreateExternalTableExec {
                    catalog_version: self.catalog_version,
                    reference: lp.reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    tunnel: lp.tunnel.clone(),
                    table_options: lp.table_options.clone(),
                })))
            }
            ExtensionType::CreateSchema => {
                let lp = require_downcast_lp::<CreateSchema>(node);
                Ok(Some(Arc::new(CreateSchemaExec {
                    catalog_version: self.catalog_version,
                    reference: lp.reference.clone(),
                    if_not_exists: lp.if_not_exists,
                })))
            }
            ExtensionType::CreateTable => {
                let lp = require_downcast_lp::<CreateTable>(node);
                Ok(Some(Arc::new(CreateTableExec {
                    catalog_version: self.catalog_version,
                    reference: lp.reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    arrow_schema: Arc::new(lp.schema.as_ref().into()),
                    source: physical_inputs.get(0).cloned(),
                })))
            }
            ExtensionType::CreateTempTable => {
                let lp = require_downcast_lp::<CreateTempTable>(node);
                Ok(Some(Arc::new(CreateTempTableExec {
                    reference: lp.reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    arrow_schema: Arc::new(lp.schema.as_ref().into()),
                    source: physical_inputs.get(0).cloned(),
                })))
            }
            ExtensionType::CreateTunnel => {
                let lp = require_downcast_lp::<CreateTunnel>(node);
                Ok(Some(Arc::new(CreateTunnelExec {
                    catalog_version: self.catalog_version,
                    name: lp.name.clone(),
                    if_not_exists: lp.if_not_exists,
                    options: lp.options.clone(),
                })))
            }
            ExtensionType::CreateView => {
                let lp = require_downcast_lp::<CreateView>(node);
                Ok(Some(Arc::new(CreateViewExec {
                    catalog_version: self.catalog_version,
                    reference: lp.reference.clone(),
                    sql: lp.sql.clone(),
                    columns: lp.columns.clone(),
                    or_replace: lp.or_replace,
                })))
            }
            ExtensionType::DropTables => {
                let lp = require_downcast_lp::<DropTables>(node);
                Ok(Some(Arc::new(DropTablesExec {
                    catalog_version: self.catalog_version,
                    references: lp.references.clone(),
                    if_exists: lp.if_exists,
                })))
            }
            ExtensionType::DropCredentials => {
                let lp = require_downcast_lp::<DropCredentials>(node);
                Ok(Some(Arc::new(DropCredentialsExec {
                    catalog_version: self.catalog_version,
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                })))
            }
            ExtensionType::DropDatabase => {
                let lp = require_downcast_lp::<DropDatabase>(node);
                let exec = DropDatabaseExec {
                    catalog_version: self.catalog_version,
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropSchemas => {
                let lp = require_downcast_lp::<DropSchemas>(node);
                // TODO: Error if catalog provided in names.
                let exec = DropSchemasExec {
                    catalog_version: self.catalog_version,
                    references: lp.references.clone(),
                    if_exists: lp.if_exists,
                    cascade: lp.cascade,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropTunnel => {
                let lp = require_downcast_lp::<DropTunnel>(node);
                let exec: DropTunnelExec = DropTunnelExec {
                    catalog_version: self.catalog_version,
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropViews => {
                let lp = require_downcast_lp::<DropViews>(node);
                // TODO: Fix this.
                let exec = DropViewsExec {
                    catalog_version: self.catalog_version,
                    references: lp.references.clone(),
                    if_exists: lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::SetVariable => todo!(),
            ExtensionType::CopyTo => todo!(),
            ExtensionType::Update => {
                let lp = require_downcast_lp::<Update>(node);
                Ok(Some(Arc::new(UpdateExec {
                    table: lp.table.clone(),
                    updates: lp.updates.clone(),
                    where_expr: lp.where_expr.clone(),
                })))
            }
            ExtensionType::Insert => {
                let lp = require_downcast_lp::<Insert>(node);
                Ok(Some(Arc::new(InsertExec {
                    table: lp.table.clone(),
                    source: physical_inputs.get(0).unwrap().clone(),
                })))
            }
            ExtensionType::Delete => {
                let lp = require_downcast_lp::<Delete>(node);
                Ok(Some(Arc::new(DeleteExec {
                    table: lp.table.clone(),
                    where_expr: lp.where_expr.clone(),
                })))
            }
        }
    }
}

pub struct RemotePhysicalPlanner<'a> {
    pub remote_client: RemoteSessionClient,
    pub catalog: &'a SessionCatalog,
}

#[async_trait]
impl<'a> PhysicalPlanner for RemotePhysicalPlanner<'a> {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // TODO: This can be refactored a bit to better handle explains.

        // Rewrite logical plan to ensure tables that have a
        // "local" hint have their providers replaced with ones
        // that will produce client recv and send execs.
        let mut rewriter = LocalSideTableRewriter::new(self.remote_client.clone());
        // TODO: Figure out where this should actually be called to remove need
        // to clone.
        //
        // Datafusion steps:
        // 1. Analyze logical
        // 2. Optimize logical
        // 3. Plan physical
        // 4. Optimize physical
        //
        // Ideally this rewrite happens between 2 and 3. We could also add an
        // optimzer rule to do this, but that kinda goes against what
        // optimzation is for. We would also need to figure out how to pass
        // around the exec refs, so probably not worth it.
        let logical_plan = logical_plan.clone();
        let logical_plan = logical_plan.rewrite(&mut rewriter)?;

        // Create the physical plans. This will call `scan` on
        // the custom table providers meaning we'll have the
        // correct exec refs.
        let catalog_version = self.catalog.version();
        let physical = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            DDLExtensionPlanner::new(catalog_version),
        )])
        .create_physical_plan(&logical_plan, session_state)
        .await?;

        // Temporary coalesce exec until our custom plans support partition.
        let physical = Arc::new(CoalescePartitionsExec::new(physical));

        // Wrap in exec that will send the plan to the remote machine.
        let physical = Arc::new(RemoteExecutionExec::new(
            self.remote_client.clone(),
            physical,
        ));

        // Create a wrapper physical plan which drives both the
        // result stream, and the send execs
        //
        // At this point, the send execs should have been
        // populated.
        let physical = Arc::new(SendRecvJoinExec::new(physical, rewriter.exec_refs));

        Ok(physical)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        DefaultPhysicalPlanner::default().create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )
    }
}

fn require_downcast_lp<P: 'static>(plan: &dyn UserDefinedLogicalNode) -> &P {
    match plan.as_any().downcast_ref::<P>() {
        Some(p) => p,
        None => panic!("Invalid downcast reference for plan: {}", plan.name()),
    }
}
