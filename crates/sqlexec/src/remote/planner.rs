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

use crate::errors::internal;
use crate::metastore::catalog::SessionCatalog;
use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{
    AlterDatabaseRename, AlterTableRename, AlterTunnelRotateKeys, CreateCredentials, DropDatabase,
    DropSchemas, DropTunnel, DropViews,
};
use crate::planner::physical_plan::alter_database_rename::AlterDatabaseRenameExec;
use crate::planner::physical_plan::alter_table_rename::AlterTableRenameExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
use crate::planner::physical_plan::create_credentials_exec::CreateCredentialsExec;
use crate::planner::physical_plan::drop_database::DropDatabaseExec;
use crate::planner::physical_plan::drop_schemas::DropSchemasExec;
use crate::planner::physical_plan::drop_tunnel::DropTunnelExec;
use crate::planner::physical_plan::drop_views::DropViewsExec;
use crate::planner::physical_plan::remote_exec::RemoteExecutionExec;
use crate::planner::physical_plan::send_recv::SendRecvJoinExec;

use super::client::RemoteSessionClient;
use super::rewriter::LocalSideTableRewriter;

pub struct RemotePhysicalPlanner<'a> {
    pub remote_client: RemoteSessionClient,
    pub catalog: &'a SessionCatalog,
}

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
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let extension_type = node.name().parse::<ExtensionType>().unwrap();

        match extension_type {
            ExtensionType::AlterDatabaseRename => {
                let alter_database_rename_lp =
                    match node.as_any().downcast_ref::<AlterDatabaseRename>() {
                        Some(s) => Ok(s.clone()),
                        None => Err(internal!(
                            "AlterDatabaseRename::try_decode_extension failed",
                        )),
                    }
                    .unwrap();

                let exec: AlterDatabaseRenameExec = AlterDatabaseRenameExec {
                    catalog_version: self.catalog_version,
                    name: alter_database_rename_lp.name,
                    new_name: alter_database_rename_lp.new_name,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::AlterTableRename => {
                let alter_table_rename_lp =
                    match node.as_any().downcast_ref::<AlterTableRename>() {
                        Some(s) => Ok(s.clone()),
                        None => Err(internal!("AlterTableRename::try_decode_extension failed",)),
                    }
                    .unwrap();

                let exec: AlterTableRenameExec = AlterTableRenameExec {
                    catalog_version: self.catalog_version,
                    name: alter_table_rename_lp.name.table().to_string(),
                    new_name: alter_table_rename_lp.new_name.table().to_string(),
                    schema: alter_table_rename_lp.schema().to_string(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::AlterTunnelRotateKeys => {
                let alter_tunnel_rotate_keys_lp =
                    match node.as_any().downcast_ref::<AlterTunnelRotateKeys>() {
                        Some(s) => Ok(s.clone()),
                        None => Err(internal!(
                            "AlterTunnelRotateKeys::try_decode_extension failed",
                        )),
                    }
                    .unwrap();

                let exec: AlterTunnelRotateKeysExec = AlterTunnelRotateKeysExec {
                    catalog_version: self.catalog_version,
                    name: alter_tunnel_rotate_keys_lp.name,
                    if_exists: alter_tunnel_rotate_keys_lp.if_exists,
                    new_ssh_key: alter_tunnel_rotate_keys_lp.new_ssh_key,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CreateCredentials => {
                let create_credentials_lp = match node.as_any().downcast_ref::<CreateCredentials>()
                {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("CreateCredentials::try_decode_extension failed",)),
                }
                .unwrap();

                let exec = CreateCredentialsExec {
                    catalog_version: self.catalog_version,
                    name: create_credentials_lp.name,
                    options: create_credentials_lp.options,
                    comment: create_credentials_lp.comment,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CreateExternalDatabase => todo!(),
            ExtensionType::CreateExternalTable => todo!(),
            ExtensionType::CreateSchema => todo!(),
            ExtensionType::CreateTable => todo!(),
            ExtensionType::CreateTempTable => todo!(),
            ExtensionType::CreateTunnel => todo!(),
            ExtensionType::CreateView => todo!(),
            ExtensionType::DropTables => todo!(),
            ExtensionType::DropCredentials => todo!(),
            ExtensionType::DropDatabase => {
                let drop_database_lp = match node.as_any().downcast_ref::<DropDatabase>() {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("DropDatabase::try_decode_extension failed",)),
                }
                .unwrap();

                let exec: DropDatabaseExec = DropDatabaseExec {
                    catalog_version: self.catalog_version,
                    names: drop_database_lp.names,
                    if_exists: drop_database_lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropSchemas => {
                let drop_schemas_lp = match node.as_any().downcast_ref::<DropSchemas>() {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("DropSchemas::try_decode_extension failed",)),
                }
                .unwrap();

                let exec: DropSchemasExec = DropSchemasExec {
                    catalog_version: self.catalog_version,
                    names: drop_schemas_lp
                        .names
                        .into_iter()
                        .map(|n| n.schema_name().to_string())
                        .collect(),
                    if_exists: drop_schemas_lp.if_exists,
                    cascade: drop_schemas_lp.cascade,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropTunnel => {
                let drop_tunnel_lp = match node.as_any().downcast_ref::<DropTunnel>() {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("DropTunnel::try_decode_extension failed",)),
                }
                .unwrap();

                let exec: DropTunnelExec = DropTunnelExec {
                    catalog_version: self.catalog_version,
                    names: drop_tunnel_lp.names,
                    if_exists: drop_tunnel_lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::DropViews => {
                let drop_views_lp = match node.as_any().downcast_ref::<DropViews>() {
                    Some(s) => Ok(s.clone()),
                    None => Err(internal!("DropViews::try_decode_extension failed",)),
                }
                .unwrap();

                let exec: DropViewsExec = DropViewsExec {
                    catalog_version: self.catalog_version,
                    names: drop_views_lp
                        .names
                        .clone()
                        .into_iter()
                        .map(|n| n.table().to_string())
                        .collect(),
                    if_exists: drop_views_lp.if_exists,
                    schema: drop_views_lp
                        .names
                        .into_iter()
                        .map(|n| n.schema().unwrap_or_default().to_string())
                        .collect(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::SetVariable => todo!(),
            ExtensionType::CopyTo => todo!(),
        }
    }
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
