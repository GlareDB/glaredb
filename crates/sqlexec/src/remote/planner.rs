use std::sync::Arc;

use async_trait::async_trait;
use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::Transformed;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan as DfLogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use datafusion_ext::metrics::WriteOnlyDataSourceMetricsExecAdapter;
use datafusion_ext::runtime::runtime_group::RuntimeGroupExec;
use datafusion_ext::transform::TreeNodeExt;
use protogen::metastore::types::catalog::RuntimePreference;
use protogen::metastore::types::options::CopyToDestinationOptions;
use tracing::debug;
use uuid::Uuid;

use super::client::RemoteSessionClient;
use crate::planner::extension::ExtensionType;
use crate::planner::logical_plan::{
    AlterDatabase,
    AlterTable,
    AlterTunnelRotateKeys,
    CopyTo,
    CreateCredential,
    CreateCredentials,
    CreateExternalDatabase,
    CreateExternalTable,
    CreateSchema,
    CreateTable,
    CreateTempTable,
    CreateTunnel,
    CreateView,
    Delete,
    DescribeTable,
    DropCredentials,
    DropDatabase,
    DropSchemas,
    DropTables,
    DropTunnel,
    DropViews,
    Insert,
    SetVariable,
    ShowVariable,
    Update,
};
use crate::planner::physical_plan::alter_database::AlterDatabaseExec;
use crate::planner::physical_plan::alter_table::AlterTableExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
use crate::planner::physical_plan::client_recv::ClientExchangeRecvExec;
use crate::planner::physical_plan::client_send::ClientExchangeSendExec;
use crate::planner::physical_plan::copy_to::CopyToExec;
use crate::planner::physical_plan::create_credential::CreateCredentialExec;
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
use crate::planner::physical_plan::drop_temp_tables::DropTempTablesExec;
use crate::planner::physical_plan::drop_tunnel::DropTunnelExec;
use crate::planner::physical_plan::drop_views::DropViewsExec;
use crate::planner::physical_plan::insert::InsertExec;
use crate::planner::physical_plan::remote_exec::RemoteExecutionExec;
use crate::planner::physical_plan::remote_scan::ProviderReference;
use crate::planner::physical_plan::send_recv::SendRecvJoinExec;
use crate::planner::physical_plan::set_var::SetVarExec;
use crate::planner::physical_plan::show_var::ShowVarExec;
use crate::planner::physical_plan::update::UpdateExec;

pub struct DDLExtensionPlanner {
    catalog: SessionCatalog,
}
impl DDLExtensionPlanner {
    pub fn new(catalog: SessionCatalog) -> Self {
        Self { catalog }
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
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let extension_type = node.name().parse::<ExtensionType>().unwrap();

        let runtime_group_exec = match extension_type {
            ExtensionType::AlterDatabase => {
                let lp = require_downcast_lp::<AlterDatabase>(node);
                let exec = AlterDatabaseExec {
                    catalog_version: self.catalog.version(),
                    name: lp.name.to_string(),
                    operation: lp.operation.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::AlterTable => {
                let lp = require_downcast_lp::<AlterTable>(node);
                let exec = AlterTableExec {
                    catalog_version: self.catalog.version(),
                    schema: lp.schema.to_owned(),
                    name: lp.name.to_owned(),
                    operation: lp.operation.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::AlterTunnelRotateKeys => {
                let lp = require_downcast_lp::<AlterTunnelRotateKeys>(node);
                let exec = AlterTunnelRotateKeysExec {
                    catalog_version: self.catalog.version(),
                    name: lp.name.to_string(),
                    if_exists: lp.if_exists,
                    new_ssh_key: lp.new_ssh_key.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateCredential => {
                let lp = require_downcast_lp::<CreateCredential>(node);
                let exec = CreateCredentialExec {
                    catalog_version: self.catalog.version(),
                    name: lp.name.clone(),
                    options: lp.options.clone(),
                    comment: lp.comment.clone(),
                    or_replace: lp.or_replace,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateCredentials => {
                let lp = require_downcast_lp::<CreateCredentials>(node);
                let exec = CreateCredentialsExec {
                    catalog_version: self.catalog.version(),
                    name: lp.name.clone(),
                    options: lp.options.clone(),
                    comment: lp.comment.clone(),
                    or_replace: lp.or_replace,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateExternalDatabase => {
                let lp = require_downcast_lp::<CreateExternalDatabase>(node);
                let exec = CreateExternalDatabaseExec {
                    catalog_version: self.catalog.version(),
                    database_name: lp.database_name.clone(),
                    if_not_exists: lp.if_not_exists,
                    options: lp.options.clone(),
                    tunnel: lp.tunnel.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateExternalTable => {
                let lp = require_downcast_lp::<CreateExternalTable>(node);
                let exec = CreateExternalTableExec {
                    catalog_version: self.catalog.version(),
                    tbl_reference: lp.tbl_reference.clone(),
                    or_replace: lp.or_replace,
                    if_not_exists: lp.if_not_exists,
                    tunnel: lp.tunnel.clone(),
                    table_options: lp.table_options.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateSchema => {
                let lp = require_downcast_lp::<CreateSchema>(node);
                let exec = CreateSchemaExec {
                    catalog_version: self.catalog.version(),
                    schema_reference: lp.schema_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateTable => {
                let lp = require_downcast_lp::<CreateTable>(node);
                let exec = CreateTableExec {
                    catalog_version: self.catalog.version(),
                    tbl_reference: lp.tbl_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    or_replace: lp.or_replace,
                    arrow_schema: Arc::new(lp.schema.as_ref().into()),
                    source: physical_inputs.first().cloned(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateTempTable => {
                let lp = require_downcast_lp::<CreateTempTable>(node);
                let exec = CreateTempTableExec {
                    tbl_reference: lp.tbl_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    or_replace: lp.or_replace,
                    arrow_schema: Arc::new(lp.schema.as_ref().into()),
                    source: physical_inputs.first().cloned(),
                };
                RuntimeGroupExec::new(RuntimePreference::Local, Arc::new(exec))
            }
            ExtensionType::CreateTunnel => {
                let lp = require_downcast_lp::<CreateTunnel>(node);
                let exec = CreateTunnelExec {
                    catalog_version: self.catalog.version(),
                    name: lp.name.clone(),
                    if_not_exists: lp.if_not_exists,
                    options: lp.options.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::CreateView => {
                let lp = require_downcast_lp::<CreateView>(node);
                let exec = CreateViewExec {
                    catalog_version: self.catalog.version(),
                    view_reference: lp.view_reference.clone(),
                    sql: lp.sql.clone(),
                    columns: lp.columns.clone(),
                    or_replace: lp.or_replace,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::DescribeTable => {
                let DescribeTable { entry } = require_downcast_lp::<DescribeTable>(node);
                let runtime = if entry.meta.is_temp || entry.meta.builtin {
                    RuntimePreference::Local
                } else {
                    RuntimePreference::Remote
                };
                let exec = DescribeTableExec {
                    entry: entry.clone(),
                };
                RuntimeGroupExec::new(runtime, Arc::new(exec))
            }
            ExtensionType::DropTables => {
                let plan = require_downcast_lp::<DropTables>(node);
                let mut drops = Vec::with_capacity(plan.tbl_references.len());
                let mut temp_table_drops = Vec::with_capacity(plan.tbl_references.len());

                for r in &plan.tbl_references {
                    if self.catalog.get_temp_catalog().contains_table(&r.name) {
                        temp_table_drops.push(r.clone());
                    } else if self
                        .catalog
                        .resolve_table(&r.database, &r.schema, &r.name)
                        .is_some()
                        || plan.if_exists
                    {
                        drops.push(r.clone());
                    } else {
                        return Err(DataFusionError::Plan(format!(
                            "Table '{}' does not exist",
                            r.name
                        )));
                    }
                }
                match (temp_table_drops.is_empty(), drops.is_empty()) {
                    // both temp and remote tables
                    (false, false) => {
                        return Err(DataFusionError::Plan("Unable to drop temp and native tables in the same statement. Please use separate statements.".to_string()))?;
                    }
                    // only temp tables
                    (false, true) => {
                        let tmp_exec = Arc::new(DropTempTablesExec {
                            catalog_version: self.catalog.version(),
                            tbl_references: temp_table_drops,
                            if_exists: plan.if_exists,
                        });
                        RuntimeGroupExec::new(RuntimePreference::Local, tmp_exec)
                    }
                    // only remote tables
                    (true, false) => {
                        let exec = Arc::new(DropTablesExec {
                            catalog_version: self.catalog.version(),
                            tbl_references: drops,
                            if_exists: plan.if_exists,
                        });
                        RuntimeGroupExec::new(RuntimePreference::Remote, exec)
                    }
                    // no tables
                    (true, true) => {
                        return Err(DataFusionError::Plan("No tables to drop".to_string()))?
                    }
                }
            }
            ExtensionType::DropCredentials => {
                let lp = require_downcast_lp::<DropCredentials>(node);
                let exec = DropCredentialsExec {
                    catalog_version: self.catalog.version(),
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::DropDatabase => {
                let lp = require_downcast_lp::<DropDatabase>(node);
                let exec = DropDatabaseExec {
                    catalog_version: self.catalog.version(),
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::DropSchemas => {
                let lp = require_downcast_lp::<DropSchemas>(node);
                let exec = DropSchemasExec {
                    catalog_version: self.catalog.version(),
                    schema_references: lp.schema_references.clone(),
                    if_exists: lp.if_exists,
                    cascade: lp.cascade,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::DropTunnel => {
                let lp = require_downcast_lp::<DropTunnel>(node);
                let exec = DropTunnelExec {
                    catalog_version: self.catalog.version(),
                    names: lp.names.clone(),
                    if_exists: lp.if_exists,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::DropViews => {
                let lp = require_downcast_lp::<DropViews>(node);
                // TODO: Fix this.
                let exec = DropViewsExec {
                    catalog_version: self.catalog.version(),
                    view_references: lp.view_references.clone(),
                    if_exists: lp.if_exists,
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::SetVariable => {
                let lp = require_downcast_lp::<SetVariable>(node);
                let exec = SetVarExec {
                    variable: lp.variable.clone(),
                    values: lp.values.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Local, Arc::new(exec))
            }
            ExtensionType::ShowVariable => {
                let lp = require_downcast_lp::<ShowVariable>(node);
                let exec = ShowVarExec {
                    variable: lp.variable.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Local, Arc::new(exec))
            }
            ExtensionType::CopyTo => {
                let lp = require_downcast_lp::<CopyTo>(node);
                let runtime = match lp.dest {
                    CopyToDestinationOptions::Local(_) => RuntimePreference::Local,
                    _ => RuntimePreference::Remote,
                };
                let exec = Arc::new(CopyToExec {
                    format: lp.format.clone(),
                    dest: lp.dest.clone(),
                    source: Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(
                        physical_inputs.first().unwrap().clone(),
                    )),
                });
                RuntimeGroupExec::new(runtime, exec)
            }
            ExtensionType::Update => {
                let lp = require_downcast_lp::<Update>(node);
                let exec = UpdateExec {
                    table: lp.table.clone(),
                    updates: lp.updates.clone(),
                    where_expr: lp.where_expr.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
            ExtensionType::Insert => {
                let lp = require_downcast_lp::<Insert>(node);
                let provider = match &lp.provider {
                    ProviderReference::RemoteReference(_)
                        if lp.runtime_preference == RuntimePreference::Local =>
                    {
                        unreachable!("required local table, found remote reference to table")
                    }
                    other => other.clone(),
                };
                let exec = Arc::new(InsertExec {
                    provider,
                    source: Arc::new(WriteOnlyDataSourceMetricsExecAdapter::new(
                        physical_inputs.first().unwrap().clone(),
                    )),
                });
                RuntimeGroupExec::new(lp.runtime_preference, exec)
            }
            ExtensionType::Delete => {
                let lp = require_downcast_lp::<Delete>(node);
                let exec = DeleteExec {
                    table: lp.table.clone(),
                    where_expr: lp.where_expr.clone(),
                };
                RuntimeGroupExec::new(RuntimePreference::Remote, Arc::new(exec))
            }
        };

        Ok(Some(Arc::new(runtime_group_exec)))
    }
}

pub struct RemotePhysicalPlanner<'a> {
    pub database_id: Uuid,
    pub query_text: &'a str,
    pub remote_client: RemoteSessionClient,
    pub catalog: &'a SessionCatalog,
}

impl<'a> RemotePhysicalPlanner<'a> {
    /// Push down "remote" exec as far down as possible and replace any "local"
    /// execs that are found (children to the remote). Finally, pack the
    /// remote exec in a [`SendRecvJoinExec`] so the plan transformation looks
    /// something like:
    ///
    /// ### Original plan:
    ///
    /// ```txt
    ///                  +-----------+
    ///                  | A (Local) |
    ///                  +-----+-----+
    ///                        |
    ///                        v
    ///               +-----------------+
    ///       +-------+ B (Unspecified) +-------+
    ///       |       +--------+--------+       |
    ///       v                |                v
    /// +------------+         |          +-----------+
    /// | C (Remote) |         |          | F (Local) |
    /// +-----+------+         v          +-----------+
    ///       |          +------------+
    ///       v          | E (Remote) |
    /// +-----------+    +------------+
    /// | D (Local) |
    /// +-----------+
    /// ```
    ///
    /// ### Transformed plan:
    ///
    /// ```txt
    ///                           +-----------+
    ///                           | A (Local) |
    ///                           +-----+-----+
    ///                                 |
    ///                                 v
    ///                        +-----------------+
    ///          +-------------+ B (Unspecified) +-------+
    ///          |             +--------+--------+       |
    ///          |                      |                v
    ///          v                      |          +-----------+
    /// +------------------+            |          | F (Local) |
    /// | SendRecvJoinExec |            v          +-----------+
    /// |                  |   +------------------+
    /// |        C         |   | SendRecvJoinExec |
    /// |        |         |   |                  |
    /// |        v         |   |        E         |
    /// |   D (Send-Recv)  |   +------------------+
    /// +------------------+
    /// ```
    ///
    /// > **Note:** All nodes shown in the graphs above (except `B`) are
    /// > [`RuntimeGroupExec`]s. Other nodes are omitted for simplicity.
    ///
    /// We don't "pull-up" the remote exec as much as possible because we want
    /// to stop pulling once a "local" node is met but there's no certain way of
    /// knowing that the node is to be run locally (since that information lies
    /// with its n'th parent, i.e., [`RuntimeGroupExec`]).
    ///
    /// For example, take the following plan in consideration which tries to
    /// copy contents from a remote table in a local file:
    ///
    /// ```txt
    /// RuntimeExec (local)
    ///         |
    ///     CopyToExec
    ///         |
    ///       Limit
    ///         |
    /// RuntimeExec (remote)
    ///         |
    ///     TableScan
    /// ```
    ///
    /// If we were to pull "remote" up until the "local", we would end up with
    /// something like:
    ///
    /// ```txt
    /// RuntimeExec (local)
    ///         |
    /// RuntimeExec (remote)
    ///         |
    ///     CopyToExec
    ///         |
    ///       Limit
    ///         |
    ///     TableScan
    /// ```
    ///
    /// This ends up running the `CopyToExec` on remote which is completely
    /// incorrect.
    // TODO: Figure out how to "pull-up" remote more accurately. One solution
    // might be to specialize cases where we know we can pull up, i.e., know
    // that the node has to execute on local.
    fn pushdown_remote_pref(&self, root: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let root_pref = root
            .as_any()
            .downcast_ref::<RuntimeGroupExec>()
            .map(|r| r.preference)
            .unwrap_or(RuntimePreference::Unspecified);

        if matches!(root_pref, RuntimePreference::Remote) {
            let mut sends = Vec::new();

            // Replace all "local" execs with recv-send pairs.
            let plan = root.transform_up_mut(&mut |plan| {
                let mut did_modify = false;
                let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

                for child in plan.children() {
                    match child.as_any().downcast_ref::<RuntimeGroupExec>() {
                        Some(exec) if matches!(exec.preference, RuntimePreference::Local) => {
                            did_modify = true;

                            let work_id = Uuid::new_v4();
                            debug!(%work_id, "creating send and recv execs");

                            let mut input = exec.child.clone();

                            // Create the receive exec. This will be executed on
                            // the remote node.
                            let recv = ClientExchangeRecvExec {
                                work_id,
                                schema: input.schema(),
                            };

                            // Temporary coalesce exec until our custom plans
                            // support partition.
                            if input.output_partitioning().partition_count() != 1 {
                                input = Arc::new(CoalescePartitionsExec::new(input));
                            }

                            // And create the associated send exec. This will
                            // be executed locally, and pushes batches over the
                            // broadcast endpoint.
                            let send = ClientExchangeSendExec {
                                database_id: self.database_id,
                                work_id,
                                client: self.remote_client.clone(),
                                input,
                            };
                            sends.push(send);

                            new_children.push(Arc::new(recv));
                        }
                        _ => new_children.push(child),
                    }
                }

                let transformed = if did_modify {
                    let new_plan = plan.with_new_children(new_children)?;
                    Transformed::Yes(new_plan)
                } else {
                    Transformed::No(plan)
                };

                Ok(transformed)
            })?;

            Ok(self.create_join_exec(plan, sends))
        } else {
            let new_children = root
                .children()
                .into_iter()
                .map(|child| self.pushdown_remote_pref(child))
                .collect::<Result<Vec<_>>>()?;

            if new_children.is_empty() {
                Ok(root)
            } else {
                Ok(root.with_new_children(new_children)?)
            }
        }
    }

    fn create_join_exec(
        &self,
        mut physical: Arc<dyn ExecutionPlan>,
        sends: Vec<ClientExchangeSendExec>,
    ) -> Arc<SendRecvJoinExec> {
        // Temporary coalesce exec until our custom plans support partition.
        if physical.output_partitioning().partition_count() != 1 {
            physical = Arc::new(CoalescePartitionsExec::new(physical));
        }

        // Wrap in exec that will send the plan to the remote machine.
        let physical = Arc::new(RemoteExecutionExec::new(
            self.remote_client.clone(),
            physical,
            self.query_text.to_owned(),
        ));

        Arc::new(SendRecvJoinExec::new(physical, sends))
    }
}

#[async_trait]
impl<'a> PhysicalPlanner for RemotePhysicalPlanner<'a> {
    async fn create_physical_plan(
        &self,
        logical_plan: &DfLogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Rewrite any DDL that needs to process locally so we can only send the
        // query on remote and process it later on.

        // Create the physical plans. This will call `scan` on the custom table
        // providers meaning we'll have the correct exec refs.

        let physical = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            DDLExtensionPlanner::new(self.catalog.clone()),
        )])
        .create_physical_plan(logical_plan, session_state)
        .await?;

        // Push down "remote" as down as possible. This ensures the correctness
        // of the plan.
        self.pushdown_remote_pref(physical)
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
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
