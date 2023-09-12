use datafusion::common::display::ToStringifiedPlan;

use datafusion::common::tree_node::Transformed;
use datafusion::datasource::source_as_provider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::utils::generate_sort_key;
use datafusion::logical_expr::{
    Aggregate, EmptyRelation, Join, Projection, Sort, SubqueryAlias, TableScan, Unnest,
    UserDefinedLogicalNode, Window,
};
use datafusion::logical_expr::{
    CrossJoin, Expr, LogicalPlan, Partitioning as LogicalPartitioning, PlanType, Repartition, Union,
};

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{internal_err, not_impl_err, plan_err, DFSchema};
use datafusion::logical_expr::expr::{Alias, GroupingSet, WindowFunction};
use datafusion::logical_expr::expr_rewriter::{unalias, unnormalize_cols};
use datafusion::logical_expr::logical_plan::builder::wrap_projection_for_join_if_necessary;
use datafusion::logical_expr::StringifiedPlan;
use datafusion::logical_expr::{Limit, Values};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use datafusion::physical_plan::{
    empty::EmptyExec, joins::PartitionMode, union::UnionExec, values::ValuesExec,
};
use datafusion::physical_plan::{joins::utils as join_utils, Partitioning};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::sql::utils::window_expr_common_partition_keys;
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use datafusion_ext::runtime::group_pull_up::RuntimeGroupPullUp;
use datafusion_ext::runtime::runtime_group::RuntimeGroupExec;
use datafusion_ext::transform::TreeNodeExt;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::{multiunzip, Itertools};
use protogen::metastore::types::catalog::RuntimePreference;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};
use uuid::Uuid;

use crate::metastore::catalog::SessionCatalog;
use crate::remote::client::RemoteSessionClient;

use super::expr_planner::{
    create_aggregate_expr_and_maybe_filter, create_cube_physical_expr, create_physical_sort_expr,
    create_rollup_physical_expr, create_window_expr, merge_grouping_set_physical_expr,
    physical_name,
};
use super::extension::ExtensionType;

use crate::planner::physical_plan::alter_database_rename::AlterDatabaseRenameExec;
use crate::planner::physical_plan::alter_table_rename::AlterTableRenameExec;
use crate::planner::physical_plan::alter_tunnel_rotate_keys::AlterTunnelRotateKeysExec;
use crate::planner::physical_plan::client_recv::ClientExchangeRecvExec;
use crate::planner::physical_plan::client_send::ClientExchangeSendExec;
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
use crate::planner::physical_plan::remote_exec::RemoteExecutionExec;
use crate::planner::physical_plan::send_recv::SendRecvJoinExec;
use crate::planner::physical_plan::set_var::SetVarExec;
use crate::planner::physical_plan::show_var::ShowVarExec;
use crate::planner::physical_plan::update::UpdateExec;

use crate::planner::logical_plan::{
    AlterDatabaseRename, AlterTableRename, AlterTunnelRotateKeys, CopyTo, CreateCredentials,
    CreateExternalDatabase, CreateExternalTable, CreateSchema, CreateTable, CreateTempTable,
    CreateTunnel, CreateView, Delete, DropCredentials, DropDatabase, DropSchemas, DropTables,
    DropTunnel, DropViews, Insert, SetVariable, ShowVariable, Update,
};
/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
pub struct CustomPhysicalPlanner<'a> {
    pub remote_client: Option<RemoteSessionClient>,
    pub catalog: &'a SessionCatalog,
    catalog_version: u64,
}

impl<'a> CustomPhysicalPlanner<'a> {
    pub fn new(catalog: &'a SessionCatalog, remote_client: Option<RemoteSessionClient>) -> Self {
        Self {
            catalog_version: catalog.version(),
            catalog,
            remote_client,
        }
    }
}

#[async_trait]
impl PhysicalPlanner for CustomPhysicalPlanner<'_> {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.handle_explain(logical_plan, session_state).await? {
            Some(plan) => Ok(plan),
            None => {
                let plan = self
                    .create_initial_plan(logical_plan, session_state)
                    .await?;
                self.optimize_internal(plan, session_state, |_, _| {})
            }
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `e`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    ///
    /// `input_schema`: the physical schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            session_state.execution_props(),
        )
    }
}

impl CustomPhysicalPlanner<'_> {
    /// Create a physical plans for multiple logical plans.
    ///
    /// This is the same as [`create_initial_plan`](Self::create_initial_plan) but runs the planning concurrently.
    ///
    /// The result order is the same as the input order.
    fn create_initial_plan_multi<'a>(
        &'a self,
        logical_plans: impl IntoIterator<Item = &'a LogicalPlan> + Send + 'a,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Vec<Arc<dyn ExecutionPlan>>>> {
        async move {
            // First build futures with as little references as possible, then performing some stream magic.
            // Otherwise rustc bails out w/:
            //
            //   error: higher-ranked lifetime error
            //   ...
            //   note: could not prove `[async block@...]: std::marker::Send`
            let futures = logical_plans
                .into_iter()
                .enumerate()
                .map(|(idx, lp)| async move {
                    let plan = self.create_initial_plan(lp, session_state).await?;
                    Ok((idx, plan)) as Result<_>
                })
                .collect::<Vec<_>>();

            let mut physical_plans = futures::stream::iter(futures)
                .buffer_unordered(
                    session_state
                        .config_options()
                        .execution
                        .planning_concurrency,
                )
                .try_collect::<Vec<(usize, Arc<dyn ExecutionPlan>)>>()
                .await?;
            physical_plans.sort_by_key(|(idx, _plan)| *idx);
            let physical_plans = physical_plans
                .into_iter()
                .map(|(_idx, plan)| plan)
                .collect::<Vec<_>>();
            Ok(physical_plans)
        }
        .boxed()
    }

    /// Create a physical plan from a logical plan
    fn create_initial_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            self.create_initial_plan_impl(logical_plan, session_state)
                .await
        }
        .boxed()
    }

    fn create_grouping_physical_expr(
        &self,
        group_expr: &[Expr],
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<PhysicalGroupBy> {
        if group_expr.len() == 1 {
            match &group_expr[0] {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    merge_grouping_set_physical_expr(
                        grouping_sets,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                    create_cube_physical_expr(exprs, input_dfschema, input_schema, session_state)
                }
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    create_rollup_physical_expr(exprs, input_dfschema, input_schema, session_state)
                }
                expr => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                    self.create_physical_expr(expr, input_dfschema, input_schema, session_state),
                    physical_name(expr),
                ))?])),
            }
        } else {
            Ok(PhysicalGroupBy::new_single(
                group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                input_dfschema,
                                input_schema,
                                session_state,
                            ),
                            physical_name(e),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
    }

    fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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
                    tbl_reference: lp.tbl_reference.clone(),
                    new_tbl_reference: lp.new_tbl_reference.clone(),
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
                    tbl_reference: lp.tbl_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    tunnel: lp.tunnel.clone(),
                    table_options: lp.table_options.clone(),
                })))
            }
            ExtensionType::CreateSchema => {
                let lp = require_downcast_lp::<CreateSchema>(node);
                Ok(Some(Arc::new(CreateSchemaExec {
                    catalog_version: self.catalog_version,
                    schema_reference: lp.schema_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                })))
            }
            ExtensionType::CreateTable => {
                let lp = require_downcast_lp::<CreateTable>(node);
                Ok(Some(Arc::new(CreateTableExec {
                    catalog_version: self.catalog_version,
                    tbl_reference: lp.tbl_reference.clone(),
                    if_not_exists: lp.if_not_exists,
                    arrow_schema: Arc::new(lp.schema.as_ref().into()),
                    source: physical_inputs.get(0).cloned(),
                })))
            }
            ExtensionType::CreateTempTable => {
                let lp = require_downcast_lp::<CreateTempTable>(node);
                Ok(Some(Arc::new(CreateTempTableExec {
                    tbl_reference: lp.tbl_reference.clone(),
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
                    view_reference: lp.view_reference.clone(),
                    sql: lp.sql.clone(),
                    columns: lp.columns.clone(),
                    or_replace: lp.or_replace,
                })))
            }
            ExtensionType::DropTables => {
                let lp = require_downcast_lp::<DropTables>(node);
                Ok(Some(Arc::new(DropTablesExec {
                    catalog_version: self.catalog_version,
                    tbl_references: lp.tbl_references.clone(),
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
                let exec = DropSchemasExec {
                    catalog_version: self.catalog_version,
                    schema_references: lp.schema_references.clone(),
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
                    view_references: lp.view_references.clone(),
                    if_exists: lp.if_exists,
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::SetVariable => {
                let lp = require_downcast_lp::<SetVariable>(node);
                let exec = SetVarExec {
                    variable: lp.variable.clone(),
                    values: lp.values.clone(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::ShowVariable => {
                let lp = require_downcast_lp::<ShowVariable>(node);
                let exec = ShowVarExec {
                    variable: lp.variable.clone(),
                };
                Ok(Some(Arc::new(exec)))
            }
            ExtensionType::CopyTo => {
                let lp = require_downcast_lp::<CopyTo>(node);
                Ok(Some(Arc::new(CopyToExec {
                    format: lp.format.clone(),
                    dest: lp.dest.clone(),
                    source: physical_inputs.get(0).unwrap().clone(),
                })))
            }
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
                if lp.table.meta.is_temp {
                    Ok(Some(Arc::new(InsertExec {
                        table: lp.table.clone(),
                        source: physical_inputs.get(0).unwrap().clone(),
                    })))
                } else {
                    todo!()
                }
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

    async fn create_initial_plan_impl<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
            LogicalPlan::TableScan(TableScan {
                source,
                projection,
                filters,
                fetch,
                ..
            }) => {
                let source = source_as_provider(source)?;
                // Remove all qualifiers from the scan as the provider
                // doesn't know (nor should care) how the relation was
                // referred to in the query
                let filters = unnormalize_cols(filters.iter().cloned());
                let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                source
                    .scan(session_state, projection.as_ref(), &unaliased, *fetch)
                    .await
            }
            LogicalPlan::Values(Values { values, schema }) => {
                let exec_schema = schema.as_ref().to_owned().into();
                let exprs = values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                self.create_physical_expr(expr, schema, &exec_schema, session_state)
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let value_exec = ValuesExec::try_new(SchemaRef::new(exec_schema), exprs)?;
                Ok(Arc::new(value_exec))
            }
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => {
                if window_expr.is_empty() {
                    return internal_err!("Impossibly got empty window expression");
                }

                let input_exec = self.create_initial_plan(input, session_state).await?;

                // at this moment we are guaranteed by the logical planner
                // to have all the window_expr to have equal sort key
                let partition_keys = window_expr_common_partition_keys(window_expr)?;

                let can_repartition = !partition_keys.is_empty()
                    && session_state.config().target_partitions() > 1
                    && session_state.config().repartition_window_functions();

                let physical_partition_keys = if can_repartition {
                    partition_keys
                        .iter()
                        .map(|e| {
                            self.create_physical_expr(
                                e,
                                input.schema(),
                                &input_exec.schema(),
                                session_state,
                            )
                        })
                        .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?
                } else {
                    vec![]
                };

                let get_sort_keys = |expr: &Expr| match expr {
                    Expr::WindowFunction(WindowFunction {
                        ref partition_by,
                        ref order_by,
                        ..
                    }) => generate_sort_key(partition_by, order_by),
                    Expr::Alias(Alias { expr, .. }) => {
                        // Convert &Box<T> to &T
                        match &**expr {
                            Expr::WindowFunction(WindowFunction {
                                ref partition_by,
                                ref order_by,
                                ..
                            }) => generate_sort_key(partition_by, order_by),
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };
                let sort_keys = get_sort_keys(&window_expr[0])?;
                if window_expr.len() > 1 {
                    debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr).unwrap() == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                }

                let logical_input_schema = input.schema();
                let physical_input_schema = input_exec.schema();
                let window_expr = window_expr
                    .iter()
                    .map(|e| {
                        create_window_expr(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let uses_bounded_memory = window_expr.iter().all(|e| e.uses_bounded_memory());
                // If all window expressions can run with bounded memory,
                // choose the bounded window variant:
                Ok(if uses_bounded_memory {
                    Arc::new(BoundedWindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_input_schema,
                        physical_partition_keys,
                        PartitionSearchMode::Sorted,
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_input_schema,
                        physical_partition_keys,
                    )?)
                })
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            }) => {
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = self.create_initial_plan(input, session_state).await?;
                let physical_input_schema = input_exec.schema();
                let logical_input_schema = input.as_ref().schema();

                let groups = self.create_grouping_physical_expr(
                    group_expr,
                    logical_input_schema,
                    &physical_input_schema,
                    session_state,
                )?;

                let agg_filter = aggr_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr_and_maybe_filter(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let (aggregates, filters, order_bys): (Vec<_>, Vec<_>, Vec<_>) =
                    multiunzip(agg_filter);

                let initial_aggr = Arc::new(AggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    filters.clone(),
                    order_bys,
                    input_exec,
                    physical_input_schema.clone(),
                )?);

                // update group column indices based on partial aggregate plan evaluation
                let final_group: Vec<Arc<dyn PhysicalExpr>> = initial_aggr.output_group_expr();

                let can_repartition = !groups.is_empty()
                    && session_state.config().target_partitions() > 1
                    && session_state.config().repartition_aggregations();

                // Some aggregators may be modified during initialization for
                // optimization purposes. For example, a FIRST_VALUE may turn
                // into a LAST_VALUE with the reverse ordering requirement.
                // To reflect such changes to subsequent stages, use the updated
                // `AggregateExpr`/`PhysicalSortExpr` objects.
                let updated_aggregates = initial_aggr.aggr_expr().to_vec();
                let updated_order_bys = initial_aggr.order_by_expr().to_vec();

                let (initial_aggr, next_partition_mode): (Arc<dyn ExecutionPlan>, AggregateMode) =
                    if can_repartition {
                        // construct a second aggregation with 'AggregateMode::FinalPartitioned'
                        (initial_aggr, AggregateMode::FinalPartitioned)
                    } else {
                        // construct a second aggregation, keeping the final column name equal to the
                        // first aggregation and the expressions corresponding to the respective aggregate
                        (initial_aggr, AggregateMode::Final)
                    };

                let final_grouping_set = PhysicalGroupBy::new_single(
                    final_group
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (expr.clone(), groups.expr()[i].1.clone()))
                        .collect(),
                );

                Ok(Arc::new(AggregateExec::try_new(
                    next_partition_mode,
                    final_grouping_set,
                    updated_aggregates,
                    filters,
                    updated_order_bys,
                    initial_aggr,
                    physical_input_schema.clone(),
                )?))
            }
            LogicalPlan::Projection(Projection { input, expr, .. }) => {
                let input_exec = self.create_initial_plan(input, session_state).await?;
                let input_schema = input.as_ref().schema();

                let physical_exprs = expr
                    .iter()
                    .map(|e| {
                        // For projections, SQL planner and logical plan builder may convert user
                        // provided expressions into logical Column expressions if their results
                        // are already provided from the input plans. Because we work with
                        // qualified columns in logical plane, derived columns involve operators or
                        // functions will contain qualifiers as well. This will result in logical
                        // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                        //
                        // If we run these logical columns through physical_name function, we will
                        // get physical names with column qualifiers, which violates DataFusion's
                        // field name semantics. To account for this, we need to derive the
                        // physical name from physical input instead.
                        //
                        // This depends on the invariant that logical schema field index MUST match
                        // with physical schema field index.
                        let physical_name = if let Expr::Column(col) = e {
                            match input_schema.index_of_column(col) {
                                Ok(idx) => {
                                    // index physical field using logical field index
                                    Ok(input_exec.schema().field(idx).name().to_string())
                                }
                                // logical column is not a derived column, safe to pass along to
                                // physical_name
                                Err(_) => physical_name(e),
                            }
                        } else {
                            physical_name(e)
                        };

                        tuple_err((
                            self.create_physical_expr(
                                e,
                                input_schema,
                                &input_exec.schema(),
                                session_state,
                            ),
                            physical_name,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(ProjectionExec::try_new(
                    physical_exprs,
                    input_exec,
                )?))
            }
            LogicalPlan::Filter(filter) => {
                let physical_input = self
                    .create_initial_plan(&filter.input, session_state)
                    .await?;
                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = filter.input.schema();

                let runtime_expr = self.create_physical_expr(
                    &filter.predicate,
                    input_dfschema,
                    &input_schema,
                    session_state,
                )?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?))
            }
            LogicalPlan::Union(Union { inputs, schema }) => {
                let physical_plans = self
                    .create_initial_plan_multi(inputs.iter().map(|lp| lp.as_ref()), session_state)
                    .await?;

                if schema.fields().len() < physical_plans[0].schema().fields().len() {
                    // `schema` could be a subset of the child schema. For example
                    // for query "select count(*) from (select a from t union all select a from t)"
                    // `schema` is empty but child schema contains one field `a`.
                    Ok(Arc::new(UnionExec::try_new_with_schema(
                        physical_plans,
                        schema.clone(),
                    )?))
                } else {
                    Ok(Arc::new(UnionExec::new(physical_plans)))
                }
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                let physical_input = self.create_initial_plan(input, session_state).await?;
                let input_schema = physical_input.schema();
                let input_dfschema = input.as_ref().schema();
                let physical_partitioning = match partitioning_scheme {
                    LogicalPartitioning::RoundRobinBatch(n) => Partitioning::RoundRobinBatch(*n),
                    LogicalPartitioning::Hash(expr, n) => {
                        let runtime_expr = expr
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input_dfschema,
                                    &input_schema,
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Partitioning::Hash(runtime_expr, *n)
                    }
                    LogicalPartitioning::DistributeBy(_) => {
                        return not_impl_err!(
                            "Physical plan does not support DistributeBy partitioning"
                        );
                    }
                };
                Ok(Arc::new(RepartitionExec::try_new(
                    physical_input,
                    physical_partitioning,
                )?))
            }
            LogicalPlan::Sort(Sort {
                expr, input, fetch, ..
            }) => {
                let physical_input = self.create_initial_plan(input, session_state).await?;
                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = input.as_ref().schema();
                let sort_expr = expr
                    .iter()
                    .map(|e| {
                        create_physical_sort_expr(
                            e,
                            input_dfschema,
                            &input_schema,
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let new_sort = SortExec::new(sort_expr, physical_input).with_fetch(*fetch);
                Ok(Arc::new(new_sort))
            }
            LogicalPlan::Join(Join {
                left,
                right,
                on: keys,
                filter,
                join_type,
                null_equals_null,
                schema: join_schema,
                ..
            }) => {
                let null_equals_null = *null_equals_null;

                // If join has expression equijoin keys, add physical projecton.
                let has_expr_join_key = keys
                    .iter()
                    .any(|(l, r)| !(matches!(l, Expr::Column(_)) && matches!(r, Expr::Column(_))));
                if has_expr_join_key {
                    let left_keys = keys.iter().map(|(l, _r)| l).cloned().collect::<Vec<_>>();
                    let right_keys = keys.iter().map(|(_l, r)| r).cloned().collect::<Vec<_>>();
                    let (left, right, column_on, added_project) = {
                        let (left, left_col_keys, left_projected) =
                            wrap_projection_for_join_if_necessary(
                                left_keys.as_slice(),
                                left.as_ref().clone(),
                            )?;
                        let (right, right_col_keys, right_projected) =
                            wrap_projection_for_join_if_necessary(
                                &right_keys,
                                right.as_ref().clone(),
                            )?;
                        (
                            left,
                            right,
                            (left_col_keys, right_col_keys),
                            left_projected || right_projected,
                        )
                    };

                    let join_plan = LogicalPlan::Join(Join::try_new_with_project_input(
                        logical_plan,
                        Arc::new(left),
                        Arc::new(right),
                        column_on,
                    )?);

                    // Remove temporary projected columns
                    let join_plan = if added_project {
                        let final_join_result = join_schema
                            .fields()
                            .iter()
                            .map(|field| Expr::Column(field.qualified_column()))
                            .collect::<Vec<_>>();
                        let projection =
                            Projection::try_new(final_join_result, Arc::new(join_plan))?;
                        LogicalPlan::Projection(projection)
                    } else {
                        join_plan
                    };

                    return self.create_initial_plan(&join_plan, session_state).await;
                }

                // All equi-join keys are columns now, create physical join plan
                let left_right = self
                    .create_initial_plan_multi([left.as_ref(), right.as_ref()], session_state)
                    .await?;
                let [physical_left, physical_right]: [Arc<dyn ExecutionPlan>; 2] =
                    left_right.try_into().map_err(|_| {
                        DataFusionError::Internal(
                            "`create_initial_plan_multi` is broken".to_string(),
                        )
                    })?;
                let left_df_schema = left.schema();
                let right_df_schema = right.schema();
                let join_on = keys
                    .iter()
                    .map(|(l, r)| {
                        let l = l.try_into_col()?;
                        let r = r.try_into_col()?;
                        Ok((
                            Column::new(&l.name, left_df_schema.index_of_column(&l)?),
                            Column::new(&r.name, right_df_schema.index_of_column(&r)?),
                        ))
                    })
                    .collect::<Result<join_utils::JoinOn>>()?;

                let join_filter = match filter {
                    Some(expr) => {
                        // Extract columns from filter expression and saved in a HashSet
                        let cols = expr.to_columns()?;

                        // Collect left & right field indices, the field indices are sorted in ascending order
                        let left_field_indices = cols
                            .iter()
                            .filter_map(|c| match left_df_schema.index_of_column(c) {
                                Ok(idx) => Some(idx),
                                _ => None,
                            })
                            .sorted()
                            .collect::<Vec<_>>();
                        let right_field_indices = cols
                            .iter()
                            .filter_map(|c| match right_df_schema.index_of_column(c) {
                                Ok(idx) => Some(idx),
                                _ => None,
                            })
                            .sorted()
                            .collect::<Vec<_>>();

                        // Collect DFFields and Fields required for intermediate schemas
                        let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) =
                            left_field_indices
                                .clone()
                                .into_iter()
                                .map(|i| {
                                    (
                                        left_df_schema.field(i).clone(),
                                        physical_left.schema().field(i).clone(),
                                    )
                                })
                                .chain(right_field_indices.clone().into_iter().map(|i| {
                                    (
                                        right_df_schema.field(i).clone(),
                                        physical_right.schema().field(i).clone(),
                                    )
                                }))
                                .unzip();

                        // Construct intermediate schemas used for filtering data and
                        // convert logical expression to physical according to filter schema
                        let filter_df_schema =
                            DFSchema::new_with_metadata(filter_df_fields, HashMap::new())?;
                        let filter_schema =
                            Schema::new_with_metadata(filter_fields, HashMap::new());
                        let filter_expr = create_physical_expr(
                            expr,
                            &filter_df_schema,
                            &filter_schema,
                            session_state.execution_props(),
                        )?;
                        let column_indices = join_utils::JoinFilter::build_column_indices(
                            left_field_indices,
                            right_field_indices,
                        );

                        Some(join_utils::JoinFilter::new(
                            filter_expr,
                            column_indices,
                            filter_schema,
                        ))
                    }
                    _ => None,
                };

                let prefer_hash_join = session_state.config_options().optimizer.prefer_hash_join;
                if join_on.is_empty() {
                    // there is no equal join condition, use the nested loop join
                    // TODO optimize the plan, and use the config of `target_partitions` and `repartition_joins`
                    Ok(Arc::new(NestedLoopJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_filter,
                        join_type,
                    )?))
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && !prefer_hash_join
                {
                    // Use SortMergeJoin if hash join is not preferred
                    // Sort-Merge join support currently is experimental
                    if join_filter.is_some() {
                        // TODO SortMergeJoinExec need to support join filter
                        not_impl_err!("SortMergeJoinExec does not support join_filter now.")
                    } else {
                        let join_on_len = join_on.len();
                        Ok(Arc::new(SortMergeJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_on,
                            *join_type,
                            vec![SortOptions::default(); join_on_len],
                            null_equals_null,
                        )?))
                    }
                } else if session_state.config().target_partitions() > 1
                    && session_state.config().repartition_joins()
                    && prefer_hash_join
                {
                    let partition_mode = {
                        if session_state.config().collect_statistics() {
                            PartitionMode::Auto
                        } else {
                            PartitionMode::Partitioned
                        }
                    };
                    Ok(Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        partition_mode,
                        null_equals_null,
                    )?))
                } else {
                    Ok(Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_filter,
                        join_type,
                        PartitionMode::CollectLeft,
                        null_equals_null,
                    )?))
                }
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let left_right = self
                    .create_initial_plan_multi([left.as_ref(), right.as_ref()], session_state)
                    .await?;
                let [left, right]: [Arc<dyn ExecutionPlan>; 2] =
                    left_right.try_into().map_err(|_| {
                        DataFusionError::Internal(
                            "`create_initial_plan_multi` is broken".to_string(),
                        )
                    })?;
                Ok(Arc::new(CrossJoinExec::new(left, right)))
            }
            LogicalPlan::Subquery(_) => todo!(),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row,
                schema,
            }) => Ok(Arc::new(EmptyExec::new(
                *produce_one_row,
                SchemaRef::new(schema.as_ref().to_owned().into()),
            ))),
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                self.create_initial_plan(input, session_state).await
            }
            LogicalPlan::Limit(Limit {
                input, skip, fetch, ..
            }) => {
                let input = self.create_initial_plan(input, session_state).await?;

                // GlobalLimitExec requires a single partition for input
                let input = if input.output_partitioning().partition_count() == 1 {
                    input
                } else {
                    // Apply a LocalLimitExec to each partition. The optimizer will also insert
                    // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                    if let Some(fetch) = fetch {
                        Arc::new(LocalLimitExec::new(input, *fetch + skip))
                    } else {
                        input
                    }
                };

                Ok(Arc::new(GlobalLimitExec::new(input, *skip, *fetch)))
            }
            LogicalPlan::Unnest(Unnest {
                input,
                column,
                schema,
                options,
            }) => {
                let input = self.create_initial_plan(input, session_state).await?;
                let column_exec = schema
                    .index_of_column(column)
                    .map(|idx| Column::new(&column.name, idx))?;
                let schema = SchemaRef::new(schema.as_ref().to_owned().into());
                Ok(Arc::new(UnnestExec::new(
                    input,
                    column_exec,
                    schema,
                    options.clone(),
                )))
            }
            LogicalPlan::Ddl(ddl) => {
                // There is no default plan for DDl statements --
                // it must be handled at a higher level (so that
                // the appropriate table can be registered with
                // the context)
                let name = ddl.name();
                not_impl_err!("Unsupported logical plan: {name}")
            }
            LogicalPlan::Prepare(_) => {
                // There is no default plan for "PREPARE" -- it must be
                // handled at a higher level (so that the appropriate
                // statement can be prepared)
                not_impl_err!("Unsupported logical plan: Prepare")
            }
            LogicalPlan::Dml(_) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                not_impl_err!("Unsupported logical plan: Dml")
            }
            LogicalPlan::Statement(statement) => {
                // DataFusion is a read-only query engine, but also a library, so consumers may implement this
                let name = statement.name();
                not_impl_err!("Unsupported logical plan: Statement({name})")
            }
            LogicalPlan::DescribeTable(_) => {
                internal_err!("Unsupported logical plan: DescribeTable must be root of the plan")
            }
            LogicalPlan::Explain(_) => {
                internal_err!("Unsupported logical plan: Explain must be root of the plan")
            }
            LogicalPlan::Distinct(_) => {
                internal_err!("Unsupported logical plan: Distinct should be replaced to Aggregate")
            }
            LogicalPlan::Analyze(_) => {
                internal_err!("Unsupported logical plan: Analyze must be root of the plan")
            }
            LogicalPlan::Extension(e) => {
                let physical_inputs = self
                    .create_initial_plan_multi(e.node.inputs(), session_state)
                    .await?;

                let logical_input = e.node.inputs();
                let maybe_plan = self.plan_extension(
                    self,
                    e.node.as_ref(),
                    &logical_input,
                    &physical_inputs,
                    session_state,
                )?;

                let plan = match maybe_plan {
                    Some(v) => Ok(v),
                    _ => plan_err!(
                        "Unable to convert the custom node to an execution plan: {:?}",
                        e.node
                    ),
                }?;

                // Ensure the ExecutionPlan's schema matches the
                // declared logical schema to catch and warn about
                // logic errors when creating user defined plans.
                if !e.node.schema().matches_arrow_schema(&plan.schema()) {
                    plan_err!(
                        "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                        e.node,
                        e.node.schema(),
                        plan.schema()
                    )
                } else {
                    Ok(plan)
                }
            }
            LogicalPlan::Copy(_) => todo!(),
        };
        exec_plan
    }
}

impl CustomPhysicalPlanner<'_> {
    /// Handles capturing the various plans for EXPLAIN queries
    ///
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let LogicalPlan::Explain(e) = logical_plan {
            use PlanType::*;
            let mut stringified_plans = vec![];

            let config = &session_state.config_options().explain;

            if !config.physical_plan_only {
                stringified_plans = e.stringified_plans.clone();
                if e.logical_optimization_succeeded {
                    stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));
                }
            }

            if !config.logical_plan_only && e.logical_optimization_succeeded {
                match self
                    .create_initial_plan(e.plan.as_ref(), session_state)
                    .await
                {
                    Ok(input) => {
                        stringified_plans.push(
                            displayable(input.as_ref())
                                .to_stringified(e.verbose, InitialPhysicalPlan),
                        );

                        match self.optimize_internal(input, session_state, |plan, optimizer| {
                            let optimizer_name = optimizer.name().to_string();
                            let plan_type = OptimizedPhysicalPlan { optimizer_name };
                            stringified_plans
                                .push(displayable(plan).to_stringified(e.verbose, plan_type));
                        }) {
                            Ok(input) => stringified_plans.push(
                                displayable(input.as_ref())
                                    .to_stringified(e.verbose, FinalPhysicalPlan),
                            ),
                            Err(DataFusionError::Context(optimizer_name, e)) => {
                                let plan_type = OptimizedPhysicalPlan { optimizer_name };
                                stringified_plans
                                    .push(StringifiedPlan::new(plan_type, e.to_string()))
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => stringified_plans
                        .push(StringifiedPlan::new(InitialPhysicalPlan, e.to_string())),
                }
            }

            Ok(Some(Arc::new(ExplainExec::new(
                SchemaRef::new(e.schema.as_ref().to_owned().into()),
                stringified_plans,
                e.verbose,
            ))))
        } else if let LogicalPlan::Analyze(a) = logical_plan {
            let input = self.create_physical_plan(&a.input, session_state).await?;
            let schema = SchemaRef::new((*a.schema).clone().into());
            Ok(Some(Arc::new(AnalyzeExec::new(a.verbose, input, schema))))
        } else {
            Ok(None)
        }
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one)
    fn optimize_internal<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule),
    {
        let optimizers = session_state.physical_optimizers();
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent(false)
        );
        trace!(
            "Detailed input physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let mut new_plan = plan;
        for optimizer in optimizers {
            let before_schema = new_plan.schema();
            new_plan = optimizer
                .optimize(new_plan, session_state.config_options())
                .map_err(|e| DataFusionError::Context(optimizer.name().to_string(), Box::new(e)))?;
            if optimizer.schema_check() && new_plan.schema() != before_schema {
                let e = DataFusionError::Internal(format!(
                    "PhysicalOptimizer rule '{}' failed, due to generate a different schema, original schema: {:?}, new schema: {:?}",
                    optimizer.name(),
                    before_schema,
                    new_plan.schema()
                ));
                return Err(DataFusionError::Context(
                    optimizer.name().to_string(),
                    Box::new(e),
                ));
            }
            trace!(
                "Optimized physical plan by {}:\n{}\n",
                optimizer.name(),
                displayable(new_plan.as_ref()).indent(false)
            );
            observer(new_plan.as_ref(), optimizer.as_ref())
        }

        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent(false)
        );
        trace!("Detailed optimized physical plan:\n{:#?}", new_plan);

        if let Some(remote_client) = &self.remote_client {
            let new_plan =
                RuntimeGroupPullUp::new().optimize(new_plan, session_state.config().options())?;
            let (new_plan, send_execs) = self.replace_local_runtime_groups(new_plan)?;

            // If the root of the plan indicates a local runtime preference, then we
            // can just execute everything locally by omitting the remote execution
            // exec.
            //
            // TODO: We could probably have an option to configure this. Note that
            // if we do add this as an option, we'll need to change
            // `replace_local_runtime_groups` to handle the root of the plan too.
            let new_plan = match new_plan.as_any().downcast_ref::<RuntimeGroupExec>() {
                Some(exec)
                    if exec.preference == RuntimePreference::Local && send_execs.is_empty() =>
                {
                    new_plan
                }
                _ => {
                    let mut physical = new_plan;
                    // Temporary coalesce exec until our custom plans support partition.
                    if physical.output_partitioning().partition_count() != 1 {
                        physical = Arc::new(CoalescePartitionsExec::new(physical));
                    }

                    // Wrap in exec that will send the plan to the remote machine.
                    let physical =
                        Arc::new(RemoteExecutionExec::new(remote_client.clone(), physical));

                    // Create a wrapper physical plan which drives both the
                    // result stream, and the send execs
                    Arc::new(SendRecvJoinExec::new(physical, send_execs))
                }
            };
            Ok(new_plan)
        } else {
            Ok(new_plan)
        }
    }
    /// Replace all local runtime groups that are not the root of the plan with
    /// equivalent client recv execs.
    ///
    /// The modifed execution plan with client recv execs will be returned,
    /// along with the send execs that will be responsible for pushing batches
    /// to the remote node.
    ///
    /// This should be ran after all optimizations have been made to the
    /// physical plan.
    fn replace_local_runtime_groups(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<ClientExchangeSendExec>)> {
        if self.remote_client.is_none() {
            return Ok((plan, Vec::new()));
        }

        let mut sends = Vec::new();

        let plan = plan.transform_up_mut(&mut |plan| {
            let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
            let mut did_modify = false;

            for child in plan.children() {
                match child.as_any().downcast_ref::<RuntimeGroupExec>() {
                    Some(exec) if exec.preference == RuntimePreference::Local => {
                        did_modify = true;

                        let broadcast_id = Uuid::new_v4();
                        debug!(%broadcast_id, "creating send and recv execs");

                        let mut input = exec.child.clone();

                        // Create the receive exec. This will be executed on the
                        // remote node.
                        let recv = ClientExchangeRecvExec {
                            broadcast_id,
                            schema: input.schema(),
                        };

                        // Temporary coalesce exec until our custom plans support partition.
                        if input.output_partitioning().partition_count() != 1 {
                            input = Arc::new(CoalescePartitionsExec::new(input));
                        }

                        // And create the associated send exec. This will be
                        // executed locally, and pushes batches over the
                        // broadcast endpoint.
                        let send = ClientExchangeSendExec {
                            broadcast_id,
                            client: self.remote_client.clone().unwrap(),
                            input,
                        };
                        sends.push(send);

                        new_children.push(Arc::new(recv));
                    }
                    _ => new_children.push(child),
                }
            }

            if !did_modify {
                return Ok(Transformed::No(plan));
            }

            let new_plan = plan.with_new_children(new_children)?;
            Ok(Transformed::Yes(new_plan))
        })?;

        Ok((plan, sends))
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

fn require_downcast_lp<P: 'static>(plan: &dyn UserDefinedLogicalNode) -> &P {
    match plan.as_any().downcast_ref::<P>() {
        Some(p) => p,
        None => panic!("Invalid downcast reference for plan: {}", plan.name()),
    }
}
