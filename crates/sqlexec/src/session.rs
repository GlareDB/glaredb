use crate::context::SessionContext;
use crate::errors::{internal, ExecError, Result};
use crate::executor::ExecutionResult;
use crate::extended::{Portal, PreparedStatement};
use crate::logical_plan::*;
use crate::parameters::{ParameterValue, SessionParameters, SEARCH_PATH_PARAM};
use crate::placeholders::bind_placeholders;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::catalog::CatalogList;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, EmptyRecordBatchStream, ExecutionPlan,
    SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{self, ObjectType};
use datafusion::sql::{ResolvedTableReference, TableReference};
use futures::StreamExt;
use jsoncat::catalog::Catalog;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tracing::debug;

/// A per-client user session.
///
/// This type acts as the bridge between datafusion planning/execution and the
/// rest of the system.
pub struct Session {
    pub(crate) ctx: SessionContext,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    pub fn new(catalog: Arc<Catalog>) -> Result<Session> {
        let ctx = SessionContext::new(catalog);
        Ok(Session { ctx })
    }

    /// Create a physical plan for a given datafusion logical plan.
    pub(crate) async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.ctx.get_df_state().create_physical_plan(&plan).await?;
        Ok(plan)
    }

    /// Execute a datafusion physical plan.
    pub(crate) fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.ctx.task_context();
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan.execute(0, context)?),
            _ => {
                let plan = CoalescePartitionsExec::new(plan);
                Ok(plan.execute(0, context)?)
            }
        }
    }

    pub(crate) async fn create_table(&self, plan: CreateTable) -> Result<()> {
        unimplemented!()
        // let resolved = self.resolve_table_reference(plan.table_name.as_str());

        // let schema = self
        //     .sess_catalog
        //     .user_schema(resolved.schema)
        //     .await?
        //     .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;

        // schema
        //     .create_table(&self.sess_ctx, resolved.table, &Schema::new(plan.columns))
        //     .await?;

        // Ok(())
    }

    pub(crate) async fn create_external_table(&self, _plan: CreateExternalTable) -> Result<()> {
        // This will need an additional system table to track external tables.
        todo!()
    }

    pub(crate) async fn create_table_as(&self, plan: CreateTableAs) -> Result<()> {
        unimplemented!()
        // let resolved = self.resolve_table_reference(plan.table_name.as_ref());
        // let schema = self
        //     .sess_catalog
        //     .user_schema(resolved.schema)
        //     .await?
        //     .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;

        // // Plan and execute the source. We'll use the first batch from the
        // // stream to create the table with the correct arrow schema.
        // let physical = self.create_physical_plan(plan.source).await?;
        // let mut stream = self.execute_physical(physical)?;

        // // Create the table and insert the first batch.
        // let table = match stream.next().await {
        //     Some(result) => {
        //         let batch = result?;
        //         let arrow_schema = batch.schema();
        //         schema
        //             .create_table(&self.sess_ctx, &plan.table_name, &arrow_schema)
        //             .await?;
        //         let table = schema
        //             .get_mutable_table(&plan.table_name)
        //             .await?
        //             .ok_or_else(|| {
        //                 internal!("failed to get table after create: {}", plan.table_name)
        //             })?;
        //         table.insert(&self.sess_ctx, batch).await?;
        //         table
        //     }
        //     None => {
        //         return Err(internal!(
        //             "source stream empty, cannot infer schema from empty stream"
        //         ))
        //     }
        // };

        // // Insert the rest of the stream.
        // while let Some(result) = stream.next().await {
        //     let batch = result?;
        //     table.insert(&self.sess_ctx, batch).await?;
        // }

        // Ok(())
    }

    pub(crate) async fn create_schema(&self, plan: CreateSchema) -> Result<()> {
        unimplemented!()
        // self.sess_catalog
        //     .create_schema(&self.sess_ctx, &plan.schema_name)
        //     .await?;
        // Ok(())
    }

    pub(crate) async fn insert(&self, plan: Insert) -> Result<()> {
        unimplemented!()
        // let resolved = self.resolve_table_reference(plan.table_name.as_ref());
        // let schema = self
        //     .sess_catalog
        //     .user_schema(resolved.schema)
        //     .await?
        //     .ok_or_else(|| internal!("missing schema: {:?}", resolved.schema))?;

        // let table = schema
        //     .get_mutable_table(resolved.table)
        //     .await?
        //     .ok_or_else(|| internal!("missing table: {:?}", resolved.table))?;

        // let physical = self.create_physical_plan(plan.source).await?;
        // let mut stream = self.execute_physical(physical)?;

        // while let Some(result) = stream.next().await {
        //     let batch = result?;
        //     table.insert(&self.sess_ctx, batch).await?;
        // }

        // Ok(())
    }

    pub(crate) fn set_parameter(&mut self, plan: SetParameter) -> Result<()> {
        unimplemented!()
        // self.parameters.set_parameter(plan.variable, plan.values)
    }

    /// Store the prepared statement in the current session.
    /// It will later be readied for execution by using `bind_prepared_statement`.
    pub fn create_prepared_statement(
        &mut self,
        name: Option<String>,
        sql: String,
        params: Vec<i32>,
    ) -> Result<()> {
        unimplemented!()
        // match name {
        //     None => {
        //         // Store the unnamed prepared statement.
        //         // This will persist until the session is dropped or another unnamed prepared statement is created
        //         self.unnamed_statement = Some(PreparedStatement::new(sql, params)?);
        //     }
        //     Some(name) => {
        //         // Named prepared statements must be explicitly closed before being redefined
        //         match self.named_statements.entry(name) {
        //             Entry::Occupied(ent) => {
        //                 return Err(internal!(
        //                     "prepared statement already exists: {}",
        //                     ent.key()
        //                 ))
        //             }
        //             Entry::Vacant(ent) => {
        //                 ent.insert(PreparedStatement::new(sql, params)?);
        //             }
        //         }
        //     }
        // }

        // Ok(())
    }

    pub fn get_prepared_statement(&self, name: &Option<String>) -> Option<&PreparedStatement> {
        unimplemented!()
        // match name {
        //     None => self.unnamed_statement.as_ref(),
        //     Some(name) => self.named_statements.get(name),
        // }
    }

    pub fn get_portal(&self, portal_name: &Option<String>) -> Option<&Portal> {
        unimplemented!()
        // match portal_name {
        //     None => self.unnamed_portal.as_ref(),
        //     Some(name) => self.named_portals.get(name),
        // }
    }

    /// Bind the parameters of a prepared statement to the given values.
    /// If successful, the bound statement will create a portal which can be used to execute the statement.
    pub fn bind_prepared_statement(
        &mut self,
        portal_name: Option<String>,
        statement_name: Option<String>,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<()> {
        unimplemented!()
        // let (statement, types) = {
        //     let prepared_statement = match statement_name {
        //         None => self
        //             .unnamed_statement
        //             .as_mut()
        //             .ok_or_else(|| internal!("no unnamed prepared statement"))?,
        //         Some(name) => self
        //             .named_statements
        //             .get_mut(&name)
        //             .ok_or_else(|| internal!("no prepared statement named: {}", name))?,
        //     };

        //     (
        //         prepared_statement.statement.clone(),
        //         prepared_statement.param_types.clone(),
        //     )
        // };

        // let bound_statement = bind_placeholders(statement, &param_formats, &param_values, &types)?;
        // debug!(?bound_statement, "bound statement");
        // let plan = self.plan_sql(bound_statement)?;

        // match portal_name {
        //     None => {
        //         // Store the unnamed portal.
        //         // This will persist until the session is dropped or another unnamed portal is created
        //         self.unnamed_portal = Some(Portal::new(
        //             plan,
        //             param_formats,
        //             param_values,
        //             result_formats,
        //         )?);
        //     }
        //     Some(name) => {
        //         // Named portals must be explicitly closed before being redefined
        //         match self.named_portals.entry(name) {
        //             Entry::Occupied(ent) => {
        //                 return Err(internal!("portal already exists: {}", ent.key()))
        //             }
        //             Entry::Vacant(_ent) => {
        //                 todo!("plan named portal");
        //                 // let plan = self.plan_sql(statement)?;
        //                 // ent.insert(Portal::new(plan, param_formats, param_values, result_formats)?);
        //             }
        //         }
        //     }
        // }

        // Ok(())
    }

    pub async fn drop_table(&self, _plan: DropTable) -> Result<()> {
        todo!()
    }

    pub async fn execute_portal(
        &mut self,
        portal_name: &Option<String>,
        _max_rows: i32,
    ) -> Result<ExecutionResult> {
        unimplemented!()
        // // TODO: respect max_rows
        // let portal = match portal_name {
        //     None => self
        //         .unnamed_portal
        //         .as_mut()
        //         .ok_or_else(|| internal!("no unnamed portal"))?,
        //     Some(name) => self
        //         .named_portals
        //         .get_mut(name)
        //         .ok_or_else(|| internal!("no portal named: {}", name))?,
        // };

        // match portal.plan.clone() {
        //     LogicalPlan::Ddl(DdlPlan::CreateTable(plan)) => {
        //         self.create_table(plan).await?;
        //         Ok(ExecutionResult::CreateTable)
        //     }
        //     LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)) => {
        //         self.create_external_table(plan).await?;
        //         Ok(ExecutionResult::CreateTable)
        //     }
        //     LogicalPlan::Ddl(DdlPlan::CreateTableAs(plan)) => {
        //         self.create_table_as(plan).await?;
        //         Ok(ExecutionResult::CreateTable)
        //     }
        //     LogicalPlan::Ddl(DdlPlan::DropTable(plan)) => {
        //         self.drop_table(plan).await?;
        //         Ok(ExecutionResult::DropTables)
        //     }
        //     LogicalPlan::Write(WritePlan::Insert(plan)) => {
        //         self.insert(plan).await?;
        //         Ok(ExecutionResult::WriteSuccess)
        //     }
        //     LogicalPlan::Query(plan) => {
        //         let physical = self.create_physical_plan(plan).await?;
        //         let stream = self.execute_physical(physical)?;
        //         Ok(ExecutionResult::Query { stream })
        //     }
        //     other => Err(internal!("unimplemented logical plan: {:?}", other)),
        // }
    }
}
