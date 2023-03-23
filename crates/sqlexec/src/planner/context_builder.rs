use crate::{context::SessionContext, errors::Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::OwnedTableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::AggregateUDF;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::TableSource;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use datafusion::sql::{
    planner::object_name_to_table_reference,
    sqlparser::ast::{self, Visitor},
};
use sqlparser::ast::Visit;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

/// Helper for building a context provider for use with Datafusion's SQL
/// planner.
pub struct PlanContextBuilder<'a> {
    ctx: &'a SessionContext,
}

impl<'a> PlanContextBuilder<'a> {
    pub fn new(ctx: &'a SessionContext) -> Self {
        PlanContextBuilder { ctx }
    }

    /// Build a context suitable for planning a given sql statement.
    ///
    /// Under the hood, this will get all table providers from the session
    /// context that's taking part in the query.
    pub async fn build_plan_context(
        &self,
        statement: ast::Statement,
    ) -> Result<PartialContextProvider<'a>> {
        let mut visitor = RelationVistor::default();
        statement.visit(&mut visitor);
        let relations = visitor.0;

        let normalize = self
            .ctx
            .get_df_state()
            .config_options()
            .sql_parser
            .enable_ident_normalization;

        let references = relations
            .into_iter()
            .map(|rel| object_name_to_table_reference(rel, normalize))
            .collect::<Result<Vec<_>, _>>()?;

        // TODO: Resolve references and get table providers from dispatcher.

        Ok(PartialContextProvider {
            providers: HashMap::new(),
            ctx: self.ctx,
        })
    }
}

/// Partial context provider with table providers required to fulfill a single
/// query.
///
/// NOTE: While `ContextProvider` is for _logical_ planning, DataFusion will
/// actually try to downcast the `TableSource` to a `TableProvider` during
/// physical planning. This only works with `DefaultTableSource` which is what
/// this adapter uses.
pub struct PartialContextProvider<'a> {
    providers: HashMap<String, Arc<dyn TableProvider>>,
    ctx: &'a SessionContext,
}

impl<'a> ContextProvider for PartialContextProvider<'a> {
    fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        let name = reference_to_string(name); // TODO: Don't create string?

        // It's a bug to have a missing provider here. It means we missed
        // resolving the table before we started SQL planning.
        let provider = self.providers.get(&name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Context provider for SQL planner is missing a table: {}",
                name
            ))
        })?;

        Ok(Arc::new(DefaultTableSource::new(provider.clone())))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        unimplemented!()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        unimplemented!()
    }

    fn options(&self) -> &ConfigOptions {
        unimplemented!()
    }
}

fn reference_to_string(r: TableReference) -> String {
    match r {
        TableReference::Bare { table } => format!("{table}"),
        TableReference::Partial { schema, table } => format!("{schema}.{table}"),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => format!("{catalog}.{schema}.{table}"),
    }
}

#[derive(Debug, Default)]
struct RelationVistor(HashSet<ast::ObjectName>);

impl Visitor for RelationVistor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ast::ObjectName) -> ControlFlow<()> {
        if !self.0.contains(relation) {
            self.0.insert(relation.clone());
        }
        ControlFlow::Continue(())
    }
}
