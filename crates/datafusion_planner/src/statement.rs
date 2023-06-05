use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    common::{DFField, DFSchema, DataFusionError, OwnedTableReference, Result, ToDFSchema},
    logical_expr::{
        builder::project, Analyze, Explain, ExprSchemable, LogicalPlan, PlanType, ToStringifiedPlan,
    },
    sql::{
        planner::PlannerContext,
        sqlparser::ast::{self, Query, SetExpr, Statement},
    },
};

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub async fn explain_statement_to_plan(
        &mut self,
        verbose: bool,
        analyze: bool,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        let plan = match statement {
            Statement::Query(query) => self.query_to_plan(*query).await?,
            x => {
                return Err(DataFusionError::Plan(format!(
                    "Explain only supported for Query, not '{x}'"
                )))
            }
        };
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans = vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }

    pub async fn insert_to_source_plan(
        &mut self,
        table_name: &OwnedTableReference,
        columns: &Vec<String>,
        source: Box<Query>,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let provider = self
            .schema_provider
            .get_table_provider(table_name.clone())
            .await?;
        let arrow_schema = (*provider.schema()).clone();
        let table_schema = DFSchema::try_from(arrow_schema)?;

        let fields = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            table_schema.fields().clone()
        } else {
            let fields = columns
                .iter()
                .map(|c| Ok(table_schema.field_with_unqualified_name(c)?.clone()))
                .collect::<Result<Vec<DFField>>>()?;
            // Validate no duplicate fields
            let table_schema =
                DFSchema::new_with_metadata(fields, table_schema.metadata().clone())?;
            table_schema.fields().clone()
        };

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(ast::Values { rows, .. }) = (*source.body).clone() {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let ast::Expr::Value(ast::Value::Placeholder(name)) = val {
                        let name = name.replace('$', "").parse::<usize>().map_err(|_| {
                            DataFusionError::Plan(format!("Can't parse placeholder: {name}"))
                        })? - 1;
                        let field = fields.get(idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            ))
                        })?;
                        let dt = field.field().data_type().clone();
                        let _ = prepare_param_data_types.insert(name, dt);
                    }
                }
            }
        }
        let prepare_param_data_types = prepare_param_data_types.into_values().collect();

        // Projection
        let mut planner_context =
            PlannerContext::new().with_prepare_param_data_types(prepare_param_data_types);
        let source = self
            .query_to_plan_with_context(*source, &mut planner_context)
            .await?;
        if fields.len() != source.schema().fields().len() {
            Err(DataFusionError::Plan(
                "Column count doesn't match insert query!".to_owned(),
            ))?;
        }
        let exprs = fields
            .iter()
            .zip(source.schema().fields().iter())
            .map(|(target_field, source_field)| {
                let expr =
                    datafusion::logical_expr::Expr::Column(source_field.unqualified_column())
                        .cast_to(target_field.data_type(), source.schema())?
                        .alias(target_field.name());
                Ok(expr)
            })
            .collect::<Result<Vec<datafusion::logical_expr::Expr>>>()?;

        let source = project(source, exprs)?;
        Ok(source)
    }
}
