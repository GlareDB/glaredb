use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    common::{
        plan_datafusion_err, plan_err, unqualified_field_not_found, DFField, DFSchema,
        DataFusionError, OwnedTableReference, Result, ToDFSchema,
    },
    logical_expr::{
        builder::project, Analyze, Explain, ExprSchemable, LogicalPlan, PlanType, ToStringifiedPlan,
    },
    scalar::ScalarValue,
    sql::{
        planner::PlannerContext,
        sqlparser::ast::{self, Query, SetExpr, Statement, Value},
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
        let table_source = self
            .context_provider
            .get_table_source(table_name.clone())
            .await?;

        let arrow_schema = (*table_source.schema()).clone();
        let table_schema = DFSchema::try_from(arrow_schema)?;

        // Get insert fields and target table's value indices
        //
        // if value_indices[i] = Some(j), it means that the value of the i-th target table's column is
        // derived from the j-th output of the source.
        //
        // if value_indices[i] = None, it means that the value of the i-th target table's column is
        // not provided, and should be filled with a default value later.
        let (fields, value_indices) = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            (
                table_schema.fields().clone(),
                (0..table_schema.fields().len())
                    .map(Some)
                    .collect::<Vec<_>>(),
            )
        } else {
            let mut value_indices = vec![None; table_schema.fields().len()];
            let fields = columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let column_index = table_schema
                        .index_of_column_by_name(None, c)?
                        .ok_or_else(|| unqualified_field_not_found(c, &table_schema))?;
                    if value_indices[column_index].is_some() {
                        return Err(DataFusionError::SchemaError(
                            datafusion::common::SchemaError::DuplicateUnqualifiedField {
                                name: c.clone(),
                            },
                        ));
                    } else {
                        value_indices[column_index] = Some(i);
                    }
                    Ok(table_schema.field(column_index).clone())
                })
                .collect::<Result<Vec<DFField>>>()?;
            (fields, value_indices)
        };

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(ast::Values { rows, .. }) = (*source.body).clone() {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let ast::Expr::Value(Value::Placeholder(name)) = val {
                        let name =
                            name.replace('$', "").parse::<usize>().map_err(|_| {
                                plan_datafusion_err!("Can't parse placeholder: {name}")
                            })? - 1;
                        let field = fields.get(idx).ok_or_else(|| {
                            plan_datafusion_err!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            )
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
            plan_err!("Column count doesn't match insert query!")?;
        }

        let exprs = value_indices
            .into_iter()
            .enumerate()
            .map(|(i, value_index)| {
                let target_field = table_schema.field(i);
                let expr = match value_index {
                    Some(v) => {
                        let source_field = source.schema().field(v);
                        datafusion::logical_expr::Expr::Column(source_field.qualified_column())
                            .cast_to(target_field.data_type(), source.schema())?
                    }
                    // The value is not specified. Fill in the default value for the column.
                    None => table_source
                        .get_column_default(target_field.name())
                        .cloned()
                        .unwrap_or_else(|| {
                            // If there is no default for the column, then the default is NULL
                            datafusion::logical_expr::Expr::Literal(ScalarValue::Null)
                        })
                        .cast_to(target_field.data_type(), &DFSchema::empty())?,
                };
                Ok(expr.alias(target_field.name()))
            })
            .collect::<Result<Vec<datafusion::logical_expr::Expr>>>()?;
        let source = project(source, exprs)?;
        Ok(source)
    }
}
