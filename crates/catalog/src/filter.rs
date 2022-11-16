use datafusion::common::{DFField, DFSchema};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{create_physical_expr, execution_props::ExecutionProps};
use datafusion::physical_plan::{filter::FilterExec, ExecutionPlan};
use std::sync::Arc;

/// A helper function for ensuring a physical plan has an appropriate filter
/// node.
///
/// This is useful since not all sources support predicate pushdown.
///
/// NOTE: This does not accept a projection list. All columns will be returned.
/// Rationale being combining filtering and projections would be a bit too much
/// to do for a first pass.
pub async fn filter_scan<T, R>(
    table: R,
    state: &SessionState,
    filters: &[Expr],
    limit: Option<usize>,
) -> DatafusionResult<Arc<dyn ExecutionPlan>>
where
    T: TableProvider + ?Sized,
    R: AsRef<T>,
{
    let table = table.as_ref();
    if filters.is_empty() {
        let result = table.scan(state, &None, &[], limit).await;
        result
    } else {
        // TODO: Check if table has full support for filter pushdown, removing
        // the need to wrap the plan in a filter exec.

        let mut iter = filters.iter();
        let init = iter.next().unwrap().clone();
        let expr = iter.fold(init, |expr, next| expr.and(next.clone()));
        let schema = table.schema();
        let df_schema = DFSchema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from(f.clone()))
                .collect(),
            schema.metadata().clone(),
        )?;
        // Note that we don't pass a projection down since the filter might make
        // use of one of the fields not being projected.
        let table_scan = table.scan(state, &None, filters, limit).await?;
        let physical =
            create_physical_expr(&expr, &df_schema, &schema, &ExecutionProps::default())?;
        let plan = Arc::new(FilterExec::try_new(physical, table_scan)?);
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_types::context::SessionContext;
    use datafusion::arrow::array::{Int8Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::datasource::MemTable;

    #[tokio::test]
    async fn no_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(&["hello", "world"])),
                Arc::new(Int8Array::from_iter_values([3, 5])),
            ],
        )
        .unwrap();

        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let filter = Vec::new();
        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(&["hello", "world"])),
                Arc::new(Int8Array::from_iter_values([3, 5])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();

        let plan = filter_scan(&table, ctx.get_df_state(), &filter, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn filter_single_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(&["hello", "world"])),
                Arc::new(Int8Array::from_iter_values([3, 5])),
            ],
        )
        .unwrap();

        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let filter = vec![Expr::Column(Column::from_name("col1"))
            .eq(Expr::Literal(ScalarValue::Utf8(Some("world".to_string()))))];
        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(&["world"])),
                Arc::new(Int8Array::from_iter_values([5])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();

        let plan = filter_scan(&table, ctx.get_df_state(), &filter, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn filter_no_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(&["hello", "world"])),
                Arc::new(Int8Array::from_iter_values([3, 5])),
            ],
        )
        .unwrap();

        let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap());
        let filter =
            vec![
                Expr::Column(Column::from_name("col1")).eq(Expr::Literal(ScalarValue::Utf8(Some(
                    "doesn't match".to_string(),
                )))),
            ];
        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values::<String, _>(Vec::new())),
                Arc::new(Int8Array::from_iter_values([])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();

        let plan = filter_scan(&table, ctx.get_df_state(), &filter, None)
            .await
            .unwrap();
    }
}
