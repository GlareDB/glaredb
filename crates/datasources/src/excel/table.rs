use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef as ArrowSchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};

use super::errors::ExcelError;
use crate::excel;
use crate::excel::stream::ExcelStream;
use crate::excel::ExcelTable;

pub struct ExcelTableProvider {
    cell_range: calamine::Range<calamine::Data>,
    has_header: bool,
    schema: Arc<Schema>,
}

impl ExcelTableProvider {
    pub async fn try_new(t: ExcelTable) -> Result<Self, ExcelError> {
        let infer_num = t.cell_range.width();
        Self::try_new_with_inferred(t, infer_num).await
    }

    pub async fn try_new_with_inferred(t: ExcelTable, num: usize) -> Result<Self, ExcelError> {
        let cell_range = t.cell_range;
        let has_header = t.has_header;
        let schema = Arc::new(excel::infer_schema(&cell_range, t.has_header, num)?);

        Ok(Self {
            schema,
            cell_range,
            has_header,
        })
    }
}

#[async_trait]
impl TableProvider for ExcelTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        // Basic Projection
        let projected_schema = match projection {
            Some(projection) if !projection.is_empty() => {
                Arc::new(self.schema.project(projection)?)
            }
            _ => self.schema.clone(),
        };

        Ok(Arc::new(ExcelExecutionPlan {
            arrow_schema: projected_schema,
            cell_range: self.cell_range.clone(),
            header: self.has_header,
        }))
    }
}

#[derive(Debug)]
struct ExcelExecutionPlan {
    arrow_schema: ArrowSchemaRef,
    cell_range: calamine::Range<calamine::Data>,
    header: bool,
}

impl ExecutionPlan for ExcelExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Execution(
                "cannot replace children for ExcelExecutionPlan".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "only single excel sheet partition supported".to_string(),
            ));
        }

        let stream = ExcelStream::new(self.cell_range.clone(), self.header, self.schema());

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DatafusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for ExcelExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelExec")
    }
}
