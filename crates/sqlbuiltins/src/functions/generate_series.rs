use std::collections::HashMap;
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FromFuncParamValue, FuncParamValue, TableFunc, TableFuncContextProvider,
};
use futures::Stream;
use num_traits::Zero;

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeries;

#[async_trait]
impl TableFunc for GenerateSeries {
    fn name(&self) -> &str {
        "generate_series"
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();

                if i64::is_param_valid(&start) && i64::is_param_valid(&stop) {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.param_into()?,
                        stop.param_into()?,
                        1,
                    )
                } else if f64::is_param_valid(&start) && f64::is_param_valid(&stop) {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.param_into()?,
                        stop.param_into()?,
                        1.0_f64,
                    )
                } else {
                    return Err(ExtensionError::InvalidParamValue {
                        param: format!("({start}, {stop})"),
                        expected: "integers or floats",
                    });
                }
            }
            3 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();
                let step = args.next().unwrap();

                if i64::is_param_valid(&start)
                    && i64::is_param_valid(&stop)
                    && i64::is_param_valid(&step)
                {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        start.param_into()?,
                        stop.param_into()?,
                        step.param_into()?,
                    )
                } else if f64::is_param_valid(&start)
                    && f64::is_param_valid(&stop)
                    && f64::is_param_valid(&step)
                {
                    create_straming_table::<GenerateSeriesTypeFloat>(
                        start.param_into()?,
                        stop.param_into()?,
                        step.param_into()?,
                    )
                } else {
                    return Err(ExtensionError::InvalidParamValue {
                        param: format!("({start}, {stop}, {step})"),
                        expected: "integers or floats",
                    });
                }
            }
            _ => return Err(ExtensionError::InvalidNumArgs),
        }
    }
}

fn create_straming_table<T: GenerateSeriesType>(
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
) -> Result<Arc<dyn TableProvider>> {
    if step.is_zero() {
        return Err(ExtensionError::String("'step' may not be zero".to_string()));
    }

    let partition: GenerateSeriesPartition<T> = GenerateSeriesPartition::new(start, stop, step);
    let table = StreamingTable::try_new(partition.schema().clone(), vec![Arc::new(partition)])?;

    Ok(Arc::new(table))
}

trait GenerateSeriesType: Send + Sync + 'static {
    type PrimType: Send + Sync + PartialOrd + AddAssign + Add + Zero + Copy + Unpin;
    const ARROW_TYPE: DataType;

    fn collect_array(batch: Vec<Self::PrimType>) -> Arc<dyn Array>;
}

struct GenerateSeriesTypeInt;

impl GenerateSeriesType for GenerateSeriesTypeInt {
    type PrimType = i64;
    const ARROW_TYPE: DataType = DataType::Int64;

    fn collect_array(series: Vec<i64>) -> Arc<dyn Array> {
        let arr = Int64Array::from_iter_values(series);
        Arc::new(arr)
    }
}

struct GenerateSeriesTypeFloat;

impl GenerateSeriesType for GenerateSeriesTypeFloat {
    type PrimType = f64;
    const ARROW_TYPE: DataType = DataType::Float64;

    fn collect_array(series: Vec<f64>) -> Arc<dyn Array> {
        let arr = Float64Array::from_iter_values(series);
        Arc::new(arr)
    }
}

struct GenerateSeriesPartition<T: GenerateSeriesType> {
    schema: Arc<Schema>,
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
}

impl<T: GenerateSeriesType> GenerateSeriesPartition<T> {
    fn new(start: T::PrimType, stop: T::PrimType, step: T::PrimType) -> Self {
        GenerateSeriesPartition {
            schema: Arc::new(Schema::new([Arc::new(Field::new(
                "generate_series",
                T::ARROW_TYPE,
                false,
            ))])),
            start,
            stop,
            step,
        }
    }
}

impl<T: GenerateSeriesType> PartitionStream for GenerateSeriesPartition<T> {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(GenerateSeriesStream::<T> {
            schema: self.schema.clone(),
            exhausted: false,
            curr: self.start,
            stop: self.stop,
            step: self.step,
        })
    }
}

struct GenerateSeriesStream<T: GenerateSeriesType> {
    schema: Arc<Schema>,
    exhausted: bool,
    curr: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
}

impl<T: GenerateSeriesType> GenerateSeriesStream<T> {
    fn generate_next(&mut self) -> Option<RecordBatch> {
        if self.exhausted {
            return None;
        }

        const BATCH_SIZE: usize = 1000;

        let mut series: Vec<_> = Vec::new();
        if self.curr < self.stop && self.step > T::PrimType::zero() {
            // Going up.
            let mut count = 0;
            while self.curr <= self.stop && count < BATCH_SIZE {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        } else if self.curr > self.stop && self.step < T::PrimType::zero() {
            // Going down.
            let mut count = 0;
            while self.curr >= self.stop && count < BATCH_SIZE {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        }

        if series.len() < BATCH_SIZE {
            self.exhausted = true
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        let arr = T::collect_array(series);
        assert_eq!(arr.data_type(), &T::ARROW_TYPE);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![arr]).unwrap();
        Some(batch)
    }
}

impl<T: GenerateSeriesType> Stream for GenerateSeriesStream<T> {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().generate_next().map(Ok))
    }
}

impl<T: GenerateSeriesType> RecordBatchStream for GenerateSeriesStream<T> {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
