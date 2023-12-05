use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Add, AddAssign};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Decimal128Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use decimal::Decimal128;
use futures::Stream;
use num_traits::Zero;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct GenerateSeries;

impl ConstBuiltinFunction for GenerateSeries {
    const NAME: &'static str = "generate_series";
    const DESCRIPTION: &'static str = "Generate a series of values";
    const EXAMPLE: &'static str = "SELECT * FROM generate_series(1, 10, 2)";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Uniform(2, vec![DataType::Int64, DataType::Decimal128(38, 0)]),
                TypeSignature::Uniform(3, vec![DataType::Int64, DataType::Decimal128(38, 0)]),
            ]),
            Volatility::Immutable,
        ))
    }
}

#[async_trait]
impl TableFunc for GenerateSeries {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Unspecified
    }
    fn detect_runtime(
        &self,
        _: &[FuncParamValue],
        parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        match parent {
            RuntimePreference::Unspecified => Ok(RuntimePreference::Local),
            other => Ok(other),
        }
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let start = args.next().unwrap();
                let stop = args.next().unwrap();

                if start.is_valid::<i64>() || stop.is_valid::<i64>() {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        GenerateSeriesTypeInt,
                        start.try_into()?,
                        stop.try_into()?,
                        1,
                    )
                } else if start.is_valid::<Decimal128>() && stop.is_valid::<Decimal128>() {
                    let start: Decimal128 = start.try_into()?;
                    let stop: Decimal128 = stop.try_into()?;
                    let step = Decimal128::new(1, 0)?;
                    let scale = [start, stop, step].iter().map(|s| s.scale()).max().unwrap();
                    create_straming_table::<GenerateSeriesTypeDecimal128>(
                        GenerateSeriesTypeDecimal128 { scale },
                        start,
                        stop,
                        step,
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

                if start.is_valid::<i64>() && stop.is_valid::<i64>() && step.is_valid::<i64>() {
                    create_straming_table::<GenerateSeriesTypeInt>(
                        GenerateSeriesTypeInt,
                        start.try_into()?,
                        stop.try_into()?,
                        step.try_into()?,
                    )
                } else if start.is_valid::<Decimal128>()
                    && stop.is_valid::<Decimal128>()
                    && step.is_valid::<Decimal128>()
                {
                    let start: Decimal128 = start.try_into()?;
                    let stop: Decimal128 = stop.try_into()?;
                    let step: Decimal128 = step.try_into()?;
                    let scale = [start, stop, step].iter().map(|s| s.scale()).max().unwrap();
                    create_straming_table::<GenerateSeriesTypeDecimal128>(
                        GenerateSeriesTypeDecimal128 { scale },
                        start,
                        stop,
                        step,
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
    gen_series_type: T,
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
) -> Result<Arc<dyn TableProvider>> {
    if step.is_zero() {
        return Err(ExtensionError::String("'step' may not be zero".to_string()));
    }

    let partition: GenerateSeriesPartition<T> =
        GenerateSeriesPartition::new(gen_series_type, start, stop, step);
    let table = StreamingTable::try_new(partition.schema().clone(), vec![Arc::new(partition)])?;

    Ok(Arc::new(table))
}

trait GenerateSeriesType: Send + Sync + 'static {
    type PrimType: Send + Sync + PartialOrd + AddAssign + Add + Zero + Copy + Unpin;

    fn arrow_type(&self) -> DataType;
    fn collect_array(&self, series: Vec<Self::PrimType>) -> Arc<dyn Array>;
}

struct GenerateSeriesTypeInt;

impl GenerateSeriesType for GenerateSeriesTypeInt {
    type PrimType = i64;

    fn arrow_type(&self) -> DataType {
        DataType::Int64
    }

    fn collect_array(&self, series: Vec<Self::PrimType>) -> Arc<dyn Array> {
        let arr = Int64Array::from_iter_values(series);
        Arc::new(arr)
    }
}

struct GenerateSeriesTypeDecimal128 {
    scale: i8,
}

impl GenerateSeriesType for GenerateSeriesTypeDecimal128 {
    type PrimType = Decimal128;

    fn arrow_type(&self) -> DataType {
        DataType::Decimal128(38, self.scale)
    }

    fn collect_array(&self, series: Vec<Self::PrimType>) -> Arc<dyn Array> {
        let series = series.into_iter().map(|mut d| {
            d.rescale(self.scale);
            d.mantissa()
        });
        let arr = Decimal128Array::from_iter_values(series).with_data_type(self.arrow_type());
        Arc::new(arr)
    }
}

struct GenerateSeriesPartition<T: GenerateSeriesType> {
    gen_series_type: Arc<T>,
    schema: Arc<Schema>,
    start: T::PrimType,
    stop: T::PrimType,
    step: T::PrimType,
}

impl<T: GenerateSeriesType> GenerateSeriesPartition<T> {
    fn new(gen_series_type: T, start: T::PrimType, stop: T::PrimType, step: T::PrimType) -> Self {
        GenerateSeriesPartition {
            schema: Arc::new(Schema::new([Arc::new(Field::new(
                "generate_series",
                gen_series_type.arrow_type(),
                false,
            ))])),
            start,
            stop,
            step,
            gen_series_type: Arc::new(gen_series_type),
        }
    }
}

impl<T: GenerateSeriesType> PartitionStream for GenerateSeriesPartition<T> {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(GenerateSeriesStream::<T> {
            gen_series_type: Arc::clone(&self.gen_series_type),
            schema: self.schema.clone(),
            exhausted: false,
            curr: self.start,
            stop: self.stop,
            step: self.step,
        })
    }
}

struct GenerateSeriesStream<T: GenerateSeriesType> {
    gen_series_type: Arc<T>,
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

        let arrow_dt = self.gen_series_type.arrow_type();
        let arr = self.gen_series_type.collect_array(series);
        assert_eq!(arr.data_type(), &arrow_dt);
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
