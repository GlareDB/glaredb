use std::sync::Arc;

use futures::future::BoxFuture;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::{Field, Schema};
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};

use super::{PlannedTableFunction, TableFunction, TableFunctionArgs};
use crate::database::DatabaseContext;
use crate::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerateSeries;

impl TableFunction for GenerateSeries {
    fn name(&self) -> &'static str {
        "generate_series"
    }

    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        args: TableFunctionArgs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { Self::plan_and_initialize_inner(args) })
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        let mut packed = PackedDecoder::new(state);
        let start = packed.decode_next()?;
        let stop = packed.decode_next()?;
        let step = packed.decode_next()?;
        Ok(Box::new(GenerateSeriesI64 { start, stop, step }))
    }
}

impl GenerateSeries {
    fn plan_and_initialize_inner(args: TableFunctionArgs) -> Result<Box<dyn PlannedTableFunction>> {
        if !args.named.is_empty() {
            return Err(RayexecError::new(
                "generate_series does not accept named arguments",
            ));
        }

        let mut args = args.clone();
        let [start, stop, step] = match args.positional.len() {
            2 => {
                let stop = args.positional.pop().unwrap().try_as_i64()?;
                let start = args.positional.pop().unwrap().try_as_i64()?;
                [start, stop, 1]
            }
            3 => {
                let step = args.positional.pop().unwrap().try_as_i64()?;
                let stop = args.positional.pop().unwrap().try_as_i64()?;
                let start = args.positional.pop().unwrap().try_as_i64()?;
                [start, stop, step]
            }
            _ => {
                return Err(RayexecError::new(
                    "generate_series requires 2 or 3 arguments",
                ));
            }
        };

        if step == 0 {
            return Err(RayexecError::new("'step' may not be zero"));
        }

        Ok(Box::new(GenerateSeriesI64 { start, stop, step }) as _)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerateSeriesI64 {
    start: i64,
    stop: i64,
    step: i64,
}

impl PlannedTableFunction for GenerateSeriesI64 {
    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        packed.encode_next(&self.start)?;
        packed.encode_next(&self.stop)?;
        packed.encode_next(&self.step)?;
        Ok(())
    }

    fn table_function(&self) -> &dyn TableFunction {
        &GenerateSeries
    }

    fn schema(&self) -> Schema {
        Schema::new([Field::new("generate_series", DataType::Int64, false)])
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(self.clone()))
    }
}

impl DataTable for GenerateSeriesI64 {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(ProjectedScan::new(
            GenerateSeriesScan {
                batch_size: 1024,
                exhausted: false,
                curr: self.start,
                stop: self.stop,
                step: self.step,
            },
            projections,
        ))];
        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

#[derive(Debug, PartialEq)]
struct GenerateSeriesScan {
    batch_size: usize,
    exhausted: bool,
    curr: i64,
    stop: i64,
    step: i64,
}

impl GenerateSeriesScan {
    fn generate_next(&mut self) -> Option<Batch> {
        if self.exhausted {
            return None;
        }

        let mut series: Vec<_> = Vec::new();
        if self.curr < self.stop && self.step > 0 {
            // Going up.
            let mut count = 0;
            while self.curr <= self.stop && count < self.batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        } else if self.curr > self.stop && self.step < 0 {
            // Going down.
            let mut count = 0;
            while self.curr >= self.stop && count < self.batch_size {
                series.push(self.curr);
                self.curr += self.step;
                count += 1;
            }
        }

        if series.len() < self.batch_size {
            self.exhausted = true;
        }

        // Calculate the start value for the next iteration.
        if let Some(last) = series.last() {
            self.curr = *last + self.step;
        }

        let col =
            Array::new_with_array_data(DataType::Int64, ArrayData::Int64(Arc::new(series.into())));
        let batch = Batch::try_new([col]).expect("batch to be valid");

        Some(batch)
    }
}

impl DataTableScan for GenerateSeriesScan {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { Ok(self.generate_next()) })
    }
}
