use crate::{
    database::table::{DataTable, DataTableScan, EmptyTableScan},
    execution::operators::PollPull,
    runtime::ExecutionRuntime,
};
use futures::future::BoxFuture;
use rayexec_bullet::{
    array::{Array, Int64Array},
    batch::Batch,
    datatype::DataType,
    field::{Field, Schema},
};
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, task::Context};

use super::{PlannedTableFunction, TableFunction, TableFunctionArgs};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerateSeries;

impl TableFunction for GenerateSeries {
    fn name(&self) -> &'static str {
        "generate_series"
    }

    fn plan_and_initialize(
        &self,
        _runtime: &Arc<dyn ExecutionRuntime>,
        args: TableFunctionArgs,
    ) -> BoxFuture<Result<Box<dyn PlannedTableFunction>>> {
        Box::pin(async move { Self::plan_and_initialize_inner(args) })
    }

    fn state_deserialize(
        &self,
        deserializer: &mut dyn erased_serde::Deserializer,
    ) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(GenerateSeriesI64::deserialize(deserializer)?))
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
    fn serializable_state(&self) -> &dyn erased_serde::Serialize {
        self
    }

    fn table_function(&self) -> &dyn TableFunction {
        &GenerateSeries
    }

    fn schema(&self) -> Schema {
        Schema::new([Field::new("generate_series", DataType::Int64, false)])
    }

    fn datatable(&self, _runtime: &Arc<dyn ExecutionRuntime>) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(self.clone()))
    }
}

impl DataTable for GenerateSeriesI64 {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(GenerateSeriesScan {
            batch_size: 1024,
            exhausted: false,
            curr: self.start,
            stop: self.stop,
            step: self.step,
        })];
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

        let col = Array::Int64(Int64Array::from(series));
        let batch = Batch::try_new([col]).expect("batch to be valid");

        Some(batch)
    }
}

impl DataTableScan for GenerateSeriesScan {
    fn poll_pull(&mut self, _cx: &mut Context) -> Result<PollPull> {
        match self.generate_next() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => Ok(PollPull::Exhausted),
        }
    }
}
