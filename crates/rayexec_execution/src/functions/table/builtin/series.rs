use std::sync::Arc;
use std::task::{Context, Waker};

use futures::future::BoxFuture;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::physical_type::PhysicalI64;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};

use crate::database::DatabaseContext;
use crate::execution::operators::{PollFinalize, PollPull, PollPush};
use crate::functions::table::inout::{TableInOutFunction, TableInOutPartitionState};
use crate::functions::table::{PlannedTableFunction2, TableFunction, TableFunctionInputs};
use crate::functions::{FunctionInfo, Signature};
use crate::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerateSeries;

impl FunctionInfo for GenerateSeries {
    fn name(&self) -> &'static str {
        "generate_series"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Any,
            },
            Signature {
                positional_args: &[DataTypeId::Int64, DataTypeId::Int64, DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Any,
            },
        ]
    }
}

impl TableFunction for GenerateSeries {
    fn plan_and_initialize<'a>(
        &self,
        _context: &'a DatabaseContext,
        args: TableFunctionInputs,
    ) -> BoxFuture<'a, Result<Box<dyn PlannedTableFunction2>>> {
        Box::pin(async move { Self::plan_and_initialize_inner(args) })
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedTableFunction2>> {
        let mut packed = PackedDecoder::new(state);
        let start = packed.decode_next()?;
        let stop = packed.decode_next()?;
        let step = packed.decode_next()?;
        Ok(Box::new(GenerateSeriesI64 { start, stop, step }))
    }
}

impl GenerateSeries {
    fn plan_and_initialize_inner(
        args: TableFunctionInputs,
    ) -> Result<Box<dyn PlannedTableFunction2>> {
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

impl PlannedTableFunction2 for GenerateSeriesI64 {
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

#[derive(Debug, Clone)]
pub struct GenerateSeriesInOutImpl;

impl TableInOutFunction for GenerateSeriesInOutImpl {
    fn create_states(
        &self,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn TableInOutPartitionState>>> {
        let states: Vec<_> = (0..num_partitions)
            .map(|_| {
                Box::new(GenerateSeriesInOutPartitionState {
                    batch_size: 1024, // TODO
                    batch: None,
                    row_idx: 0,
                    finished: false,
                    params: SeriesParams::default(),
                    push_waker: None,
                    pull_waker: None,
                }) as _
            })
            .collect();

        Ok(states)
    }
}

#[derive(Debug, Clone, Default)]
struct SeriesParams {
    batch_size: usize,
    exhausted: bool,

    curr: i64,
    stop: i64,
    step: i64,
}

impl SeriesParams {
    /// Generate the next set of rows using the current parameters.
    fn generate_next(&mut self) -> Array {
        debug_assert!(!self.exhausted);

        let mut series: Vec<i64> = Vec::new();
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

        Array::new_with_array_data(DataType::Int64, PrimitiveStorage::from(series))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesInOutPartitionState {
    batch_size: usize,
    /// Batch we're working on.
    batch: Option<Batch>,
    /// Row index we're on.
    row_idx: usize,
    /// If we're finished.
    finished: bool,
    /// Current params.
    params: SeriesParams,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

impl TableInOutPartitionState for GenerateSeriesInOutPartitionState {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        if self.batch.is_some() {
            // Still processing current batch, come back later.
            self.push_waker = Some(cx.waker().clone());
            if let Some(pull_waker) = self.pull_waker.take() {
                pull_waker.wake();
            }
            return Ok(PollPush::Pending(batch));
        }

        self.batch = Some(batch);
        self.row_idx = 0;

        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push(&mut self, _cx: &mut Context) -> Result<PollFinalize> {
        self.finished = true;
        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(&mut self, cx: &mut Context) -> Result<PollPull> {
        let batch = match &self.batch {
            Some(batch) => batch,
            None => {
                if self.finished {
                    return Ok(PollPull::Exhausted);
                }

                // No batch to work on, come back later.
                self.pull_waker = Some(cx.waker().clone());
                if let Some(push_waker) = self.push_waker.take() {
                    push_waker.wake()
                }
                return Ok(PollPull::Pending);
            }
        };

        if self.params.exhausted {
            // Move to next row to process.
            self.row_idx += 1;

            if self.row_idx >= batch.num_rows() {
                // Need more input.
                self.batch = None;
                self.pull_waker = Some(cx.waker().clone());
                if let Some(push_waker) = self.push_waker.take() {
                    push_waker.wake()
                }

                return Ok(PollPull::Pending);
            }

            // Generate new params from row.
            let start =
                UnaryExecutor::value_at::<PhysicalI64>(batch.column(0).unwrap(), self.row_idx)?;
            let end =
                UnaryExecutor::value_at::<PhysicalI64>(batch.column(1).unwrap(), self.row_idx)?;

            // Use values from start/end if they're both not null. Otherwise use
            // parameters that produce an empty array.
            match (start, end) {
                (Some(start), Some(end)) => {
                    self.params = SeriesParams {
                        batch_size: self.batch_size,
                        exhausted: false,
                        curr: start,
                        stop: end,
                        step: 1, // TODO
                    }
                }
                _ => {
                    self.params = SeriesParams {
                        batch_size: self.batch_size,
                        exhausted: false,
                        curr: 1,
                        stop: 0,
                        step: 1,
                    }
                }
            }

            // TODO: Validate params.
        }

        let out = self.params.generate_next();
        let batch = Batch::try_new([out])?;

        Ok(PollPull::Computed(batch.into()))
    }
}
