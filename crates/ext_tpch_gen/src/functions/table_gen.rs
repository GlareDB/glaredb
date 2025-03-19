use std::fmt::Debug;
use std::task::Context;

use glaredb_error::Result;
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::datatype::DataType;
use glaredb_execution::arrays::field::{ColumnSchema, Field};
use glaredb_execution::catalog::context::DatabaseContext;
use glaredb_execution::execution::operators::{ExecutionProperties, PollPull};
use glaredb_execution::functions::table::scan::TableScanFunction;
use glaredb_execution::functions::table::{TableFunctionBindState, TableFunctionInput};
use glaredb_execution::logical::statistics::StatisticsValue;
use glaredb_execution::storage::projections::Projections;

/// Describes a single column in a tpch table.
#[derive(Debug)]
pub struct TpchColumn {
    pub name: &'static str,
    pub datatype: DataType,
}

impl TpchColumn {
    pub const fn new(name: &'static str, datatype: DataType) -> Self {
        TpchColumn { name, datatype }
    }
}

pub trait TpchTable: Debug + Clone + Copy + Sync + Send + 'static {
    /// Column descriptions.
    const COLUMNS: &[TpchColumn];

    type RowIter: Iterator<Item = Self::Row> + Sync + Send;
    type Row: Sync + Send;

    /// Creates a row iterator using the provided scale factor.
    ///
    /// May be ignored if the table doesn't take into account a scale factor
    /// (e.g. region).
    fn create_row_iter(sf: f64) -> Self::RowIter;

    /// Generate the column schema for the table.
    fn column_schema() -> ColumnSchema {
        ColumnSchema::new(
            Self::COLUMNS
                .iter()
                .map(|c| Field::new(c.name.to_string(), c.datatype.clone(), false)),
        )
    }

    /// Scan the rows into the output batch.
    ///
    /// The number of rows provided will not exceed the write capacity of the
    /// batch.
    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()>;
}

pub struct TableGenBindState {}

pub struct TableGenOperatorState {
    projections: Projections,
}

pub struct TableGenPartitionState<T: TpchTable> {
    row_iter: Option<T::RowIter>,
    row_buffer: Vec<T::Row>,
}

#[derive(Debug, Clone, Copy)]
pub struct TableGen<T: TpchTable> {
    _table: T,
}

impl<T> TableGen<T>
where
    T: TpchTable,
{
    pub const fn new(table: T) -> Self {
        TableGen { _table: table }
    }
}

impl<T> TableScanFunction for TableGen<T>
where
    T: TpchTable,
{
    type BindState = TableGenBindState;
    type OperatorState = TableGenOperatorState;
    type PartitionState = TableGenPartitionState<T>;

    fn bind<'a>(
        &self,
        _db_context: &'a DatabaseContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + Sync + Send + 'a
    {
        async {
            Ok(TableFunctionBindState {
                state: TableGenBindState {},
                input,
                schema: T::column_schema(),
                cardinality: StatisticsValue::Unknown,
            })
        }
    }

    fn create_pull_operator_state(
        _bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(TableGenOperatorState {
            projections: projections.clone(),
        })
    }

    fn create_pull_partition_states(
        _op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions >= 1);

        // Single threaded for now, one partition generates, all others just
        // immediately exhuast.
        let mut states = vec![TableGenPartitionState {
            row_iter: Some(T::create_row_iter(0.01)),
            row_buffer: Vec::with_capacity(props.batch_size),
        }];
        states.resize_with(partitions, || TableGenPartitionState {
            row_iter: None,
            row_buffer: Vec::new(),
        });

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let row_iter = match state.row_iter.as_mut() {
            Some(iter) => iter,
            None => {
                // This partition isn't generating anything.
                output.set_num_rows(0)?;
                return Ok(PollPull::Exhausted);
            }
        };

        let cap = output.write_capacity()?;

        // Generate the next batch of regions.
        state.row_buffer.clear();
        state.row_buffer.extend(row_iter.take(cap));

        T::scan(&state.row_buffer, &op_state.projections, output)?;

        let count = state.row_buffer.len();
        output.set_num_rows(count)?;

        if count < cap {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}
