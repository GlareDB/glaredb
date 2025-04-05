use std::fmt::Debug;
use std::task::Context;

use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::DataType;
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_core::catalog::context::DatabaseContext;
use glaredb_core::execution::operators::{ExecutionProperties, PollPull};
use glaredb_core::functions::table::scan::TableScanFunction;
use glaredb_core::functions::table::{TableFunctionBindState, TableFunctionInput};
use glaredb_core::logical::statistics::StatisticsValue;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::storage::projections::Projections;
use glaredb_error::Result;

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

    /// Creates a row iterator using an optional scale factor.
    ///
    /// If generating a table requires a scale factor (e.g. lineimtem), this
    /// should error.
    ///
    /// If a table _doesn't_ require a scale factor, but is provided one, then
    /// it should ignore it (e.g. region).
    ///
    /// The discrepancy in behavior here is to provide consistency when
    /// generating all tables at the same time and not having to worry about
    /// which table needs a scale factor, and which doesn't.
    fn create_row_iter(sf: Option<f64>) -> Result<Self::RowIter>;

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

pub struct TableGenBindState {
    scale_factor: Option<f64>,
}

pub struct TableGenOperatorState {
    scale_factor: Option<f64>,
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

    async fn bind(
        &'static self,
        _db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        // TODO: Use named arguments.
        let scale_factor = match input.positional.first() {
            Some(arg) => {
                // TODO: Would be nice not having to worry about const
                // folding in the functions themselves.
                let arg = ConstFold::rewrite(arg.clone())?;
                Some(arg.try_as_scalar()?.try_as_f64()?)
            }
            None => None,
        };

        Ok(TableFunctionBindState {
            state: TableGenBindState { scale_factor },
            input,
            schema: T::column_schema(),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(TableGenOperatorState {
            scale_factor: bind_state.scale_factor,
            projections,
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions >= 1);

        // Single threaded for now, one partition generates, all others just
        // immediately exhuast.
        let mut states = vec![TableGenPartitionState {
            row_iter: Some(T::create_row_iter(op_state.scale_factor)?),
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

        // Generate the next batch of rows.
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
