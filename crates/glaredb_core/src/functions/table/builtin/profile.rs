use std::fmt::Debug;
use std::str::FromStr;
use std::task::Context;
use std::time::Duration;

use glaredb_error::{Result, ResultExt};
use uuid::Uuid;

use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF64,
    PhysicalI32,
    PhysicalU32,
    PhysicalU64,
    PhysicalUtf8,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::profile::QueryProfile;
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::TableScanFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::storage::projections::{ProjectedColumn, Projections};

// TODO: Should empty args for these just return the profiles for all queries?
// Doing so would enable something like:
// ```
// SELECT * FROM planning_profile()
//   WHERE query_id = (SELECT query_id FROM query_info());
// ```

pub const FUNCTION_SET_PLANNING_PROFILE: TableFunctionSet = TableFunctionSet {
    name: "planning_profile",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Get the timings generating during query planning.",
        arguments: &[],
        example: None,
    }],
    functions: &[
        // planning_profile()
        // Get profile for most recent query.
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &ProfileTableGen::new(PlanningProfileTable),
        ),
        // planning_profile(n)
        // Get profile for the nth query, starting at the most recent (0).
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Table),
            &ProfileTableGen::new(PlanningProfileTable),
        ),
        // planning_profile(id)
        // Get profile for specific query by id
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ProfileTableGen::new(PlanningProfileTable),
        ),
    ],
};

pub const FUNCTION_SET_OPTIMIZER_PROFILE: TableFunctionSet = TableFunctionSet {
    name: "optimizer_profile",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Get the timings generated for each optimizer rule.",
        arguments: &[],
        example: None,
    }],
    functions: &[
        // optimizer_profile()
        // Get profile for most recent query.
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &ProfileTableGen::new(OptimizerProfileTable),
        ),
        // optimizer_profile(n)
        // Get profile for the nth query, starting at the most recent (0).
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Table),
            &ProfileTableGen::new(OptimizerProfileTable),
        ),
        // optimizer_profile(id)
        // Get profile for specific query by id
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ProfileTableGen::new(OptimizerProfileTable),
        ),
    ],
};

pub const FUNCTION_SET_EXECUTION_PROFILE: TableFunctionSet = TableFunctionSet {
    name: "execution_profile",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Get the timings generating during query execution.",
        arguments: &[],
        example: None,
    }],
    functions: &[
        // execution_profile()
        // Get profile for most recent query.
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &ProfileTableGen::new(ExecutionProfileTable),
        ),
        // execution_profile(n)
        // Get profile for the nth query, starting at the most recent (0).
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Table),
            &ProfileTableGen::new(ExecutionProfileTable),
        ),
        // execution_profile(id)
        // Get profile for specific query by id
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Table),
            &ProfileTableGen::new(ExecutionProfileTable),
        ),
    ],
};

pub const FUNCTION_SET_QUERY_INFO: TableFunctionSet = TableFunctionSet {
    name: "query_info",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Get information about executed queries.",
        arguments: &[],
        example: None,
    }],
    functions: &[
        // query_info()
        // Get info for most recent query.
        RawTableFunction::new_scan(
            &Signature::new(&[], DataTypeId::Table),
            &ProfileTableGen::new(QueryInfoTable),
        ),
        // query_profile(n)
        // Get info for the nth query, starting at the most recent (0).
        RawTableFunction::new_scan(
            &Signature::new(&[DataTypeId::Int64], DataTypeId::Table),
            &ProfileTableGen::new(QueryInfoTable),
        ),
    ],
};

#[derive(Debug, Clone)]
pub struct ProfileColumn {
    pub name: &'static str,
    pub datatype: DataType,
}

impl ProfileColumn {
    pub const fn new(name: &'static str, datatype: DataType) -> Self {
        ProfileColumn { name, datatype }
    }
}

pub trait ProfileTable: Debug + Send + Sync + Clone + Copy + 'static {
    const COLUMNS: &[ProfileColumn];

    type Row: Debug + Sync + Send;

    /// Generate the column schema for the table.
    fn column_schema() -> ColumnSchema {
        ColumnSchema::new(
            Self::COLUMNS
                .iter()
                .map(|c| Field::new(c.name.to_string(), c.datatype.clone(), true)),
        )
    }

    /// Converts the query profile into rows.
    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>>;

    /// Scan the rows into the output batch.
    ///
    /// The number of rows provided will not exceed the write capacity of the
    /// batch.
    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct PlanningProfileTable;

// TODO: Show individual expression rewrite rules too.
#[derive(Debug)]
pub struct PlanningProfileRow {
    query_id: Uuid,
    step_order: usize,
    step: &'static str,
    duration_seconds: Option<f64>,
}

impl PlanningProfileRow {
    fn new(query_id: Uuid, step_order: usize, step: &'static str, dur: Option<Duration>) -> Self {
        PlanningProfileRow {
            query_id,
            step_order,
            step,
            duration_seconds: dur.map(|d| d.as_secs_f64()),
        }
    }
}

impl ProfileTable for PlanningProfileTable {
    const COLUMNS: &[ProfileColumn] = &[
        ProfileColumn::new("query_id", DataType::Utf8),
        ProfileColumn::new("step_order", DataType::Int32),
        ProfileColumn::new("step", DataType::Utf8),
        ProfileColumn::new("duration_seconds", DataType::Float64),
    ];

    type Row = PlanningProfileRow;

    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>> {
        let plan_prof = match &profile.plan {
            Some(plan_prof) => plan_prof,
            None => return Ok(Vec::new()),
        };

        let id = profile.id;

        Ok(vec![
            PlanningProfileRow::new(id, 0, "resolve_step", plan_prof.resolve_step),
            PlanningProfileRow::new(id, 1, "bind_step", plan_prof.bind_step),
            PlanningProfileRow::new(id, 2, "plan_logical_step", plan_prof.plan_logical_step),
            PlanningProfileRow::new(
                id,
                3,
                "plan_optimize_step",
                plan_prof.plan_optimize_step.as_ref().map(|s| s.total),
            ),
            PlanningProfileRow::new(id, 4, "plan_physical_step", plan_prof.plan_physical_step),
            PlanningProfileRow::new(
                id,
                5,
                "plan_executable_step",
                plan_prof.plan_executable_step,
            ),
        ])
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, array| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut ids = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    ids.put(idx, &row.query_id.to_string());
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut orders = PhysicalI32::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    orders.put(idx, &(row.step_order as i32));
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut steps = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    steps.put(idx, row.step);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let (data, validity) = array.data_and_validity_mut();
                let mut durations = PhysicalF64::get_addressable_mut(data)?;
                for (idx, row) in rows.iter().enumerate() {
                    match row.duration_seconds {
                        Some(dur) => durations.put(idx, &dur),
                        None => validity.set_invalid(idx),
                    }
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OptimizerProfileTable;

#[derive(Debug)]
pub struct OptimizerProfileRow {
    query_id: Uuid,
    rule_order: usize,
    rule_name: &'static str,
    duration_seconds: f64,
}

impl ProfileTable for OptimizerProfileTable {
    const COLUMNS: &[ProfileColumn] = &[
        ProfileColumn::new("query_id", DataType::Utf8),
        ProfileColumn::new("rule_order", DataType::Int32),
        ProfileColumn::new("rule", DataType::Utf8),
        ProfileColumn::new("duration_seconds", DataType::Float64),
    ];

    type Row = OptimizerProfileRow;

    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>> {
        let opt_prof = match profile
            .plan
            .as_ref()
            .and_then(|p| p.plan_optimize_step.as_ref())
        {
            Some(prof) => prof,
            None => return Ok(Vec::new()),
        };

        let rows = opt_prof
            .timings
            .iter()
            .enumerate()
            .map(|(idx, (name, timing))| OptimizerProfileRow {
                query_id: profile.id,
                rule_order: idx,
                rule_name: name,
                duration_seconds: timing.as_secs_f64(),
            })
            .collect();

        Ok(rows)
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, array| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut ids = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    ids.put(idx, &row.query_id.to_string());
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut orders = PhysicalI32::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    orders.put(idx, &(row.rule_order as i32));
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut rule_names = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    rule_names.put(idx, row.rule_name);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut durations = PhysicalF64::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    durations.put(idx, &row.duration_seconds);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutionProfileTable;

#[derive(Debug)]
pub struct ExecutionProfileRow {
    /// ID of this query.
    query_id: Uuid,
    /// Name of the operator.
    operator_name: &'static str,
    /// Identifier of the operator.
    operator_id: u32,
    /// Partition index within a pipeline.
    partition_idx: u32,
    /// Rows pushed into this operator.
    rows_in: u64,
    /// Rows produces by this operator.
    rows_out: u64,
    /// Time taken by this operator during its execution.
    execution_time_seconds: f64,
}

impl ProfileTable for ExecutionProfileTable {
    const COLUMNS: &[ProfileColumn] = &[
        ProfileColumn::new("query_id", DataType::Utf8),
        ProfileColumn::new("operator_name", DataType::Utf8),
        ProfileColumn::new("operator_id", DataType::UInt32),
        ProfileColumn::new("partition_idx", DataType::UInt32),
        ProfileColumn::new("rows_in", DataType::UInt64),
        ProfileColumn::new("rows_out", DataType::UInt64),
        ProfileColumn::new("execution_time_seconds", DataType::Float64),
    ];

    type Row = ExecutionProfileRow;

    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>> {
        let prof = match &profile.execution {
            Some(prof) => prof,
            None => return Ok(Vec::new()),
        };

        let query_id = profile.id;

        let rows = prof
            .partition_pipeline_profiles
            .iter()
            .flat_map(|part_prof| {
                part_prof
                    .operator_profiles
                    .iter()
                    .map(|op_prof| ExecutionProfileRow {
                        query_id,
                        operator_name: op_prof.operator_name,
                        operator_id: op_prof.operator_id.0 as u32,
                        partition_idx: part_prof.partition_idx as u32,
                        rows_in: op_prof.rows_in,
                        rows_out: op_prof.rows_out,
                        execution_time_seconds: op_prof.execution_duration.as_secs_f64(),
                    })
            })
            .collect();

        Ok(rows)
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, array| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut ids = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    ids.put(idx, &row.query_id.to_string());
                }
                Ok(())
            }
            ProjectedColumn::Data(1) => {
                let mut names = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    names.put(idx, row.operator_name);
                }
                Ok(())
            }
            ProjectedColumn::Data(2) => {
                let mut op_ids = PhysicalU32::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    op_ids.put(idx, &row.operator_id);
                }
                Ok(())
            }
            ProjectedColumn::Data(3) => {
                let mut part_indices = PhysicalU32::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    part_indices.put(idx, &row.partition_idx);
                }
                Ok(())
            }
            ProjectedColumn::Data(4) => {
                let mut rows_in = PhysicalU64::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    rows_in.put(idx, &row.rows_in);
                }
                Ok(())
            }
            ProjectedColumn::Data(5) => {
                let mut rows_out = PhysicalU64::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    rows_out.put(idx, &row.rows_out);
                }
                Ok(())
            }
            ProjectedColumn::Data(6) => {
                let mut times = PhysicalF64::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    times.put(idx, &row.execution_time_seconds);
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueryInfoTable;

impl ProfileTable for QueryInfoTable {
    const COLUMNS: &[ProfileColumn] = &[ProfileColumn::new("query_id", DataType::Utf8)];
    type Row = Uuid;

    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>> {
        Ok(vec![profile.id])
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, array| match col_idx {
            ProjectedColumn::Data(0) => {
                let mut ids = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    ids.put(idx, &row.to_string());
                }
                Ok(())
            }
            other => panic!("invalid projection {other:?}"),
        })
    }
}

#[derive(Debug)]
pub struct ProfileTableGenBindState {
    profile: Option<QueryProfile>,
}

#[derive(Debug)]
pub struct ProfileTableGenOperatorState {
    projections: Projections,
    profile: Option<QueryProfile>,
}

#[derive(Debug)]
pub struct ProfileTableGenPartitionState<T: ProfileTable> {
    row_idx: usize,
    rows: Vec<T::Row>,
}

#[derive(Debug, Clone, Copy)]
pub struct ProfileTableGen<T: ProfileTable> {
    _table: T,
}

impl<T> ProfileTableGen<T>
where
    T: ProfileTable,
{
    pub const fn new(table: T) -> Self {
        ProfileTableGen { _table: table }
    }
}

impl<T> TableScanFunction for ProfileTableGen<T>
where
    T: ProfileTable,
{
    type BindState = ProfileTableGenBindState;
    type OperatorState = ProfileTableGenOperatorState;
    type PartitionState = ProfileTableGenPartitionState<T>;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let profile = match input.positional.first() {
            Some(arg) => {
                // TODO: Do this automatically
                let arg = ConstFold::rewrite(arg.clone())?;
                let arg = arg.try_as_scalar()?;

                if arg.datatype().is_utf8() {
                    // We're querying by query_id.
                    let arg = arg.try_as_str()?;
                    let id = Uuid::from_str(arg).context("failed to parse query id as UUID")?;
                    db_context.profiles().get_profile_by_id(id)
                } else {
                    // We're querying by index.
                    let idx = arg.try_as_usize()?;
                    db_context.profiles().get_profile(idx)
                }
            }
            None => {
                // Default to just getting the most recent query.
                db_context.profiles().get_profile(0)
            }
        };

        Ok(TableFunctionBindState {
            state: ProfileTableGenBindState { profile },
            input,
            schema: T::column_schema(),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ProfileTableGenOperatorState {
            projections: projections.clone(),
            profile: bind_state.profile.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions >= 1);

        // Only a single partition will generate profile rows (if we even have a
        // profile).
        let mut states = match &op_state.profile {
            Some(profile) => {
                let rows = T::profile_as_rows(profile)?;
                vec![ProfileTableGenPartitionState { rows, row_idx: 0 }]
            }
            None => Vec::new(),
        };

        states.resize_with(partitions, || ProfileTableGenPartitionState {
            rows: Vec::new(),
            row_idx: 0,
        });

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let cap = output.write_capacity()?;
        let remaining = state.rows.len() - state.row_idx;
        let count = usize::min(cap, remaining);

        let rows = &state.rows[state.row_idx..(state.row_idx + count)];
        state.row_idx += count;

        T::scan(rows, &op_state.projections, output)?;
        output.set_num_rows(count)?;

        if count < cap {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}
