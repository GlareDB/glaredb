use std::fmt::Debug;
use std::task::Context;
use std::time::Duration;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalF64,
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
use crate::storage::projections::Projections;

pub const FUNCTION_SET_PLANNING_PROFILE: TableFunctionSet = TableFunctionSet {
    name: "planning_profile",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::General,
        description: "Get the timings generating during query planning.",
        arguments: &[],
        example: None,
    }),
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
            &Signature::new(&[DataTypeId::UInt64], DataTypeId::Table),
            &ProfileTableGen::new(PlanningProfileTable),
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

#[derive(Debug)]
pub struct PlanningProfileRow {
    step: &'static str,
    duration_seconds: Option<f64>,
}

impl PlanningProfileRow {
    fn new(step: &'static str, dur: Option<Duration>) -> Self {
        PlanningProfileRow {
            step,
            duration_seconds: dur.map(|d| d.as_secs_f64()),
        }
    }
}

impl ProfileTable for PlanningProfileTable {
    const COLUMNS: &[ProfileColumn] = &[
        ProfileColumn::new("step", DataType::Utf8),
        ProfileColumn::new("duration_seconds", DataType::Float64),
    ];

    type Row = PlanningProfileRow;

    fn profile_as_rows(profile: &QueryProfile) -> Result<Vec<Self::Row>> {
        let plan_prof = match &profile.plan {
            Some(plan_prof) => plan_prof,
            None => return Ok(Vec::new()),
        };

        Ok(vec![
            PlanningProfileRow::new("resolve_step", plan_prof.resolve_step),
            PlanningProfileRow::new("bind_step", plan_prof.bind_step),
            PlanningProfileRow::new("plan_logical_step", plan_prof.plan_logical_step),
            PlanningProfileRow::new(
                "plan_optimize_step",
                plan_prof.plan_optimize_step.as_ref().map(|s| s.total),
            ),
            PlanningProfileRow::new("plan_physical_step", plan_prof.plan_physical_step),
            PlanningProfileRow::new("plan_executable_step", plan_prof.plan_executable_step),
        ])
    }

    fn scan(rows: &[Self::Row], projections: &Projections, output: &mut Batch) -> Result<()> {
        projections.for_each_column(output, &mut |col_idx, array| match col_idx {
            0 => {
                let mut steps = PhysicalUtf8::get_addressable_mut(array.data_mut())?;
                for (idx, row) in rows.iter().enumerate() {
                    steps.put(idx, row.step);
                }
                Ok(())
            }
            1 => {
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
            other => panic!("invalid projection {other}"),
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
        let idx = match input.positional.get(0) {
            Some(arg) => arg.try_as_scalar()?.try_as_usize()?,
            None => 0,
        };

        let profile = db_context.profiles().get_profile(idx);

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
