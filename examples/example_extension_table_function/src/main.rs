//! Example of defining a custom extension that exports a table scan function.

use std::error::Error;
use std::task::Context;

use glaredb_core::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI64,
};
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::field::{ColumnSchema, Field};
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::error::Result;
use glaredb_core::execution::operators::{ExecutionProperties, PollPull};
use glaredb_core::extension::{Extension, ExtensionFunctions, ExtensionTableFunction};
use glaredb_core::functions::Signature;
use glaredb_core::functions::documentation::{Category, Documentation, Example};
use glaredb_core::functions::function_set::TableFunctionSet;
use glaredb_core::functions::table::scan::{ScanContext, TableScanFunction};
use glaredb_core::functions::table::{
    RawTableFunction,
    TableFunctionBindState,
    TableFunctionInput,
};
use glaredb_core::statistics::value::StatisticsValue;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};

/// Unit struct that we'll define the extension on.
#[derive(Debug, Clone, Copy)]
pub struct MyCustomExtension;

impl Extension for MyCustomExtension {
    const NAME: &str = "my_custom_extension";
    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        // This will create a 'schema' that holds all the functions for this
        // extension.
        namespace: "custom",
        // List of scalar functions exported by this extension.
        scalar: &[],
        aggregate: &[],
        table: &[ExtensionTableFunction::new(&FUNCTION_SET_REPEAT)],
    });
}

pub const FUNCTION_SET_REPEAT: TableFunctionSet = TableFunctionSet {
    // Name of the function.
    name: "repeat",
    // Optional set of aliases for this function.
    aliases: &[],
    // Optional documentation objects. These are used when producing out of the
    // `list_functions` table function.
    doc: &[&Documentation {
        category: Category::Table,
        description: "Repeat a value the specified number of times.",
        arguments: &["value", "count"],
        example: None,
    }],
    // The function implementations.
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Table),
        // Reference to the unit struct implementing the table scan function.
        &Repeat,
    )],
};

/// Unit struct we'll be defining our custom table scan function on.
#[derive(Debug, Clone, Copy)]
pub struct Repeat;

#[derive(Debug)]
pub struct RepeatBindState {
    value: i64,
    count: i64,
}

#[derive(Debug)]
pub struct RepeatOperatorState {
    value: i64,
    count: i64,
    projections: Projections,
}

#[derive(Debug)]
pub struct RepeatPartitionState {
    remaining: usize,
}

impl TableScanFunction for Repeat {
    type BindState = RepeatBindState;
    type OperatorState = RepeatOperatorState;
    type PartitionState = RepeatPartitionState;

    async fn bind(
        _scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let value = input.positional[0]
            .clone()
            .try_into_scalar()?
            .try_as_i64()?;
        let count = input.positional[1]
            .clone()
            .try_into_scalar()?
            .try_as_i64()?;

        Ok(TableFunctionBindState {
            state: RepeatBindState { value, count },
            input,
            data_schema: ColumnSchema::new([Field::new("repeat", DataType::int64(), false)]),
            meta_schema: None,
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(RepeatOperatorState {
            value: bind_state.value,
            count: bind_state.count,
            projections,
        })
    }

    fn create_pull_partition_states(
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions > 0);

        let total_count = op_state.count.max(0) as usize;
        let states = (0..partitions)
            .map(|partition_idx| {
                let start = (total_count * partition_idx) / partitions;
                let end = (total_count * (partition_idx + 1)) / partitions;
                RepeatPartitionState {
                    remaining: end - start,
                }
            })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let out_cap = output.write_capacity()?;
        let count = usize::min(state.remaining, out_cap);

        if count == 0 {
            return Ok(PollPull::Exhausted);
        }

        op_state
            .projections
            .for_each_column(output, &mut |col, arr| match col {
                ProjectedColumn::Data(0) => {
                    let mut data = PhysicalI64::get_addressable_mut(arr.data_mut())?;
                    for idx in 0..count {
                        data.put(idx, &op_state.value);
                    }
                    Ok(())
                }
                other => panic!("invalid projection: {other:?}"),
            })?;

        state.remaining -= count;
        output.set_num_rows(count)?;

        if state.remaining > 0 {
            Ok(PollPull::HasMore)
        } else {
            Ok(PollPull::Exhausted)
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    // Engine setup.
    let executor = ThreadedNativeExecutor::try_new()?;
    let runtime = NativeSystemRuntime::new(tokio::runtime::Handle::current());
    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;

    // Register our extension.
    engine.register_extension(MyCustomExtension)?;

    // Helper function to run a query and print the results.
    async fn query_and_print(
        engine: &SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>,
        query: &str,
    ) -> Result<(), Box<dyn Error>> {
        let mut q_res = engine.session().query(query).await?;
        let batches = q_res.output.collect().await?;
        let table =
            PrettyTable::try_new(&q_res.output_schema, &batches, 100, None, PRETTY_COMPONENTS)?;
        println!("{query}");
        println!("{table}");
        println!();
        Ok(())
    }

    println!("=== Custom Table Scan Function Example ===");
    println!("Demonstrating the 'repeat' table function that repeats a value N times.\n");

    query_and_print(&engine, "SELECT * FROM custom.repeat(5, 3)").await?;

    query_and_print(&engine, "SELECT * FROM custom.repeat(10, 0)").await?;

    query_and_print(&engine, "SELECT * FROM custom.repeat(-7, 2)").await?;

    query_and_print(&engine, "SELECT * FROM custom.repeat(42, 8)").await?;

    query_and_print(
        &engine,
        "SELECT * FROM custom.repeat(100, 5) WHERE repeat > 50",
    )
    .await?;

    query_and_print(
        &engine,
        "SELECT COUNT(*), SUM(repeat) FROM custom.repeat(3, 4)",
    )
    .await?;

    query_and_print(
        &engine,
        "SELECT r.repeat, g.generate_series 
         FROM custom.repeat(7, 3) r 
         CROSS JOIN generate_series(1, 2) g",
    )
    .await?;

    // Get information about our custom function.
    query_and_print(
        &engine,
        "SELECT function_name, alias_of, description, example, example_output 
         FROM list_functions() 
         WHERE schema_name = 'custom'",
    )
    .await?;

    println!("=== Example completed successfully! ===");

    Ok(())
}
