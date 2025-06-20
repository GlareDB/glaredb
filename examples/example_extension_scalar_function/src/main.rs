//! Example of defining a custom extension that exports a scalar function.
//!
//! Our custom "double_it" function accepts an integer and doubles it.

use std::error::Error;

use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::PhysicalI64;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::executor::OutBuffer;
use glaredb_core::arrays::executor::scalar::UnaryExecutor;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::error::Result;
use glaredb_core::expr::Expression;
use glaredb_core::extension::{Extension, ExtensionFunctions};
use glaredb_core::functions::Signature;
use glaredb_core::functions::bind_state::BindState;
use glaredb_core::functions::documentation::{Category, Documentation, Example};
use glaredb_core::functions::function_set::ScalarFunctionSet;
use glaredb_core::functions::scalar::{RawScalarFunction, ScalarFunction};
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
        scalar: &[&FUNCTION_SET_DOUBLE_IT],
        aggregate: &[],
        table: &[],
    });
}

/// Defines a "double_it" function set which can hold some number of function
/// implementations. For example, we might have multiple implementations
/// depending on the input type. This example will only use a single function
/// implementation.
pub const FUNCTION_SET_DOUBLE_IT: ScalarFunctionSet = ScalarFunctionSet {
    // Name of the function.
    name: "double_it",
    // Optional set of aliases for this function.
    aliases: &["double_it_alias"],
    // Optional documentation objects. These are used when producing out of the
    // `list_functions` table function.
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Doubles the input value.",
        arguments: &["input"],
        example: Some(Example {
            example: "double_it(4)",
            output: "8",
        }),
    }],
    // The function implementations.
    functions: &[RawScalarFunction::new(
        // This function accepts a single int64 and produces an int64.
        &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
        // Reference to the unit struct implementing the int64 -> int64 function.
        &DoubleIt,
    )],
};

/// Unit struct we'll be defining our custom function on.
#[derive(Debug, Clone, Copy)]
pub struct DoubleIt;

impl ScalarFunction for DoubleIt {
    /// This function requires no state.
    type State = ();

    /// Do any pre-processing of the expressions. Since this function is
    /// stateless, there's nothing for us to do.
    ///
    /// Note that inputs will only contain expressions that exactly match
    /// signature on the function set.
    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    /// Execute the function, writing the results to `output`.
    fn execute(_: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        // UnaryExecutor should be used for functions that accept a single
        // input. This will handle selections/nulls automatically
        UnaryExecutor::execute::<PhysicalI64, PhysicalI64, _>(
            &input.arrays()[0],             // The '0'th array is guaranteed to exist.
            input.selection(),              // Get the selection from the input batch.
            OutBuffer::from_array(output)?, // Helper for destructuring the output array.
            |v, buf| {
                // Our core logic.
                let value = v * 2;
                // This will put the value into the output buffer.
                buf.put(&value)
            },
        )
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

    // Helper to run a query and print the results.
    let query_and_print = async |query: &str| -> Result<()> {
        let mut q_res = engine.session().query(query).await?;
        let batches = q_res.output.collect().await?;
        let table =
            PrettyTable::try_new(&q_res.output_schema, &batches, 100, None, PRETTY_COMPONENTS)?;
        println!("{query}");
        println!("{table}");
        Ok(())
    };

    // Now we can call our custom double_it function. Note that this function is
    // in the 'custom' schema which is automatically created during extension
    // registration.
    query_and_print("SELECT a, custom.double_it(a) FROM generate_series(1, 5) g(a)").await?;

    // Can also call our function with the alias.
    query_and_print("SELECT a, custom.double_it_alias(a) FROM generate_series(1, 5) g(a)").await?;

    // NULLs are automatically handled for us.
    query_and_print("SELECT a, custom.double_it(a) FROM VALUES (4), (5), (NULL), (7) v(a)").await?;

    // Casting is also handled automatically.
    query_and_print(
        "SELECT a, custom.double_it(a) FROM (SELECT a::TINYINT FROM VALUES (4), (5), (6), (7) v(a))",
    )
    .await?;

    // Get information about our custom function.
    query_and_print("SELECT function_name, alias_of, description, example, example_output FROM list_functions() WHERE schema_name = 'custom'").await?;

    Ok(())
}
