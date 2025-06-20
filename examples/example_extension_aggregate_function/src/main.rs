//! Example of defining a custom extension that exports an aggregate function.

use std::error::Error;
use std::fmt::Debug;

use glaredb_core::arrays::array::physical_type::{AddressableMut, PhysicalI64};
use glaredb_core::arrays::datatype::{DataType, DataTypeId};
use glaredb_core::arrays::executor::PutBuffer;
use glaredb_core::arrays::executor::aggregate::AggregateState;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::error::Result;
use glaredb_core::expr::Expression;
use glaredb_core::extension::{Extension, ExtensionFunctions};
use glaredb_core::functions::Signature;
use glaredb_core::functions::aggregate::RawAggregateFunction;
use glaredb_core::functions::aggregate::simple::{SimpleUnaryAggregate, UnaryAggregate};
use glaredb_core::functions::bind_state::BindState;
use glaredb_core::functions::documentation::{Category, Documentation};
use glaredb_core::functions::function_set::AggregateFunctionSet;
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};

#[derive(Debug, Clone, Copy)]
pub struct MyCustomExtension;

impl Extension for MyCustomExtension {
    const NAME: &str = "my_custom_extension";
    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        namespace: "custom",
        scalar: &[],
        aggregate: &[&FUNCTION_SET_DOUBLE_SUM],
        table: &[],
    });
}

pub const FUNCTION_SET_DOUBLE_SUM: AggregateFunctionSet = AggregateFunctionSet {
    name: "double_sum",
    aliases: &["double_sum_alias"],
    doc: &[&Documentation {
        category: Category::GENERAL_PURPOSE_AGGREGATE,
        description: "Computes the sum of all non-NULL integer inputs and doubles the result.",
        arguments: &["input"],
        example: None,
    }],
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Int64], DataTypeId::Int64),
        &SimpleUnaryAggregate::new(&DoubleSum),
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct DoubleSum;

impl UnaryAggregate for DoubleSum {
    type Input = PhysicalI64;
    type Output = PhysicalI64;

    type BindState = ();
    type GroupState = DoubleSumState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        Default::default()
    }
}

#[derive(Debug, Default)]
pub struct DoubleSumState {
    sum: i64,
    valid: bool,
}

impl AggregateState<&i64, i64> for DoubleSumState {
    type BindState = ();

    fn merge(&mut self, _state: &(), other: &mut Self) -> Result<()> {
        self.sum = self.sum.wrapping_add(other.sum);
        self.valid = self.valid || other.valid;
        Ok(())
    }

    fn update(&mut self, _state: &(), &input: &i64) -> Result<()> {
        self.sum = self.sum.wrapping_add(input);
        self.valid = true;
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &(), output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = i64>,
    {
        if self.valid {
            let double = self.sum * 2;
            output.put(&double);
        } else {
            output.put_null();
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let executor = ThreadedNativeExecutor::try_new()?;
    let runtime = NativeSystemRuntime::new(tokio::runtime::Handle::current());
    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;

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

    query_and_print("SELECT custom.double_sum(a) FROM generate_series(1, 5) g(a)").await?;

    query_and_print("SELECT custom.double_sum(a) FROM generate_series(1, 10) g(a) WHERE false")
        .await?;

    query_and_print("SELECT custom.double_sum_alias(a) FROM generate_series(1, 3) g(a)").await?;

    query_and_print("SELECT custom.double_sum(a) FROM VALUES (4), (5), (NULL), (7) v(a)").await?;

    query_and_print("SELECT function_name, alias_of, description FROM list_functions() WHERE schema_name = 'custom'").await?;

    Ok(())
}
