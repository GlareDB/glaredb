use std::marker::PhantomData;
use std::ops::Div;

use glaredb_error::Result;
use num_traits::{PrimInt, Signed, Zero};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_LCM: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("lcm"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Calculates the least common multiple of two integers.",
        arguments: &["left", "right"],
        example: Some(Example {
            example: "lcm(12, 18)",
            output: "36",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Lcm::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Lcm::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Lcm::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Lcm::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Lcm::<PhysicalI128>::new(DataType::INT128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Lcm<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Lcm<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Lcm {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Lcm<S>
where
    S: MutableScalarStorage,
    S::StorageType: PrimInt + Signed + Div<Output = S::StorageType>,
{
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: self.return_type.clone(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| {
                if a.is_zero() || b.is_zero() {
                    buf.put(&S::StorageType::zero());
                    return;
                }

                let abs_a = a.abs();
                let abs_b = b.abs();

                let mut x = abs_a;
                let mut y = abs_b;

                while !y.is_zero() {
                    let temp = y;
                    y = x % y;
                    x = temp;
                }

                let gcd = x;

                let lcm = (abs_a / gcd) * abs_b;

                buf.put(&lcm);
            },
        )
    }
}
