use std::marker::PhantomData;

use glaredb_error::Result;
use num_traits::{Signed, Zero};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    MutableScalarStorage, PhysicalI8, PhysicalI16, PhysicalI32, PhysicalI64, PhysicalI128,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_GCD: ScalarFunctionSet = ScalarFunctionSet {
    name: "gcd",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Numeric,
        description: "Compute the greatest common divisor of two integers.",
        arguments: &["a", "b"],
        example: Some(Example {
            example: "gcd(12, 8)",
            output: "4",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Gcd::<PhysicalI8>::new(DataType::INT8),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Gcd::<PhysicalI16>::new(DataType::INT16),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Gcd::<PhysicalI32>::new(DataType::INT32),
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Gcd::<PhysicalI64>::new(DataType::INT64),
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Gcd::<PhysicalI128>::new(DataType::INT128),
        ),
    ],
};

#[derive(Debug, Clone, Copy)]
pub struct Gcd<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Gcd<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Gcd {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Gcd<S>
where
    S: MutableScalarStorage,
    S::StorageType: Signed + std::ops::Rem<Output = S::StorageType> + Copy + PartialEq,
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
                let mut a = a.abs();
                let mut b = b.abs();

                if a == S::StorageType::zero() {
                    return buf.put(&b);
                }
                if b == S::StorageType::zero() {
                    return buf.put(&a);
                }

                while b != S::StorageType::zero() {
                    let temp = b;
                    b = a % b;
                    a = temp;
                }

                buf.put(&a)
            },
        )
    }
}
