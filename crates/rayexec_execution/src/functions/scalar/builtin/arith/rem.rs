use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::{
    MutableScalarStorage,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;

pub const FUNCTION_SET_REM: ScalarFunctionSet = ScalarFunctionSet {
    name: "%",
    aliases: &["rem"],
    doc: None,
    functions: &[
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float16, DataTypeId::Float16],
                DataTypeId::Float16,
            ),
            &Rem::<PhysicalF16>::new(&DataType::Float16),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float32, DataTypeId::Float32],
                DataTypeId::Float32,
            ),
            &Rem::<PhysicalF32>::new(&DataType::Float32),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Float64, DataTypeId::Float64],
                DataTypeId::Float64,
            ),
            &Rem::<PhysicalF64>::new(&DataType::Float64),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int8, DataTypeId::Int8], DataTypeId::Int8),
            &Rem::<PhysicalI8>::new(&DataType::Int8),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int16, DataTypeId::Int16], DataTypeId::Int16),
            &Rem::<PhysicalI16>::new(&DataType::Int16),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int32, DataTypeId::Int32], DataTypeId::Int32),
            &Rem::<PhysicalI32>::new(&DataType::Int32),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::Int64, DataTypeId::Int64], DataTypeId::Int64),
            &Rem::<PhysicalI64>::new(&DataType::Int64),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::Int128, DataTypeId::Int128],
                DataTypeId::Int128,
            ),
            &Rem::<PhysicalI128>::new(&DataType::Int128),
        ),
        RawScalarFunction::new(
            Signature::new(&[DataTypeId::UInt8, DataTypeId::UInt8], DataTypeId::UInt8),
            &Rem::<PhysicalU8>::new(&DataType::UInt8),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt16, DataTypeId::UInt16],
                DataTypeId::UInt16,
            ),
            &Rem::<PhysicalU16>::new(&DataType::UInt16),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt32, DataTypeId::UInt32],
                DataTypeId::UInt32,
            ),
            &Rem::<PhysicalU32>::new(&DataType::UInt32),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt64, DataTypeId::UInt64],
                DataTypeId::UInt64,
            ),
            &Rem::<PhysicalU64>::new(&DataType::UInt64),
        ),
        RawScalarFunction::new(
            Signature::new(
                &[DataTypeId::UInt128, DataTypeId::UInt128],
                DataTypeId::UInt128,
            ),
            &Rem::<PhysicalU128>::new(&DataType::UInt128),
        ),
    ],
};

#[derive(Debug, Clone)]
pub struct Rem<S> {
    return_type: &'static DataType,
    _s: PhantomData<S>,
}

impl<S> Rem<S> {
    pub const fn new(return_type: &'static DataType) -> Self {
        Rem {
            return_type,
            _s: PhantomData,
        }
    }
}

impl<S> ScalarFunction for Rem<S>
where
    S: MutableScalarStorage,
    S::StorageType: std::ops::Rem<Output = S::StorageType> + Sized + Copy,
{
    type State = ();

    fn bind(
        &self,
        _table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: self.return_type.clone(),
            inputs,
        })
    }

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let a = &input.arrays()[0];
        let b = &input.arrays()[1];

        BinaryExecutor::execute::<S, S, S, _>(
            a,
            sel,
            b,
            sel,
            OutBuffer::from_array(output)?,
            |&a, &b, buf| buf.put(&(a % b)),
        )
    }
}
