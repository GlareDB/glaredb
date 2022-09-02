use crate::arrow::datatype::{DataType, GetArrowDataType};
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{internal, LemurError, Result};
use arrow2::array::{Array, BooleanArray, PrimitiveArray};
use arrow2::compute::filter::filter;
use arrow2::compute::{
    arithmetics::basic::{add, add_scalar},
    boolean::{is_not_null, is_null, not},
    cast::{cast, CastOptions},
    comparison::{can_eq, eq_and_validity, eq_scalar_and_validity},
    concatenate::concatenate,
};
use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::types::NativeType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod compute;

/// A boolean column.
///
/// Useful to have this as a concrete type since it's used as a mask.
#[derive(Debug, Clone, Copy)]
pub struct BoolColumn<'a>(&'a BooleanArray);

impl<'a> BoolColumn<'a> {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Clone)]
pub struct Column(pub(crate) Arc<dyn Array + 'static>);

impl Column {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn try_downcast_bool(&self) -> Option<BoolColumn<'_>> {
        if !matches!(self.0.data_type(), ArrowDataType::Boolean) {
            return None;
        }
        self.0
            .as_any()
            .downcast_ref::<BooleanArray>()
            .and_then(|b| Some(BoolColumn(b)))
    }

    pub fn get_datatype(&self) -> Result<DataType> {
        let arrow = self.0.data_type();
        arrow.try_into()
    }

    pub fn filter(&self, mask: BoolColumn<'_>) -> Result<Self> {
        let arr = filter(self.0.as_ref(), &mask.0)?;
        Ok(Column(arr.into()))
    }

    pub fn concat(&self, other: &Self) -> Result<Self> {
        let arr = concatenate(&[self.0.as_ref(), other.0.as_ref()])?;
        Ok(Column(arr.into()))
    }

    pub fn slice(&self, offset: usize, len: usize) -> Result<Column> {
        if offset + len > self.len() {
            return Err(LemurError::RangeOutOfBounds { offset, len });
        }
        let arr = self.0.as_ref().slice(offset, len);
        Ok(Column(arr.into()))
    }

    pub fn cast(&self, dt: DataType) -> Result<Column> {
        let opts = CastOptions {
            wrapped: false,
            partial: false,
        };
        let arr = cast(self.0.as_ref(), &dt.into(), opts)?;
        Ok(Column(arr.into()))
    }

    pub fn is_null(&self) -> Result<Column> {
        let arr = is_null(self.0.as_ref());
        Ok(Column(arr.to_boxed().into()))
    }

    pub fn is_not_null(&self) -> Result<Column> {
        let arr = is_not_null(self.0.as_ref());
        Ok(Column(arr.to_boxed().into()))
    }

    pub fn not(&self) -> Result<Column> {
        let bools = self.try_downcast_bool().ok_or(LemurError::TypeMismatch)?;
        let arr = not(bools.0);
        Ok(Column(arr.to_boxed().into()))
    }
}

impl GetArrowDataType for Column {
    fn get_arrow_data_type(&self) -> ArrowDataType {
        self.0.data_type().clone()
    }
}

impl From<Box<dyn Array>> for Column {
    fn from(arr: Box<dyn Array>) -> Self {
        Column(arr.into())
    }
}

impl From<Arc<dyn Array>> for Column {
    fn from(arr: Arc<dyn Array>) -> Self {
        Column(arr)
    }
}

impl<T: NativeType> From<PrimitiveArray<T>> for Column {
    fn from(arr: PrimitiveArray<T>) -> Self {
        Column(arr.arced())
    }
}

impl From<BooleanArray> for Column {
    fn from(arr: BooleanArray) -> Self {
        Column(arr.arced())
    }
}

// Note this is pretty inefficient. Currently doing this to avoid implementing
// compute ops on scalars.
impl From<ScalarOwned> for Column {
    fn from(scalar: ScalarOwned) -> Self {
        use arrow2::array::{BinaryArray, NullArray, PrimitiveArray, Utf8Array};
        match scalar {
            ScalarOwned::Null => NullArray::new(ArrowDataType::Null, 1).boxed().into(),
            ScalarOwned::Bool(None) => NullArray::new(ArrowDataType::Boolean, 1).boxed().into(),
            ScalarOwned::Bool(Some(b)) => BooleanArray::from_slice(&[b]).boxed().into(),
            ScalarOwned::Int8(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Int16(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Int32(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Int64(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Uint8(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Uint16(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Uint32(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Uint64(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Float32(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Float64(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Binary(v) => BinaryArray::<i32>::from([v]).boxed().into(),
            ScalarOwned::Utf8(v) => Utf8Array::<i32>::from([v]).boxed().into(),
            ScalarOwned::Date32(v) => PrimitiveArray::from([v]).boxed().into(),
            ScalarOwned::Date64(v) => PrimitiveArray::from([v]).boxed().into(),
        }
    }
}
