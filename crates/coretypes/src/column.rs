use crate::datatype::{DataType, DataValue, NullableType};
use bitvec::vec::BitVec;
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
#[error("type error, expected {expected}, got {got}")]
pub struct TypeError {
    expected: DataType,
    got: DataType,
}

pub trait NativeType: Debug + Sync + Send {
    const DATATYPE: DataType;
}

impl NativeType for bool {
    const DATATYPE: DataType = DataType::Bool;
}

impl NativeType for i8 {
    const DATATYPE: DataType = DataType::Int8;
}

impl NativeType for i16 {
    const DATATYPE: DataType = DataType::Int16;
}

impl NativeType for i32 {
    const DATATYPE: DataType = DataType::Int32;
}

impl NativeType for i64 {
    const DATATYPE: DataType = DataType::Int64;
}

impl NativeType for f32 {
    const DATATYPE: DataType = DataType::Float32;
}

impl NativeType for f64 {
    const DATATYPE: DataType = DataType::Float64;
}

impl NativeType for str {
    const DATATYPE: DataType = DataType::Utf8;
}

impl NativeType for [u8] {
    const DATATYPE: DataType = DataType::Binary;
}

pub trait FixedlenType: NativeType + Into<DataValue<'static>> + Copy + Default + PartialEq {}

impl FixedlenType for bool {}
impl FixedlenType for i8 {}
impl FixedlenType for i16 {}
impl FixedlenType for i32 {}
impl FixedlenType for i64 {}
impl FixedlenType for f32 {}
impl FixedlenType for f64 {}

#[derive(Debug)]
struct FixedlenColumn<T> {
    validity: BitVec,
    values: Vec<T>,
}

impl<T: FixedlenType> FixedlenColumn<T> {
    fn new() -> Self {
        FixedlenColumn {
            validity: BitVec::new(),
            values: Vec::new(),
        }
    }

    fn get_type(&self) -> NullableType {
        NullableType {
            datatype: T::DATATYPE,
            nullable: true,
        }
    }

    fn push_native(&mut self, val: T) {
        self.validity.push(true);
        self.values.push(val);
    }

    fn push_null(&mut self) {
        self.validity.push(false);
        self.values.push(T::default());
    }

    fn insert_native(&mut self, idx: usize, val: T) {
        self.validity.insert(idx, true);
        self.values.insert(idx, val);
    }

    fn get_value(&self, idx: usize) -> Option<DataValue<'static>> {
        let valid = self.validity.get(idx)?;
        if *valid {
            let val = *self.values.get(idx).unwrap(); // Validity and values vec must always be equal length.
            Some(val.into())
        } else {
            Some(DataValue::Null)
        }
    }
}

pub trait VarlenType: NativeType + AsRef<[u8]> {
    /// Convert a slice of bytes to self. The length of bytes should be exact.
    fn from_bytes(bs: &[u8]) -> &Self;

    /// Convert self into a data value, keeping the lifetime of self.
    fn as_data_value<'a>(&'a self) -> DataValue<'a>;
}

impl VarlenType for str {
    fn from_bytes(bs: &[u8]) -> &Self {
        // We should only ever be dealing with utf8 strings in the system.
        std::str::from_utf8(bs).unwrap()
    }

    fn as_data_value<'a>(&'a self) -> DataValue<'a> {
        self.into()
    }
}

impl VarlenType for [u8] {
    fn from_bytes(bs: &[u8]) -> &Self {
        bs
    }

    fn as_data_value<'a>(&'a self) -> DataValue<'a> {
        self.into()
    }
}

#[derive(Debug)]
struct VarlenColumn<T> {
    validity: BitVec,
    offsets: Vec<usize>,
    bytes: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T: VarlenType> VarlenColumn<T> {
    fn new() -> Self {
        let validity = BitVec::new();
        let offsets = vec![0]; // Always starts with at least one element.
        let bytes = Vec::new();
        VarlenColumn {
            validity,
            offsets,
            bytes,
            _phantom: PhantomData,
        }
    }

    fn push_native(&mut self, val: T) {
        self.validity.push(true);
        let bs = val.as_ref();
        let next_offset = self.bytes.len() + bs.len();
        self.offsets.push(next_offset);
        self.bytes.extend_from_slice(bs);
    }

    fn get_value<'a>(&'a self, idx: usize) -> Option<DataValue<'a>> {
        let valid = self.validity.get(idx)?;
        // Offsets always has one more than the number of elements in the
        // column.
        let (start, end) = (self.offsets[idx], self.offsets[idx + 1]);

        if *valid {
            let val = T::from_bytes(&self.bytes[start..end]);
            Some(val.as_data_value())
        } else {
            Some(DataValue::Null)
        }
    }
}
