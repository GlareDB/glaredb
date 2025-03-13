use crate::arrays::datatype::DataTypeId;
use crate::functions::Signature;

#[derive(Debug, Clone, Copy)]
pub struct DecimalArithSignatures {
    pub d64_d64: &'static Signature,
    pub d128_d128: &'static Signature,

    pub d64_i8: &'static Signature,
    pub d64_i16: &'static Signature,
    pub d64_i32: &'static Signature,
    pub d64_i64: &'static Signature,

    pub i8_d64: &'static Signature,
    pub i16_d64: &'static Signature,
    pub i32_d64: &'static Signature,
    pub i64_d64: &'static Signature,

    pub d128_i8: &'static Signature,
    pub d128_i16: &'static Signature,
    pub d128_i32: &'static Signature,
    pub d128_i64: &'static Signature,

    pub i8_d128: &'static Signature,
    pub i16_d128: &'static Signature,
    pub i32_d128: &'static Signature,
    pub i64_d128: &'static Signature,
}

pub const D_SIGS: DecimalArithSignatures = DecimalArithSignatures {
    d64_d64: &Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Decimal64],
        DataTypeId::Decimal64,
    ),
    d128_d128: &Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Decimal128],
        DataTypeId::Decimal128,
    ),

    d64_i8: &Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Int8],
        DataTypeId::Decimal64,
    ),
    d64_i16: &Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Int16],
        DataTypeId::Decimal64,
    ),
    d64_i32: &Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Int32],
        DataTypeId::Decimal64,
    ),
    d64_i64: &Signature::new(
        &[DataTypeId::Decimal64, DataTypeId::Int64],
        DataTypeId::Decimal64,
    ),

    i8_d64: &Signature::new(
        &[DataTypeId::Int8, DataTypeId::Decimal64],
        DataTypeId::Decimal64,
    ),
    i16_d64: &Signature::new(
        &[DataTypeId::Int16, DataTypeId::Decimal64],
        DataTypeId::Decimal64,
    ),
    i32_d64: &Signature::new(
        &[DataTypeId::Int32, DataTypeId::Decimal64],
        DataTypeId::Decimal64,
    ),
    i64_d64: &Signature::new(
        &[DataTypeId::Int64, DataTypeId::Decimal64],
        DataTypeId::Decimal64,
    ),

    d128_i8: &Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Int8],
        DataTypeId::Decimal128,
    ),
    d128_i16: &Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Int16],
        DataTypeId::Decimal128,
    ),
    d128_i32: &Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Int32],
        DataTypeId::Decimal128,
    ),
    d128_i64: &Signature::new(
        &[DataTypeId::Decimal128, DataTypeId::Int64],
        DataTypeId::Decimal128,
    ),

    i8_d128: &Signature::new(
        &[DataTypeId::Int8, DataTypeId::Decimal128],
        DataTypeId::Decimal128,
    ),
    i16_d128: &Signature::new(
        &[DataTypeId::Int16, DataTypeId::Decimal128],
        DataTypeId::Decimal128,
    ),
    i32_d128: &Signature::new(
        &[DataTypeId::Int32, DataTypeId::Decimal128],
        DataTypeId::Decimal128,
    ),
    i64_d128: &Signature::new(
        &[DataTypeId::Int64, DataTypeId::Decimal128],
        DataTypeId::Decimal128,
    ),
};
