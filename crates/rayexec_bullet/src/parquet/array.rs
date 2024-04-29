use parquet::data_type::DataType as ParquetDataType;
use rayexec_error::{RayexecError, Result};

use crate::field::DataType;

pub struct ArrayReader<T> {
    /// Rayexec data type.
    datatype: DataType,

    /// Parquet data type.
    _parquet: T,
}

impl<T: ParquetDataType> ArrayReader<T> {}

// fn read_records()
