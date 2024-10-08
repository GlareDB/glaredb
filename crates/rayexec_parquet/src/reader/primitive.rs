use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::data_type::{DataType as ParquetDataType, Int96};
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::storage::{BooleanStorage, PrimitiveStorage};
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, IntoArray, ValuesReader};

pub struct PrimitiveArrayReader<T: ParquetDataType, P: PageReader> {
    batch_size: usize,
    datatype: DataType,
    values_reader: ValuesReader<T, P>,
    values_buffer: Vec<T::T>,
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        PrimitiveArrayReader {
            batch_size,
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: Vec::with_capacity(batch_size),
        }
    }

    /// Take the currently read values and convert into an array.
    pub fn take_array(&mut self) -> Result<Array> {
        let data = std::mem::replace(&mut self.values_buffer, Vec::with_capacity(self.batch_size));
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // TODO: Other types.
        let arr = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BOOLEAN, DataType::Boolean) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT32, DataType::Int32) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT32, DataType::Date32) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT64, DataType::Int64) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT64, DataType::Decimal64(_)) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT64, DataType::Timestamp(_)) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::INT96, DataType::Timestamp(_)) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::FLOAT, DataType::Float32) => data.into_array(self.datatype.clone(), def_levels),
            (PhysicalType::DOUBLE, DataType::Float64) => data.into_array(self.datatype.clone(), def_levels),
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in primitive reader; parquet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}

impl<T, P> ArrayBuilder<P> for PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    fn build(&mut self) -> Result<Array> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        self.values_reader.set_page_reader(page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}

impl IntoArray for Vec<bool> {
    fn into_array(self, datatype: DataType, def_levels: Option<Vec<i16>>) -> Array {
        match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                let values = insert_null_values(self, &bitmap);
                let values = Bitmap::from_iter(values);
                Array::new_with_validity_and_array_data(
                    datatype,
                    bitmap,
                    BooleanStorage::from(values),
                )
            }
            None => {
                Array::new_with_array_data(datatype, BooleanStorage::from(Bitmap::from_iter(self)))
            }
        }
    }
}

macro_rules! impl_into_array_primitive {
    ($prim:ty) => {
        impl IntoArray for Vec<$prim> {
            fn into_array(self, datatype: DataType, def_levels: Option<Vec<i16>>) -> Array {
                match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);
                        let values = insert_null_values(self, &bitmap);
                        Array::new_with_validity_and_array_data(
                            datatype,
                            bitmap,
                            PrimitiveStorage::from(values),
                        )
                    }
                    None => Array::new_with_array_data(datatype, PrimitiveStorage::from(self)),
                }
            }
        }
    };
}

impl_into_array_primitive!(i8);
impl_into_array_primitive!(i16);
impl_into_array_primitive!(i32);
impl_into_array_primitive!(i64);
impl_into_array_primitive!(i128);
impl_into_array_primitive!(u8);
impl_into_array_primitive!(u16);
impl_into_array_primitive!(u32);
impl_into_array_primitive!(u64);
impl_into_array_primitive!(u128);
impl_into_array_primitive!(f32);
impl_into_array_primitive!(f64);

impl IntoArray for Vec<Int96> {
    fn into_array(self, datatype: DataType, def_levels: Option<Vec<i16>>) -> Array {
        let values = self.into_iter().map(|v| v.to_nanos()).collect();
        match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                let values = insert_null_values(values, &bitmap);
                Array::new_with_validity_and_array_data(
                    datatype,
                    bitmap,
                    PrimitiveStorage::from(values),
                )
            }
            None => Array::new_with_array_data(datatype, PrimitiveStorage::from(values)),
        }
    }
}

/// Insert null (meaningless) values into the vec according to the validity
/// bitmap.
///
/// The resulting vec will have its length equal to the bitmap's length.
fn insert_null_values<T: Copy + Default>(mut values: Vec<T>, bitmap: &Bitmap) -> Vec<T> {
    values.resize(bitmap.len(), T::default());

    for (current_idx, new_idx) in (0..values.len()).rev().zip(bitmap.index_iter().rev()) {
        if current_idx <= new_idx {
            break;
        }
        values[new_idx] = values[current_idx];
    }

    values
}
