use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::data_type::{DataType as ParquetDataType, Int96};
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::{
    Array, BooleanArray, Decimal64Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, TimestampNanosecondsArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, IntoArray, ValuesReader};

pub struct PrimitiveArrayReader<T: ParquetDataType, P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<T, P>,
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    pub fn new(datatype: DataType, desc: ColumnDescPtr) -> Self {
        PrimitiveArrayReader {
            datatype,
            values_reader: ValuesReader::new(desc),
        }
    }

    /// Take the currently read values and convert into an array.
    pub fn take_array(&mut self) -> Result<Array> {
        let data = self.values_reader.take_values();
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // TODO: Other types.
        let arr = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BOOLEAN, DataType::Boolean) => data.into_array(def_levels),
            (PhysicalType::INT32, DataType::Int32) => data.into_array(def_levels),
            (PhysicalType::INT32, DataType::Date32) => {
                let arr = data.into_array(def_levels);
                match arr {
                    Array::Int32(arr) => Array::Date32(arr),
                    other => return Err(RayexecError::new(format!("Unexpected array type when converting to Date32: {}", other.datatype())))
                }
            },
            (PhysicalType::INT64, DataType::Int64) => data.into_array(def_levels),
            (PhysicalType::INT64, DataType::Decimal64(meta)) => {
                let arr = data.into_array(def_levels);
                match arr {
                    Array::Int64(arr) => Array::Decimal64(Decimal64Array::new(meta.precision, meta.scale, arr)),
                    other => return Err(RayexecError::new(format!("Unexpected array type when converting to Decimal64: {}", other.datatype())))
                }
            }
            (PhysicalType::INT96, DataType::TimestampNanoseconds) => data.into_array(def_levels),
            (PhysicalType::FLOAT, DataType::Float32) => data.into_array(def_levels),
            (PhysicalType::DOUBLE, DataType::Float64) => data.into_array(def_levels),
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in primitive reader; parqet: {p_other}, bullet: {d_other}")))
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
        self.values_reader.read_records(n)
    }
}

impl IntoArray for Vec<bool> {
    fn into_array(self, def_levels: Option<Vec<i16>>) -> Array {
        match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                let values = insert_null_values(self, &bitmap);
                let values = Bitmap::from_iter(values);
                Array::Boolean(BooleanArray::new(values, Some(bitmap)))
            }
            None => Array::Boolean(BooleanArray::from_iter(self)),
        }
    }
}

impl IntoArray for Vec<Int96> {
    fn into_array(self, def_levels: Option<Vec<i16>>) -> Array {
        let values = self.into_iter().map(|v| v.to_nanos()).collect();
        match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                let values = insert_null_values(values, &bitmap);
                Array::TimestampNanoseconds(TimestampNanosecondsArray::new(values, Some(bitmap)))
            }
            None => Array::TimestampNanoseconds(TimestampNanosecondsArray::from_iter(values)),
        }
    }
}

macro_rules! into_array_prim {
    ($prim:ty, $variant:ident, $array:ty) => {
        impl IntoArray for Vec<$prim> {
            fn into_array(self, def_levels: Option<Vec<i16>>) -> Array {
                match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);
                        let values = insert_null_values(self, &bitmap);
                        Array::$variant(<$array>::new(values, Some(bitmap)))
                    }
                    None => Array::$variant(<$array>::from(self)),
                }
            }
        }
    };
}

into_array_prim!(i8, Int8, Int8Array);
into_array_prim!(i16, Int16, Int16Array);
into_array_prim!(i32, Int32, Int32Array);
into_array_prim!(i64, Int64, Int64Array);
into_array_prim!(u8, UInt8, UInt8Array);
into_array_prim!(u16, UInt16, UInt16Array);
into_array_prim!(u32, UInt32, UInt32Array);
into_array_prim!(u64, UInt64, UInt64Array);
into_array_prim!(f32, Float32, Float32Array);
into_array_prim!(f64, Float64, Float64Array);

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
