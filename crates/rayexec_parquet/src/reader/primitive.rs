use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::column::reader::basic::BasicColumnValueDecoder;
use parquet::data_type::{DataType as ParquetDataType, Int96};
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::{ArrayData, ArrayOld};
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::compute::cast::array::cast_array;
use rayexec_bullet::compute::cast::behavior::CastFailBehavior;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::storage::{BooleanStorage, PrimitiveStorage};
use rayexec_error::{RayexecError, Result};

use super::{
    def_levels_into_bitmap,
    insert_null_values,
    ArrayBuilder,
    IntoArrayData,
    ValuesReader,
};

pub struct PrimitiveArrayReader<T: ParquetDataType, P: PageReader> {
    batch_size: usize,
    datatype: DataType,
    values_reader: ValuesReader<BasicColumnValueDecoder<T>, P>,
    values_buffer: Vec<T::T>,
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    T::T: Copy + Default,
    Vec<T::T>: IntoArrayData,
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
    pub fn take_array(&mut self) -> Result<ArrayOld> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // Basis of the array.
        let mut data =
            std::mem::replace(&mut self.values_buffer, Vec::with_capacity(self.batch_size));

        // Insert nulls as needed.
        let bitmap = match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                insert_null_values(&mut data, &bitmap);
                Some(bitmap)
            }
            None => None,
        };

        // The build type for the array may differ than the desired output data
        // type.
        //
        // E.g. we may need to convert physical INT64 -> Decimal128. If that
        // happens, we'll apply cast after building.
        let (array_data, build_type) = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BOOLEAN, DataType::Boolean) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT32, DataType::Int16) => (data.into_array_data(), DataType::Int32),
            (PhysicalType::INT32, DataType::Int32) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT32, DataType::UInt16) => (data.into_array_data(), DataType::Int32),
            (PhysicalType::INT32, DataType::Date32) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT32, DataType::Decimal64(_)) => (data.into_array_data(), DataType::Int32),
            (PhysicalType::INT32, DataType::Decimal128(_)) => (data.into_array_data(), DataType::Int32),
            (PhysicalType::INT64, DataType::Int64) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT64, DataType::Decimal64(_)) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT64, DataType::Decimal128(_)) => (data.into_array_data(), DataType::Int64), // TODO
            (PhysicalType::INT64, DataType::Timestamp(_)) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::INT96, DataType::Timestamp(_)) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::FLOAT, DataType::Float32) => (data.into_array_data(), self.datatype.clone()),
            (PhysicalType::DOUBLE, DataType::Float64) => (data.into_array_data(), self.datatype.clone()),
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in primitive reader; parquet: {p_other}, bullet: {d_other}")))
        };

        let needs_cast = build_type != self.datatype;

        let mut array = match bitmap {
            Some(bitmap) => {
                ArrayOld::new_with_validity_and_array_data(build_type, bitmap, array_data)
            }
            None => ArrayOld::new_with_array_data(build_type, array_data),
        };

        if needs_cast {
            array = cast_array(&array, self.datatype.clone(), CastFailBehavior::Null)?;
        }

        Ok(array)
    }
}

impl<T, P> ArrayBuilder<P> for PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    T::T: Copy + Default,
    Vec<T::T>: IntoArrayData,
{
    fn build(&mut self) -> Result<ArrayOld> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let decoder = BasicColumnValueDecoder::new(&self.values_reader.description);
        self.values_reader.set_page_reader(decoder, page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}

impl IntoArrayData for Vec<bool> {
    fn into_array_data(self) -> ArrayData {
        let values = Bitmap::from_iter(self);
        BooleanStorage::from(values).into()
    }
}

macro_rules! impl_into_array_primitive {
    ($prim:ty) => {
        impl IntoArrayData for Vec<$prim> {
            fn into_array_data(self) -> ArrayData {
                PrimitiveStorage::from(self).into()
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

impl IntoArrayData for Vec<Int96> {
    fn into_array_data(self) -> ArrayData {
        let values: Vec<_> = self.into_iter().map(|v| v.to_nanos()).collect();
        PrimitiveStorage::from(values).into()
    }
}
