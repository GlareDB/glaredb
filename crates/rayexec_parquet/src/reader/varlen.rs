use bytes::Bytes;
use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::data_type::{ByteArray, ByteArrayType, DataType as ParquetDataType};
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::storage::SharedHeapStorage;
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, ValuesReader};

#[derive(Debug)]
pub struct VarlenArrayReader<P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<ByteArrayType, P>,
    // TODO: Change varlen decoding to not use this type and instead write into
    // a contiguous buffer that we can construct array data out of.
    values_buffer: Vec<ByteArray>,
}

impl<P> VarlenArrayReader<P>
where
    P: PageReader,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        VarlenArrayReader {
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: Vec::with_capacity(batch_size),
        }
    }

    pub fn take_array(&mut self) -> Result<Array> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        let arr = match (ByteArrayType::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, _) => {
                match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);

                        let mut buf_iter = self.values_buffer.drain(..);
                        let mut values = Vec::with_capacity(bitmap.len());

                        for valid in bitmap.iter() {
                            if valid {
                                values.push(buf_iter.next().expect("buffered value to exist").bytes_data())
                            } else {
                                values.push(Bytes::new())
                            }
                        }

                        Array::new_with_validity_and_array_data(self.datatype.clone(),bitmap, SharedHeapStorage::from(values))
                    }
                    None => {
                        let values: Vec<_> = self.values_buffer.drain(..).map(|b| b.bytes_data()).collect();

                        Array::new_with_array_data(self.datatype.clone(), SharedHeapStorage::from(values))
                    }
                }
            }
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        self.values_buffer.clear();

        Ok(arr)
    }
}

impl<P> ArrayBuilder<P> for VarlenArrayReader<P>
where
    P: PageReader,
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
