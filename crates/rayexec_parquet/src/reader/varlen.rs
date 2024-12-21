use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::column::reader::view::ViewColumnValueDecoder;
use parquet::data_type::{ByteArray, DataType as ParquetDataType};
use parquet::decoding::view::ViewBuffer;
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::builder::ArrayDataBuffer;
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, insert_null_values, ArrayBuilder, ValuesReader};

#[derive(Debug)]
pub struct VarlenArrayReader<P: PageReader> {
    batch_size: usize,
    datatype: DataType,
    values_reader: ValuesReader<ViewColumnValueDecoder, P>,
    values_buffer: ViewBuffer,
}

impl<P> VarlenArrayReader<P>
where
    P: PageReader,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        VarlenArrayReader {
            batch_size,
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: ViewBuffer::new(batch_size),
        }
    }

    pub fn take_array(&mut self) -> Result<ArrayOld> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // Replace the view buffer, what we take is the basis for the array.
        let view_buffer =
            std::mem::replace(&mut self.values_buffer, ViewBuffer::new(self.batch_size));

        let arr = match (ByteArray::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, _) => {
                match def_levels {
                    Some(levels) => {
                        // Logical validities, used to insert null values into
                        // the metadata vec.
                        let bitmap = def_levels_into_bitmap(levels);

                        let mut buffer = view_buffer.into_buffer();

                        // Insert nulls into the correct location.
                        //
                        // The "null" values will just be zeroed metadata fields.
                        insert_null_values(buffer.metadata_mut(), &bitmap);

                        ArrayOld::new_with_validity_and_array_data(self.datatype.clone(), bitmap, buffer.into_data())
                    }
                    None => {
                        ArrayOld::new_with_array_data(self.datatype.clone(), view_buffer.into_buffer().into_data())
                    }
                }
            }
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}

impl<P> ArrayBuilder<P> for VarlenArrayReader<P>
where
    P: PageReader,
{
    fn build(&mut self) -> Result<ArrayOld> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let decoder = ViewColumnValueDecoder::new(&self.values_reader.description);
        self.values_reader.set_page_reader(decoder, page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}
