use parquet::column::page::PageReader;
use parquet::data_type::DataType as ParquetDataType;
use parquet::{column::reader::ColumnReaderImpl, schema::types::ColumnDescPtr};
use rayexec_error::Result;

pub struct RecordsReader<V, T: ParquetDataType> {
    /// Pointer to columns we're reading from.
    column: ColumnDescPtr,

    reader: ColumnReaderImpl<T>,

    /// Values read from the column.
    values: Vec<V>,
}

impl<V, T: ParquetDataType> RecordsReader<V, T> {
    pub fn set_reader(&mut self, reader: Box<dyn PageReader>) {}

    /// Try to read some number of records, returning the actual number of
    /// records read.
    pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
        unimplemented!()
    }
}
