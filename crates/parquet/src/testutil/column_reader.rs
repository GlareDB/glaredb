use crate::basic::*;
use crate::column::page::PageReader;
use crate::column::reader::basic::BasicColumnValueDecoder;
use crate::column::reader::{BasicColumnReader, GenericColumnReader};
use crate::data_type::*;
use crate::errors::ParquetResult;
use crate::file::reader::RowGroupReader;
use crate::schema::types::ColumnDescPtr;

/// Constructs a basic column reader for a column in the row group reader.
pub fn column_reader_from_row_group_reader<P, R>(
    row_group: &R,
    col: usize,
) -> ParquetResult<BasicColumnReader<P>>
where
    P: PageReader,
    R: RowGroupReader<P>,
{
    let schema_descr = row_group.metadata().schema_descr();
    let col_descr = schema_descr.column(col);
    let col_page_reader = row_group.get_column_page_reader(col)?;
    let reader = get_column_reader(col_descr, col_page_reader);
    Ok(reader)
}

/// Gets a specific column reader corresponding to column descriptor
/// `col_descr`. The column reader will read from pages in `col_page_reader`.
pub fn get_column_reader<P: PageReader>(
    col_descr: ColumnDescPtr,
    col_page_reader: P,
) -> BasicColumnReader<P> {
    match col_descr.physical_type() {
        Type::BOOLEAN => BasicColumnReader::BoolColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::INT32 => BasicColumnReader::Int32ColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::INT64 => BasicColumnReader::Int64ColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::INT96 => BasicColumnReader::Int96ColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::FLOAT => BasicColumnReader::FloatColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::DOUBLE => BasicColumnReader::DoubleColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::BYTE_ARRAY => BasicColumnReader::ByteArrayColumnReader(GenericColumnReader::new(
            col_descr,
            col_page_reader,
        )),
        Type::FIXED_LEN_BYTE_ARRAY => BasicColumnReader::FixedLenByteArrayColumnReader(
            GenericColumnReader::new(col_descr, col_page_reader),
        ),
    }
}

/// Gets a typed column reader for the specific type `T`, by "up-casting"
/// `col_reader` of non-generic type to a generic column reader type
/// `ColumnReaderImpl`.
///
/// Panics if actual enum value for `col_reader` does not match the type `T`.
pub fn get_typed_column_reader<T: DataType, P: PageReader>(
    col_reader: BasicColumnReader<P>,
) -> GenericColumnReader<BasicColumnValueDecoder<T>, P> {
    T::get_column_reader(col_reader).unwrap_or_else(|| {
        panic!(
            "Failed to convert column reader into a typed column reader for `{}` type",
            T::get_physical_type()
        )
    })
}
