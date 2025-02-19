use std::pin::Pin;
use std::task::Context;

use parquet::column::page::PageReader;
use parquet::column::reader::basic::BasicColumnValueDecoder;
use parquet::column::reader::decoder::ColumnValueDecoder;
use parquet::column::reader::GenericColumnReader;
use parquet::schema::types::ColumnDescPtr;
use rayexec_error::Result;
use rayexec_execution::arrays::array::Array;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::execution::operators::source::operation::PollPull;
use rayexec_io::exp::AsyncReadStream;

#[derive(Debug)]
pub struct ColumnReader<V: ColumnValueDecoder, P: PageReader> {
    descr: ColumnDescPtr,
    reader: GenericColumnReader<V, P>,

    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
}

impl<V, P> ColumnReader<V, P>
where
    V: ColumnValueDecoder,
    P: PageReader,
{
    pub fn read(&mut self, array: &mut Array) -> Result<usize> {
        unimplemented!()
    }
}
