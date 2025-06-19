use std::marker::PhantomData;

use glaredb_core::arrays::array::Array;
use glaredb_error::{DbError, Result};

use super::Definitions;
use crate::column::encoding::plain::PlainDecoder;
use crate::column::read_buffer::ReadCursor;
use crate::column::value_reader::ValueReader;

#[derive(Debug)]
pub struct ByteStreamSplitDecoder<const K: usize, V: ValueReader> {
    // TODO: Buffer manage.
    buf: Vec<[u8; K]>,
    stream: ByteStreamSplit<K>,
    _v: PhantomData<V>,
}

impl<const K: usize, V> ByteStreamSplitDecoder<K, V>
where
    V: ValueReader,
{
    pub fn try_new(cursor: ReadCursor) -> Result<Self> {
        // TODO: Probably want to assert that size of scalar we read in
        // ValueReader equals K.

        let stream = ByteStreamSplit::try_new(cursor)?;
        let buf = Vec::new();

        Ok(ByteStreamSplitDecoder {
            buf,
            stream,
            _v: PhantomData,
        })
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        let read_count = match definitions {
            Definitions::HasDefinitions { levels, max } => {
                debug_assert_eq!(levels.len(), count);

                levels.iter().filter(|&&def| def == max).count()
            }
            Definitions::NoDefinitions => count,
        };

        self.buf.resize(read_count, [0; K]);
        self.stream.read(&mut self.buf)?;

        // Use plain decoder to properly emplace the values in the output
        // array...
        //
        // The decoder is already reading from a byte stream, so there's nothing
        // we need to here do to convert to the type.
        let read_cursor = ReadCursor::from_slice(&self.buf);
        let mut plain = PlainDecoder {
            buffer: read_cursor,
            value_reader: V::default(),
        };

        // Use original offset and count.
        plain.read_plain(definitions, output, offset, count)
    }
}

#[derive(Debug)]
struct ByteStreamSplit<const K: usize> {
    #[allow(unused)]
    total_values: usize,
    streams: [ReadCursor; K],
}

impl<const K: usize> ByteStreamSplit<K> {
    fn try_new(mut cursor: ReadCursor) -> Result<Self> {
        let num_bytes = cursor.remaining();
        if num_bytes % K != 0 {
            return Err(DbError::new(format!("Cursor size not a multiple of {K}")));
        }

        let total_values = num_bytes / K;
        let streams = std::array::from_fn(|_| unsafe { cursor.take(total_values) });

        Ok(ByteStreamSplit {
            total_values,
            streams,
        })
    }

    fn read(&mut self, values: &mut [[u8; K]]) -> Result<()> {
        for (idx, stream) in self.streams.iter_mut().enumerate() {
            for value in values.iter_mut() {
                value[idx] = unsafe { stream.read_next_unchecked::<u8>() };
            }
        }

        Ok(())
    }
}
