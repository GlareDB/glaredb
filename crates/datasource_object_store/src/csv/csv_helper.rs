//! Helpers for handling csv files from datafusion.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::physical_plan::file_format::{FileMeta, FileOpenFuture, FileOpener};
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectStore};

#[derive(Debug, Clone)]
pub struct CsvConfig {
    pub batch_size: usize,
    pub file_schema: ArrowSchemaRef,
    pub file_projection: Option<Vec<usize>>,
    pub has_header: bool,
    pub delimiter: u8,
    pub object_store: Arc<dyn ObjectStore>,
}

impl CsvConfig {
    fn open<R: std::io::Read>(&self, reader: R, first_chunk: bool) -> csv::Reader<R> {
        let datetime_format = None;
        csv::Reader::new(
            reader,
            Arc::clone(&self.file_schema),
            self.has_header && first_chunk,
            Some(self.delimiter),
            self.batch_size,
            None,
            self.file_projection.clone(),
            datetime_format,
        )
    }
}

pub struct CsvOpener {
    pub config: CsvConfig,
    pub file_compression_type: FileCompressionType,
}

impl FileOpener for CsvOpener {
    fn open(&self, file_meta: FileMeta) -> DatafusionResult<FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.to_owned();
        Ok(Box::pin(async move {
            match config.object_store.get(file_meta.location()).await? {
                GetResult::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    Ok(futures::stream::iter(config.open(decoder, true)).boxed())
                }
                GetResult::Stream(s) => {
                    let mut first_chunk = true;
                    let s = s.map_err(Into::<DataFusionError>::into);
                    let decoder = file_compression_type.convert_stream(Box::pin(s))?;
                    Ok(newline_delimited_stream(decoder)
                        .map_ok(move |bytes| {
                            let reader = config.open(bytes.reader(), first_chunk);
                            first_chunk = false;
                            futures::stream::iter(reader)
                        })
                        .try_flatten()
                        .boxed())
                }
            }
        }))
    }
}

/// Given a [`Stream`] of [`Bytes`] returns a [`Stream`] where each
/// yielded [`Bytes`] contains a whole number of new line delimited records
/// accounting for `\` style escapes and `"` quotes
pub fn newline_delimited_stream<S>(s: S) -> impl Stream<Item = DatafusionResult<Bytes>>
where
    S: Stream<Item = DatafusionResult<Bytes>> + Unpin,
{
    let delimiter = LineDelimiter::new();

    futures::stream::unfold(
        (s, delimiter, false),
        |(mut s, mut delimiter, mut exhausted)| async move {
            loop {
                if let Some(next) = delimiter.next() {
                    return Some((Ok(next), (s, delimiter, exhausted)));
                } else if exhausted {
                    return None;
                }

                match s.next().await {
                    Some(Ok(bytes)) => delimiter.push(bytes),
                    Some(Err(e)) => return Some((Err(e), (s, delimiter, exhausted))),
                    None => {
                        exhausted = true;
                        match delimiter.finish() {
                            Ok(true) => return None,
                            Ok(false) => continue,
                            Err(e) => return Some((Err(e), (s, delimiter, exhausted))),
                        }
                    }
                }
            }
        },
    )
}

/// The ASCII encoding of `"`
const QUOTE: u8 = b'"';

/// The ASCII encoding of `\n`
const NEWLINE: u8 = b'\n';

/// The ASCII encoding of `\`
const ESCAPE: u8 = b'\\';

/// [`LineDelimiter`] is provided with a stream of [`Bytes`] and returns an
/// iterator of [`Bytes`] containing a whole number of new line delimited
/// records
#[derive(Debug, Default)]
struct LineDelimiter {
    /// Complete chunks of [`Bytes`]
    complete: VecDeque<Bytes>,
    /// Remainder bytes that form the next record
    remainder: Vec<u8>,
    /// True if the last character was the escape character
    is_escape: bool,
    /// True if currently processing a quoted string
    is_quote: bool,
}

impl LineDelimiter {
    /// Creates a new [`LineDelimiter`] with the provided delimiter
    fn new() -> Self {
        Self::default()
    }

    /// Adds the next set of [`Bytes`]
    fn push(&mut self, val: impl Into<Bytes>) {
        let val: Bytes = val.into();

        let is_escape = &mut self.is_escape;
        let is_quote = &mut self.is_quote;
        let mut record_ends = val.iter().enumerate().filter_map(|(idx, v)| {
            if *is_escape {
                *is_escape = false;
                None
            } else if *v == ESCAPE {
                *is_escape = true;
                None
            } else if *v == QUOTE {
                *is_quote = !*is_quote;
                None
            } else if *is_quote {
                None
            } else {
                (*v == NEWLINE).then_some(idx + 1)
            }
        });

        let start_offset = match self.remainder.is_empty() {
            true => 0,
            false => match record_ends.next() {
                Some(idx) => {
                    self.remainder.extend_from_slice(&val[0..idx]);
                    self.complete
                        .push_back(Bytes::from(std::mem::take(&mut self.remainder)));
                    idx
                }
                None => {
                    self.remainder.extend_from_slice(&val);
                    return;
                }
            },
        };
        let end_offset = record_ends.last().unwrap_or(start_offset);
        if start_offset != end_offset {
            self.complete.push_back(val.slice(start_offset..end_offset));
        }

        if end_offset != val.len() {
            self.remainder.extend_from_slice(&val[end_offset..])
        }
    }

    /// Marks the end of the stream, delimiting any remaining bytes
    ///
    /// Returns `true` if there is no remaining data to be read
    fn finish(&mut self) -> DatafusionResult<bool> {
        if !self.remainder.is_empty() {
            if self.is_quote {
                return Err(DataFusionError::Execution(
                    "encountered unterminated string".to_string(),
                ));
            }

            if self.is_escape {
                return Err(DataFusionError::Execution(
                    "encountered trailing escape character".to_string(),
                ));
            }

            self.complete
                .push_back(Bytes::from(std::mem::take(&mut self.remainder)))
        }
        Ok(self.complete.is_empty())
    }
}

impl Iterator for LineDelimiter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.complete.pop_front()
    }
}
