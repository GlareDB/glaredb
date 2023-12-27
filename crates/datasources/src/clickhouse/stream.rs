use chrono::{DateTime, NaiveDate};
use chrono_tz::Tz;
use clickhouse_rs::{
    types::{column::iter::Iterable, Column, Simple},
    Block, ClientHandle,
};
use datafusion::{arrow::array::Date32Array, error::DataFusionError};
use datafusion::{
    arrow::{
        array::{
            Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        },
        datatypes::{DataType, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    physical_plan::RecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tracing::trace;

use crate::clickhouse::errors::ClickhouseError;

use super::errors::Result;

/// A stream that converts blocks from clickhouse into a stream of record
/// batches.
#[derive(Debug)]
pub struct BlockStream {
    /// Schema of the output batches.
    schema: Arc<Schema>,
    /// Receiver side for getting blocks from the clickhouse client.
    receiver: mpsc::Receiver<Result<Block, clickhouse_rs::errors::Error>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl BlockStream {
    /// Execute a query against a client, and return a stream of record batches.
    /// The provided schema should match the output of the query.
    ///
    /// This will spin up a separate tokio thread in the background to satisfy
    /// lifetime requirements of the stream and client.
    pub fn execute(mut handle: ClientHandle, query: String, schema: Arc<Schema>) -> BlockStream {
        let (sender, receiver) = mpsc::channel(1);

        let thread_handle = tokio::spawn(async move {
            let mut stream = handle.query(query).stream_blocks();
            while let Some(block) = stream.next().await {
                if sender.send(block).await.is_err() {
                    // This is fine, receiver side was dropped due to a global
                    // limit, or a query execution error.
                    trace!("block receiver closed");
                }
            }
        });

        BlockStream {
            schema,
            receiver,
            _handle: thread_handle,
        }
    }
}

impl Stream for BlockStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(result)) => match result {
                Ok(block) => Poll::Ready(Some(
                    block_to_batch(self.schema.clone(), block)
                        .map_err(|e| DataFusionError::Execution(e.to_string())),
                )),
                Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                    "failed to convert block to batch: {e}"
                ))))),
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for BlockStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Convert a block to a record batch.
fn block_to_batch(schema: Arc<Schema>, block: Block) -> Result<RecordBatch> {
    let mut arrs = Vec::with_capacity(schema.fields.len());
    for (field, col) in schema.fields.iter().zip(block.columns()) {
        let arr = column_to_array(field.data_type().clone(), col, field.is_nullable())?;
        arrs.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrs)?)
}

/// Converts a column from a block into an arrow array.
///
/// The column's data type should be known beforehand.
fn column_to_array(
    datatype: DataType,
    column: &Column<Simple>,
    nullable: bool,
) -> Result<Arc<dyn Array>> {
    // TODO: This could be a function, but I'm not too keen on figuring out the
    // types right now.
    macro_rules! make_primitive_array {
        ($primitive:ty, $arr_type:ty, $nullable:expr) => {{
            if nullable {
                let vals: Vec<_> = column
                    .iter::<Option<$primitive>>()?
                    .map(|opt| opt.cloned())
                    .collect();
                Arc::new(<$arr_type>::from(vals))
            } else {
                let vals: Vec<_> = column.iter::<$primitive>()?.cloned().collect();
                Arc::new(<$arr_type>::from(vals))
            }
        }};
    }

    let arr: Arc<dyn Array> = match datatype {
        DataType::Boolean => make_primitive_array!(bool, BooleanArray, nullable),
        DataType::UInt8 => make_primitive_array!(u8, UInt8Array, nullable),
        DataType::UInt16 => make_primitive_array!(u16, UInt16Array, nullable),
        DataType::UInt32 => make_primitive_array!(u32, UInt32Array, nullable),
        DataType::UInt64 => make_primitive_array!(u64, UInt64Array, nullable),
        DataType::Int8 => make_primitive_array!(i8, Int8Array, nullable),
        DataType::Int16 => make_primitive_array!(i16, Int16Array, nullable),
        DataType::Int32 => make_primitive_array!(i32, Int32Array, nullable),
        DataType::Int64 => make_primitive_array!(i64, Int64Array, nullable),
        DataType::Float32 => make_primitive_array!(f32, Float32Array, nullable),
        DataType::Float64 => make_primitive_array!(f64, Float64Array, nullable),
        DataType::Utf8 => {
            if nullable {
                let vals: Vec<_> =
                    <Option<&[u8]> as Iterable<Simple>>::iter(column, column.sql_type())?
                        .map(|bs| bs.map(|bs| str::from_utf8(bs).unwrap()))
                        .collect();
                Arc::new(StringArray::from(vals))
            } else {
                let vals: Vec<_> = <&[u8]>::iter(column, column.sql_type())?
                    .map(|bs| str::from_utf8(bs).unwrap())
                    .collect();
                Arc::new(StringArray::from(vals))
            }
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

            if nullable {
                let vals: Vec<_> =
                    <Option<NaiveDate> as Iterable<Simple>>::iter(column, column.sql_type())?
                        .collect();
                Arc::new(Date32Array::from(
                    vals.into_iter()
                        .map(|date| {
                            date.map(|date| date.signed_duration_since(epoch).num_days() as i32)
                        })
                        .collect::<Vec<_>>(),
                ))
            } else {
                let vals: Vec<_> = <NaiveDate>::iter(column, column.sql_type())?.collect();
                Arc::new(Date32Array::from(
                    vals.into_iter()
                        .map(|date| date.signed_duration_since(epoch).num_days() as i32)
                        .collect::<Vec<_>>(),
                ))
            }
        }
        DataType::Timestamp(unit, _tz) => {
            if nullable {
                let vals: Vec<_> =
                    <Option<DateTime<Tz>> as Iterable<Simple>>::iter(column, column.sql_type())?
                        .collect();
                match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from(
                        vals.into_iter()
                            .map(|time| time.map(|time| time.timestamp()))
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(
                        vals.into_iter()
                            .map(|time| time.map(|time| time.timestamp_millis()))
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.map(|time| time.timestamp_micros()))
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.map(|time| time.timestamp_nanos_opt().unwrap()))
                            .collect::<Vec<_>>(),
                    )),
                }
            } else {
                let vals: Vec<_> = <DateTime<Tz>>::iter(column, column.sql_type())?.collect();
                match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from(
                        vals.into_iter()
                            .map(|time| time.timestamp())
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(
                        vals.into_iter()
                            .map(|time| time.timestamp_millis())
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.timestamp_micros())
                            .collect::<Vec<_>>(),
                    )),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(
                        vals.into_iter()
                            .map(|time| time.timestamp_nanos_opt().unwrap())
                            .collect::<Vec<_>>(),
                    )),
                }
            }
        }
        other => {
            return Err(ClickhouseError::String(format!(
                "unhandled data type trying to convert to arrow array: {other}"
            )))
        }
    };

    Ok(arr)
}
