use clickhouse_rs::{
    types::{
        column::iter::{Iterable, StringIterator},
        Column, ColumnType, SqlType,
    },
    Block, ClientHandle, Options, Pool,
};
use datafusion::arrow::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};
use std::str;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::trace;

use super::errors::Result;

pub struct BlockStreamReceiver {
    schema: Arc<Schema>,
    handle: tokio::task::JoinHandle<()>,
    receiver: mpsc::Receiver<Result<Block, clickhouse_rs::errors::Error>>,
}

impl BlockStreamReceiver {
    pub fn new(
        mut handle: ClientHandle,
        query: String,
        schema: Arc<Schema>,
    ) -> BlockStreamReceiver {
        let (sender, receiver) = mpsc::channel(1);

        let thread_handle = tokio::spawn(async move {
            let mut stream = handle.query(query).stream_blocks();
            while let Some(block) = stream.next().await {
                if sender.send(block).await.is_err() {
                    trace!("block receiver closed");
                }
            }
        });

        BlockStreamReceiver {
            schema,
            handle: thread_handle,
            receiver,
        }
    }
}

fn block_to_batch(schema: Arc<Schema>, block: Block) -> Result<RecordBatch> {
    let mut arrs = Vec::with_capacity(schema.fields.len());
    for (field, col) in schema.fields.iter().zip(block.columns()) {
        let arr = column_to_array(field.data_type().clone(), col)?;
        arrs.push(arr);
    }

    Ok(RecordBatch::try_new(schema, arrs)?)
}

/// Converts a column from a block into an arrow array.
///
/// The column's data type should be known beforehand.
fn column_to_array(
    datatype: DataType,
    column: &Column<clickhouse_rs::types::Simple>,
) -> Result<Arc<dyn Array>> {
    macro_rules! make_primitive_array {
        ($primitive:ty, $arr_type:ty) => {{
            let vals: Vec<_> = column.iter::<$primitive>()?.cloned().collect();
            Arc::new(<$arr_type>::from(vals))
        }};
    }

    let arr: Arc<dyn Array> = match datatype {
        DataType::Boolean => make_primitive_array!(bool, BooleanArray),
        DataType::UInt8 => make_primitive_array!(u8, UInt8Array),
        DataType::UInt16 => make_primitive_array!(u16, UInt16Array),
        DataType::UInt32 => make_primitive_array!(u32, UInt32Array),
        DataType::UInt64 => make_primitive_array!(u64, UInt64Array),
        DataType::Int8 => make_primitive_array!(i8, Int8Array),
        DataType::Int16 => make_primitive_array!(i16, Int16Array),
        DataType::Int32 => make_primitive_array!(i32, Int32Array),
        DataType::Int64 => make_primitive_array!(i64, Int64Array),
        DataType::Float32 => make_primitive_array!(f32, Float32Array),
        DataType::Float64 => make_primitive_array!(f64, Float64Array),
        DataType::Utf8 => {
            // Idk if this is what I should be doing here.
            let vals: Vec<_> = <&[u8]>::iter(column, column.sql_type())?
                .map(|bs| str::from_utf8(bs).unwrap())
                .collect();
            Arc::new(StringArray::from(vals))
        }
        _ => unimplemented!(),
    };

    Ok(arr)
}
