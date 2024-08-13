//! Conversion to/from ipc for batches.
use std::collections::VecDeque;

use crate::{
    array::{
        Array, Decimal128Array, Decimal64Array, OffsetIndex, PrimitiveArray, VarlenArray,
        VarlenType,
    },
    batch::Batch,
    bitmap::Bitmap,
    bitutil::byte_ceil,
    datatype::DataType,
    field::Schema,
    ipc::gen::message::RecordBatchBuilder,
    storage::PrimitiveStorage,
};

use super::{
    compression::CompressionType,
    gen::{
        message::{FieldNode as IpcFieldNode, RecordBatch as IpcRecordBatch},
        schema::Buffer as IpcBuffer,
    },
    IpcConfig,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use rayexec_error::{not_implemented, OptionExt, Result};

pub fn ipc_to_batch(
    batch: IpcRecordBatch,
    data: &[u8],
    schema: &Schema,
    _conf: &IpcConfig,
) -> Result<Batch> {
    let mut buffers = BufferReader {
        data,
        _decompress_buffer: Vec::new(),
        compression: None,
        buffers: batch.buffers().unwrap().iter().collect(),
        nodes: batch.nodes().unwrap().iter().collect(),
    };

    let mut columns = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let array = ipc_buffers_to_array(&mut buffers, &field.datatype)?;
        columns.push(array);
    }

    Batch::try_new(columns)
}

struct BufferReader<'a> {
    /// Complete message data.
    data: &'a [u8],

    /// Buffer for holding decompressed data.
    _decompress_buffer: Vec<u8>,

    compression: Option<CompressionType>,

    /// "Buffers" from a record batch message. These only contain offsets and
    /// lengths, not the actual data.
    buffers: VecDeque<&'a IpcBuffer>,

    nodes: VecDeque<&'a IpcFieldNode>,
}

impl<'a> BufferReader<'a> {
    fn try_next_buf(&mut self) -> Result<&'a [u8]> {
        let buf = self.buffers.pop_front().required("missing next buffer")?;

        match self.compression {
            Some(_) => {
                // TODO: Decompress into buffer, return that.
                not_implemented!("ipc decompression")
            }
            None => {
                let end = buf.offset() + buf.length();
                let slice = &self.data[buf.offset() as usize..end as usize];

                Ok(slice)
            }
        }
    }

    fn try_next_node(&mut self) -> Result<&'a IpcFieldNode> {
        self.nodes.pop_front().required("missing next node")
    }
}

fn ipc_buffers_to_array(buffers: &mut BufferReader, datatype: &DataType) -> Result<Array> {
    match datatype {
        DataType::Int8 => Ok(Array::Int8(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int16 => Ok(Array::Int16(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int32 => Ok(Array::Int32(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Int64 => Ok(Array::Int64(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt8 => Ok(Array::UInt8(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt16 => Ok(Array::UInt16(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt32 => Ok(Array::UInt32(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::UInt64 => Ok(Array::UInt64(ipc_buffers_to_primitive(
            buffers.try_next_node()?,
            [buffers.try_next_buf()?, buffers.try_next_buf()?],
        )?)),
        DataType::Decimal64(m) => {
            let primitive = ipc_buffers_to_primitive(
                buffers.try_next_node()?,
                [buffers.try_next_buf()?, buffers.try_next_buf()?],
            )?;
            Ok(Array::Decimal64(Decimal64Array::new(
                m.precision,
                m.scale,
                primitive,
            )))
        }
        DataType::Decimal128(m) => {
            let primitive = ipc_buffers_to_primitive(
                buffers.try_next_node()?,
                [buffers.try_next_buf()?, buffers.try_next_buf()?],
            )?;
            Ok(Array::Decimal128(Decimal128Array::new(
                m.precision,
                m.scale,
                primitive,
            )))
        }
        DataType::Utf8 => Ok(Array::Utf8(ipc_buffers_to_varlen(
            buffers.try_next_node()?,
            [
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
            ],
        )?)),
        DataType::LargeUtf8 => Ok(Array::Utf8(ipc_buffers_to_varlen(
            buffers.try_next_node()?,
            [
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
            ],
        )?)),
        DataType::Binary => Ok(Array::Utf8(ipc_buffers_to_varlen(
            buffers.try_next_node()?,
            [
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
            ],
        )?)),
        DataType::LargeBinary => Ok(Array::Utf8(ipc_buffers_to_varlen(
            buffers.try_next_node()?,
            [
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
                buffers.try_next_buf()?,
            ],
        )?)),

        other => not_implemented!("ipc to array {other}"),
    }
}

fn ipc_buffers_to_primitive<T: Default + Copy>(
    node: &IpcFieldNode,
    buffers: [&[u8]; 2],
) -> Result<PrimitiveArray<T>> {
    let validity = if node.null_count() > 0 {
        let bitmap = Bitmap::try_new(buffers[0].to_vec(), node.length() as usize)?;
        Some(bitmap)
    } else {
        None
    };

    let values = PrimitiveStorage::<T>::copy_from_bytes(buffers[1])?;

    Ok(PrimitiveArray::new(values, validity))
}

fn ipc_buffers_to_varlen<T, O>(
    node: &IpcFieldNode,
    buffers: [&[u8]; 3],
) -> Result<VarlenArray<T, O>>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    let validity = if node.null_count() > 0 {
        let bitmap = Bitmap::try_new(buffers[0].to_vec(), node.length() as usize)?;
        Some(bitmap)
    } else {
        None
    };

    let offsets = PrimitiveStorage::<O>::copy_from_bytes(buffers[1])?;
    let data = PrimitiveStorage::<u8>::copy_from_bytes(buffers[2])?;

    VarlenArray::try_from_buffers(data, offsets, validity)
}

/// Encode a batch into `data`, returning the message header.
pub fn batch_to_ipc<'a>(
    batch: &Batch,
    data: &mut Vec<u8>,
    builder: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<IpcRecordBatch<'a>>> {
    let mut fields: Vec<IpcFieldNode> = Vec::new();
    let mut buffers: Vec<IpcBuffer> = Vec::new();

    // TODO: There's some ambiguity in the spec about what the offset should
    // actually be.
    //
    // > The memory offset and length of each constituent Buffer in the record
    // > batch’s body
    //
    // This can be interpreted as the offset _not_ including the encapsulated
    // message metadata.
    //
    // However, it later on says:
    //
    // > The Buffer Flatbuffers value describes the location and size of a piece
    // > of memory. Generally these are interpreted relative to the encapsulated
    // > message format defined below.
    //
    // The **encapsulated message format** section describes a message with
    // continuation bytes and metadata. So I don't know. Also the "generally"
    // part isn't amazing. Like how can we be sure we're compatible with other
    // arrow implementations.
    //
    // arrow-rs follows the first part for offset, so that's what we'll do. It's
    // also easier than the alternative.

    for col in batch.columns() {
        encode_array(col.as_ref(), data, &mut fields, &mut buffers)?;
    }

    let fields = builder.create_vector(&fields);
    let buffers = builder.create_vector(&buffers);

    let mut batch_builder = RecordBatchBuilder::new(builder);
    batch_builder.add_length(batch.num_rows() as i64);
    batch_builder.add_nodes(fields);
    batch_builder.add_buffers(buffers);

    Ok(batch_builder.finish())
}

fn encode_array(
    array: &Array,
    data: &mut Vec<u8>,
    fields: &mut Vec<IpcFieldNode>,
    buffers: &mut Vec<IpcBuffer>,
) -> Result<()> {
    // TODO: These encoding methods aren't entirely IPC compliant. We'll need to
    // optionally compress the buffers, include the compressed length, and
    // ensure they're padded out to 8 bytes.

    match array {
        Array::Int8(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::Int16(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::Int32(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::Int64(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::UInt8(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::UInt16(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::UInt32(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::UInt64(arr) => {
            encode_primitive(arr, data, fields, buffers);
        }
        Array::Decimal64(arr) => encode_primitive(arr.get_primitive(), data, fields, buffers),
        Array::Decimal128(arr) => encode_primitive(arr.get_primitive(), data, fields, buffers),
        Array::Utf8(arr) => {
            encode_varlen(arr, data, fields, buffers);
        }
        Array::LargeUtf8(arr) => {
            encode_varlen(arr, data, fields, buffers);
        }
        Array::Binary(arr) => {
            encode_varlen(arr, data, fields, buffers);
        }
        Array::LargeBinary(arr) => {
            encode_varlen(arr, data, fields, buffers);
        }
        other => not_implemented!("array type to field and buffers: {}", other.datatype()),
    }

    Ok(())
}

fn encode_varlen<T, O>(
    array: &VarlenArray<T, O>,
    data: &mut Vec<u8>,
    fields: &mut Vec<IpcFieldNode>,
    buffers: &mut Vec<IpcBuffer>,
) where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    let valid_count = array.validity().map(|v| v.popcnt()).unwrap_or(array.len());
    let null_count = array.len() - valid_count;
    let field = IpcFieldNode::new(array.len() as i64, null_count as i64);

    fields.push(field);

    let offset = data.len();
    match array.validity() {
        Some(validity) => {
            data.extend_from_slice(validity.data());
        }
        None => {
            data.extend(std::iter::repeat(255).take(byte_ceil(array.len())));
        }
    }

    // Buffer order:
    // 1. Validity
    // 2. Offsets
    // 3. Data

    let validity_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
    buffers.push(validity_buffer);

    let offsets_offset = data.len();
    data.extend_from_slice(array.offsets().as_bytes());

    let offsets_buffer =
        IpcBuffer::new(offsets_offset as i64, (data[offsets_offset..]).len() as i64);
    buffers.push(offsets_buffer);

    let values_offset = data.len();
    data.extend_from_slice(array.data().as_bytes());

    let values_buffer = IpcBuffer::new(values_offset as i64, (data[values_offset..]).len() as i64);
    buffers.push(values_buffer);
}

fn encode_primitive<T>(
    array: &PrimitiveArray<T>,
    data: &mut Vec<u8>,
    fields: &mut Vec<IpcFieldNode>,
    buffers: &mut Vec<IpcBuffer>,
) {
    let valid_count = array.validity().map(|v| v.popcnt()).unwrap_or(array.len());
    let null_count = array.len() - valid_count;
    let field = IpcFieldNode::new(array.len() as i64, null_count as i64);

    fields.push(field);

    let offset = data.len();
    match array.validity() {
        Some(validity) => {
            data.extend_from_slice(validity.data());
        }
        None => {
            data.extend(std::iter::repeat(255).take(byte_ceil(array.len())));
        }
    }

    let validity_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
    buffers.push(validity_buffer);

    let offset = data.len();
    data.extend_from_slice(array.values().as_bytes());

    let values_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
    buffers.push(values_buffer);
}

#[cfg(test)]
mod tests {
    use crate::{array::Utf8Array, datatype::DecimalTypeMeta, field::Field};

    use super::*;

    fn roundtrip(schema: Schema, batch: Batch) {
        let mut builder = FlatBufferBuilder::new();
        let mut data_buf = Vec::new();

        let ipc = batch_to_ipc(&batch, &mut data_buf, &mut builder).unwrap();
        builder.finish(ipc, None);
        // Note that this doesn't include the 'data_buf'.
        let buf = builder.finished_data();

        let ipc = flatbuffers::root::<IpcRecordBatch>(buf).unwrap();
        let got = ipc_to_batch(ipc, &data_buf, &schema, &IpcConfig::default()).unwrap();

        assert_eq!(batch, got);
    }

    #[test]
    fn simple_batch_roundtrip() {
        let batch = Batch::try_new([
            Array::Int32(vec![3, 2, 1].into()),
            Array::UInt64(vec![9, 8, 7].into()),
        ])
        .unwrap();

        let schema = Schema::new([
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::UInt64, true),
        ]);

        roundtrip(schema, batch);
    }

    #[test]
    fn utf8_roundtrip() {
        let batch = Batch::try_new([Array::Utf8(Utf8Array::from_iter([
            "mario", "peach", "yoshi",
        ]))])
        .unwrap();

        let schema = Schema::new([Field::new("f1", DataType::Utf8, true)]);

        roundtrip(schema, batch);
    }

    #[test]
    fn decimal128_roundtrip() {
        let batch = Batch::try_new([Array::Decimal128(Decimal128Array::new(
            4,
            2,
            PrimitiveArray::from_iter([1000, 1200, 1250]),
        ))])
        .unwrap();

        let schema = Schema::new([Field::new(
            "f1",
            DataType::Decimal128(DecimalTypeMeta::new(4, 2)),
            true,
        )]);

        roundtrip(schema, batch)
    }
}
