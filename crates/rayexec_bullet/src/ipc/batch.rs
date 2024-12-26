//! Conversion to/from ipc for batches.
use std::collections::VecDeque;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use half::f16;
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};

use super::compression::CompressionType;
use super::gen::message::{FieldNode as IpcFieldNode, RecordBatch as IpcRecordBatch};
use super::gen::schema::Buffer as IpcBuffer;
use super::IpcConfig;

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
        let array = decode_array(&mut buffers, &field.datatype)?;
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

fn decode_array(buffers: &mut BufferReader, datatype: &DataType) -> Result<Array> {
    let node = buffers.try_next_node()?;
    let len = node.length() as usize;
    // Validity buffer always exists for primitive+varlen arrays even if there's
    // no nulls.
    let validity_buffer = buffers.try_next_buf()?;
    let validity = if node.null_count() > 0 {
        let bitmap = Bitmap::try_new(validity_buffer.to_vec(), len)?;
        Some(bitmap.into())
    } else {
        None
    };

    let data: ArrayData = match datatype.physical_type()? {
        PhysicalType::UntypedNull => not_implemented!("IPC-decode untyped null"),
        PhysicalType::Boolean => {
            let data = buffers.try_next_buf()?.to_vec();
            BooleanStorage(Bitmap::try_new(data, node.length() as usize)?).into()
        }
        PhysicalType::Int8 => decode_primitive_values::<i8>(buffers.try_next_buf()?)?,
        PhysicalType::Int16 => decode_primitive_values::<i16>(buffers.try_next_buf()?)?,
        PhysicalType::Int32 => decode_primitive_values::<i32>(buffers.try_next_buf()?)?,
        PhysicalType::Int64 => decode_primitive_values::<i64>(buffers.try_next_buf()?)?,
        PhysicalType::Int128 => decode_primitive_values::<i128>(buffers.try_next_buf()?)?,
        PhysicalType::UInt8 => decode_primitive_values::<u8>(buffers.try_next_buf()?)?,
        PhysicalType::UInt16 => decode_primitive_values::<u16>(buffers.try_next_buf()?)?,
        PhysicalType::UInt32 => decode_primitive_values::<u32>(buffers.try_next_buf()?)?,
        PhysicalType::UInt64 => decode_primitive_values::<u64>(buffers.try_next_buf()?)?,
        PhysicalType::UInt128 => decode_primitive_values::<u128>(buffers.try_next_buf()?)?,
        PhysicalType::Float16 => decode_primitive_values::<f16>(buffers.try_next_buf()?)?,
        PhysicalType::Float32 => decode_primitive_values::<f32>(buffers.try_next_buf()?)?,
        PhysicalType::Float64 => decode_primitive_values::<f64>(buffers.try_next_buf()?)?,
        PhysicalType::Interval => decode_primitive_values::<Interval>(buffers.try_next_buf()?)?,
        PhysicalType::Binary => {
            decode_varlen_values([buffers.try_next_buf()?, buffers.try_next_buf()?])?
        }
        PhysicalType::Utf8 => {
            decode_varlen_values([buffers.try_next_buf()?, buffers.try_next_buf()?])?
        }
        PhysicalType::List => not_implemented!("IPC-decode untyped null"),
    };

    Ok(Array {
        datatype: datatype.clone(),
        selection: None,
        validity,
        data,
    })
}

fn decode_primitive_values<T>(buffer: &[u8]) -> Result<ArrayData>
where
    T: Copy + Default,
    PrimitiveStorage<T>: Into<ArrayData>,
{
    Ok(PrimitiveStorage::copy_from_bytes(buffer)?.into())
}

// TODO: Currently only assumes a single string type.
fn decode_varlen_values(buffers: [&[u8]; 2]) -> Result<ArrayData> {
    let metadata = PrimitiveStorage::<UnionedGermanMetadata>::copy_from_bytes(buffers[0])?;
    let data = PrimitiveStorage::<u8>::copy_from_bytes(buffers[1])?;

    Ok(GermanVarlenStorage { metadata, data }.into())
}

// fn decode_primitive

/// Encode a batch into `data`, returning the message header.
pub fn batch_to_ipc<'a>(
    batch: &Batch,
    data: &mut Vec<u8>,
    builder: &mut FlatBufferBuilder<'a>,
) -> Result<WIPOffset<IpcRecordBatch<'a>>> {
    let mut fields: Vec<IpcFieldNode> = Vec::new();
    let mut buffers: Vec<IpcBuffer> = Vec::new();
    let mut variadic_counts: Vec<i64> = Vec::new();

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
        encode_array(col, data, &mut fields, &mut buffers, &mut variadic_counts)?;
    }

    let fields = builder.create_vector(&fields);
    let buffers = builder.create_vector(&buffers);
    let variadic_counts = builder.create_vector(&variadic_counts);

    let mut batch_builder = RecordBatchBuilder::new(builder);
    batch_builder.add_length(batch.num_rows() as i64);
    batch_builder.add_nodes(fields);
    batch_builder.add_buffers(buffers);
    batch_builder.add_variadicBufferCounts(variadic_counts);

    Ok(batch_builder.finish())
}

fn encode_array(
    array: &Array,
    data: &mut Vec<u8>,
    fields: &mut Vec<IpcFieldNode>,
    buffers: &mut Vec<IpcBuffer>,
    variadic_counts: &mut Vec<i64>,
) -> Result<()> {
    if array.has_selection() {
        return Err(RayexecError::new(
            "Array needs to be unselected before being IPC-encoded",
        ));
    }

    // TODO: These encoding methods aren't entirely IPC compliant. We'll need to
    // optionally compress the buffers, include the compressed length, and
    // ensure they're padded out to 8 bytes.

    // Buffer listing: <https://arrow.apache.org/docs/format/Columnar.html#buffer-listing-for-each-layout>

    // Since arrow doesn't have selection vectors, need to fully materialize the
    // array.
    let array = array.unselect()?;

    // Encode validity.
    let valid_count = array
        .validity()
        .map(|v| v.count_trues())
        .unwrap_or(array.logical_len());
    let null_count = array.logical_len() - valid_count;
    let field = IpcFieldNode::new(array.logical_len() as i64, null_count as i64);
    fields.push(field);

    let offset = data.len();
    match array.validity() {
        Some(validity) => {
            data.extend_from_slice(validity.data());
        }
        None => {
            data.extend(std::iter::repeat(255).take(byte_ceil(array.logical_len())));
        }
    }

    let validity_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
    buffers.push(validity_buffer);

    // Encode buffers.
    match array.array_data() {
        ArrayData::UntypedNull(_) => not_implemented!("IPC-encode untyped null"), // Spec says do nothing.
        ArrayData::Boolean(d) => {
            let offset = data.len();
            data.extend_from_slice(d.0.data());
            let values_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
            buffers.push(values_buffer);
        }
        ArrayData::Int8(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Int16(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Int32(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Int64(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Int128(d) => encode_primitive_values(d, data, buffers),
        ArrayData::UInt8(d) => encode_primitive_values(d, data, buffers),
        ArrayData::UInt16(d) => encode_primitive_values(d, data, buffers),
        ArrayData::UInt32(d) => encode_primitive_values(d, data, buffers),
        ArrayData::UInt64(d) => encode_primitive_values(d, data, buffers),
        ArrayData::UInt128(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Float16(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Float32(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Float64(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Interval(d) => encode_primitive_values(d, data, buffers),
        ArrayData::Binary(d) => match d {
            BinaryData::Binary(d) => {
                encode_primitive_values(&d.offsets, data, buffers);
                encode_primitive_values(&d.data, data, buffers);
            }
            BinaryData::LargeBinary(d) => {
                encode_primitive_values(&d.offsets, data, buffers);
                encode_primitive_values(&d.data, data, buffers);
            }
            BinaryData::German(d) => {
                encode_primitive_values(&d.metadata, data, buffers);
                // Currently only hold 1 data array for these.
                variadic_counts.push(1);
                encode_primitive_values(&d.data, data, buffers);
            }
        },
        ArrayData::List(_) => not_implemented!("IPC-encode list"),
    }

    Ok(())
}

fn encode_primitive_values<T>(
    vals: &PrimitiveStorage<T>,
    data: &mut Vec<u8>,
    buffers: &mut Vec<IpcBuffer>,
) {
    let offset = data.len();
    data.extend_from_slice(vals.as_bytes());
    let values_buffer = IpcBuffer::new(offset as i64, (data[offset..]).len() as i64);
    buffers.push(values_buffer);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatype::DecimalTypeMeta;
    use crate::field::Field;

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
        let batch =
            Batch::try_new([Array::from_iter([3, 2, 1]), Array::from_iter([9, 8, 7])]).unwrap();

        let schema = Schema::new([
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Int32, true),
        ]);

        roundtrip(schema, batch);
    }

    #[test]
    fn utf8_roundtrip() {
        let batch = Batch::try_new([Array::from_iter(["mario", "peach", "yoshi"])]).unwrap();

        let schema = Schema::new([Field::new("f1", DataType::Utf8, true)]);

        roundtrip(schema, batch);
    }

    #[test]
    fn decimal128_roundtrip() {
        let datatype = DataType::Decimal128(DecimalTypeMeta::new(4, 2));
        let arr = Array::new_with_array_data(
            datatype.clone(),
            PrimitiveStorage::from(vec![1000_i128, 1200, 1250]),
        );

        let batch = Batch::try_new([arr]).unwrap();

        let schema = Schema::new([Field::new("f1", datatype, true)]);

        roundtrip(schema, batch)
    }
}
