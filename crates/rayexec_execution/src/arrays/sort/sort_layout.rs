use std::borrow::Borrow;
use std::collections::BTreeMap;

use half::f16;
use rayexec_error::Result;

use crate::arrays::array::flat::FlattenedArray;
use crate::arrays::array::physical_type::{
    Addressable,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    ScalarStorage,
    UntypedNull,
};
use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;
use crate::arrays::row::row_blocks::BlockAppendState;
use crate::arrays::row::row_layout::RowLayout;
use crate::arrays::scalar::interval::Interval;

/// Configuration for how to encode a column for sorting.
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// If we should encode columns to reverse the natural sort order for
    /// values.
    ///
    /// If this is false, this should correspond to sorting in ascending order
    /// (e.g. '1 < 2' evaluates to true).
    ///
    /// If true, this is reverse the sort order (e.g. '1 < 2' evaluates to
    /// false, causing '2' to come before '1').
    pub desc: bool,
    /// If we should encode nulls such that they should be ordered before any
    /// valid values.
    pub nulls_first: bool,
    /// Datatype of the column.
    pub datatype: DataType,
}

impl SortColumn {
    /// Create a new column for a datatype that sorts ascending with nulls last.
    pub fn new_asc_nulls_last(datatype: DataType) -> Self {
        SortColumn {
            desc: false,
            nulls_first: false,
            datatype,
        }
    }

    const fn invalid_byte(&self) -> u8 {
        if self.nulls_first {
            0
        } else {
            0xFF
        }
    }

    const fn valid_byte(&self) -> u8 {
        if self.nulls_first {
            0xFF
        } else {
            0
        }
    }

    /// Invert all bits in buf if this column should be ordered descending.
    ///
    /// Does nothing if the column is ascending.
    ///
    /// This is done to encode ordering in the encoding which lets us skip
    /// having extra logic to handle ordering in the operators. While that would
    /// be easy with something like 'ORDER BY a DESC', it would get tricky with
    /// 'ORDER BY a DESC, b ASC, c DESC'.
    fn invert_if_desc(&self, buf: &mut [u8]) {
        if self.desc {
            for b in buf {
                *b = !*b;
            }
        }
    }
}

/// Row layout for sorting.
///
/// Each column is encoded where the resulting bytes can be compared directly
/// and retain the sort properties desired (nulls first/last, asc/desc).
///
/// Key columns that require heap blocks (strings, nested) will an associated
/// row layout to use in additional to prefix encoding. This avoids the need to
/// encode entire strings or nested data in our sort blocks which means they're
/// able to be fixed sized.
///
/// Each encoded row will have space at the end of a single u32. This will be
/// the original row index for a single block prior to sorting that block. After
/// we sort, the reordered indices then can be used to reorder the heap keys and
/// data blocks.
// TODO: Size (184)
#[derive(Debug, Clone)]
pub struct SortLayout {
    /// Columns that are part of the sort.
    pub(crate) columns: Vec<SortColumn>,
    /// Size in bytes for each column in the sort layout.
    pub(crate) column_widths: Vec<usize>,
    /// Byte offsets within the encoded row to the start of the value.
    pub(crate) offsets: Vec<usize>,
    /// Size in bytes of the portion of the row to compare.
    pub(crate) compare_width: usize,
    /// Size in bytes of a single row.
    pub(crate) row_width: usize,
    /// Row layout for columns that require heap blocks (varlen, nested).
    pub(crate) heap_layout: RowLayout,
    /// Mapping between columns in the sort layout to column the heap layout.
    ///
    /// Empty if there are no columns in this layout that require heap blocks.
    ///
    /// BTreeMap as we want to iterate this in order when pulling out keys that
    /// require heap blocks when appending.
    pub(crate) heap_mapping: BTreeMap<usize, usize>,
}

impl SortLayout {
    pub const ROW_INDEX_WIDTH: usize = std::mem::size_of::<u32>();

    pub fn new(columns: impl IntoIterator<Item = SortColumn>) -> Self {
        let columns: Vec<_> = columns.into_iter().collect();

        let mut heap_types = Vec::new();
        let mut heap_mapping = BTreeMap::new();

        let mut offset = 0;
        let mut offsets = Vec::with_capacity(columns.len());

        let mut column_widths = Vec::with_capacity(columns.len());

        for (idx, col) in columns.iter().enumerate() {
            let phys_type = col.datatype.physical_type();
            let width = key_width_for_physical_type(phys_type);

            offsets.push(offset);
            offset += width;

            column_widths.push(width);

            if matches!(col.datatype, DataType::Utf8 | DataType::Binary) {
                heap_mapping.insert(idx, heap_types.len());
                heap_types.push(col.datatype.clone());
            }
        }

        let heap_layout = RowLayout::new(heap_types);
        let compare_width = offset;
        let row_width = compare_width + Self::ROW_INDEX_WIDTH;

        SortLayout {
            columns,
            column_widths,
            offsets,
            compare_width,
            row_width,
            heap_layout,
            heap_mapping,
        }
    }

    pub fn datatypes(&self) -> impl ExactSizeIterator<Item = DataType> + '_ {
        self.columns.iter().map(|col| col.datatype.clone())
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn any_requires_heap(&self) -> bool {
        self.heap_layout.num_columns() > 0
    }

    pub fn column_requires_heap(&self, col: usize) -> bool {
        self.heap_mapping.contains_key(&col)
    }

    /// Return the buffer size needed to store some number of rows.
    pub const fn buffer_size(&self, rows: usize) -> usize {
        self.row_width * rows
    }

    /// Writes key arrays to row pointers in state.
    ///
    /// For utf8/binary arrays, this will only write the prefix to the row
    /// block.
    pub(crate) unsafe fn write_key_arrays<A>(
        &self,
        state: &mut BlockAppendState,
        arrays: &[A],
        num_rows: usize,
    ) -> Result<()>
    where
        A: Borrow<Array>,
    {
        for (array_idx, array) in arrays.iter().enumerate() {
            let array = array.borrow().flatten()?;
            write_key_array(
                self,
                array.physical_type(),
                array_idx,
                array,
                &state.row_pointers,
                num_rows,
            )?;
        }

        Ok(())
    }

    /// Get a mutable buffer of the exact size for a column in a row.
    ///
    /// # Safety
    ///
    /// The row pointer must be a pointer to a row with the correct width
    /// according to this layout.
    unsafe fn column_buffer_mut(&self, row_ptr: *mut u8, col: usize) -> &mut [u8] {
        let buf = std::slice::from_raw_parts_mut(row_ptr, self.row_width);
        let size = self.column_widths[col];
        let offset = self.offsets[col];
        &mut buf[offset..offset + size]
    }

    /// Get a mut ptr to the row index for a row.
    ///
    /// The returned pointer is **not** guaranteed to be aligned.
    ///
    /// # Safetey
    ///
    /// ...
    pub(crate) unsafe fn row_index_mut_ptr(&self, row_ptr: *mut u8) -> *mut u32 {
        row_ptr.byte_add(self.compare_width).cast()
    }
}

/// Get the sort key width for a physical type.
const fn key_width_for_physical_type(phys_type: PhysicalType) -> usize {
    let val_width = match phys_type {
        PhysicalType::UntypedNull => UntypedNull::ENCODE_WIDTH,
        PhysicalType::Boolean => bool::ENCODE_WIDTH,
        PhysicalType::Int8 => i8::ENCODE_WIDTH,
        PhysicalType::Int16 => i16::ENCODE_WIDTH,
        PhysicalType::Int32 => i32::ENCODE_WIDTH,
        PhysicalType::Int64 => i64::ENCODE_WIDTH,
        PhysicalType::Int128 => i128::ENCODE_WIDTH,
        PhysicalType::UInt8 => u8::ENCODE_WIDTH,
        PhysicalType::UInt16 => u16::ENCODE_WIDTH,
        PhysicalType::UInt32 => u32::ENCODE_WIDTH,
        PhysicalType::UInt64 => u64::ENCODE_WIDTH,
        PhysicalType::UInt128 => u128::ENCODE_WIDTH,
        PhysicalType::Float16 => f16::ENCODE_WIDTH,
        PhysicalType::Float32 => f32::ENCODE_WIDTH,
        PhysicalType::Float64 => f64::ENCODE_WIDTH,
        PhysicalType::Interval => Interval::ENCODE_WIDTH,
        PhysicalType::Binary => StringPrefix::ENCODE_WIDTH,
        PhysicalType::Utf8 => StringPrefix::ENCODE_WIDTH,
        _ => unimplemented!(),
    };

    // Include 1 for the validity byte at the start.
    val_width + 1
}

unsafe fn write_key_array(
    layout: &SortLayout,
    phys_type: PhysicalType,
    array_idx: usize,
    array: FlattenedArray,
    row_pointers: &[*mut u8],
    num_rows: usize,
) -> Result<()> {
    match phys_type {
        PhysicalType::UntypedNull => {
            write_scalar::<PhysicalUntypedNull>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Boolean => {
            write_scalar::<PhysicalBool>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Int8 => {
            write_scalar::<PhysicalI8>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Int16 => {
            write_scalar::<PhysicalI16>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Int32 => {
            write_scalar::<PhysicalI32>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Int64 => {
            write_scalar::<PhysicalI64>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Int128 => {
            write_scalar::<PhysicalI128>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::UInt8 => {
            write_scalar::<PhysicalU8>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::UInt16 => {
            write_scalar::<PhysicalU16>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::UInt32 => {
            write_scalar::<PhysicalU32>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::UInt64 => {
            write_scalar::<PhysicalU64>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::UInt128 => {
            write_scalar::<PhysicalU128>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Float16 => {
            write_scalar::<PhysicalF16>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Float32 => {
            write_scalar::<PhysicalF32>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Float64 => {
            write_scalar::<PhysicalF64>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Interval => {
            write_scalar::<PhysicalInterval>(layout, array_idx, array, row_pointers, num_rows)
        }
        PhysicalType::Utf8 | PhysicalType::Binary => {
            write_binary_prefix(layout, array_idx, array, row_pointers, num_rows)
        }
        other => unimplemented!("other: {other}"),
    }
}

unsafe fn write_scalar<S>(
    layout: &SortLayout,
    array_idx: usize,
    array: FlattenedArray,
    row_pointers: &[*mut u8],
    num_rows: usize,
) -> Result<()>
where
    S: ScalarStorage,
    S::StorageType: ComparableEncode + Default + Copy + Sized,
{
    debug_assert_eq!(num_rows, row_pointers.len());

    let col = &layout.columns[array_idx];
    let valid_b = col.valid_byte();
    let invalid_b = col.invalid_byte();

    let null_val = <S::StorageType>::default();

    let data = S::get_addressable(array.array_buffer)?;
    let validity = array.validity;

    for row_idx in 0..num_rows {
        let col_buf = layout.column_buffer_mut(row_pointers[row_idx], array_idx);

        if validity.is_valid(row_idx) {
            let sel_idx = array.selection.get(row_idx).unwrap();
            col_buf[0] = valid_b;

            let v = data.get(sel_idx).unwrap();
            let val_buf = &mut col_buf[1..];
            v.encode(val_buf);
            col.invert_if_desc(val_buf);
        } else {
            col_buf[0] = invalid_b;
            null_val.encode(&mut col_buf[1..]);
        }
    }

    Ok(())
}

unsafe fn write_binary_prefix(
    layout: &SortLayout,
    array_idx: usize,
    array: FlattenedArray,
    row_pointers: &[*mut u8],
    num_rows: usize,
) -> Result<()> {
    debug_assert_eq!(num_rows, row_pointers.len());

    let col = &layout.columns[array_idx];
    let valid_b = col.valid_byte();
    let invalid_b = col.invalid_byte();

    let data = PhysicalBinary::get_addressable(array.array_buffer)?;
    let validity = array.validity;

    for row_idx in 0..num_rows {
        let col_buf = layout.column_buffer_mut(row_pointers[row_idx], array_idx);

        if validity.is_valid(row_idx) {
            let sel_idx = array.selection.get(row_idx).unwrap();
            col_buf[0] = valid_b;

            let v = data.get(sel_idx).unwrap();
            let prefix = StringPrefix::new_from_buf(v);

            let val_buf = &mut col_buf[1..];
            prefix.encode(val_buf);
            col.invert_if_desc(val_buf);
        } else {
            col_buf[0] = invalid_b;
            StringPrefix::EMPTY.encode(&mut col_buf[1..]);
        }
    }

    Ok(())
}

/// Trait for types that can encode themselves into a comparable binary
/// representation.
trait ComparableEncode: Sized {
    const ENCODE_WIDTH: usize = std::mem::size_of::<Self>();
    fn encode(&self, buf: &mut [u8]);
}

impl ComparableEncode for UntypedNull {
    const ENCODE_WIDTH: usize = 0;
    fn encode(&self, buf: &mut [u8]) {
        // Do nothing, we encode nothing for null values as a column containing
        // just nulls has no order.
    }
}

/// Implements `ComparableEncode` for unsigned ints.
macro_rules! comparable_encode_unsigned {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let b = self.to_be_bytes();
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_unsigned!(u8);
comparable_encode_unsigned!(u16);
comparable_encode_unsigned!(u32);
comparable_encode_unsigned!(u64);
comparable_encode_unsigned!(u128);

/// Implements `ComparableEncode` for signed ints.
macro_rules! comparable_encode_signed {
    ($type:ty) => {
        impl ComparableEncode for $type {
            fn encode(&self, buf: &mut [u8]) {
                let mut b = self.to_be_bytes();
                b[0] ^= 128; // Flip sign bit.
                buf.copy_from_slice(&b);
            }
        }
    };
}

comparable_encode_signed!(i8);
comparable_encode_signed!(i16);
comparable_encode_signed!(i32);
comparable_encode_signed!(i64);
comparable_encode_signed!(i128);

impl ComparableEncode for f16 {
    fn encode(&self, buf: &mut [u8]) {
        let bits = self.to_bits() as i16;
        let v = bits ^ (((bits >> 15) as u16) >> 1) as i16;
        v.encode(buf)
    }
}

impl ComparableEncode for f32 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i32;
        let v = bits ^ (((bits >> 31) as u32) >> 1) as i32;
        v.encode(buf)
    }
}

impl ComparableEncode for f64 {
    fn encode(&self, buf: &mut [u8]) {
        // Adapted from <https://github.com/rust-lang/rust/blob/791adf759cc065316f054961875052d5bc03e16c/library/core/src/num/f32.rs#L1456-L1485>
        let bits = self.to_bits() as i64;
        let v = bits ^ (((bits >> 31) as u64) >> 1) as i64;
        v.encode(buf)
    }
}

impl ComparableEncode for Interval {
    fn encode(&self, buf: &mut [u8]) {
        // TODO: We'll probably need to ensure intervals are normalized.
        self.months.encode(&mut buf[0..4]);
        self.days.encode(&mut buf[4..8]);
        self.nanos.encode(&mut buf[8..]);
    }
}

// FALSE < TRUE
impl ComparableEncode for bool {
    fn encode(&self, buf: &mut [u8]) {
        if *self {
            buf[0] = 0;
        } else {
            buf[0] = 1;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StringPrefix {
    prefix: [u8; 12],
}

impl StringPrefix {
    const EMPTY: Self = StringPrefix { prefix: [0; 12] };

    fn new_from_buf(buf: &[u8]) -> Self {
        let mut prefix = [0; 12];
        let count = usize::min(buf.len(), 12);

        (&mut prefix[0..count]).copy_from_slice(&buf[0..count]);

        StringPrefix { prefix }
    }
}

impl ComparableEncode for StringPrefix {
    fn encode(&self, buf: &mut [u8]) {
        buf.copy_from_slice(&self.prefix);
    }
}
