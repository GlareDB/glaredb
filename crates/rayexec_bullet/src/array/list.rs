use crate::{
    bitmap::Bitmap,
    compute::interleave::interleave,
    datatype::{DataType, ListTypeMeta},
    scalar::ScalarValue,
    storage::PrimitiveStorage,
};
use rayexec_error::Result;
use std::sync::Arc;

use super::{Array, NullArray, OffsetIndex};

// TODO: This array does not yet have an implementation of ArrayAccessor. I
// don't know what that would actually look like for this yet. I'm implementing
// this mostly get support for reading hugging face parquet files (since a lot
// contain lists).
//
// Pulling values out via `list_extract` should be good enough for a lot of
// cases in the meantime.
#[derive(Debug)]
pub struct VariableListArray<O: OffsetIndex> {
    /// Value validities.
    validity: Option<Bitmap>,

    /// Offsets into the child array.
    ///
    /// Length should be one more than the number of values being held in this
    /// array.
    offsets: PrimitiveStorage<O>,

    /// Child array containing the actual data.
    child: Arc<Array>,
}

pub type ListArray = VariableListArray<i32>;

impl<O> VariableListArray<O>
where
    O: OffsetIndex,
{
    pub fn new(child: impl Into<Arc<Array>>, offsets: Vec<O>, validity: Option<Bitmap>) -> Self {
        debug_assert_eq!(
            offsets.len() - 1,
            validity
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(offsets.len() - 1)
        );

        let child = child.into();
        VariableListArray {
            validity,
            offsets: offsets.into(),
            child,
        }
    }

    /// Produce a list array containing `n` rows with each row being an empty
    /// list.
    pub fn new_empty_with_n_rows(n: usize) -> Self {
        let mut offsets = vec![O::from_usize(0); n];
        offsets.push(O::from_usize(0));
        Self::new(Array::Null(NullArray::new(0)), offsets, None)
    }

    /// Create a list array from some number of equal length child arrays.
    ///
    /// The index of each child array corresponds to a value at the same
    /// position in the scalar list value.
    pub fn try_from_children(children: &[&Array]) -> Result<Self> {
        let len = match children.first() {
            Some(arr) => arr.len(),
            None => {
                let offsets = vec![O::from_usize(0)];
                return Ok(Self::new(Array::Null(NullArray::new(0)), offsets, None));
            }
        };

        let mut indices = Vec::with_capacity(len * children.len());
        for row_idx in 0..len {
            for arr_idx in 0..children.len() {
                indices.push((arr_idx, row_idx));
            }
        }

        let child = interleave(children, &indices)?;

        let mut offsets = Vec::with_capacity(len);
        let mut offset = 0;
        for _ in 0..len {
            offsets.push(O::from_usize(offset));
            offset += children.len();
        }
        offsets.push(O::from_usize(offset));

        // TODO: How do we want to handle validity here? If one of the inputs is
        // a null array, mark it as invalid?

        Ok(Self::new(child, offsets, None))
    }

    pub fn data_type(&self) -> DataType {
        DataType::List(ListTypeMeta {
            datatype: Box::new(self.child.datatype()),
        })
    }

    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if idx >= self.len() {
            return None;
        }

        let start = self.offsets.as_ref()[idx].as_usize();
        let end = self.offsets.as_ref()[idx + 1].as_usize();

        let mut vals = Vec::with_capacity(end - start);

        for idx in start..end {
            let val = self.child.scalar(idx)?; // TODO: Should probably unwrap here.
            vals.push(val);
        }

        Some(ScalarValue::List(vals))
    }

    pub fn child_array(&self) -> &Arc<Array> {
        &self.child
    }

    pub fn offsets(&self) -> &[O] {
        self.offsets.as_ref()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.as_ref().map(|v| v.value(idx)).unwrap_or(true))
    }

    pub fn len(&self) -> usize {
        self.offsets.as_ref().len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}

impl<O> PartialEq for VariableListArray<O>
where
    O: OffsetIndex,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        if self.validity != other.validity {
            return false;
        }

        self.child == other.child
    }
}
