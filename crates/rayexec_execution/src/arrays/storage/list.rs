use rayexec_error::{RayexecError, Result};

use super::PrimitiveStorage;
use crate::arrays::array::Array;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata2 {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug, PartialEq)]
pub struct ListStorage {
    pub(crate) metadata: PrimitiveStorage<ListItemMetadata2>,
    pub(crate) array: Array,
}

impl ListStorage {
    pub fn try_new(
        metadata: impl Into<PrimitiveStorage<ListItemMetadata2>>,
        array: Array,
    ) -> Result<Self> {
        let metadata = metadata.into();

        let mut max_idx = 0;
        for m in metadata.as_ref() {
            let end_idx = m.offset + m.len;
            if end_idx > max_idx {
                max_idx = end_idx;
            }
        }

        if max_idx as usize > array.logical_len() {
            return Err(
                RayexecError::new("Metadata index exceeds child array length")
                    .with_field("max_idx", max_idx)
                    .with_field("logical_len", array.logical_len()),
            );
        }

        Ok(ListStorage { metadata, array })
    }

    pub fn empty_list(array: Array) -> Self {
        ListStorage {
            metadata: vec![ListItemMetadata2 { offset: 0, len: 0 }].into(),
            array,
        }
    }

    pub fn single_list(array: Array) -> Self {
        let len = array.logical_len();

        ListStorage {
            metadata: vec![ListItemMetadata2 {
                offset: 0,
                len: len as i32,
            }]
            .into(),
            array,
        }
    }

    pub fn inner_array(&self) -> &Array {
        &self.array
    }

    pub fn len(&self) -> usize {
        self.metadata.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
