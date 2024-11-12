use super::PrimitiveStorage;
use crate::array::Array;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListItemMetadata {
    pub offset: i32,
    pub len: i32,
}

#[derive(Debug, PartialEq)]
pub struct ListStorage {
    pub(crate) metadata: PrimitiveStorage<ListItemMetadata>,
    pub(crate) array: Array,
}

impl ListStorage {
    pub fn empty_list(array: Array) -> Self {
        ListStorage {
            metadata: vec![ListItemMetadata { offset: 0, len: 0 }].into(),
            array,
        }
    }

    pub fn single_list(array: Array) -> Self {
        let len = array.logical_len();

        ListStorage {
            metadata: vec![ListItemMetadata {
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
