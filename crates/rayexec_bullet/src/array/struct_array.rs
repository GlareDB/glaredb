use rayexec_error::{RayexecError, Result};

use crate::{bitmap::Bitmap, datatype::DataType, scalar::ScalarValue};

use super::Array;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct StructArray {
    validity: Option<Bitmap>,
    arrays: Vec<(String, Arc<Array>)>,
}

impl StructArray {
    pub fn try_new(keys: Vec<String>, values: Vec<Arc<Array>>) -> Result<Self> {
        if keys.len() != values.len() {
            return Err(RayexecError::new(format!(
                "Received {} keys for struct, but only {} values",
                keys.len(),
                values.len()
            )));
        }

        let arrays = keys.into_iter().zip(values).collect();

        Ok(StructArray {
            validity: None,
            arrays,
        })
    }

    pub fn len(&self) -> usize {
        self.arrays[0].1.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(super::is_valid(self.validity.as_ref(), idx))
    }

    pub fn datatype(&self) -> DataType {
        unimplemented!()
    }

    pub fn array_for_key(&self, key: &str) -> Option<&Arc<Array>> {
        self.arrays
            .iter()
            .find(|(k, _arr)| k == key)
            .map(|(_, arr)| arr)
    }

    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if idx >= self.len() {
            return None;
        }

        let scalars: Vec<_> = self
            .arrays
            .iter()
            .map(|(_, arr)| arr.scalar(idx).unwrap())
            .collect();

        Some(ScalarValue::Struct(scalars))
    }
}
