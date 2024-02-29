use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result};

/// A batch of same-length arrays.
///
/// Almost equivalent to an arrow RecordBatch except for not having an
/// associated schema since it's not needed in many cases.
#[derive(Debug, Clone, PartialEq)]
pub struct DataBatch {
    cols: Vec<ArrayRef>,
    num_rows: usize,
}

impl DataBatch {
    pub fn empty() -> Self {
        DataBatch {
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        DataBatch {
            cols: Vec::new(),
            num_rows,
        }
    }

    pub fn try_new(cols: Vec<ArrayRef>) -> Result<Self> {
        let len = match cols.first() {
            Some(arr) => arr.len(),
            None => return Ok(Self::empty()),
        };

        for col in &cols {
            if col.len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}",
                    col.len()
                )));
            }
        }

        Ok(DataBatch {
            cols,
            num_rows: len,
        })
    }

    pub fn schema(&self) -> DataBatchSchema {
        let types = self
            .cols
            .iter()
            .map(|col| col.data_type().clone())
            .collect();
        DataBatchSchema::new(types)
    }

    pub fn column(&self, idx: usize) -> Option<&ArrayRef> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[ArrayRef] {
        &self.cols
    }

    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices
            .iter()
            .map(|idx| self.cols.get(*idx).unwrap().clone())
            .collect();
        DataBatch {
            cols,
            num_rows: self.num_rows,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

impl From<RecordBatch> for DataBatch {
    fn from(value: RecordBatch) -> Self {
        // record batch sucks
        let cols = value.columns().to_vec();
        DataBatch {
            cols,
            num_rows: value.num_rows(),
        }
    }
}

/// A schema for a DataBatch.
///
/// Differs from a normal arrow schema in that there's no names associated with
/// the columns. Execution uses column positions, not names, when referencing
/// columns in a batch.
#[derive(Debug, Clone)]
pub struct DataBatchSchema {
    types: Vec<DataType>,
}

impl DataBatchSchema {
    pub const fn empty() -> Self {
        DataBatchSchema { types: Vec::new() }
    }

    pub fn new(types: Vec<DataType>) -> Self {
        DataBatchSchema { types }
    }

    pub fn get_types(&self) -> &[DataType] {
        &self.types
    }
}

impl<S: AsRef<Schema>> From<S> for DataBatchSchema {
    fn from(value: S) -> Self {
        let schema = value.as_ref();
        let types = schema
            .fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Self::new(types)
    }
}

#[derive(Debug, Clone)]
pub struct NamedDataBatchSchema {
    names: Vec<String>,
    types: Vec<DataType>,
}

impl NamedDataBatchSchema {
    pub fn try_new<S: Into<String>>(names: Vec<S>, types: Vec<DataType>) -> Result<Self> {
        if names.len() != types.len() {
            return Err(RayexecError::new(format!(
                "Names and type vectors having differing lengths, names: {}, types: {}",
                names.len(),
                types.len()
            )));
        }

        let names = names.into_iter().map(|s| s.into()).collect();

        Ok(NamedDataBatchSchema { names, types })
    }

    pub fn get_name_and_type(&self, idx: usize) -> Option<(&str, &DataType)> {
        let name = self.names.get(idx)?;
        let typ = self.types.get(idx)?;
        Some((name, typ))
    }

    pub fn into_names_and_types(self) -> (Vec<String>, Vec<DataType>) {
        (self.names, self.types)
    }
}

impl From<&Schema> for NamedDataBatchSchema {
    fn from(schema: &Schema) -> Self {
        let types = schema
            .fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let names = schema.fields.iter().map(|f| f.name().clone()).collect();
        Self { names, types }
    }
}

/// Try to widen a data type such that both `left` and `right` fit inside that
/// data type.
///
/// Only "reasonable" widening happens here. E.g. this won't return Utf8 to fit
/// both a List type and a numeric type.
pub fn maybe_widen(left: &DataType, right: &DataType) -> Option<DataType> {
    Some(match (left, right) {
        // Both are signed integers.
        (DataType::Int64, right) if right.is_signed_integer() => DataType::Int64,
        (DataType::Int32, right) if right.is_signed_integer() => DataType::Int32,
        (DataType::Int16, right) if right.is_signed_integer() => DataType::Int16,
        (DataType::Int8, right) if right.is_signed_integer() => DataType::Int8,

        // Both are unsigned integers.
        (DataType::UInt64, right) if right.is_unsigned_integer() => DataType::UInt64,
        (DataType::UInt32, right) if right.is_unsigned_integer() => DataType::UInt32,
        (DataType::UInt16, right) if right.is_unsigned_integer() => DataType::UInt16,
        (DataType::UInt8, right) if right.is_unsigned_integer() => DataType::UInt8,
        _ => return None,
    })
}
