use crate::arrays::datatype::DataType;

/// Describes the layout of a row for use with a row collection.
#[derive(Debug)]
pub struct RowLayout {
    /// Data types for each column in the row.
    pub(crate) types: Vec<DataType>,
}
