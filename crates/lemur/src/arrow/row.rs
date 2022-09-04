use crate::arrow::chunk::TypeSchema;
use crate::arrow::expr::{ScalarExpr, ScalarExprResult};
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{LemurError, Result};
use serde::{Deserialize, Serialize};

// Note this a pretty inefficient way of storing row data. Eventually we'll want
// to have "packed" and "unpacked" variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row(Vec<ScalarOwned>);

impl Row {
    pub fn from_constant_exprs<'a>(exprs: impl IntoIterator<Item = &'a ScalarExpr>) -> Result<Row> {
        Ok(exprs
            .into_iter()
            .map(|expr| expr.try_evaluate_constant())
            .collect::<Result<Vec<_>>>()?
            .into())
    }

    pub fn type_schema(&self) -> TypeSchema {
        self.0
            .iter()
            .map(|scalar| scalar.data_type())
            .collect::<Vec<_>>()
            .into()
    }

    pub fn matches_type_schema(&self, schema: &TypeSchema) -> bool {
        if self.num_columns() != schema.num_columns() {
            return false;
        }

        self.0
            .iter()
            .zip(schema.0.iter())
            .all(|(row_val, datatype)| &row_val.data_type() == datatype)
    }

    pub fn num_columns(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ScalarOwned> {
        self.0.iter()
    }

    /// Pop the right-most scalar from the row.
    pub fn pop_last(&mut self) -> Option<ScalarOwned> {
        self.0.pop()
    }
}

impl From<Vec<ScalarOwned>> for Row {
    fn from(vals: Vec<ScalarOwned>) -> Self {
        Row(vals)
    }
}
