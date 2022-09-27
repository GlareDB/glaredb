use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct Row(Vec<ScalarValue>);

impl Row {
    /// Generate a schema for this row.
    ///
    /// Every field will be '?'.
    pub fn schema(&self) -> Schema {
        let fields = self
            .0
            .iter()
            .map(|val| Field::new("?", val.get_datatype(), true))
            .collect();
        Schema::new(fields)
    }

    /// Check if the schema types for this row matches the provided schema.
    /// Field names are ignored.
    pub fn schema_types_match(&self, schema: &Schema) -> bool {
        if self.0.len() != schema.fields.len() {
            return false;
        }
        self.0
            .iter()
            .zip(schema.fields.iter())
            .all(|(l, r)| &l.get_datatype() == r.data_type())
    }
}

impl From<Vec<ScalarValue>> for Row {
    fn from(values: Vec<ScalarValue>) -> Self {
        Row(values)
    }
}
