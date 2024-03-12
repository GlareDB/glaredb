use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use parser::errors::ParserError;
use parser::options::{OptionValue as SqlOptionValue, ParseOptionValue};
use protogen::metastore::types::options::TableOptionsImpl;
use serde::{Deserialize, Serialize};

use super::errors::DebugError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugTableType {
    /// A table that will always return an error on the record batch stream.
    ErrorDuringExecution,
    /// A table that never stops sending record batches.
    NeverEnding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOptionsDebug {
    pub table_type: DebugTableType,
}

impl ParseOptionValue<DebugTableType> for SqlOptionValue {
    fn parse_opt(self) -> Result<DebugTableType, ParserError> {
        let opt = match self {
            Self::QuotedLiteral(s) | Self::UnquotedLiteral(s) => s
                .parse()
                .map_err(|e: DebugError| ParserError::ParserError(e.to_string()))?,
            o => {
                return Err(ParserError::ParserError(format!(
                    "Expected a string, got: {}",
                    o
                )))
            }
        };
        Ok(opt)
    }
}

impl std::fmt::Display for DebugTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for DebugTableType {
    type Err = DebugError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "error_during_execution" => DebugTableType::ErrorDuringExecution,
            "never_ending" => DebugTableType::NeverEnding,
            other => return Err(DebugError::UnknownDebugTableType(other.to_string())),
        })
    }
}

impl DebugTableType {
    /// Get the arrow schema for the debug table type.
    pub fn arrow_schema(&self) -> ArrowSchema {
        match self {
            DebugTableType::ErrorDuringExecution => {
                ArrowSchema::new(vec![Field::new("a", DataType::Int32, false)])
            }
            DebugTableType::NeverEnding => ArrowSchema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ]),
        }
    }

    /// Get the projected arrow schema.
    pub fn projected_arrow_schema(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> ArrowResult<ArrowSchema> {
        match projection {
            Some(proj) => self.arrow_schema().project(proj),
            None => Ok(self.arrow_schema()),
        }
    }

    /// Produces a record batch that matches this debug table's schema.
    pub fn record_batch(&self, tunnel: bool) -> RecordBatch {
        let base = if tunnel { 10_i32 } else { 1_i32 };
        match self {
            DebugTableType::ErrorDuringExecution => RecordBatch::try_new(
                Arc::new(self.arrow_schema()),
                vec![Arc::new(Int32Array::from_value(base, 30))],
            )
            .unwrap(),
            DebugTableType::NeverEnding => RecordBatch::try_new(
                Arc::new(self.arrow_schema()),
                vec![
                    Arc::new(Int32Array::from_value(base, 30)),
                    Arc::new(Int32Array::from_value(base * 2, 30)),
                    Arc::new(Int32Array::from_value(base * 3, 30)),
                ],
            )
            .unwrap(),
        }
    }

    /// Get a projected record batch for this debug table type.
    pub fn projected_record_batch(
        &self,
        tunnel: bool,
        projection: Option<&Vec<usize>>,
    ) -> ArrowResult<RecordBatch> {
        match projection {
            Some(proj) => self.record_batch(tunnel).project(proj),
            None => Ok(self.record_batch(tunnel)),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DebugTableType::ErrorDuringExecution => "error_during_execution",
            DebugTableType::NeverEnding => "never_ending",
        }
    }
}


impl TableOptionsImpl for TableOptionsDebug {
    const NAME: &'static str = "debug";
}


impl Default for TableOptionsDebug {
    fn default() -> Self {
        TableOptionsDebug {
            table_type: DebugTableType::NeverEnding,
        }
    }
}
