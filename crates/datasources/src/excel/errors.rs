use std::fmt::Debug;

use calamine::{self, XlsxError};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion_ext::errors::ExtensionError;

use crate::object_store::errors::ObjectStoreSourceError;

#[derive(Debug, thiserror::Error)]
pub enum ExcelError {
    #[error("Failed to load XLSX: {0}")]
    Load(String),
    #[error("Failed to create record batch: {0}")]
    CreateRecordBatch(#[from] ArrowError),
    #[error(transparent)]
    CalamineError(#[from] calamine::XlsxError),
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum CalamineError {
    #[error("Failed to open workbook: {0}")]
    OpenWorkbook(#[from] calamine::XlsxError),
}


impl From<calamine::Error> for ExcelError {
    fn from(error: calamine::Error) -> Self {
        ExcelError::Load(error.to_string())
    }
}


impl From<ExcelError> for DataFusionError {
    fn from(e: ExcelError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

impl From<CalamineError> for ExcelError {
    fn from(error: CalamineError) -> Self {
        ExcelError::Load(error.to_string())
    }
}

impl From<ObjectStoreSourceError> for ExcelError {
    fn from(error: ObjectStoreSourceError) -> Self {
        ExcelError::Load(error.to_string())
    }
}

impl From<std::io::Error> for ExcelError {
    fn from(error: std::io::Error) -> Self {
        ExcelError::Load(error.to_string())
    }
}

impl From<ExcelError> for calamine::XlsxError {
    fn from(e: ExcelError) -> Self {
        XlsxError::FileNotFound(e.to_string())
    }
}

impl From<ExcelError> for ExtensionError {
    fn from(e: ExcelError) -> Self {
        ExtensionError::String(e.to_string())
    }
}
