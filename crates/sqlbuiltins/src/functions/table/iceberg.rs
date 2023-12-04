mod data_files;
mod scan;
mod snapshots;

use std::collections::HashMap;
use std::sync::Arc;

use super::table_location_and_opts;
use crate::builtins::TableFunc;
use async_trait::async_trait;
pub(crate) use data_files::*;
use datafusion::arrow::array::{Int32Builder, Int64Builder, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::lake::iceberg::table::IcebergTable;
use datasources::lake::storage_options_into_object_store;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
pub(crate) use scan::*;
pub(crate) use snapshots::*;

fn box_err<E>(err: E) -> ExtensionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    ExtensionError::Access(Box::new(err))
}
