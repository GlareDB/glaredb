use std::sync::Arc;

use datafusion::{arrow::datatypes::Schema, common::DFSchema};
use once_cell::sync::Lazy;

pub mod dispatch;
pub mod errors;
pub mod extension;
pub mod logical_plan;
pub mod physical_plan;
pub mod session_planner;

pub(crate) mod context_builder;

mod preprocess;
static EMPTY_DFSCHEMA: Lazy<Arc<DFSchema>> = Lazy::new(|| Arc::new(DFSchema::empty()));
static EMPTY_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| Arc::new(Schema::empty()));
