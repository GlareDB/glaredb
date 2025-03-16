use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::arrays::field::{Field, Schema};
use crate::arrays::scalar::ScalarValue;
use crate::database::memory_catalog::MemoryCatalog;
use crate::database::{AttachInfo, DatabaseContext};
use crate::functions::table::{
    PlannedTableFunction2,
    ScanPlanner2,
    TableFunction2,
    TableFunctionPlanner2,
};
use crate::functions::{FunctionInfo, Signature};
