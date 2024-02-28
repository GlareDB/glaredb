use std::any::Any;
use std::ffi::{c_char, CString};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::ffi::FFI_Signature;
use glaredb_ffi::*;
use paste::paste;

struct EchoExpr {
    signature: Signature,
}

impl EchoExpr {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl FFIExpr for EchoExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "echo"
    }

    fn description(&self) -> &str {
        "Echoes the input value."
    }

    fn example(&self) -> &str {
        "SELECT echo(1)"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Expected 1 argument, got {}",
                arg_types.len()
            )));
        }
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Expected 1 argument, got {}",
                args.len()
            )));
        }
        Ok(args[0].clone())
    }
}

generate_ffi_expr!(echo, EchoExpr, ECHO);
