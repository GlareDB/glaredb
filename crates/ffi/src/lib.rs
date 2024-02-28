use std::any::Any;
use std::cell::RefCell;
use std::ffi::CString;

use arrow::array::ArrayRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Signature;
// reexports
pub use datafusion_ext::ffi;
pub use {arrow, arrow_schema, datafusion};

pub mod prelude {
    pub use std::ffi::CString;

    pub use arrow::array::{make_array, ArrayRef};
    pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    pub use arrow_schema::DataType;
    pub use datafusion::arrow::ffi::{from_ffi, to_ffi};
    pub use datafusion::error::{DataFusionError, Result};
    pub use datafusion::logical_expr::{Signature, Volatility};
    pub use datafusion_ext::ffi::*;
    pub use paste::paste;

    pub use crate::{import_array, FFIExpr};
}
pub trait FFIExpr: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn example(&self) -> &str;
    fn signature(&self) -> &Signature;
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}


#[macro_export]
macro_rules! generate_ffi_expr {
    ($name:ident, $UDF:ty, $GNAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: std::sync::OnceLock<std::sync::Arc<dyn FFIExpr>> =
            std::sync::OnceLock::new();
        fn $name() -> std::sync::Arc<dyn FFIExpr> {
            $GNAME
                .get_or_init(|| std::sync::Arc::new(<$UDF>::new()))
                .clone()
        }
        paste! {
            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_name_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let name = expr.name();
                let name = CString::new(name).unwrap();
                name.into_raw()
            }

            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_description_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let description = expr.description();
                let description = CString::new(description).unwrap();
                description.into_raw()
            }

            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_example_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let example = expr.example();
                let example = CString::new(example).unwrap();
                example.into_raw()
            }

            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_signature_ $name>](
                return_value: *mut FFI_Signature,
            ) {
                let expr = $name();
                let signature = expr.signature().clone();
                let signature = FFI_Signature::try_from(signature).unwrap();
                *return_value = signature;
            }

            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_return_type_ $name>](
                arg_types: *mut FFI_ArrowSchema,
                n_args: usize,
                return_value: *mut FFI_ArrowSchema,
            ) {
                let expr = $name();
                let arg_types = std::slice::from_raw_parts(arg_types, n_args)
                    .iter()
                    .map(|c_schema| DataType::try_from(c_schema).unwrap())
                    .collect::<Vec<_>>();
                let return_type = expr.return_type(&arg_types).unwrap();

                let schema: FFI_ArrowSchema = return_type.try_into().unwrap();
                *return_value = schema;
            }
            #[no_mangle]
            pub unsafe extern "C" fn [<_glaredb_plugin_ $name>](
                arrays: *mut FFI_ArrowArray,
                schemas: *mut FFI_ArrowSchema,
                input_len: usize,
                return_value: *mut FFI_ArrowArray,
                return_schema: *mut FFI_ArrowSchema,
            ) {
                let expr = $name();

                let arrays = {
                    assert!(!arrays.is_null());
                    Vec::from_raw_parts(arrays, input_len, input_len)
                };
                let schemas = {
                    assert!(!schemas.is_null());
                    Vec::from_raw_parts(schemas, input_len, input_len)
                };
                let arrays = arrays
                    .into_iter()
                    .zip(schemas.iter())
                    .map(|(arr, schema)| {
                        let arr = import_array(arr, schema).unwrap();
                        arr
                    })
                    .collect::<Vec<_>>();

                let result = expr.invoke(&arrays).unwrap();
                let data = result.to_data();
                let (array, schema) = to_ffi(&data).unwrap();

                *return_value = array;
                *return_schema = schema;
            }
        }
    };
}


thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

pub fn _update_last_error(err: DataFusionError) {
    let msg = format!("{}", err);
    let msg = CString::new(msg).unwrap();
    LAST_ERROR.with(|prev| *prev.borrow_mut() = msg)
}

pub fn _set_panic() {
    let msg = format!("PANIC");
    let msg = CString::new(msg).unwrap();
    LAST_ERROR.with(|prev| *prev.borrow_mut() = msg)
}

#[no_mangle]
pub unsafe extern "C" fn _glaredb_plugin_get_last_error_message() -> *const std::os::raw::c_char {
    LAST_ERROR.with(|prev| prev.borrow_mut().as_ptr())
}

/// # Safety
/// `ArrowArray` and `ArrowSchema` must be valid
pub unsafe fn import_array(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> datafusion::error::Result<ArrayRef> {
    let data = datafusion::arrow::ffi::from_ffi(array, schema).unwrap();
    let arr = datafusion::arrow::array::make_array(data);
    Ok(arr)
}
