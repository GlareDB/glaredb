use std::any::Any;

use arrow::array::ArrayRef;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
#[cfg(feature = "datafusion")]
pub use datafusion;
#[cfg(feature = "datafusion")]
use datafusion::logical_expr::Signature;
#[cfg(not(feature = "datafusion"))]
use signature::Signature;
// reexports
pub use {arrow, arrow_schema};
pub mod ffi;
#[cfg(not(feature = "datafusion"))]
pub mod signature;

#[derive(thiserror::Error, Debug)]
pub enum FFIError {
    #[cfg(feature = "datafusion")]
    #[error("DataFusionError: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("ArrowError: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, FFIError>;
pub mod prelude {
    pub use std::ffi::CString;

    pub use arrow::array::{make_array, ArrayRef};
    pub use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    pub use arrow_schema::DataType;
    #[cfg(feature = "datafusion")]
    pub use datafusion::error::{DataFusionError, Result};
    #[cfg(feature = "datafusion")]
    pub use datafusion::logical_expr::{Signature, Volatility};
    pub use paste::paste;

    pub use crate::ffi::*;
    #[cfg(not(feature = "datafusion"))]
    pub use crate::signature::*;
    pub use crate::{import_array, FFIError, FFIExpr, Result as FFIResult};
}

pub trait FFIExpr: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn example(&self) -> &str;
    fn signature(&self) -> &Signature;
    /// UNKNOWN = 0;
    ///
    /// AGGREGATE = 1;
    ///
    /// SCALAR = 2;
    ///
    /// TABLE_RETURNING = 3;
    fn function_type(&self) -> i32;
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef>;
}


#[macro_export]
/// $name: name of the function
/// $UDF: the function implementation
/// $GNAME: global name of the function
/// example
/// ```text
/// struct EchoExpr;
/// impl FFIExpr for EchoExpr {
/// //...
/// }
/// generate_ffi_expr!(echo, EchoExpr, ECHO_EXPR);
/// ```
/// This will generate a set of ffi functions for the `EchoExpr` struct
/// The symbol that will be used by `GlaredbFFIPlugin` correlates to the first argument "echo"
/// The symbol will be `glaredb_plugin_echo`
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
            #[allow(non_snake_case)]
            pub unsafe extern "C" fn [<_glaredb_plugin_name_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let name = expr.name();
                let name = CString::new(name).unwrap();
                name.into_raw()
            }

            #[no_mangle]
            #[allow(non_snake_case)]
            pub unsafe extern "C" fn [<_glaredb_plugin_description_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let description = expr.description();
                let description = CString::new(description).unwrap();
                description.into_raw()
            }

            #[no_mangle]
            #[allow(non_snake_case)]
            pub unsafe extern "C" fn [<_glaredb_plugin_example_ $name>]() -> *const std::ffi::c_char {
                let expr = $name();
                let example = expr.example();
                let example = CString::new(example).unwrap();
                example.into_raw()
            }

            #[no_mangle]
            #[allow(non_snake_case)]
            pub unsafe extern "C" fn [<_glaredb_plugin_signature_ $name>](
                return_value: *mut FFI_Signature,
            ) {
                let expr = $name();
                let signature = expr.signature().clone();
                let signature = FFI_Signature::try_from(signature).unwrap();
                *return_value = signature;
            }

            #[no_mangle]
            #[allow(non_snake_case)]
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
            #[allow(non_snake_case)]
            pub unsafe extern "C" fn [<_glaredb_plugin_function_type_ $name>]() -> i32 {
                let expr = $name();
                expr.function_type()
            }

            #[no_mangle]
            #[allow(non_snake_case)]
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


/// Usage:
/// ```text
/// generate_lib!(
/// my_lib // the namespace
/// (echo, foo, bar), // the functions
/// );
/// ```
#[macro_export]
macro_rules! generate_lib {
    ($namespace:ident, (($name:ident))) => {
        generate_lib!($namespace, ($($name),+));
    };
    ($namespace:ident, ($($name:ident),+)) => {
        #[no_mangle]
        /// returns a vector of all functions
        pub unsafe extern "C" fn _glaredb_plugin_functions () -> *const *const std::os::raw::c_char {
            let functions = vec![$(stringify!($name)),+];
            let c_strings: Vec<CString> = functions
                .into_iter()
                .map(|s| CString::new(s).unwrap())
                .collect();

            let c_ptrs: Vec<*const std::os::raw::c_char> = c_strings.iter().map(|s| s.as_ptr()).collect();
            let ptr = c_ptrs.as_ptr();

            // the memory is managed by the caller
            std::mem::forget(c_strings);
            std::mem::forget(c_ptrs);
            ptr
        }

        /// # Safety
        #[no_mangle]
        pub unsafe extern "C" fn _glaredb_plugin_get_last_error_message() -> *const std::os::raw::c_char {
            LAST_ERROR.with(|prev| prev.borrow_mut().as_ptr())
        }

        pub const MAJOR: u16 = 0;
        pub const MINOR: u16 = 0;

        pub const fn get_version() -> (u16, u16) {
            (MAJOR, MINOR)
        }
        thread_local! {
            static LAST_ERROR: std::cell::RefCell<CString> = std::cell::RefCell::new(CString::default());
        }

        pub fn _update_last_error(err: glaredb_ffi::FFIError) {
            let msg = format!("{}", err);
            let msg = CString::new(msg).unwrap();
            LAST_ERROR.with(|prev| *prev.borrow_mut() = msg)
        }
        #[no_mangle]
        /// # Safety
        /// this should be safe
        pub unsafe extern "C" fn _glaredb_plugin_get_version() -> u32 {
            let (major, minor) = get_version();
            // Stack bits together
            ((major as u32) << 16) + minor as u32
        }

    }
}

/// # Safety
/// `ArrowArray` and `ArrowSchema` must be valid
pub unsafe fn import_array(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayRef> {
    let data = arrow::ffi::from_ffi(array, schema)?;
    let arr = arrow::array::make_array(data);
    Ok(arr)
}
