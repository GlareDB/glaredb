use std::cell::RefCell;
use std::ffi::{c_char, CString};

use arrow::array::ArrayRef;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::ffi::FFI_Signature;

#[no_mangle]
pub unsafe extern "C" fn _glaredb_plugin_return_type_echo(
    arg_types: *mut FFI_ArrowSchema,
    n_args: usize,
    return_value: *mut FFI_ArrowSchema,
) {
    if n_args != 1 {
        let schema: FFI_ArrowSchema = DataType::Utf8.try_into().unwrap();
        *return_value = schema;
        return;
    }
    let args = std::slice::from_raw_parts(arg_types, n_args)
        .iter()
        .map(|c_schema| DataType::try_from(c_schema).unwrap())
        .collect::<Vec<_>>();
    let dtype = args.get(0).unwrap().clone();
    let schema: FFI_ArrowSchema = dtype.try_into().unwrap();

    *return_value = schema;
}

#[no_mangle]
pub unsafe extern "C" fn _glaredb_plugin_get_version() -> u32 {
    0
}

#[no_mangle]
unsafe extern "C" fn _glaredb_plugin_signature_echo(return_value: *mut FFI_Signature) {
    let signature = Signature::exact(vec![DataType::Int64], Volatility::Immutable);
    let signature = FFI_Signature::try_from(signature).unwrap();
    *return_value = signature;
}

/// Unknown = 0,
/// Aggregate = 1,
/// Scalar = 2,
/// TableReturning = 3,
#[no_mangle]
pub static _glaredb_plugin_function_type_echo: i32 = 2;

#[no_mangle]
unsafe extern "C" fn _glaredb_plugin_sql_example_echo() -> *const c_char {
    let example = "SELECT echo(1)";
    let example = CString::new(example).unwrap();
    example.into_raw()
}

#[no_mangle]
unsafe extern "C" fn _glaredb_plugin_description_echo() -> *const c_char {
    let example = "Echoes the input value.";
    let example = CString::new(example).unwrap();
    example.into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn _glaredb_plugin_echo(
    arrays: *mut FFI_ArrowArray,
    schemas: *mut FFI_ArrowSchema,
    input_len: usize,
    return_value: *mut FFI_ArrowArray,
    return_schema: *mut FFI_ArrowSchema,
) {
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

    let first_value = arrays.get(0).unwrap().clone();
    let data = first_value.to_data();
    let (array, schema) = to_ffi(&data).unwrap();

    *return_value = array;
    *return_schema = schema;
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
unsafe fn import_array(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> datafusion::error::Result<ArrayRef> {
    let data = datafusion::arrow::ffi::from_ffi(array, schema).unwrap();
    let arr = datafusion::arrow::array::make_array(data);
    Ok(arr)
}
