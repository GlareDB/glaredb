use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    ColumnarValue,
    Expr,
    ReturnTypeFunction,
    ScalarFunctionImplementation,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
};
use datafusion_ext::ffi::FFI_Signature;
use libloading::Library;
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::FunctionType;

use crate::functions::{BuiltinFunction, BuiltinScalarUDF};

type PluginAndVersion = (Library, u16, u16);

static LOADED: Lazy<RwLock<HashMap<String, PluginAndVersion>>> = Lazy::new(Default::default);

#[derive(Debug, Clone)]
pub struct GlaredbFFIPlugin {
    pub namespace: Option<&'static str>,
    pub name: Arc<str>,
    /// Shared library.
    pub lib: Arc<str>,
    /// Identifier in the shared lib.
    pub symbol: Arc<str>,
    signature: Signature,
}


impl GlaredbFFIPlugin {
    pub fn try_new(
        name: &str,
        lib: &str,
        symbol: &str,
        namespace: Option<&'static str>,
    ) -> Result<Self> {
        let signature = unsafe { plugin_signature(lib, symbol)? };

        Ok(Self {
            namespace,
            name: Arc::from(name),
            lib: Arc::from(lib),
            symbol: Arc::from(symbol),
            signature,
        })
    }
}

impl GlaredbFFIPlugin {
    pub fn scalar_fn_impl(&self) -> ScalarFunctionImplementation {
        let slf = self.clone();
        Arc::new(move |args| unsafe { slf.invoke_impl(args) })
    }

    pub fn return_type_impl(&self) -> ReturnTypeFunction {
        let slf = self.clone();
        Arc::new(move |arg_types: &[DataType]| slf.return_type(arg_types).map(Arc::new))
    }

    unsafe fn function_type_impl(&self) -> FunctionType {
        let plugin = get_lib(&self.lib).expect("plugin not found");
        let lib = &plugin.0;
        check_version(plugin);

        let symbol: libloading::Symbol<*mut i32> = lib
            .get((format!("_glaredb_plugin_function_type_{}", self.symbol)).as_bytes())
            .unwrap();
        FunctionType::try_from(**symbol).expect("invalid function type from plugin")
    }

    unsafe fn sql_example_impl(&self) -> Option<&str> {
        let plugin = get_lib(&self.lib).expect("plugin not found");
        let lib = &plugin.0;
        check_version(plugin);

        let symbol: libloading::Symbol<unsafe extern "C" fn() -> *const std::os::raw::c_char> = lib
            .get((format!("_glaredb_plugin_sql_example_{}", self.symbol)).as_bytes())
            .unwrap();
        let example = symbol();
        if example.is_null() {
            None
        } else {
            Some(CStr::from_ptr(example).to_str().unwrap())
        }
    }

    unsafe fn description_impl(&self) -> Option<&str> {
        let plugin = get_lib(&self.lib).expect("plugin not found");
        let lib = &plugin.0;
        check_version(plugin);

        lib.get((format!("_glaredb_plugin_description_{}", self.symbol)).as_bytes())
            .ok()
            .and_then(
                |sym: libloading::Symbol<unsafe extern "C" fn() -> *const std::os::raw::c_char>| {
                    let description = sym();
                    if description.is_null() {
                        None
                    } else {
                        Some(CStr::from_ptr(description).to_str().unwrap())
                    }
                },
            )
    }

    unsafe fn invoke_impl(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let plugin = get_lib(&self.lib)?;
        let lib = &plugin.0;
        check_version(plugin);

        let symbol: libloading::Symbol<
            unsafe extern "C" fn(
                // input arrays
                *mut FFI_ArrowArray,
                // input schemas
                *mut FFI_ArrowSchema,
                // length of the input arrays/schemas
                usize,
                // return value
                *mut FFI_ArrowArray,
                // return data type
                *mut FFI_ArrowSchema,
            ),
        > = lib
            .get(format!("_glaredb_plugin_{}", self.symbol).as_bytes())
            .map_err(|e| {
                let msg = e.to_string();
                datafusion::error::DataFusionError::Execution(msg)
            })?;

        let (arrays, schemas): (Vec<_>, Vec<_>) = args
            .iter()
            .map(|arg| {
                let input = match arg {
                    datafusion::physical_plan::ColumnarValue::Array(array) => array.clone(),
                    ColumnarValue::Scalar(s) => s.to_array().unwrap(),
                };
                let data = input.to_data();
                let (ffi_arr, ffi_schema) = datafusion::arrow::ffi::to_ffi(&data).unwrap();

                (ffi_arr, ffi_schema)
            })
            .unzip();

        let mut arrays = arrays.into_boxed_slice();
        let mut schemas = schemas.into_boxed_slice();

        let input_len = args.len();
        let slice_ptr = arrays.as_mut_ptr();
        let schema_ptr = schemas.as_mut_ptr();

        // the ffi functions will take ownership of the arrays and schemas
        // TODO: should we instead pass a double reference (*mut *mut) instead of (*mut)?
        std::mem::forget(arrays);
        std::mem::forget(schemas);

        let mut return_value = FFI_ArrowArray::empty();
        let mut return_schema = FFI_ArrowSchema::empty();

        let return_value_ptr = &mut return_value as *mut FFI_ArrowArray;
        let return_schema_ptr = &mut return_schema as *mut FFI_ArrowSchema;

        symbol(
            slice_ptr,
            schema_ptr,
            input_len,
            return_value_ptr,
            return_schema_ptr,
        );

        if return_value.is_empty() {
            let msg = retrieve_error_msg(lib);
            let msg = msg.to_string_lossy();
            panic!("{}", msg.as_ref());
        }
        let return_value = import_array(return_value, &return_schema)?;
        Ok(ColumnarValue::Array(return_value))
    }
}

fn check_version(plugin: &PluginAndVersion) {
    if !matches!((plugin.1, plugin.2), (0, 0)) {
        todo!("unsupported versions");
    }
}

impl ScalarUDFImpl for GlaredbFFIPlugin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        let dtype = unsafe { plugin_return_type(arg_types, &self.lib, &self.symbol)? };

        Ok(dtype)
    }

    fn invoke(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        unsafe { self.invoke_impl(args) }
    }
}

impl BuiltinFunction for GlaredbFFIPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn function_type(&self) -> FunctionType {
        unsafe { self.function_type_impl() }
    }

    fn signature(&self) -> Option<Signature> {
        Some(self.signature.clone())
    }


    fn sql_example(&self) -> Option<&str> {
        unsafe { self.sql_example_impl() }
    }

    fn description(&self) -> Option<&str> {
        unsafe { self.description_impl() }
    }
}

impl BuiltinScalarUDF for GlaredbFFIPlugin {
    fn try_as_expr(
        &self,
        _: &catalog::session_catalog::SessionCatalog,
        args: Vec<Expr>,
    ) -> datafusion::error::Result<Expr> {
        let udf = ScalarUDF::new(
            &self.symbol,
            &self.signature,
            &self.return_type_impl(),
            &self.scalar_fn_impl(),
        );

        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        )))
    }

    fn try_into_scalar_udf(self: Arc<Self>) -> datafusion::error::Result<ScalarUDF> {
        let udf = ScalarUDF::new(
            &self.symbol,
            &self.signature,
            &self.return_type_impl(),
            &self.scalar_fn_impl(),
        );
        Ok(udf)
    }

    fn namespace(&self) -> crate::functions::FunctionNamespace {
        match &self.namespace {
            Some(ns) => crate::functions::FunctionNamespace::Required(ns),
            None => crate::functions::FunctionNamespace::None,
        }
    }
}

fn get_lib(lib: &str) -> Result<&'static PluginAndVersion> {
    let lib_map = LOADED.read().unwrap();
    if let Some(library) = lib_map.get(lib) {
        // lifetime is static as we never remove libraries.
        Ok(unsafe { std::mem::transmute::<&PluginAndVersion, &'static PluginAndVersion>(library) })
    } else {
        drop(lib_map);
        let library = unsafe { Library::new(lib).unwrap() };
        let version_function: libloading::Symbol<unsafe extern "C" fn() -> u32> = unsafe {
            library
                .get("_glaredb_plugin_get_version".as_bytes())
                .unwrap()
        };

        let version = unsafe { version_function() };
        let major = (version >> 16) as u16;
        let minor = version as u16;

        let mut lib_map = LOADED.write().unwrap();
        lib_map.insert(lib.to_string(), (library, major, minor));
        drop(lib_map);

        get_lib(lib)
    }
}


/// # Safety
/// `lib` and `symbol` must be valid
unsafe fn plugin_return_type(fields: &[DataType], lib: &str, symbol: &str) -> Result<DataType> {
    let plugin = get_lib(lib)?;
    let lib = &plugin.0;
    check_version(plugin);


    // we deallocate the fields buffer
    let ffi_fields = fields
        .iter()
        .map(|field| field.try_into().unwrap())
        .collect::<Vec<FFI_ArrowSchema>>()
        .into_boxed_slice();
    let n_args = ffi_fields.len();
    let slice_ptr = ffi_fields.as_ptr();

    let mut return_value = FFI_ArrowSchema::empty();
    let return_value_ptr = &mut return_value as *mut FFI_ArrowSchema;


    // *const ArrowSchema: pointer to heap Box<ArrowSchema>
    // usize: length of the boxed slice
    // *mut ArrowSchema: pointer where the return value can be written
    let symbol: libloading::Symbol<
        unsafe extern "C" fn(*const FFI_ArrowSchema, usize, *mut FFI_ArrowSchema),
    > = lib
        .get((format!("_glaredb_plugin_return_type_{}", symbol)).as_bytes())
        .unwrap();
    symbol(slice_ptr, n_args, return_value_ptr);

    if !return_value_ptr.is_null() {
        let out = DataType::try_from(&return_value).unwrap();

        Ok(out)
    } else {
        let msg = retrieve_error_msg(lib);
        let msg = msg.to_string_lossy();
        panic!("{}", msg.as_ref());
    }
}

/// # Safety
/// `lib` and `symbol` must be valid
unsafe fn plugin_signature(lib: &str, symbol: &str) -> Result<Signature> {
    let plugin = get_lib(lib)?;
    let lib = &plugin.0;
    check_version(plugin);

    let symbol: libloading::Symbol<unsafe extern "C" fn(*mut FFI_Signature)> = lib
        .get((format!("_glaredb_plugin_signature_{}", symbol)).as_bytes())
        .map_err(|e| {
            let msg = e.to_string();
            datafusion::error::DataFusionError::Execution(msg)
        })?;

    let signature = FFI_Signature::empty();
    let signature_ptr = &signature as *const FFI_Signature as *mut FFI_Signature;

    symbol(signature_ptr);
    if !signature_ptr.is_null() {
        let signature = std::ptr::read(signature_ptr);
        let signature = Signature::try_from(&signature)?;

        Ok(signature)
    } else {
        let msg = retrieve_error_msg(lib);
        let msg = msg.to_string_lossy();
        Err(datafusion::error::DataFusionError::Execution(
            msg.to_string(),
        ))
    }
}

unsafe fn retrieve_error_msg(lib: &Library) -> &CStr {
    let symbol: libloading::Symbol<unsafe extern "C" fn() -> *mut std::os::raw::c_char> = lib
        .get(b"_glaredb_plugin_get_last_error_message\0")
        .unwrap();
    let msg_ptr = symbol();
    CStr::from_ptr(msg_ptr)
}

/// # Safety
/// `ArrowArray` and `ArrowSchema` must be valid
unsafe fn import_array(array: FFI_ArrowArray, schema: &FFI_ArrowSchema) -> Result<ArrayRef> {
    let data = datafusion::arrow::ffi::from_ffi(array, schema).unwrap();
    let arr = datafusion::arrow::array::make_array(data);
    Ok(arr)
}
