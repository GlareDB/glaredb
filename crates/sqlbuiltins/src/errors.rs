pub use datafusion_ext::errors::ExtensionError as BuiltinError;
pub type Result<T, E = BuiltinError> = std::result::Result<T, E>;
