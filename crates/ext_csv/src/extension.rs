use glaredb_core::extension::Extension;
use glaredb_core::runtime::io::IoRuntime;

#[derive(Debug, Clone, Copy)]
pub struct CsvExtension<R: IoRuntime> {
    pub runtime: R,
}

impl<R> CsvExtension<R>
where
    R: IoRuntime,
{
    pub const fn new(runtime: R) -> Self {
        CsvExtension { runtime }
    }
}

impl<R> Extension for CsvExtension<R>
where
    R: IoRuntime,
{
    const NAME: &str = "csv";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.
}
