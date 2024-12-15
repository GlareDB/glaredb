pub mod series;
pub mod system;

use std::sync::LazyLock;

use series::GenerateSeries;
use system::{ListDatabases, ListFunctions, ListSchemas, ListTables};

use super::TableFunction;

pub static BUILTIN_TABLE_FUNCTIONS: LazyLock<Vec<Box<dyn TableFunction>>> = LazyLock::new(|| {
    vec![
        Box::new(GenerateSeries),
        // Various list system object functions.
        Box::new(ListDatabases::new()),
        Box::new(ListSchemas::new()),
        Box::new(ListTables::new()),
        Box::new(ListFunctions::new()),
    ]
});
