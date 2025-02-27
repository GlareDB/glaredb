pub mod series;
pub mod system;
pub mod unnest;

use std::sync::LazyLock;

use series::GenerateSeries;
use system::{ListDatabases, ListFunctions, ListSchemas, ListTables};
use unnest::Unnest;

use super::TableFunction2;

pub static BUILTIN_TABLE_FUNCTIONS: LazyLock<Vec<Box<dyn TableFunction2>>> = LazyLock::new(|| {
    vec![
        Box::new(GenerateSeries),
        Box::new(Unnest),
        // Various list system object functions.
        Box::new(ListDatabases::new()),
        Box::new(ListSchemas::new()),
        Box::new(ListTables::new()),
        Box::new(ListFunctions::new()),
    ]
});
