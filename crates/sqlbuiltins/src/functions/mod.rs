//! Builtin table returning functions.
mod aggregates;
mod scalars;
mod table;

use std::collections::HashMap;

use crate::builtins::BuiltinFunction;

use self::scalars::ArrowCastFunction;
use self::table::BuiltinTableFuncs;
use datafusion::logical_expr::{AggregateFunction, BuiltinScalarFunction};
use once_cell::sync::Lazy;
use std::sync::Arc;
/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);
pub static ARROW_CAST_FUNC: Lazy<ArrowCastFunction> = Lazy::new(|| ArrowCastFunction {});
pub static BUILTIN_FUNCS: Lazy<BuiltinFuncs> = Lazy::new(BuiltinFuncs::new);

pub struct BuiltinFuncs {
    funcs: HashMap<String, Arc<dyn BuiltinFunction>>,
}

impl BuiltinFuncs {
    pub fn new() -> Self {
        use strum::IntoEnumIterator;
        let scalars = BuiltinScalarFunction::iter().map(|f| {
            let key = f.to_string();
            let value: Arc<dyn BuiltinFunction> = Arc::new(f);
            (key, value)
        });
        let aggregates = AggregateFunction::iter().map(|f| {
            let key = f.to_string();
            let value: Arc<dyn BuiltinFunction> = Arc::new(f);
            (key, value)
        });
        let arrow_cast: Arc<dyn BuiltinFunction> = Arc::new(ArrowCastFunction {});
        let arrow_cast = (arrow_cast.name().to_string(), arrow_cast);
        let arrow_cast = std::iter::once(arrow_cast);

        let funcs: HashMap<String, Arc<dyn BuiltinFunction>> =
            scalars.chain(aggregates).chain(arrow_cast).collect();

        BuiltinFuncs { funcs }
    }
    pub fn iter_funcs(&self) -> impl Iterator<Item = &Arc<dyn BuiltinFunction>> {
        self.funcs.values()
    }
}

// Define a macro to associate doc strings and examples with items
// The macro helps preserve the line wrapping. rustfmt will otherwise collapse the lines.
#[macro_export]
macro_rules! document {
    ($doc:expr, $example:expr, $item:ident) => {
        #[doc = $doc]
        pub struct $item;

        impl $item {
            const DESCRIPTION: &'static str = $doc;
            const EXAMPLE: &'static str = $example;
            const NAME: &'static str = stringify!($item);
        }
    };
    ($doc:expr, $example:expr, $name:expr => $item:ident) => {
        #[doc = $doc]
        pub struct $item;

        impl $item {
            const DESCRIPTION: &'static str = $doc;
            const EXAMPLE: &'static str = $example;
            const NAME: &'static str = $name;
        }
    };
    // uses an existing struct
    ($doc:expr, $example:expr, name => $name:expr, implementation => $item:ident) => {
        impl $item {
            const DESCRIPTION: &'static str = $doc;
            const EXAMPLE: &'static str = $example;
            const NAME: &'static str = $name;
        }
    };
}
