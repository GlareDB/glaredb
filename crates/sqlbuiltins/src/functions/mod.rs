//! Builtin table returning functions.
mod aggregates;
mod scalars;
mod table;

use self::scalars::ArrowCastFunction;
use self::table::BuiltinTableFuncs;
use datafusion::logical_expr::{AggregateFunction, BuiltinScalarFunction, Signature};
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::{
    EntryMeta, EntryType, FunctionEntry, FunctionType, RuntimePreference,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Builtin table returning functions available for all sessions.
pub static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);
pub static ARROW_CAST_FUNC: Lazy<ArrowCastFunction> = Lazy::new(|| ArrowCastFunction {});
pub static BUILTIN_FUNCS: Lazy<BuiltinFuncs> = Lazy::new(BuiltinFuncs::new);

/// A builtin function.
/// This trait is implemented by all builtin functions.
pub trait BuiltinFunction: Sync + Send {
    /// The name for this function. This name will be used when looking up
    /// function implementations.
    fn name(&self) -> &str;

    /// Return the signature for this function.
    /// Defaults to None.
    // TODO: Remove the default impl once we have `signature` implemented for all functions
    fn signature(&self) -> Option<Signature> {
        None
    }

    /// Return a sql example for this function.
    /// Defaults to None.
    fn sql_example(&self) -> Option<String> {
        None
    }

    /// Return a description for this function.
    /// Defaults to None.
    fn description(&self) -> Option<String> {
        None
    }

    // Returns the function type. 'aggregate', 'scalar', or 'table'
    fn function_type(&self) -> FunctionType;

    /// Convert to a builtin `FunctionEntry`
    ///
    /// The default implementation is suitable for aggregates and scalars. Table
    /// functions need to set runtime preference manually.
    fn as_function_entry(&self, id: u32, parent: u32) -> FunctionEntry {
        let meta = EntryMeta {
            entry_type: EntryType::Function,
            id,
            parent,
            name: self.name().to_string(),
            builtin: true,
            external: false,
            is_temp: false,
            sql_example: self.sql_example(),
            description: self.description(),
        };

        FunctionEntry {
            meta,
            func_type: self.function_type(),
            runtime_preference: RuntimePreference::Unspecified,
            signature: self.signature(),
        }
    }
}

/// The same as `BuiltinFunction` , but with const values.
pub trait ConstBuiltinFunction: Sync + Send {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    const EXAMPLE: &'static str;
    const FUNCTION_TYPE: FunctionType;
    fn signature(&self) -> Option<Signature> {
        None
    }
}

impl<T> BuiltinFunction for T
where
    T: ConstBuiltinFunction,
{
    fn name(&self) -> &str {
        Self::NAME
    }
    fn sql_example(&self) -> Option<String> {
        Some(Self::EXAMPLE.to_string())
    }
    fn description(&self) -> Option<String> {
        Some(Self::DESCRIPTION.to_string())
    }
    fn function_type(&self) -> FunctionType {
        Self::FUNCTION_TYPE
    }
    fn signature(&self) -> Option<Signature> {
        self.signature()
    }
}

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

impl Default for BuiltinFuncs {
    fn default() -> Self {
        Self::new()
    }
}

// Define a macro to associate doc strings and examples with items
// The macro helps preserve the line wrapping. rustfmt will otherwise collapse the lines.
#[macro_export]
macro_rules! document {
    (doc => $doc:expr, example => $example:expr, name => $item:ident) => {
        #[doc = $doc]
        pub struct $item;

        impl $item {
            const DESCRIPTION: &'static str = $doc;
            const EXAMPLE: &'static str = $example;
            const NAME: &'static str = stringify!($item);
        }
    };
    (doc => $doc:expr, example => $example:expr, $name:expr => $item:ident) => {
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
