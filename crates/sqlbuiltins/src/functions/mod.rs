//! Builtin table returning functions.
mod aggregates;
mod scalars;
mod table;

use crate::functions::scalars::kdl::{KDLMatches, KDLSelect};
use crate::functions::scalars::{postgres::*, ConnectionId, Version};

use self::scalars::ArrowCastFunction;
use self::table::{BuiltinTableFuncs, TableFunc};
use datafusion::logical_expr::{AggregateFunction, BuiltinScalarFunction, Expr, Signature};
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::{
    EntryMeta, EntryType, FunctionEntry, FunctionType, RuntimePreference,
};

use std::collections::HashMap;
use std::sync::Arc;

/// Builtin table returning functions available for all sessions.
static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);
pub static ARROW_CAST_FUNC: Lazy<ArrowCastFunction> = Lazy::new(|| ArrowCastFunction {});
pub static FUNCTION_REGISTRY: Lazy<FunctionRegistry> = Lazy::new(FunctionRegistry::new);

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
/// A builtin function provided by GlareDB.
/// Note: upcoming release of DataFusion will have a similar trait that'll likely be used instead.
pub trait BuiltinScalarUDF: BuiltinFunction {
    fn as_expr(&self, args: Vec<Expr>) -> Expr;
}

impl<T> BuiltinFunction for T
where
    T: ConstBuiltinFunction + Sized,
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

/// Builtin Functions available for all sessions.
/// This is functionally equivalent to the datafusion `SessionState::scalar_functions`
/// We use our own implementation to allow us to have finer grained control over them.
/// We also don't have any session specific functions (for now), so it makes more sense to have a const global.
pub struct FunctionRegistry {
    funcs: HashMap<String, Arc<dyn BuiltinFunction>>,
    udfs: HashMap<String, Arc<dyn BuiltinScalarUDF>>,
}

impl FunctionRegistry {
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

        // GlareDB specific functions
        let udfs: Vec<Arc<dyn BuiltinScalarUDF>> = vec![
            // Postgres functions
            Arc::new(HasSchemaPrivilege),
            Arc::new(HasDatabasePrivilege),
            Arc::new(HasTablePrivilege),
            Arc::new(CurrentSchemas),
            Arc::new(CurrentUser),
            Arc::new(CurrentRole),
            Arc::new(CurrentSchema),
            Arc::new(CurrentDatabase),
            Arc::new(CurrentCatalog),
            Arc::new(User),
            Arc::new(PgGetUserById),
            Arc::new(PgTableIsVisible),
            Arc::new(PgEncodingToChar),
            // KDL functions
            Arc::new(KDLMatches),
            Arc::new(KDLSelect),
            // Other functions
            Arc::new(ConnectionId),
            Arc::new(Version),
        ];
        let udfs = udfs
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect::<HashMap<_, _>>();

        let funcs: HashMap<String, Arc<dyn BuiltinFunction>> =
            scalars.chain(aggregates).chain(arrow_cast).collect();

        FunctionRegistry { funcs, udfs }
    }

    pub fn find_function(&self, name: &str) -> Option<Arc<dyn BuiltinFunction>> {
        self.funcs.get(name).cloned()
    }

    /// Find a scalar UDF by name
    /// This is separate from `find_function` because we want to avoid downcasting
    /// We already match on BuiltinScalarFunction and AggregateFunction when parsing the AST, so we just need to match on the UDF here.
    pub fn get_scalar_udf(&self, name: &str) -> Option<Arc<dyn BuiltinScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    pub fn scalar_functions(&self) -> impl Iterator<Item = &Arc<dyn BuiltinFunction>> {
        self.funcs.values()
    }

    pub fn scalar_udfs(&self) -> impl Iterator<Item = &Arc<dyn BuiltinScalarUDF>> {
        self.udfs.values()
    }
    /// Return an iterator over all builtin table functions.
    pub fn table_funcs(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        BUILTIN_TABLE_FUNCS.iter_funcs()
    }

    pub fn get_table_func(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        BUILTIN_TABLE_FUNCS.find_function(name)
    }
}

impl Default for FunctionRegistry {
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
