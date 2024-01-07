//! Builtin functions.
mod aggregates;
mod scalars;
mod table;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::{AggregateFunction, BuiltinScalarFunction, Expr, Signature};
use once_cell::sync::Lazy;

use protogen::metastore::types::catalog::FunctionType;
use scalars::df_scalars::ArrowCastFunction;
use scalars::hashing::{FnvHash, PartitionResults, SipHash};
use scalars::kdl::{KDLMatches, KDLSelect};
use scalars::postgres::*;
use scalars::{ConnectionId, Version};
use table::{BuiltinTableFuncs, TableFunc};

/// Builtin table returning functions available for all sessions.
static BUILTIN_TABLE_FUNCS: Lazy<BuiltinTableFuncs> = Lazy::new(BuiltinTableFuncs::new);

/// All builtin functions available for all sessions.
pub static FUNCTION_REGISTRY: Lazy<FunctionRegistry> = Lazy::new(FunctionRegistry::new);

/// A builtin function.
/// This trait is implemented by all builtin functions.
/// This is used to derive catalog entries for all supported functions.
/// Any new function MUST implement this trait.
pub trait BuiltinFunction: Sync + Send {
    /// The name for this function. This name will be used when looking up
    /// function implementations.
    fn name(&self) -> &str;

    /// Additional aliases for this function.
    ///
    /// Default implementation provides no additional aliases.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// Return the signature for this function.
    /// Defaults to None.
    // TODO: Remove the default impl once we have `signature` implemented for all functions
    fn signature(&self) -> Option<Signature> {
        None
    }

    /// Return a sql example for this function.
    /// Defaults to None.
    fn sql_example(&self) -> Option<&str> {
        None
    }

    /// Return a description for this function.
    /// Defaults to None.
    fn description(&self) -> Option<&str> {
        None
    }

    // Returns the function type. 'aggregate', 'scalar', or 'table'
    fn function_type(&self) -> FunctionType;
}

/// The same as [`BuiltinFunction`] , but with const values.
pub trait ConstBuiltinFunction: Sync + Send {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    const EXAMPLE: &'static str;
    const FUNCTION_TYPE: FunctionType;
    fn signature(&self) -> Option<Signature> {
        None
    }
}
/// The namespace of a function.
///
/// Optional -> "namespace.function" || "function"
///
/// Required -> "namespace.function"
///
/// None -> "function"
pub enum FunctionNamespace {
    /// The function can either be called under the namespace, or under global
    /// e.g. "pg_catalog.current_user" or "current_user"
    Optional(&'static str),
    /// The function must be called under the namespace
    /// e.g. "foo.my_function"
    Required(&'static str),
    /// The function can only be called under the global namespace
    /// e.g. "avg"
    None,
}

/// A custom builtin function provided by GlareDB.
///
/// These are functions that are implemented directly in GlareDB.
/// Unlike [`BuiltinFunction`], this contains an implementation of a UDF, and is not just a catalog entry for a DataFusion function.
///
/// Note: upcoming release of DataFusion will have a similar trait that'll likely be used instead.
pub trait BuiltinScalarUDF: BuiltinFunction {
    fn as_expr(&self, args: Vec<Expr>) -> Expr;
    /// The namespace of the function.
    /// Defaults to global (None)
    fn namespace(&self) -> FunctionNamespace {
        FunctionNamespace::None
    }
}

impl<T> BuiltinFunction for T
where
    T: ConstBuiltinFunction + Sized,
{
    fn name(&self) -> &str {
        Self::NAME
    }
    fn sql_example(&self) -> Option<&str> {
        Some(Self::EXAMPLE)
    }
    fn description(&self) -> Option<&str> {
        Some(Self::DESCRIPTION)
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
    // TODO: What's the difference between `BuiltinFunction` and
    // `BuiltinScalarUDF`?
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

        // The arrow cast function has special handling in datafusion due to the
        // dynamic return type. So it isn't a 'scalar_udf' and needs to be
        // handled a bit differently.
        let arrow_cast: Arc<dyn BuiltinFunction> = Arc::new(ArrowCastFunction);
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
            Arc::new(PgArrayToString),
            // System functions
            Arc::new(ConnectionId),
            Arc::new(Version),
            // KDL functions
            Arc::new(KDLMatches),
            Arc::new(KDLSelect),
            // Hashing/Partitioning
            Arc::new(SipHash),
            Arc::new(FnvHash),
            Arc::new(PartitionResults),
        ];
        let udfs = udfs
            .into_iter()
            .flat_map(|f| {
                let entry = (f.name().to_string(), f.clone());
                match f.namespace() {
                    // we register the function under both the namespaced entry and the normal entry
                    // e.g. select foo.my_function() or select my_function()
                    FunctionNamespace::Optional(namespace) => {
                        let namespaced_entry = (format!("{}.{}", namespace, f.name()), f.clone());
                        vec![entry, namespaced_entry]
                    }
                    // we only register the function under the namespaced entry
                    // e.g. select foo.my_function()
                    FunctionNamespace::Required(namespace) => {
                        let namespaced_entry = (format!("{}.{}", namespace, f.name()), f.clone());
                        vec![namespaced_entry]
                    }
                    // we only register the function under the normal entry
                    // e.g. select my_function()
                    FunctionNamespace::None => {
                        vec![entry]
                    }
                }
            })
            .collect::<HashMap<_, _>>();

        let funcs: HashMap<String, Arc<dyn BuiltinFunction>> =
            scalars.chain(aggregates).chain(arrow_cast).collect();

        FunctionRegistry { funcs, udfs }
    }

    /// Checks if a function with the the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.funcs.contains_key(name)
            || self.udfs.contains_key(name)
            || BUILTIN_TABLE_FUNCS.funcs.contains_key(name)
    }

    /// Find a scalar UDF by name
    /// This is separate from `find_function` because we want to avoid downcasting
    /// We already match on BuiltinScalarFunction and AggregateFunction when parsing the AST, so we just need to match on the UDF here.
    pub fn get_scalar_udf(&self, name: &str) -> Option<Arc<dyn BuiltinScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    pub fn scalar_funcs_iter(&self) -> impl Iterator<Item = &Arc<dyn BuiltinFunction>> {
        self.funcs.values()
    }

    pub fn scalar_udfs_iter(&self) -> impl Iterator<Item = &Arc<dyn BuiltinScalarUDF>> {
        self.udfs.values()
    }

    /// Return an iterator over all builtin table functions.
    pub fn table_funcs_iter(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        BUILTIN_TABLE_FUNCS.iter_funcs()
    }

    pub fn get_table_func(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        BUILTIN_TABLE_FUNCS.find_function(name).cloned()
    }

    /// Get a function description.
    ///
    /// Looks up descriptions for both scalar functions and table functions.
    pub fn get_function_description(&self, name: &str) -> Option<&str> {
        if let Some(func) = self.funcs.get(name) {
            return func.description();
        }
        if let Some(func) = self.udfs.get(name) {
            return func.description();
        }
        if let Some(func) = BUILTIN_TABLE_FUNCS.find_function(name) {
            return func.description();
        }
        None
    }

    /// Get a function example.
    ///
    /// Looks up examples for both scalar functions and table functions.
    pub fn get_function_example(&self, name: &str) -> Option<&str> {
        if let Some(func) = self.funcs.get(name) {
            return func.sql_example();
        }
        if let Some(func) = self.udfs.get(name) {
            return func.sql_example();
        }
        if let Some(func) = BUILTIN_TABLE_FUNCS.find_function(name) {
            return func.sql_example();
        }
        None
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Macro to associate doc strings and examples with items.
//
// The macro helps preserve the line wrapping. rustfmt will otherwise collapse
// the lines.
#[macro_export]
macro_rules! document {
    (doc => $doc:expr, example => $example:expr, name => $item:ident) => {
        #[doc = $doc]
        pub struct $item;

        impl $item {
            pub const DESCRIPTION: &'static str = $doc;
            pub const EXAMPLE: &'static str = $example;
            pub const NAME: &'static str = stringify!($item);
        }
    };
    (doc => $doc:expr, example => $example:expr, $name:expr => $item:ident) => {
        #[doc = $doc]
        pub struct $item;

        impl $item {
            pub const DESCRIPTION: &'static str = $doc;
            pub const EXAMPLE: &'static str = $example;
            pub const NAME: &'static str = $name;
        }
    };
}
