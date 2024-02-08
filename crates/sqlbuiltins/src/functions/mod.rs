//! Builtin functions.
mod aggregates;
mod alias_map;
mod scalars;
pub mod table;

use std::sync::Arc;

use datafusion::execution::FunctionRegistry as DataFusionFunctionRegistry;
use datafusion::logical_expr::{
    AggregateFunction,
    BuiltinScalarFunction,
    Expr,
    ScalarUDF,
    Signature,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use protogen::metastore::types::catalog::FunctionType;
use scalars::df_scalars::ArrowCastFunction;
use scalars::hashing::{FnvHash, PartitionResults, SipHash};
use scalars::kdl::{KDLMatches, KDLSelect};
use scalars::postgres::{
    CurrentCatalog,
    CurrentDatabase,
    CurrentRole,
    CurrentSchema,
    CurrentSchemas,
    CurrentUser,
    HasDatabasePrivilege,
    HasSchemaPrivilege,
    HasTablePrivilege,
    PgArrayToString,
    PgEncodingToChar,
    PgGetUserById,
    PgTableIsVisible,
    PgVersion,
    User,
};
use scalars::{ConnectionId, Version};
use table::{BuiltinTableFuncs, TableFunc};

use self::alias_map::AliasMap;
use crate::functions::scalars::openai::OpenAIEmbed;
use crate::functions::scalars::sentence_transformers::BertUdf;
use crate::functions::scalars::similarity::CosineSimilarity;

/// All builtin functions available for all sessions.
pub static FUNCTION_REGISTRY: Lazy<Mutex<FunctionRegistry>> =
    Lazy::new(|| Mutex::new(FunctionRegistry::new()));


impl DataFusionFunctionRegistry for FunctionRegistry {
    fn udfs(&self) -> std::collections::HashSet<String> {
        self.udfs
            .values()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn udf(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::ScalarUDF>> {
        match name {
            BertUdf::NAME => {
                let udf = BertUdf::new();
                let scalar_udf = ScalarUDF::new_from_impl(udf);
                Ok(Arc::new(scalar_udf))
            }
            _ => Err(datafusion::error::DataFusionError::NotImplemented(
                "udf not implemented".to_string(),
            )),
        }
    }

    fn udaf(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::AggregateUDF>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "udaf not implemented".to_string(),
        ))
    }

    fn udwf(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::WindowUDF>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "udwf not implemented".to_string(),
        ))
    }
}

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

    /// Returns the function type. 'aggregate', 'scalar', or 'table'
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
/// These are functions that are implemented directly in GlareDB. Unlike
/// [`BuiltinFunction`], this contains an implementation of a UDF, and is not
/// just a catalog entry for a DataFusion function.
///
/// Note: upcoming release of DataFusion will have a similar trait that'll
/// likely be used instead.
pub trait BuiltinScalarUDF: BuiltinFunction {
    /// Builds an expression for the function using the provided arguments.
    /// Some functions may require additional information from the catalog to build the expression.
    /// Examples of such functions are ones that require credentials to access external services such as `openai_embed`.
    fn try_as_expr(
        &self,
        catalog: &catalog::session_catalog::SessionCatalog,
        args: Vec<Expr>,
    ) -> datafusion::error::Result<Expr>;

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
    // `BuiltinScalarUDF`? Still confused.
    funcs: AliasMap<String, Arc<dyn BuiltinFunction>>,
    udfs: AliasMap<String, Arc<dyn BuiltinScalarUDF>>,

    // Table functions.
    table_funcs: BuiltinTableFuncs,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        use strum::IntoEnumIterator;
        let scalars = BuiltinScalarFunction::iter().map(|f| {
            let key = f.to_string().to_lowercase(); // Display impl is already lowercase for scalars, but lowercase here just to be sure.
            let value: Arc<dyn BuiltinFunction> = Arc::new(f);
            (vec![key], value)
        });
        let aggregates = AggregateFunction::iter().map(|f| {
            let key = f.to_string().to_lowercase(); // Display impl is uppercase for aggregates. Lowercase it to be consistent.
            let value: Arc<dyn BuiltinFunction> = Arc::new(f);
            (vec![key], value)
        });

        // The arrow cast function has special handling in datafusion due to the
        // dynamic return type. So it isn't a 'scalar_udf' and needs to be
        // handled a bit differently.
        let arrow_cast: Arc<dyn BuiltinFunction> = Arc::new(ArrowCastFunction);
        let arrow_cast = (vec![arrow_cast.name().to_string()], arrow_cast);
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
            Arc::new(PgVersion),
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
            // OpenAI
            Arc::new(OpenAIEmbed),
            // Sentence Transformers
            // Arc::new(BertUdf::new()),
            Arc::new(CosineSimilarity),
        ];
        let udfs = udfs
            .into_iter()
            .map(|f| {
                // TODO: This is very weird to do here as this completely
                // bypasses our schema resolution. It also results in duplicate
                // function entries with the same name as the function's `name`
                // argument is not aware of the namespace being added here.
                let keys = match f.namespace() {
                    // we register the function under both the namespaced entry and the normal entry
                    // e.g. select foo.my_function() or select my_function()
                    FunctionNamespace::Optional(namespace) => {
                        let namespaced_entry = format!("{}.{}", namespace, f.name());
                        vec![f.name().to_string(), namespaced_entry]
                    }
                    // we only register the function under the namespaced entry
                    // e.g. select foo.my_function()
                    FunctionNamespace::Required(namespace) => {
                        let namespaced_entry = format!("{}.{}", namespace, f.name());
                        vec![namespaced_entry]
                    }
                    // we only register the function under the normal entry
                    // e.g. select my_function()
                    FunctionNamespace::None => {
                        vec![f.name().to_string()]
                    }
                };

                (keys, f)
            })
            .collect::<AliasMap<_, _>>();

        let funcs: AliasMap<String, Arc<dyn BuiltinFunction>> =
            scalars.chain(aggregates).chain(arrow_cast).collect();

        FunctionRegistry {
            funcs,
            udfs,
            table_funcs: BuiltinTableFuncs::new(),
        }
    }

    /// Checks if a function with the the given name exists.
    pub fn contains(&self, name: &str) -> bool {
        self.funcs.get(name).is_some()
            || self.udfs.get(name).is_some()
            || self.table_funcs.funcs.get(name).is_some()
    }

    /// Find a scalar UDF by name
    /// This is separate from `find_function` because we want to avoid downcasting
    /// We already match on BuiltinScalarFunction and AggregateFunction when parsing the AST, so we just need to match on the UDF here.
    pub fn get_scalar_udf(&self, name: &str) -> Option<Arc<dyn BuiltinScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    pub fn get_table_func(&self, name: &str) -> Option<Arc<dyn TableFunc>> {
        self.table_funcs.funcs.get(name).cloned()
    }

    /// Iterate over all scalar funcs.
    ///
    /// A function will only be returned once, even if it has multiple aliases.
    pub fn scalar_funcs_iter(&self) -> impl Iterator<Item = &Arc<dyn BuiltinFunction>> {
        self.funcs.values()
    }

    /// Iterate over all scalar udfs.
    ///
    /// A function will only be returned once, even if it has multiple aliases.
    pub fn scalar_udfs_iter(&self) -> impl Iterator<Item = &Arc<dyn BuiltinScalarUDF>> {
        self.udfs.values().filter(|func| {
            // Currently we have two "array_to_string" entries, one provided by
            // datafusion, and one "aliased" to "pg_catalog.array_to_string".
            // However those exist in different maps, and so the current
            // aliasing logic doesn't work well.
            //
            // See https://github.com/GlareDB/glaredb/issues/2371
            func.name() != "array_to_string"
        })
    }

    /// Iterate over all table funcs.
    ///
    /// A function will only be returned once, even if it has multiple aliases.
    pub fn table_funcs_iter(&self) -> impl Iterator<Item = &Arc<dyn TableFunc>> {
        self.table_funcs.funcs.values()
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
        if let Some(func) = self.table_funcs.funcs.get(name) {
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
        if let Some(func) = self.table_funcs.funcs.get(name) {
            return func.sql_example();
        }
        None
    }

    fn register_udf(&mut self, f: Arc<dyn BuiltinScalarUDF>) {
        let keys = match f.namespace() {
            // we register the function under both the namespaced entry and the normal entry
            // e.g. select foo.my_function() or select my_function()
            FunctionNamespace::Optional(namespace) => {
                let namespaced_entry = format!("{}.{}", namespace, f.name());
                vec![f.name().to_string(), namespaced_entry]
            }
            // we only register the function under the namespaced entry
            // e.g. select foo.my_function()
            FunctionNamespace::Required(namespace) => {
                let namespaced_entry = format!("{}.{}", namespace, f.name());
                vec![namespaced_entry]
            }
            // we only register the function under the normal entry
            // e.g. select my_function()
            FunctionNamespace::None => {
                vec![f.name().to_string()]
            }
        };

        self.udfs.insert_aliases(keys, f);
    }

    pub fn register_extension(&mut self, ext: &FunctionExtensions) {
        for udf in &ext.udfs {
            println!("Registering UDF: {}", udf.name());
            self.register_udf(udf.clone());
        }
    }
}

pub struct FunctionExtensions {
    pub extension_name: &'static str,
    pub udfs: Vec<Arc<dyn BuiltinScalarUDF>>,
}

pub static SENTENCE_TRANSFORMER_EXTENSION: Lazy<FunctionExtensions> =
    Lazy::new(|| FunctionExtensions {
        extension_name: "sentence_transformers",
        udfs: vec![Arc::new(BertUdf::new())],
    });

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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn get_function_info() {
        // Ensure we're able to get descriptions and examples using the lower
        // case function name.

        let functions = FunctionRegistry::new();

        // Arbitrary DF scalar
        functions
            .get_function_example("repeat")
            .expect("'repeat' should have example");
        functions
            .get_function_description("repeat")
            .expect("'repeat' should have description");

        // Arbitrary DF aggregate
        functions
            .get_function_example("sum")
            .expect("'sum' should have example");
        functions
            .get_function_description("sum")
            .expect("'sum' should have description");

        // Arbitrary custom scalar
        functions
            .get_function_example("version")
            .expect("'version' should have example");
        functions
            .get_function_description("version")
            .expect("'version' should have description");

        // Arbitrary table function
        functions
            .get_function_example("read_parquet")
            .expect("'read_parquet' should have example");
        functions
            .get_function_description("read_parquet")
            .expect("'read_parquet' should have description");
    }

    // TODO: Currently there's a conflict between `version()` and `pg_catalog.version()`.
    //
    // See <https://github.com/GlareDB/glaredb/issues/2371>
    #[ignore]
    #[test]
    fn func_iters_return_one_copy() {
        // Assert that iterators only ever return a single reference to a
        // function, even if it has multiple aliases. This is needed to keep our
        // catalog clean.
        let functions = FunctionRegistry::new();

        fn find_duplicates(names: Vec<&str>) -> Vec<&str> {
            let mut deduped: HashSet<_> = names.clone().into_iter().collect();
            let diff: Vec<_> = names
                .into_iter()
                .filter(|name| {
                    let was_present = deduped.remove(name);
                    !was_present // We saw this value before, indicates a duplicated name.
                })
                .collect();
            diff
        }

        // Each iterator is currently tested separately. When
        // https://github.com/GlareDB/glaredb/issues/2371 is fixed, this should
        // concat all iterators, and ensure uniqueness on (namespace,
        // function_name) pairs.

        let names = functions.scalar_udfs_iter().map(|f| f.name()).collect();
        assert_eq!(Vec::<&str>::new(), find_duplicates(names));

        let names: Vec<_> = functions.scalar_funcs_iter().map(|f| f.name()).collect();
        assert_eq!(Vec::<&str>::new(), find_duplicates(names));

        let names: Vec<_> = functions.table_funcs_iter().map(|f| f.name()).collect();
        assert_eq!(Vec::<&str>::new(), find_duplicates(names));
    }
}
