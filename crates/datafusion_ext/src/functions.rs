use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use crate::errors::{ExtensionError, Result};
use crate::vars::SessionVars;
use async_trait::async_trait;
use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Signature;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use decimal::Decimal128;
use protogen::metastore::types::catalog::{EntryType, RuntimePreference};
use protogen::rpcsrv::types::func_param_value::{
    FuncParamValue as ProtoFuncParamValue, FuncParamValueArrayVariant,
    FuncParamValueEnum as ProtoFuncParamValueEnum,
};

#[async_trait]
pub trait TableFunc: Sync + Send {
    /// The name for this table function. This name will be used when looking up
    /// function implementations.
    fn name(&self) -> &str;
    fn runtime_preference(&self) -> RuntimePreference;
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(self.runtime_preference())
    }

    /// Return a table provider using the provided args.
    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>>;
    /// Return the signature for this function.
    /// Defaults to None.
    // TODO: Remove the default impl once we have `signature` implemented for all functions
    fn signature(&self) -> Option<Signature> {
        None
    }
}
pub trait TableFuncContextProvider: Sync + Send {
    /// Get a reference to the session catalog.
    fn get_session_catalog(&self) -> &SessionCatalog;

    // TODO: Remove this if `create_provider` runs remotely since we don't want
    // remote session vars.
    fn get_session_vars(&self) -> SessionVars;

    /// Get the session state.
    ///
    /// Both local and remote contexts should have:
    /// - NativeTableStorage
    /// - CatalogMutator
    fn get_session_state(&self) -> SessionState;

    // TODO: Remove
    fn get_catalog_lister(&self) -> Box<dyn VirtualLister + '_>;
}

pub struct DefaultTableContextProvider<'a> {
    session_catalog: &'a SessionCatalog,
    df_ctx: &'a SessionContext,
}

impl<'a> DefaultTableContextProvider<'a> {
    pub fn new(catalog: &'a SessionCatalog, df_ctx: &'a SessionContext) -> Self {
        Self {
            session_catalog: catalog,
            df_ctx,
        }
    }
}

impl<'a> TableFuncContextProvider for DefaultTableContextProvider<'a> {
    fn get_session_catalog(&self) -> &SessionCatalog {
        self.session_catalog
    }

    fn get_session_vars(&self) -> SessionVars {
        let cfg = self.df_ctx.copied_config();
        let vars = cfg.options().extensions.get::<SessionVars>().unwrap();
        vars.clone()
    }

    fn get_session_state(&self) -> SessionState {
        self.df_ctx.state()
    }

    fn get_catalog_lister(&self) -> Box<dyn VirtualLister + '_> {
        Box::new(SessionCatalogLister {
            catalog: self.session_catalog,
        })
    }
}

/// Get information about external data sources.
#[async_trait]
pub trait VirtualLister: Sync + Send {
    /// List schemas for a data source.
    async fn list_schemas(&self) -> Result<Vec<String>>;

    /// List tables for a data source.
    async fn list_tables(&self, schema: &str) -> Result<Vec<String>>;

    /// List columns for a specific table in the datasource.
    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields>;
}

/// A virtual listing implementation for the session catalog.
pub struct SessionCatalogLister<'a> {
    pub catalog: &'a SessionCatalog,
}

#[async_trait]
impl<'a> VirtualLister for SessionCatalogLister<'a> {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let schemas = self
            .catalog
            .iter_entries()
            .filter_map(|ent| {
                if ent.entry_type() == EntryType::Schema {
                    Some(ent.entry.get_meta().name.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(schemas)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let tables = self
            .catalog
            .iter_entries()
            .filter_map(|ent| {
                let is_in_schema = ent
                    .parent_entry
                    .map(|schema_ent| schema_ent.get_meta().name == schema)
                    .unwrap_or(false);
                if ent.entry_type() == EntryType::Table && is_in_schema {
                    Some(ent.entry.get_meta().name.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(tables)
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        // TODO: Database isn't actually used here. I want to avoid adding
        // `sqlbuiltins` as dep to this crate to avoid possibilities of a cyle.
        // But obviously hard coding this here isn't amazing.
        let ent = self
            .catalog
            .resolve_table("default", schema, table)
            .ok_or_else(|| ExtensionError::String(format!("Missing table: {schema}.{table}")))?;

        let cols = ent.get_internal_columns().ok_or_else(|| {
            ExtensionError::String(format!("Columns not tracked for table: {schema}.{table}"))
        })?;

        let cols: Vec<_> = cols
            .iter()
            .map(|col| Field::new(col.name.clone(), col.arrow_type.clone(), col.nullable))
            .collect();

        Ok(cols.into())
    }
}

/// Value from a function parameter.
#[derive(Debug, Clone)]
pub enum FuncParamValue {
    /// Normalized value from an ident.
    Ident(String),
    /// Scalar value.
    Scalar(ScalarValue),
    /// A list of function parameter values.
    Array(Vec<FuncParamValue>),
}

impl fmt::Display for FuncParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) => write!(f, "{s}"),
            Self::Scalar(s) => write!(f, "{s}"),
            Self::Array(vals) => write!(f, "{}", FuncParamValue::multiple_to_string(vals)),
        }
    }
}
impl TryFrom<FuncParamValue> for ProtoFuncParamValue {
    type Error = ExtensionError;

    fn try_from(value: FuncParamValue) -> std::result::Result<Self, Self::Error> {
        let variant = match value {
            FuncParamValue::Ident(id) => ProtoFuncParamValueEnum::Ident(id),
            FuncParamValue::Scalar(ref s) => ProtoFuncParamValueEnum::Scalar(s.try_into().unwrap()),
            FuncParamValue::Array(a) => {
                let values = a
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                ProtoFuncParamValueEnum::Array(FuncParamValueArrayVariant { array: values })
            }
        };
        Ok(ProtoFuncParamValue {
            func_param_value_enum: Some(variant),
        })
    }
}

impl TryFrom<ProtoFuncParamValue> for FuncParamValue {
    type Error = ExtensionError;
    fn try_from(value: ProtoFuncParamValue) -> Result<Self, Self::Error> {
        let variant = value.func_param_value_enum.unwrap();
        Ok(match variant {
            ProtoFuncParamValueEnum::Ident(id) => Self::Ident(id),
            ProtoFuncParamValueEnum::Scalar(ref scalar) => {
                let scalar = scalar
                    .try_into()
                    .map_err(|_| ExtensionError::InvalidParamValue {
                        param: format!("{:?}", scalar),
                        expected: "scalar",
                    })?;
                Self::Scalar(scalar)
            }
            ProtoFuncParamValueEnum::Array(values) => {
                let values = values
                    .array
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Self::Array(values)
            }
        })
    }
}

impl FuncParamValue {
    /// Print multiple function parameter values.
    fn multiple_to_string<T: AsRef<[Self]>>(vals: T) -> String {
        use std::fmt::Write;

        let mut s = String::new();
        write!(&mut s, "(").unwrap();
        let mut sep = "";
        for val in vals.as_ref() {
            write!(&mut s, "{sep}{val}").unwrap();
            sep = ", ";
        }
        write!(&mut s, ")").unwrap();
        s
    }

    /// Wrapper over `FromFuncParamValue::from_param`.
    pub fn param_into<T>(self) -> Result<T>
    where
        T: FromFuncParamValue,
    {
        T::from_param(self)
    }

    /// Wrapper over `FromFuncParamValue::from_param`.
    pub fn param_ref_into<T>(&self) -> Result<T>
    where
        T: FromFuncParamValue,
    {
        T::from_param_ref(self)
    }
}

pub trait FromFuncParamValue: Sized {
    /// Get the value from parameter.
    fn from_param(value: FuncParamValue) -> Result<Self>;
    fn from_param_ref(value: &FuncParamValue) -> Result<Self> {
        Self::from_param(value.clone())
    }

    /// Check if the value is valid (able to convert).
    ///
    /// If `is_param_valid` returns true, `from_param` should be safely
    /// `unwrap`able (i.e., not panic).
    fn is_param_valid(value: &FuncParamValue) -> bool;
}

impl FromFuncParamValue for String {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s),
            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "string",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(value, FuncParamValue::Scalar(ScalarValue::Utf8(Some(_))))
    }
}

impl<T> FromFuncParamValue for Vec<T>
where
    T: FromFuncParamValue,
{
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Array(arr) => {
                let mut res = Vec::with_capacity(arr.len());
                for val in arr {
                    res.push(T::from_param(val)?);
                }
                Ok(res)
            }

            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "list",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        if let FuncParamValue::Array(arr) = value {
            arr.iter().all(|v| T::is_param_valid(v))
        } else {
            false
        }
    }
}

pub struct IdentValue(String);

impl IdentValue {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for IdentValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for IdentValue {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<IdentValue> for String {
    fn from(value: IdentValue) -> Self {
        value.0
    }
}

impl FromFuncParamValue for IdentValue {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Ident(s) => Ok(Self(s)),
            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "ident",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(value, FuncParamValue::Ident(_))
    }
}

impl FromFuncParamValue for i64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as i64),
                ScalarValue::Int16(Some(v)) => Ok(v as i64),
                ScalarValue::Int32(Some(v)) => Ok(v as i64),
                ScalarValue::Int64(Some(v)) => Ok(v),
                ScalarValue::UInt8(Some(v)) => Ok(v as i64),
                ScalarValue::UInt16(Some(v)) => Ok(v as i64),
                ScalarValue::UInt32(Some(v)) => Ok(v as i64),
                ScalarValue::UInt64(Some(v)) => Ok(v as i64), // TODO: Handle overflow?
                other => Err(ExtensionError::InvalidParamValue {
                    param: other.to_string(),
                    expected: "integer",
                }),
            },

            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "integer",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Int8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Int64(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt8(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt16(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::UInt64(Some(_)))
        )
    }
}

impl FromFuncParamValue for f64 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(v as f64),
                ScalarValue::Int16(Some(v)) => Ok(v as f64),
                ScalarValue::Int32(Some(v)) => Ok(v as f64),
                ScalarValue::Int64(Some(v)) => Ok(v as f64),
                ScalarValue::UInt8(Some(v)) => Ok(v as f64),
                ScalarValue::UInt16(Some(v)) => Ok(v as f64),
                ScalarValue::UInt32(Some(v)) => Ok(v as f64),
                ScalarValue::UInt64(Some(v)) => Ok(v as f64),
                ScalarValue::Float32(Some(v)) => Ok(v as f64),
                ScalarValue::Float64(Some(v)) => Ok(v),
                other => Err(ExtensionError::InvalidParamValue {
                    param: other.to_string(),
                    expected: "double",
                }),
            },
            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "double",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Float32(Some(_)))
                | FuncParamValue::Scalar(ScalarValue::Float64(Some(_)))
        ) || i64::is_param_valid(value)
    }
}

impl FromFuncParamValue for Decimal128 {
    fn from_param(value: FuncParamValue) -> Result<Self> {
        match value {
            FuncParamValue::Scalar(s) => match s {
                ScalarValue::Int8(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::Int16(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::Int32(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::Int64(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::UInt8(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::UInt16(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::UInt32(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::UInt64(Some(v)) => Ok(Decimal128::try_from_int(v)?),
                ScalarValue::Float32(Some(v)) => Ok(Decimal128::try_from_float(v)?),
                ScalarValue::Float64(Some(v)) => Ok(Decimal128::try_from_float(v)?),
                ScalarValue::Decimal128(Some(v), _, s) => Ok(Decimal128::new(v, s)?),
                other => Err(ExtensionError::InvalidParamValue {
                    param: other.to_string(),
                    expected: "decimal",
                }),
            },
            other => Err(ExtensionError::InvalidParamValue {
                param: other.to_string(),
                expected: "decimal",
            }),
        }
    }

    fn is_param_valid(value: &FuncParamValue) -> bool {
        matches!(
            value,
            FuncParamValue::Scalar(ScalarValue::Decimal128(Some(_), _, _))
        ) || f64::is_param_valid(value)
            || i64::is_param_valid(value)
    }
}
