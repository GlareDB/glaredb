//! Server and session variables.
mod constants;
mod value;
use constants::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::variable::VarType;
use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard};
use regex::Regex;
use std::borrow::{Borrow, ToOwned};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;
use value::*;

#[derive(Debug, thiserror::Error)]
pub enum VarError {
    #[error("Invalid value for session variable: Variable name: {name}, Value: {val}")]
    InvalidSessionVarValue { name: String, val: String },

    #[error("Variable is readonly: {0}")]
    VariableReadonly(String),

    #[error("Unknown variable: {0}")]
    UnknownVariable(String),

    #[error("Empty search path, unable to resolve schema")]
    EmptySearchPath,
}

impl From<VarError> for DataFusionError {
    fn from(e: VarError) -> Self {
        DataFusionError::Execution(e.to_string())
    }
}

/// Variables for a session.
#[derive(Debug)]
pub struct SessionVarsInner {
    pub server_version: SessionVar<str>,
    pub application_name: SessionVar<str>,
    pub client_encoding: SessionVar<str>,
    pub extra_floating_digits: SessionVar<i32>,
    pub statement_timeout: SessionVar<i32>,
    pub timezone: SessionVar<str>,
    pub datestyle: SessionVar<str>,
    pub transaction_isolation: SessionVar<str>,
    pub search_path: SessionVar<[String]>,
    pub enable_debug_datasources: SessionVar<bool>,
    pub force_catalog_refresh: SessionVar<bool>,
    pub glaredb_version: SessionVar<str>,
    pub database_id: SessionVar<Uuid>,
    pub connection_id: SessionVar<Uuid>,
    pub remote_session_id: SessionVar<Option<Uuid>>,
    pub user_id: SessionVar<Uuid>,
    pub user_name: SessionVar<str>,
    pub database_name: SessionVar<str>,
    pub max_datasource_count: SessionVar<Option<usize>>,
    pub memory_limit_bytes: SessionVar<Option<usize>>,
    pub max_tunnel_count: SessionVar<Option<usize>>,
    pub max_credentials_count: SessionVar<Option<usize>>,
    pub is_cloud_instance: SessionVar<bool>,
}
#[derive(Debug)]
pub struct SessionVars {
    inner: Arc<RwLock<SessionVarsInner>>,
}

impl SessionVars {
    pub fn new(vars: SessionVarsInner) -> Self {
        Self {
            inner: Arc::new(RwLock::new(vars)),
        }
    }
}

macro_rules! generate_getters {
    ($($name:ident : $type:ty),* $(,)?) => {
        $(
            pub fn $name(&self) -> $type {
                self.inner.read().$name.value().to_owned()
            }

        )*

    };
}

impl SessionVars {
    generate_getters! {
     server_version: String,
     application_name: String,
     client_encoding: String,
     extra_floating_digits: i32,
     statement_timeout: i32,
     timezone: String,
     datestyle: String,
     transaction_isolation: String,
     search_path: Vec<String>,
     enable_debug_datasources: bool,
     force_catalog_refresh: bool,
     glaredb_version: String,
     database_id: Uuid,
     connection_id: Uuid,
     remote_session_id: Option<Uuid>,
     user_id: Uuid,
     user_name: String,
     database_name: String,
     max_datasource_count: Option<usize>,
     memory_limit_bytes: Option<usize>,
     max_tunnel_count: Option<usize>,
     max_credentials_count: Option<usize>,
     is_cloud_instance: bool,
    }
}

impl SessionVars {
    pub fn inner(&self) -> Arc<RwLock<SessionVarsInner>> {
        self.inner.clone()
    }

    pub fn read(&self) -> RwLockReadGuard<SessionVarsInner> {
        self.inner.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<SessionVarsInner> {
        self.inner.write()
    }

    /// Iterate over the implicit search path. This will have all implicit
    /// schemas prepended to the iterator.
    ///
    /// This should be used when trying to resolve existing items.
    pub fn implicit_search_path(&self) -> Vec<String> {
        IMPLICIT_SCHEMAS
            .into_iter()
            .map(|s| s.to_string())
            .chain(self.search_path().into_iter())
            .collect()
    }

    /// Get the first non-implicit schema.
    pub fn first_nonimplicit_schema(&self) -> Option<String> {
        self.search_path().get(0).cloned()
    }

    pub fn set(&mut self, name: &str, val: &str, setter: VarType) -> Result<()> {
        self.inner.write().set(name, val, setter)
    }
}

impl SessionVarsInner {
    /// Return an iterator to the variables that should be sent to the client on
    /// session start.
    pub fn startup_vars_iter(&self) -> impl Iterator<Item = &dyn AnyVar> {
        let vars: [&dyn AnyVar; 3] = [
            &self.server_version,
            &self.application_name,
            &self.client_encoding,
        ];
        vars.into_iter()
    }

    /// Get a value for a variable.
    pub fn get(&self, name: &str) -> datafusion::error::Result<&dyn AnyVar> {
        if name.eq_ignore_ascii_case(SERVER_VERSION.name) {
            Ok(&self.server_version)
        } else if name.eq_ignore_ascii_case(APPLICATION_NAME.name) {
            Ok(&self.application_name)
        } else if name.eq_ignore_ascii_case(CLIENT_ENCODING.name) {
            Ok(&self.client_encoding)
        } else if name.eq_ignore_ascii_case(EXTRA_FLOAT_DIGITS.name) {
            Ok(&self.extra_floating_digits)
        } else if name.eq_ignore_ascii_case(STATEMENT_TIMEOUT.name) {
            Ok(&self.statement_timeout)
        } else if name.eq_ignore_ascii_case(TIMEZONE.name) {
            Ok(&self.timezone)
        } else if name.eq_ignore_ascii_case(DATESTYLE.name) {
            Ok(&self.datestyle)
        } else if name.eq_ignore_ascii_case(TRANSACTION_ISOLATION.name) {
            Ok(&self.transaction_isolation)
        } else if name.eq_ignore_ascii_case(SEARCH_PATH.name) {
            Ok(&self.search_path)
        } else if name.eq_ignore_ascii_case(ENABLE_DEBUG_DATASOURCES.name) {
            Ok(&self.enable_debug_datasources)
        } else if name.eq_ignore_ascii_case(FORCE_CATALOG_REFRESH.name) {
            Ok(&self.force_catalog_refresh)
        } else if name.eq_ignore_ascii_case(GLAREDB_VERSION.name) {
            Ok(&self.glaredb_version)
        } else if name.eq_ignore_ascii_case(DATABASE_ID.name) {
            Ok(&self.database_id)
        } else if name.eq_ignore_ascii_case(USER_ID.name) {
            Ok(&self.user_id)
        } else if name.eq_ignore_ascii_case(CONNECTION_ID.name) {
            Ok(&self.connection_id)
        } else if name.eq_ignore_ascii_case(REMOTE_SESSION_ID.name) {
            Ok(&self.remote_session_id)
        } else if name.eq_ignore_ascii_case(USER_NAME.name) {
            Ok(&self.user_name)
        } else if name.eq_ignore_ascii_case(DATABASE_NAME.name) {
            Ok(&self.database_name)
        } else if name.eq_ignore_ascii_case(MAX_DATASOURCE_COUNT.name) {
            Ok(&self.max_datasource_count)
        } else if name.eq_ignore_ascii_case(MEMORY_LIMIT_BYTES.name) {
            Ok(&self.memory_limit_bytes)
        } else if name.eq_ignore_ascii_case(MAX_TUNNEL_COUNT.name) {
            Ok(&self.max_tunnel_count)
        } else if name.eq_ignore_ascii_case(MAX_CREDENTIALS_COUNT.name) {
            Ok(&self.max_credentials_count)
        } else if name.eq_ignore_ascii_case(IS_CLOUD_INSTANCE.name) {
            Ok(&self.is_cloud_instance)
        } else {
            Err(VarError::UnknownVariable(name.to_string()).into())
        }
    }

    /// Try to set a value for a variable.
    pub fn set(&mut self, name: &str, val: &str, setter: VarType) -> Result<()> {
        if name.eq_ignore_ascii_case(SERVER_VERSION.name) {
            self.server_version.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(APPLICATION_NAME.name) {
            self.application_name.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(CLIENT_ENCODING.name) {
            self.client_encoding.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(EXTRA_FLOAT_DIGITS.name) {
            self.extra_floating_digits.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(STATEMENT_TIMEOUT.name) {
            self.statement_timeout.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(TIMEZONE.name) {
            self.timezone.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(DATESTYLE.name) {
            self.datestyle.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(TRANSACTION_ISOLATION.name) {
            self.transaction_isolation.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(SEARCH_PATH.name) {
            self.search_path.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(ENABLE_DEBUG_DATASOURCES.name) {
            self.enable_debug_datasources.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(FORCE_CATALOG_REFRESH.name) {
            self.force_catalog_refresh.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(GLAREDB_VERSION.name) {
            self.glaredb_version.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(DATABASE_ID.name) {
            self.database_id.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(USER_ID.name) {
            self.user_id.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(CONNECTION_ID.name) {
            self.connection_id.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(REMOTE_SESSION_ID.name) {
            self.remote_session_id.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(USER_NAME.name) {
            self.user_name.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(DATABASE_NAME.name) {
            self.database_name.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(MAX_DATASOURCE_COUNT.name) {
            self.max_datasource_count.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(MEMORY_LIMIT_BYTES.name) {
            self.memory_limit_bytes.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(MAX_TUNNEL_COUNT.name) {
            self.max_tunnel_count.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(MAX_CREDENTIALS_COUNT.name) {
            self.max_credentials_count.set_from_str(val, setter)
        } else {
            Err(VarError::UnknownVariable(name.to_string()).into())
        }
    }
}

impl Default for SessionVarsInner {
    fn default() -> Self {
        SessionVarsInner {
            server_version: SessionVar::new(&SERVER_VERSION),
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: SessionVar::new(&CLIENT_ENCODING),
            extra_floating_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            statement_timeout: SessionVar::new(&STATEMENT_TIMEOUT),
            timezone: SessionVar::new(&TIMEZONE),
            datestyle: SessionVar::new(&DATESTYLE),
            transaction_isolation: SessionVar::new(&TRANSACTION_ISOLATION),
            search_path: SessionVar::new(&SEARCH_PATH),
            enable_debug_datasources: SessionVar::new(&ENABLE_DEBUG_DATASOURCES),
            force_catalog_refresh: SessionVar::new(&FORCE_CATALOG_REFRESH),
            glaredb_version: SessionVar::new(&GLAREDB_VERSION),
            database_id: SessionVar::new(&DATABASE_ID),
            user_id: SessionVar::new(&USER_ID),
            connection_id: SessionVar::new(&CONNECTION_ID),
            remote_session_id: SessionVar::new(&REMOTE_SESSION_ID),
            user_name: SessionVar::new(&USER_NAME),
            database_name: SessionVar::new(&DATABASE_NAME),
            max_datasource_count: SessionVar::new(&MAX_DATASOURCE_COUNT),
            memory_limit_bytes: SessionVar::new(&MEMORY_LIMIT_BYTES),
            max_tunnel_count: SessionVar::new(&MAX_TUNNEL_COUNT),
            max_credentials_count: SessionVar::new(&MAX_CREDENTIALS_COUNT),
            is_cloud_instance: SessionVar::new(&IS_CLOUD_INSTANCE),
        }
    }
}

pub trait AnyVar {
    /// Return the name of the varaible.
    fn name(&self) -> &'static str;

    /// Return the stringified value for the variable.
    fn formatted_value(&self) -> String;

    /// Create a record batch containg one row with the string value.
    fn record_batch(&self) -> RecordBatch {
        let val = self.formatted_value();
        let arr = StringArray::from(vec![Some(val.as_str())]);
        let schema = Schema::new(vec![Field::new(self.name(), DataType::Utf8, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
    }
}

/// Static configuration variables. These are all defined in code and are not
/// persisted.
#[derive(Debug)]
#[allow(dead_code)] // `group` currently unused
pub struct ServerVar<T>
where
    T: Value + ?Sized + 'static,
{
    /// Name of the variable.
    name: &'static str,

    /// Description of the variable
    description: &'static str,

    /// Starting value of the variable.
    value: &'static T,

    /// Variable group.
    ///
    /// This allows us to differentiate between variables needed for postgres
    /// compat, and variables custom to glaredb.
    group: &'static str,

    /// Whether or not this variable can be set by the user.
    user_configurable: bool,
}

impl<T> ServerVar<T>
where
    T: Value + ?Sized + 'static,
{
    /// Get a reference to underlying value.
    pub fn value(&self) -> &T {
        self.value
    }
}

impl<T> AnyVar for ServerVar<T>
where
    T: Value + ?Sized + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn formatted_value(&self) -> String {
        self.value.format()
    }
}

/// Session local variables. Unset variables will inherit the system variable.
#[derive(Debug)]
pub struct SessionVar<T>
where
    T: Value + ?Sized + 'static + std::fmt::Debug,
{
    inherit: &'static ServerVar<T>,
    value: Option<T::Owned>,
}

impl<T> SessionVar<T>
where
    T: Value + ?Sized + 'static + std::fmt::Debug,
{
    pub fn new(inherit: &'static ServerVar<T>) -> Self {
        SessionVar {
            inherit,
            value: None,
        }
    }

    /// Get a reference to the underlying value. If value hasn't been set for
    /// the session, the system value will be returned.
    pub fn value(&self) -> &T {
        match self.value.as_ref() {
            Some(v) => v.borrow(),
            None => self.inherit.value(),
        }
    }

    /// Set the value for a variable directly.
    pub fn set_raw(&mut self, v: T::Owned, setter: VarType) -> Result<()> {
        if !self.inherit.user_configurable && matches!(setter, VarType::UserDefined) {
            return Err(VarError::VariableReadonly(self.inherit.name.to_string()).into());
        }
        self.value = Some(v);
        Ok(())
    }

    /// Set a value for a variable directly, emitting an error log if it's not able to be set.
    pub fn set_and_log(&mut self, v: T::Owned, setter: VarType) {
        if let Err(e) = self.set_raw(v, setter) {
            error!(%e, "unable to set session variable");
        }
    }

    /// Parse a string as a variable value and set it.
    fn set_from_str(&mut self, s: &str, setter: VarType) -> Result<()> {
        match T::try_parse(s) {
            Some(v) => self.set_raw(v, setter)?,
            None => {
                return Err(VarError::InvalidSessionVarValue {
                    name: self.name().to_string(),
                    val: s.to_string(),
                }
                .into())
            }
        }
        Ok(())
    }

    pub fn description(&self) -> &'static str {
        self.inherit.description
    }
}

impl<T> AnyVar for SessionVar<T>
where
    T: Value + ?Sized + 'static,
{
    fn name(&self) -> &'static str {
        self.inherit.name()
    }

    fn formatted_value(&self) -> String {
        match &self.value {
            Some(v) => v.borrow().format(),
            None => self.inherit.formatted_value(),
        }
    }
}

/// Regex for matching strings delineated by commas. Will match full quoted
/// strings as well.
///
/// Taken from <https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes>
const SPLIT_ON_UNQUOTED_COMMAS: &str = r#""[^"]*"|[^,]+"#;

static COMMA_RE: Lazy<Regex> = Lazy::new(|| Regex::new(SPLIT_ON_UNQUOTED_COMMAS).unwrap());

/// Split a string on commas, preserving quoted strings.
fn split_comma_delimited(text: &str) -> Vec<String> {
    COMMA_RE
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .collect()
}

impl Value for [String] {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(split_comma_delimited(s))
    }

    fn format(&self) -> String {
        self.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_on_commas() {
        struct Test {
            input: &'static str,
            expected: Vec<String>,
        }

        let tests = vec![
            Test {
                input: "",
                expected: Vec::new(),
            },
            Test {
                input: "my_schema",
                expected: vec!["my_schema".to_string()],
            },
            Test {
                input: "a,b,c",
                expected: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            Test {
                input: "a,\"b,c\"",
                expected: vec!["a".to_string(), "\"b,c\"".to_string()],
            },
        ];

        for test in tests {
            let out = split_comma_delimited(test.input);
            assert_eq!(test.expected, out);
        }
    }

    #[test]
    fn user_configurable() {
        const SETTABLE: ServerVar<str> = ServerVar {
            name: "unsettable",
            description: "test",
            value: "test",
            group: "test",
            user_configurable: true,
        };
        let mut var = SessionVar::new(&SETTABLE);
        var.set_from_str("user", VarType::UserDefined).unwrap();
        assert_eq!("user", var.value());

        // Should also be able to be set by the system.
        var.set_from_str("system", VarType::System).unwrap();
        assert_eq!("system", var.value());
    }

    #[test]
    fn not_user_configurable() {
        const UNSETTABLE: ServerVar<str> = ServerVar {
            name: "unsettable",
            description: "test",
            value: "test",
            group: "test",
            user_configurable: false,
        };
        let mut var = SessionVar::new(&UNSETTABLE);
        var.set_from_str("custom", VarType::UserDefined)
            .expect_err("Unsettable var should not be allowed to be set by user");

        assert_eq!("test", var.value());

        // System should be able to set the var.
        var.set_from_str("custom", VarType::System).unwrap();
        assert_eq!("custom", var.value());
    }
}
