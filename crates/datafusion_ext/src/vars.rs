//! Server and session variables.
mod constants;
mod error;
mod inner;
mod value;
use constants::*;

use datafusion::variable::VarType;
use inner::*;
use uuid::Uuid;
use value::*;

pub use inner::SessionVarsInner;
use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard};
use regex::Regex;
use std::borrow::ToOwned;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
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

impl Default for SessionVars {
    fn default() -> Self {
        Self::new(SessionVarsInner::default())
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

macro_rules! with_property {
    ($self:expr, $field:ident, $setter:expr, $value:expr) => {{
        $self
            .inner
            .write()
            .$field
            .set_and_log($value.to_owned(), $setter);
        Self {
            inner: $self.inner.clone(),
        }
    }};
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

    pub fn set(&mut self, name: &str, val: &str, setter: VarType) -> datafusion::error::Result<()> {
        self.inner.write().set(name, val, setter)
    }

    pub fn with_server_version(self, value: String, setter: VarType) -> Self {
        with_property!(self, server_version, setter, value)
    }
    pub fn with_application_name(self, value: String, setter: VarType) -> Self {
        with_property!(self, application_name, setter, value)
    }
    pub fn with_client_encoding(self, value: String, setter: VarType) -> Self {
        with_property!(self, client_encoding, setter, value)
    }
    pub fn with_extra_floating_digits(self, value: i32, setter: VarType) -> Self {
        with_property!(self, extra_floating_digits, setter, value)
    }
    pub fn with_statement_timeout(self, value: i32, setter: VarType) -> Self {
        with_property!(self, statement_timeout, setter, value)
    }
    pub fn with_timezone(self, value: String, setter: VarType) -> Self {
        with_property!(self, timezone, setter, value)
    }
    pub fn with_datestyle(self, value: String, setter: VarType) -> Self {
        with_property!(self, datestyle, setter, value)
    }
    pub fn with_transaction_isolation(self, value: String, setter: VarType) -> Self {
        with_property!(self, transaction_isolation, setter, value)
    }
    pub fn with_search_path(self, value: Vec<String>, setter: VarType) -> Self {
        with_property!(self, search_path, setter, value)
    }
    pub fn with_enable_debug_datasources(self, value: bool, setter: VarType) -> Self {
        with_property!(self, enable_debug_datasources, setter, value)
    }
    pub fn with_force_catalog_refresh(self, value: bool, setter: VarType) -> Self {
        with_property!(self, force_catalog_refresh, setter, value)
    }
    pub fn with_glaredb_version(self, value: String, setter: VarType) -> Self {
        with_property!(self, glaredb_version, setter, value)
    }
    pub fn with_database_id(self, value: Uuid, setter: VarType) -> Self {
        with_property!(self, database_id, setter, value)
    }
    pub fn with_connection_id(self, value: Uuid, setter: VarType) -> Self {
        with_property!(self, connection_id, setter, value)
    }
    pub fn with_remote_session_id(self, value: Uuid, setter: VarType) -> Self {
        with_property!(self, remote_session_id, setter, Some(value))
    }
    pub fn with_user_id(self, value: Uuid, setter: VarType) -> Self {
        with_property!(self, user_id, setter, value)
    }

    pub fn with_user_name(self, value: impl AsRef<str>, setter: VarType) -> Self {
        with_property!(self, user_name, setter, value.as_ref())
    }
    pub fn with_database_name(self, value: impl AsRef<str>, setter: VarType) -> Self {
        with_property!(self, database_name, setter, value.as_ref())
    }
    pub fn with_max_datasource_count(self, value: usize, setter: VarType) -> Self {
        with_property!(self, max_datasource_count, setter, Some(value))
    }
    pub fn with_memory_limit_bytes(self, value: usize, setter: VarType) -> Self {
        with_property!(self, memory_limit_bytes, setter, Some(value))
    }
    pub fn with_max_tunnel_count(self, value: usize, setter: VarType) -> Self {
        with_property!(self, max_tunnel_count, setter, Some(value))
    }
    pub fn with_max_credentials_count(self, value: usize, setter: VarType) -> Self {
        with_property!(self, max_credentials_count, setter, Some(value))
    }
    pub fn with_is_cloud_instance(self, value: bool, setter: VarType) -> Self {
        with_property!(self, is_cloud_instance, setter, value)
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
    use datafusion::variable::VarType;

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
