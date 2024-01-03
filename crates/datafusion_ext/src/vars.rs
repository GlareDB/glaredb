//! Server and session variables.
mod constants;
mod error;
mod inner;
mod utils;
mod value;
use constants::*;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion::scalar::ScalarValue;
use utils::*;

use datafusion::variable::{VarProvider, VarType};
use inner::*;
use uuid::Uuid;

pub use inner::Dialect;
pub use inner::SessionVarsInner;
use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockReadGuard};
use std::borrow::ToOwned;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use self::error::VarError;

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
     dialect: Dialect,
     enable_experimental_scheduler: bool,
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
            .chain(self.search_path())
            .collect()
    }

    pub fn implicit_search_path_iter(&self) -> impl Iterator<Item = String> + '_ {
        IMPLICIT_SCHEMAS
            .into_iter()
            .map(|s| s.to_string())
            .chain(self.search_path())
    }

    /// Get the first non-implicit schema.
    pub fn first_nonimplicit_schema(&self) -> Option<String> {
        self.search_path().first().cloned()
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

impl ConfigExtension for SessionVars {
    const PREFIX: &'static str = "glaredb";
}

impl ExtensionOptions for SessionVars {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        self.set(key, value, VarType::UserDefined)
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        self.read().entries()
    }
}

impl VarProvider for SessionVars {
    fn get_value(
        &self,
        var_names: Vec<String>,
    ) -> datafusion::error::Result<datafusion::scalar::ScalarValue> {
        Ok(match var_names[0].as_str() {
            "version" => ScalarValue::Utf8(Some(self.glaredb_version())),
            "current_user" | "current_role" | "user" => ScalarValue::Utf8(Some(self.user_name())),
            "current_database" | "current_catalog" => ScalarValue::Utf8(Some(self.database_name())),
            "current_schema" => ScalarValue::Utf8(self.search_path().first().cloned()),
            "connection_id" => ScalarValue::Utf8(Some(self.connection_id().to_string())),
            "current_schemas" => {
                let schemas = self
                    .search_path()
                    .into_iter()
                    .map(|path| ScalarValue::Utf8(Some(path)))
                    .collect::<Vec<_>>();
                ScalarValue::List(
                    Some(schemas),
                    Field::new("item", DataType::Utf8, true).into(),
                )
            }
            "current_schemas_include_implicit" => {
                let schemas = self
                    .implicit_search_path_iter()
                    .map(|path| ScalarValue::Utf8(Some(path)))
                    .collect::<Vec<_>>();
                ScalarValue::List(
                    Some(schemas),
                    Field::new("item", DataType::Utf8, true).into(),
                )
            }
            s => Err(datafusion::error::DataFusionError::External(
                VarError::UnknownVariable(s.to_string()).into(),
            ))?,
        })
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        match var_names[0].as_str() {
            "version" | "current_user" | "current_role" | "user" | "current_database"
            | "current_catalog" | "current_schema" | "connection_id" => Some(DataType::Utf8),
            "current_schemas" | "current_schemas_include_implicit" => Some(DataType::List(
                Field::new("current_schemas", DataType::Utf8, true).into(),
            )),

            _ => None,
        }
    }
}
