use std::borrow::Borrow;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigEntry;
use datafusion::error::Result;
use datafusion::variable::VarType;
use pgrepr::notice::NoticeSeverity;
use tracing::error;
use uuid::Uuid;

use super::constants::{
    APPLICATION_NAME,
    CLIENT_ENCODING,
    CLIENT_MIN_MESSAGES,
    CONNECTION_ID,
    DATABASE_ID,
    DATABASE_NAME,
    DATESTYLE,
    DIALECT,
    ENABLE_DEBUG_DATASOURCES,
    ENABLE_EXPERIMENTAL_SCHEDULER,
    EXTENSION_DIR,
    EXTRA_FLOAT_DIGITS,
    FORCE_CATALOG_REFRESH,
    GLAREDB_VERSION,
    IS_CLOUD_INSTANCE,
    IS_SERVER_INSTANCE,
    MAX_CREDENTIALS_COUNT,
    MAX_DATASOURCE_COUNT,
    MAX_TUNNEL_COUNT,
    MEMORY_LIMIT_BYTES,
    REMOTE_SESSION_ID,
    SEARCH_PATH,
    SERVER_VERSION,
    STANDARD_CONFORMING_STRINGS,
    STATEMENT_TIMEOUT,
    TIMEZONE,
    TRANSACTION_ISOLATION,
    USER_ID,
    USER_NAME,
};
use super::error::VarError;
use super::value::Value;

#[derive(Debug, Default, Clone, Copy)]
pub enum Dialect {
    #[default]
    Sql,
    Prql,
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
    pub client_min_messages: SessionVar<NoticeSeverity>,
    pub standard_conforming_strings: SessionVar<bool>,
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
    pub dialect: SessionVar<Dialect>,
    pub enable_experimental_scheduler: SessionVar<bool>,
    pub is_server_instance: SessionVar<bool>,
    pub extension_dir: SessionVar<str>,
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
        } else if name.eq_ignore_ascii_case(CLIENT_MIN_MESSAGES.name) {
            Ok(&self.client_min_messages)
        } else if name.eq_ignore_ascii_case(STANDARD_CONFORMING_STRINGS.name) {
            Ok(&self.standard_conforming_strings)
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
        } else if name.eq_ignore_ascii_case(DIALECT.name) {
            Ok(&self.dialect)
        } else if name.eq_ignore_ascii_case(ENABLE_EXPERIMENTAL_SCHEDULER.name) {
            Ok(&self.enable_experimental_scheduler)
        } else if name.eq_ignore_ascii_case(IS_SERVER_INSTANCE.name) {
            Ok(&self.is_server_instance)
        } else if name.eq_ignore_ascii_case(EXTENSION_DIR.name) {
            Ok(&self.extension_dir)
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
        } else if name.eq_ignore_ascii_case(CLIENT_MIN_MESSAGES.name) {
            self.client_min_messages.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(STANDARD_CONFORMING_STRINGS.name) {
            self.standard_conforming_strings.set_from_str(val, setter)
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
        } else if name.eq_ignore_ascii_case(DIALECT.name) {
            self.dialect.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(ENABLE_EXPERIMENTAL_SCHEDULER.name) {
            self.enable_experimental_scheduler.set_from_str(val, setter)
        } else if name.eq_ignore_ascii_case(EXTENSION_DIR.name) {
            self.extension_dir.set_from_str(val, setter)
        } else {
            Err(VarError::UnknownVariable(name.to_string()).into())
        }
    }
    pub(super) fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            self.server_version.config_entry(),
            self.application_name.config_entry(),
            self.client_encoding.config_entry(),
            self.extra_floating_digits.config_entry(),
            self.statement_timeout.config_entry(),
            self.timezone.config_entry(),
            self.datestyle.config_entry(),
            self.transaction_isolation.config_entry(),
            self.search_path.config_entry(),
            self.enable_debug_datasources.config_entry(),
            self.force_catalog_refresh.config_entry(),
            self.glaredb_version.config_entry(),
            self.database_id.config_entry(),
            self.user_id.config_entry(),
            self.connection_id.config_entry(),
            self.remote_session_id.config_entry(),
            self.user_name.config_entry(),
            self.database_name.config_entry(),
            self.max_datasource_count.config_entry(),
            self.memory_limit_bytes.config_entry(),
            self.max_tunnel_count.config_entry(),
            self.max_credentials_count.config_entry(),
            self.is_cloud_instance.config_entry(),
            self.dialect.config_entry(),
            self.is_server_instance.config_entry(),
            self.extension_dir.config_entry(),
        ]
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
            client_min_messages: SessionVar::new(&CLIENT_MIN_MESSAGES),
            standard_conforming_strings: SessionVar::new(&STANDARD_CONFORMING_STRINGS),
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
            dialect: SessionVar::new(&DIALECT),
            enable_experimental_scheduler: SessionVar::new(&ENABLE_EXPERIMENTAL_SCHEDULER),
            is_server_instance: SessionVar::new(&IS_SERVER_INSTANCE),
            extension_dir: SessionVar::new(&EXTENSION_DIR),
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
    pub(super) name: &'static str,

    /// Description of the variable
    pub(super) description: &'static str,

    /// Starting value of the variable.
    pub(super) value: &'static T,

    /// Variable group.
    ///
    /// This allows us to differentiate between variables needed for postgres
    /// compat, and variables custom to glaredb.
    pub(super) group: &'static str,

    /// Whether or not this variable can be set by the user.
    pub(super) user_configurable: bool,
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
    pub(super) fn set_from_str(&mut self, s: &str, setter: VarType) -> Result<()> {
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

    fn config_entry(&self) -> ConfigEntry {
        ConfigEntry {
            key: self.inherit.name.to_string(),
            value: Some(self.formatted_value()),
            description: self.description(),
        }
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
