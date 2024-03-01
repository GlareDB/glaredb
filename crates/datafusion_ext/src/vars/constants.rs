use std::env::consts::{ARCH, FAMILY};

use const_format::concatcp;
use pgrepr::compatible::server_version;
use pgrepr::notice::NoticeSeverity;

use super::{Dialect, Lazy, ServerVar, ToOwned, Uuid};

pub(super) const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: "server_version",
    value: server_version(),
    group: "postgres",
    user_configurable: false,
    description: "Version of the server",
};

pub(super) const APPLICATION_NAME: ServerVar<str> = ServerVar {
    name: "application_name",
    value: "",
    group: "postgres",
    user_configurable: true,
    description: "Name of the application",
};

pub(super) const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: "client_encoding",
    value: "UTF8",
    group: "postgres",
    user_configurable: true,
    description: "Encoding of the client",
};

pub(super) const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: "extra_float_digits",
    value: &1,
    group: "postgres",
    user_configurable: true,
    description: "Extra precision in float values",
};

pub(super) const STATEMENT_TIMEOUT: ServerVar<i32> = ServerVar {
    name: "statement_timeout",
    value: &0,
    group: "postgres",
    user_configurable: true,
    description: "Statement timeout in milliseconds",
};

pub(super) const TIMEZONE: ServerVar<str> = ServerVar {
    name: "TimeZone",
    value: "UTC",
    group: "postgres",
    user_configurable: true,
    description: "Timezone of the client, default UTC",
};

pub(super) const DATESTYLE: ServerVar<str> = ServerVar {
    name: "DateStyle",
    value: "ISO",
    group: "postgres",
    user_configurable: true,
    description: "Date style of the client, default ISO",
};

pub(super) const TRANSACTION_ISOLATION: ServerVar<str> = ServerVar {
    name: "transaction_isolation",
    value: "read uncommitted",
    group: "postgres",
    user_configurable: false,
    description: "Transaction isolation level, defaults to 'read uncommitted'",
};

pub(super) static DEFAULT_SEARCH_PATH: Lazy<[String; 1]> = Lazy::new(|| ["public".to_owned()]);
pub(super) static SEARCH_PATH: Lazy<ServerVar<[String]>> = Lazy::new(|| ServerVar {
    name: "search_path",
    value: &*DEFAULT_SEARCH_PATH,
    group: "postgres",
    user_configurable: true,
    description: "Search path for schemas",
});

pub(super) const CLIENT_MIN_MESSAGES: ServerVar<NoticeSeverity> = ServerVar {
    name: "client_min_messages",
    value: &NoticeSeverity::Notice,
    group: "postgres",
    user_configurable: true,
    description: "Controls which messages are sent to the client, defaults NOTICE",
};

pub(super) const STANDARD_CONFORMING_STRINGS: ServerVar<bool> = ServerVar {
    name: "standard_conforming_strings",
    value: &true,
    group: "postgres",
    user_configurable: false,
    description: "Treat backslashes literally in string literals",
};

pub(super) static GLAREDB_VERSION_OWNED: Lazy<String> =
    Lazy::new(|| format!("v{}", env!("CARGO_PKG_VERSION")));

pub(super) static GLAREDB_VERSION: Lazy<ServerVar<str>> = Lazy::new(|| ServerVar {
    name: "glaredb_version",
    value: &GLAREDB_VERSION_OWNED,
    group: "glaredb",
    user_configurable: false,
    description: "Version of glaredb",
});

pub(super) const ENABLE_DEBUG_DATASOURCES: ServerVar<bool> = ServerVar {
    name: "enable_debug_datasources",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "Enable debug datasources",
};

pub(super) const FORCE_CATALOG_REFRESH: ServerVar<bool> = ServerVar {
    name: "force_catalog_refresh",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "Force catalog refresh",
};

pub(super) const DATABASE_ID: ServerVar<Uuid> = ServerVar {
    name: "database_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "Database ID",
};

pub(super) const CONNECTION_ID: ServerVar<Uuid> = ServerVar {
    name: "connection_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "Connection ID",
};

pub(super) const REMOTE_SESSION_ID: ServerVar<Option<Uuid>> = ServerVar {
    name: "remote_session_id",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Session ID on remote service.",
};

pub(super) const USER_ID: ServerVar<Uuid> = ServerVar {
    name: "user_id",
    value: &Uuid::nil(),
    group: "glaredb",
    user_configurable: false,
    description: "User ID",
};

pub(super) const USER_NAME: ServerVar<str> = ServerVar {
    name: "user_name",
    value: "",
    group: "glaredb",
    user_configurable: false,
    description: "User name",
};

pub(super) const DATABASE_NAME: ServerVar<str> = ServerVar {
    name: "database_name",
    value: "",
    group: "glaredb",
    user_configurable: false,
    description: "Database name",
};

pub(super) const MAX_DATASOURCE_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_datasource_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max datasource count",
};

pub(super) const MEMORY_LIMIT_BYTES: ServerVar<Option<usize>> = ServerVar {
    name: "memory_limit_bytes",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Memory limit in bytes",
};

pub(super) const MAX_TUNNEL_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_tunnel_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max tunnel count",
};

pub(super) const MAX_CREDENTIALS_COUNT: ServerVar<Option<usize>> = ServerVar {
    name: "max_credentials_count",
    value: &None,
    group: "glaredb",
    user_configurable: false,
    description: "Max credentials allowed",
};

pub(super) const IS_CLOUD_INSTANCE: ServerVar<bool> = ServerVar {
    name: "is_cloud_instance",
    value: &false,
    group: "glaredb",
    user_configurable: false,
    description: "Determines if the server is local or cloud",
};

pub(super) const DIALECT: ServerVar<Dialect> = ServerVar {
    name: "dialect",
    value: &Dialect::Sql,
    group: "glaredb",
    user_configurable: true,
    description: "Dialect of the sql engine",
};

pub(super) const ENABLE_EXPERIMENTAL_SCHEDULER: ServerVar<bool> = ServerVar {
    name: "enable_experimental_scheduler",
    value: &false,
    group: "glaredb",
    user_configurable: true,
    description: "If the experimental query scheduler should be enabled",
};

pub(super) const IS_SERVER_INSTANCE: ServerVar<bool> = ServerVar {
    name: "is_server_instance",
    value: &true,
    group: "glaredb",
    user_configurable: false,
    description: r#"
    If this is connected to, or is a server instance. 
    This is not the same as `is_cloud_instance`. This should _only_ be false if it's local or embedded execution
"#,
};

static GLAREDB_EXTENSION_PATH: Lazy<String> = Lazy::new(|| get_extension_path());
pub(super) static EXTENSION_DIRECTORY: Lazy<ServerVar<str>> = Lazy::new(|| ServerVar {
    name: "extension_directory",
    value: &GLAREDB_EXTENSION_PATH,
    group: "glaredb",
    user_configurable: true,
    description: "Directory to load extensions from",
});


#[cfg(not(target_os = "windows"))]
fn get_home_dir() -> String {
    std::env::var("HOME")
        .expect("home directory not found")
        .to_string()
}

#[cfg(target_os = "windows")]
fn get_home_dir() -> String {
    std::env::var("USERPROFILE")
        .expect("home directory not found")
        .to_string()
}

// ~/.glaredb/extensions/0.1.0/macos_x86_64
fn get_extension_path() -> String {
    let mut path = get_home_dir();
    path.push_str("/.glaredb/extensions/");
    path.push_str(env!("CARGO_PKG_VERSION"));
    path.push_str("/");
    path.push_str(concatcp!(FAMILY, "_", ARCH));
    path
}

/// Note that these are not normally shown in the search path.
pub(super) const IMPLICIT_SCHEMAS: [&str; 2] = [
    POSTGRES_SCHEMA,
    // Objects stored in current session will always have a priority over the
    // schemas in search path.
    CURRENT_SESSION_SCHEMA,
];

pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// Schema to store temporary objects (only valid for current session).
pub const CURRENT_SESSION_SCHEMA: &str = "current_session";
