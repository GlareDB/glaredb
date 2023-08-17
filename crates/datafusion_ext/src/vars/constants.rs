use super::*;

// TODO: Decide proper postgres version to spoof/support
pub(super) const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: "server_version",
    value: "15.1",
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

/// Note that these are not normally shown in the search path.
pub(super) const IMPLICIT_SCHEMAS: [&str; 2] = [
    POSTGRES_SCHEMA,
    // Objects stored in current session will always have a priority over the
    // schemas in search path.
    CURRENT_SESSION_SCHEMA,
];

/// The default catalog that exists in all GlareDB databases.
pub const DEFAULT_CATALOG: &str = "default";

/// Default schema that's created on every startup.
pub const DEFAULT_SCHEMA: &str = "public";

/// Internal schema for system tables.
pub const INTERNAL_SCHEMA: &str = "glare_catalog";

pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const POSTGRES_SCHEMA: &str = "pg_catalog";

/// Schema to store temporary objects (only valid for current session).
pub const CURRENT_SESSION_SCHEMA: &str = "current_session";
