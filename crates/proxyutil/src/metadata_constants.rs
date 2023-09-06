//! Constants shared between client and rpc proxy for authentication.

pub const USER_KEY: &str = "user";
pub const PASSWORD_KEY: &str = "password";
pub const DB_NAME_KEY: &str = "db_name";
pub const ORG_KEY: &str = "org";
pub const COMPUTE_ENGINE_KEY: &str = "compute_engine";

/// Metadata constant used by the proxy to set which database the proxy is
/// proxying the connection for. The server will read the metadata to assert
/// that the connection has been authenticated for the correct database.
pub const PROXIED_FOR_DATABASE: &str = "proxied_for_database";
