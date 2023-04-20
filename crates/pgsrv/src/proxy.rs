use crate::auth::{ConnectionAuthenticator, DatabaseDetails};
use crate::codec::{
    client::FramedClientConn,
    server::{FramedConn, PgCodec},
};
use crate::errors::{PgSrvError, Result};
use crate::messages::{BackendMessage, ErrorResponse, FrontendMessage, StartupMessage, VERSION_V3};
use crate::ssl::Connection;
use crate::ssl::SslConfig;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;
use uuid::Uuid;

/// Constant id for a database if running locally.
pub const LOCAL_DATABASE_ID: Uuid = Uuid::nil();

/// Constant id for a user if running locally.
pub const LOCAL_USER_ID: Uuid = Uuid::nil();

/// Defines a key that's set on the connection by pgsrv.
pub trait ProxyKey<R> {
    /// Get a value from params.
    ///
    /// `local` indicates that the database is not running behind a pgsrv, and
    /// failing to find a key in `params` shouldn't result in an error. Instead
    /// a well-defined value should be returned. This is useful for local
    /// development.
    fn value_from_params(&self, params: &HashMap<String, String>, local: bool) -> Result<R>;
}

/// A proxy key who's value should be parsed as a uuid.
#[derive(Debug)]
pub struct UuidProxyKey {
    /// Key to look for in params.
    key: &'static str,
    /// Default to use if key not found _and_ db is running locally.
    local_default: Uuid,
}

impl ProxyKey<Uuid> for UuidProxyKey {
    fn value_from_params(&self, params: &HashMap<String, String>, local: bool) -> Result<Uuid> {
        let id = match params.get(self.key) {
            Some(val) => match Uuid::parse_str(val.as_str()) {
                Ok(uuid) => uuid,
                Err(_) => {
                    return Err(PgSrvError::InvalidValueForProxyKey {
                        key: self.key,
                        value: val.clone(),
                    })
                }
            },
            None => {
                if !local {
                    return Err(PgSrvError::MissingProxyKey(self.key));
                }
                self.local_default
            }
        };
        Ok(id)
    }
}
/// A proxy key who's value should be parsed as a bool.
#[derive(Debug)]
pub struct BoolProxyKey {
    /// Key to look for in params.
    key: &'static str,
    /// Default to use if key not found _and_ db is running locally.
    local_default: bool,
}

impl ProxyKey<bool> for BoolProxyKey {
    fn value_from_params(&self, params: &HashMap<String, String>, local: bool) -> Result<bool> {
        match params.get(self.key) {
            Some(val) => match val.as_str() {
                "true" | "t" => Ok(true),
                "false" | "f" => Ok(false),
                _ => Err(PgSrvError::InvalidValueForProxyKey {
                    key: self.key,
                    value: val.clone(),
                }),
            },
            None if local => Ok(self.local_default),
            None => Err(PgSrvError::MissingProxyKey(self.key)),
        }
    }
}

/// A proxy key who's value should be parsed as a usize.
#[derive(Debug)]
pub struct UsizeProxyKey {
    /// Key to look for in params.
    key: &'static str,
    /// Default to use if key not found _and_ db is running locally.
    local_default: usize,
}

impl ProxyKey<usize> for UsizeProxyKey {
    fn value_from_params(&self, params: &HashMap<String, String>, local: bool) -> Result<usize> {
        match params.get(self.key) {
            Some(val) => match val.parse::<usize>() {
                Ok(n) => Ok(n),
                Err(_) => Err(PgSrvError::InvalidValueForProxyKey {
                    key: self.key,
                    value: val.clone(),
                }),
            },
            None if local => Ok(self.local_default),
            None => Err(PgSrvError::MissingProxyKey(self.key)),
        }
    }
}

/// Param key for setting the database id in startup params. Added by pgsrv
/// during proxying.
pub const GLAREDB_DATABASE_ID_KEY: UuidProxyKey = UuidProxyKey {
    key: "glaredb_database_id",
    local_default: Uuid::nil(),
};

/// Param key for setting the user id in startup params. Added by pgsrv
/// during proxying.
pub const GLAREDB_USER_ID_KEY: UuidProxyKey = UuidProxyKey {
    key: "glaredb_user_id",
    local_default: Uuid::nil(),
};

/// Param key for setting if the connection was initiated by the system and not
/// the user. Added by pgsrv during proxying.
pub const GLAREDB_IS_SYSTEM_KEY: BoolProxyKey = BoolProxyKey {
    key: "glaredb_is_system",
    local_default: false,
};

/// Param key for the max number ofdatasources allowed. Added by pgsrv during proxying.
pub const GLAREDB_MAX_DATASOURCE_COUNT_KEY: UsizeProxyKey = UsizeProxyKey {
    key: "max_datasource_count",
    local_default: 100,
};

/// Param key for the memory limit in bytes. Added by pgsrv during proxying.
pub const GLAREDB_MEMORY_LIMIT_BYTES_KEY: UsizeProxyKey = UsizeProxyKey {
    key: "memory_limit_bytes",
    local_default: 0,
};

/// ProxyHandler proxies connections to some database instance. Connections are
/// authenticated via some authenticator.
///
/// In a production scenario, the provided authenticator will communicate with
/// Cloud to authenticate.
pub struct ProxyHandler<A> {
    authenticator: A,
    ssl_conf: Option<SslConfig>,
}

impl<A: ConnectionAuthenticator> ProxyHandler<A> {
    pub fn new(authenticator: A, ssl_conf: Option<SslConfig>) -> Self {
        Self {
            authenticator,
            ssl_conf,
        }
    }

    /// Proxy a connection to some database as determined by the authenticator.
    ///
    /// Goes through the startup flow, then just acts as a dumb proxy shuffling
    /// bytes between the client and the database.
    pub async fn proxy_connection<C>(&self, conn: C) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let mut conn = Connection::new_unencrypted(conn);
        loop {
            let startup = PgCodec::decode_startup_from_conn(&mut conn).await?;
            debug!(?startup, "received startup message (proxy)");

            match startup {
                StartupMessage::StartupRequest { params, .. } => {
                    self.proxy_startup(conn, params).await?;
                    return Ok(());
                }
                StartupMessage::SSLRequest { .. } => {
                    conn = match (conn, &self.ssl_conf) {
                        (Connection::Unencrypted(mut conn), Some(conf)) => {
                            debug!("accepting ssl request");
                            // SSL supported, send back that we support it and
                            // start encrypting.
                            conn.write_all(&[b'S']).await?;
                            Connection::new_encrypted(conn, conf).await?
                        }
                        (mut conn, _) => {
                            debug!("rejecting ssl request");
                            // SSL not supported (or the connection is already
                            // wrapped). Reject and continue.
                            conn.write_all(&[b'N']).await?;
                            conn
                        }
                    }
                }
                StartupMessage::CancelRequest { .. } => {
                    self.proxy_cancel(conn).await?;
                    return Ok(());
                }
            }
        }
    }

    /// Proxy connection startup to some database. This is long lived.
    ///
    /// This will run through the startup phase of the protocol. The connection
    /// will be authenticated, and the database to proxy to will be determined
    /// by what the authenticator returns.
    async fn proxy_startup<C>(
        &self,
        conn: Connection<C>,
        mut params: HashMap<String, String>,
    ) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        let hostname = conn.servername();

        let mut framed = FramedConn::new(conn);
        framed
            .send(BackendMessage::AuthenticationCleartextPassword)
            .await?;
        let msg = match framed.read().await? {
            Some(msg) => msg,
            None => return Ok(()), // Not an error, client disconnected.
        };

        // If we fail to auth, ensure an error response is sent to the
        // connection.
        let db_details = match self.authenticate_with_msg(msg, hostname, &params).await {
            Ok(details) => details,
            Err(e) => {
                framed
                    .send(ErrorResponse::fatal_internal(format!("cloud auth: {}", e)).into())
                    .await?;
                return Err(e);
            }
        };

        // At this point, open a connection to the database and initiate a
        // startup message We need to send the same parameters as the client
        // sent us
        let db_addr = format!("{}:{}", db_details.ip, db_details.port);
        let db_conn = TcpStream::connect(db_addr).await?;
        // Note that the connection from the proxy to the db is unencrypted,
        // with no option (currently) of encrypting it.
        let mut db_framed = FramedClientConn::new(Connection::Unencrypted(db_conn));

        // Add addition params to the startup message.
        params.insert(
            GLAREDB_DATABASE_ID_KEY.key.to_string(),
            db_details.database_id,
        );
        params.insert(GLAREDB_USER_ID_KEY.key.to_string(), db_details.user_id);
        params.insert(
            GLAREDB_MAX_DATASOURCE_COUNT_KEY.key.to_string(),
            db_details.max_datasource_count.to_string(),
        );
        params.insert(
            GLAREDB_MEMORY_LIMIT_BYTES_KEY.key.to_string(),
            db_details.memory_limit_bytes.to_string(),
        );

        // More params should be inserted here. See <https://github.com/GlareDB/glaredb/issues/600>

        let startup = StartupMessage::StartupRequest {
            version: VERSION_V3,
            params,
        };
        db_framed.send_startup(startup).await?;

        // This implementation only supports AuthenticationCleartextPassword
        let auth_msg = db_framed.read().await?;
        match auth_msg {
            Some(BackendMessage::AuthenticationCleartextPassword) => {
                // TODO: rewrite password according to the response from the cloud api
                db_framed
                    .send(FrontendMessage::PasswordMessage {
                        password: "TODO: USE CLOUD PASSWORD".to_string(), // GlareDB doesn't currently check password.
                    })
                    .await?;

                // Check for AuthenticationOk and respond to the client with the same message
                let auth_ok = db_framed.read().await?;
                match auth_ok {
                    Some(BackendMessage::AuthenticationOk) => {
                        framed.send(BackendMessage::AuthenticationOk).await?;

                        // from here, we can just forward messages between the client to the database
                        let server_conn = db_framed.into_inner();
                        let client_conn = framed.into_inner();
                        tokio::io::copy_bidirectional(
                            &mut client_conn.into_inner(),
                            &mut server_conn.into_inner(),
                        )
                        .await?;

                        Ok(())
                    }
                    Some(other) => Err(PgSrvError::UnexpectedBackendMessage(other)),
                    None => Ok(()),
                }
            }
            Some(other) => Err(PgSrvError::UnexpectedBackendMessage(other)),
            None => Ok(()),
        }
    }

    /// Proxy a cancel request.
    ///
    /// Currently unimplemented. This will require that we broadcast cancel
    /// requests to the appropriate database instances.
    async fn proxy_cancel<C>(&self, mut _conn: Connection<C>) -> Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        debug!("cancel received (proxy)");
        Ok(())
    }

    /// Try to authenticate using the contents of a frontend message.
    ///
    /// Currently only supports the password message.
    async fn authenticate_with_msg(
        &self,
        msg: FrontendMessage,
        hostname: Option<String>,
        params: &HashMap<String, String>,
    ) -> Result<DatabaseDetails> {
        match msg {
            FrontendMessage::PasswordMessage { password } => {
                // Extract user (required) from startup params
                let user = match params.get("user") {
                    Some(user) => user,
                    None => return Err(PgSrvError::MissingStartupParameter("user")),
                };

                // Extract the database name (optional) from startup params
                // Defaults to the user
                let db_name = match params.get("database") {
                    Some(database) => database,
                    None => user,
                };

                let options = parse_options(params);

                let (org_id, db_name) =
                    get_org_and_db_name(hostname.as_ref(), db_name, options.as_ref())?;

                let details = self
                    .authenticator
                    .authenticate(user, &password, db_name, org_id)
                    .await?;
                Ok(details)
            }
            other => Err(PgSrvError::UnexpectedFrontendMessage(Box::new(other))),
        }
    }
}

/// Get an org identifier (either id or name) and the db_name.
///
/// 1. First try to get the org id from startup options parameter.
/// 2. If there are no options, try from the database name
///    if in the form of `<org_id>/<db_name>` or `<org_name>/<db_name>`.
/// 3. Lastly, fallback to the hostname. This will only be
///    possible if the connection is encrypted using SNI.
fn get_org_and_db_name<'a>(
    hostname: Option<&'a String>,
    db_name: &'a String,
    options: Option<&'a HashMap<String, String>>,
) -> Result<(&'a str, &'a str)> {
    fn get_org_id_from_options(options: Option<&HashMap<String, String>>) -> Option<&'_ String> {
        let options = options?;
        options.get("org")
    }

    fn get_org_id_from_hostname(hostname: Option<&String>) -> Option<&'_ str> {
        let hostname = hostname?;
        let parts = hostname.split_once('.')?;
        let org_id = parts.0;
        Some(org_id)
    }

    fn get_org_id_from_dbname(db_name: &str) -> Option<(&'_ str, &'_ str)> {
        db_name.split_once('/')
    }

    if let Some(org_id) = get_org_id_from_options(options) {
        return Ok((org_id, db_name));
    }

    if let Some(ids) = get_org_id_from_dbname(db_name) {
        return Ok(ids);
    }

    if let Some(org_id) = get_org_id_from_hostname(hostname) {
        return Ok((org_id, db_name));
    }

    Err(PgSrvError::MissingOrgId)
}

/// Parse the options provided in the startup parameters.
fn parse_options(params: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    let options = params.get("options")?;
    debug!(?options, "psql options via pgsrv");
    Some(
        options
            .split_whitespace()
            .map(|s| s.split('=').collect::<Vec<_>>())
            // If a key and value don't exist, then skip this key using filter_map
            .filter_map(|v| (v.len() == 2).then(|| (v[0], v[1])))
            .map(|(k, v)| (k.replace("--", ""), v.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_options_from_params() {
        let options_str = "--test-key=1 --another-key=2 --third-key =3";
        let mut params = HashMap::new();
        params.insert("options".to_string(), options_str.to_string());

        let options = parse_options(&params).unwrap();
        assert_eq!(Some(&String::from("1")), options.get("test-key"));
        assert_eq!(Some(&String::from("2")), options.get("another-key"));

        // This is None as it can't parse the key value correctly due to the extra whitespace
        assert_eq!(None, options.get("third-key"));
    }

    #[test]
    fn parse_options_with_whitespace() {
        let options_str = "--test-key 1";
        let mut params = HashMap::new();
        params.insert("options".to_string(), options_str.to_string());

        let options = parse_options(&params).unwrap();
        assert_eq!(None, options.get("test-key"));
    }

    #[test]
    fn proxy_key_db_id_provided() {
        let mut params = HashMap::new();
        let expected = Uuid::new_v4();
        params.insert(
            GLAREDB_DATABASE_ID_KEY.key.to_string(),
            expected.to_string(),
        );

        let got = GLAREDB_DATABASE_ID_KEY
            .value_from_params(&params, false)
            .unwrap();
        assert_eq!(expected, got)
    }

    #[test]
    fn proxy_key_db_id_missing_not_local() {
        let _ = GLAREDB_DATABASE_ID_KEY
            .value_from_params(&HashMap::new(), false)
            .unwrap_err();
    }

    #[test]
    fn proxy_key_db_id_missing_and_local() {
        let id = GLAREDB_DATABASE_ID_KEY
            .value_from_params(&HashMap::new(), true)
            .unwrap();
        assert_eq!(LOCAL_DATABASE_ID, id);
    }

    #[test]
    fn proxy_key_is_system() {
        struct TestCase {
            to_insert: Option<&'static str>,
            is_local: bool,
            expected_val: Option<bool>, // None indicates we expect an error to be returned.
        }

        let test_cases = vec![
            TestCase {
                to_insert: None,
                is_local: true,
                expected_val: Some(false),
            },
            TestCase {
                to_insert: None,
                is_local: false,
                expected_val: None,
            },
            TestCase {
                to_insert: Some("true"),
                is_local: false,
                expected_val: Some(true),
            },
            TestCase {
                to_insert: Some("not_bool"),
                is_local: false,
                expected_val: None,
            },
        ];

        for tc in test_cases {
            let mut params = HashMap::new();
            if let Some(val) = tc.to_insert {
                params.insert(GLAREDB_IS_SYSTEM_KEY.key.to_string(), val.to_string());
            }

            let result = GLAREDB_IS_SYSTEM_KEY.value_from_params(&params, tc.is_local);
            match (tc.expected_val, result) {
                (Some(expected), Ok(got)) => assert_eq!(expected, got),
                (None, Err(_)) => (), // We expected an error.
                (None, Ok(got)) => panic!("unexpectedly got value: {}", got),
                (Some(expected), Err(e)) => panic!(
                    "unexpectedly got error: {}, expected value: {}",
                    e, expected
                ),
            }
        }
    }

    #[test]
    fn test_get_org_dbname() {
        #[derive(Debug)]
        struct TestCase {
            hostname: Option<&'static str>,
            db_name: &'static str,
            options: Option<&'static [(&'static str, &'static str)]>,
            expected: Option<(&'static str, &'static str)>,
        }

        let test_cases = vec![
            // Org name in db name.
            TestCase {
                hostname: None,
                db_name: "my-org/db",
                options: None,
                expected: Some(("my-org", "db")),
            },
            // Org name in hostname.
            TestCase {
                hostname: Some("my-org.proxy.glaredb.com"),
                db_name: "db",
                options: None,
                expected: Some(("my-org", "db")),
            },
            // Org name in options.
            TestCase {
                hostname: Some("proxy.glaredb.com"),
                db_name: "db",
                options: Some(&[("org", "my-org")]),
                expected: Some(("my-org", "db")),
            },
            // Can't find org name.
            TestCase {
                hostname: None,
                db_name: "db",
                options: None,
                expected: None,
            },
            // Prefer org name in db name.
            TestCase {
                hostname: Some("a.proxy.glaredb.com"),
                db_name: "b/db",
                options: None,
                expected: Some(("b", "db")),
            },
            // "Omit" org name.
            //
            // If someone tries to hit the root proxy endpoint, has SNI enabled,
            // and doesn't specify an org name anywhere else, then we'll pull
            // out "proxy" as the org name. This test case serves to document
            // that this behavior is known.
            //
            // Why not error if we find "proxy" as an org name?
            //
            // I would prefer to keep glaredb pretty dumb in terms of what
            // hostname is used for proxying. For example, we may end up using
            // something like "us-central1.proxy.glaredb.com" for proxying in
            // different regions/clouds, and I would prefer not having to update
            // glaredb logic to account for that.
            //
            // So what happens if there's an org named "proxy"?
            //
            // Oh well. We'll be checking against that org. If the user isn't a
            // part of it, they'll just fail the credential check. Longer term
            // we might want to block more org names (e.g. "proxy") on Cloud
            // side.
            TestCase {
                hostname: Some("proxy.glaredb.com"),
                db_name: "db",
                options: None,
                expected: Some(("proxy", "db")),
            },
        ];

        for tc in test_cases {
            println!("test case: {tc:?}");
            let hostname = tc.hostname.map(|s| s.to_owned());
            let db_name = tc.db_name.to_owned();
            let options: Option<HashMap<_, _>> = tc.options.map(|vals| {
                vals.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            });
            let out = get_org_and_db_name(hostname.as_ref(), &db_name, options.as_ref());

            match (tc.expected, out) {
                (Some(a), Ok(b)) => assert_eq!(a, b),
                (Some(a), Err(e)) => panic!("expected some: {a:?}, got error: {e}"),
                (None, Ok(b)) => panic!("expected error, got {b:?}"),
                _ => (),
            }
        }
    }
}
