use crate::errors::{PgSrvError, Result};
use async_trait::async_trait;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum PasswordMode {
    /// A cleartext password is required.
    ///
    /// Should error if no password is provided.
    RequireCleartext,

    /// No password is required.
    NoPassword {
        /// Drop any authentication messages as well.
        ///
        /// A compliant frontend should not send any additional authentication
        /// messages after receiving AuthenticationOk. However, node-postgres
        /// will attempt to send a password message regardless. Setting this to
        /// true will drop that message.
        drop_auth_messages: bool,
    },
}

/// Authenticate connection on the glaredb node itself.
pub trait LocalAuthenticator: Sync + Send {
    fn password_mode(&self) -> PasswordMode;
    fn authenticate(&self, user: &str, password: &str, db_name: &str) -> Result<()>;
}

/// A simple single user authenticator.
#[derive(Clone)]
pub struct SingleUserAuthenticator {
    pub user: String,
    pub password: String,
}

impl LocalAuthenticator for SingleUserAuthenticator {
    fn password_mode(&self) -> PasswordMode {
        PasswordMode::RequireCleartext
    }

    fn authenticate(&self, user: &str, password: &str, _db_name: &str) -> Result<()> {
        if user != self.user {
            return Err(PgSrvError::InvalidUserOrPassword);
        }
        // TODO: Constant time compare.
        if password != self.password {
            return Err(PgSrvError::InvalidUserOrPassword);
        }
        Ok(())
    }
}

/// Require no password provided.
#[derive(Debug, Clone, Copy, Default)]
pub struct PasswordlessAuthenticator {
    pub drop_auth_messages: bool,
}

impl LocalAuthenticator for PasswordlessAuthenticator {
    fn password_mode(&self) -> PasswordMode {
        PasswordMode::NoPassword {
            drop_auth_messages: self.drop_auth_messages,
        }
    }

    fn authenticate(&self, _user: &str, _password: &str, _db_name: &str) -> Result<()> {
        Ok(())
    }
}

/// Connection details for a database. Returned by the connection authenticator.
#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseDetails {
    /// IP to connect to.
    // TODO: Rename to host.
    pub ip: String,
    /// Port to connect to.
    pub port: String,
    /// ID of the database we're connecting to (UUID).
    pub database_id: String,
    /// ID of the user initiating the connection (UUID).
    pub user_id: String,
    /// Bucket for session storage.
    pub gcs_storage_bucket: String,
    /// Memory limit applied to session in bytes
    pub memory_limit_bytes: usize,
}

/// Authenticate connections that go through the proxy.
///
/// It's expected that authentication happens remotely, and a set of database
/// details get returned. These details are consulted when proxying the
/// connection.
#[async_trait]
pub trait ProxyAuthenticator: Sync + Send {
    /// Authenticate a database connection.
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org: &str,
        compute_engine: &str,
    ) -> Result<DatabaseDetails>;
}

/// Authentice connections using the Cloud service.
pub struct CloudAuthenticator {
    api_url: String,
    client: reqwest::Client,
}

impl CloudAuthenticator {
    pub fn new(api_url: String, auth_code: String) -> Result<Self> {
        use reqwest::header;

        let mut default_headers = header::HeaderMap::new();
        let basic_auth = format!("Basic {auth_code}");
        default_headers.insert(header::AUTHORIZATION, basic_auth.parse()?);

        let client = reqwest::Client::builder()
            .default_headers(default_headers)
            .build()?;

        Ok(CloudAuthenticator { api_url, client })
    }
}

#[async_trait]
impl ProxyAuthenticator for CloudAuthenticator {
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org: &str,
        compute_engine: &str,
    ) -> Result<DatabaseDetails> {
        let query = if Uuid::try_parse(org).is_ok() {
            [
                ("user", user),
                ("password", password),
                ("name", db_name),
                ("org", org),
                ("compute_engine", compute_engine),
            ]
        } else {
            [
                ("user", user),
                ("password", password),
                ("name", db_name),
                ("orgname", org),
                ("compute_engine", compute_engine),
            ]
        };

        let res = self
            .client
            .get(format!(
                "{}/api/internal/databases/authenticate",
                &self.api_url
            ))
            .query(&query)
            .send()
            .await?;

        // Currently only expect '200' from the cloud service. For
        // anything else, return an erorr.
        //
        // Does not try to deserialize the error responses to allow for
        // flexibility and changes on the cloud side during initial
        // development.
        if res.status().as_u16() != 200 {
            let text = res.text().await?;
            return Err(PgSrvError::CloudResponse(text));
        }

        let db_details: DatabaseDetails = res.json().await?;
        Ok(db_details)
    }
}
