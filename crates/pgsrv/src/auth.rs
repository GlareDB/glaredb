use crate::errors::{PgSrvError, Result};
use async_trait::async_trait;
use serde::Deserialize;
use uuid::Uuid;

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
    /// Max number of data sources allowed
    pub max_datasource_count: usize,
    /// Memory limit applied to session in bytes
    pub memory_limit_bytes: usize,
    /// Max number of tunnels allowed
    pub max_tunnel_count: usize,
}

#[async_trait]
pub trait ConnectionAuthenticator: Sync + Send {
    /// Authenticate a database connection.
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org: &str,
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
impl ConnectionAuthenticator for CloudAuthenticator {
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org: &str,
    ) -> Result<DatabaseDetails> {
        let query = if Uuid::try_parse(org).is_ok() {
            [
                ("user", user),
                ("password", password),
                ("name", db_name),
                ("org", org),
            ]
        } else {
            [
                ("user", user),
                ("password", password),
                ("name", db_name),
                ("orgname", org),
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

/// Always return a single set of database details. No intermediate
/// authentications steps are performed.
#[derive(Debug)]
pub struct StaticAuthenticator {
    details: DatabaseDetails,
}

impl StaticAuthenticator {
    pub fn new(details: DatabaseDetails) -> Self {
        StaticAuthenticator { details }
    }
}

#[async_trait]
impl ConnectionAuthenticator for StaticAuthenticator {
    async fn authenticate(
        &self,
        _user: &str,
        _password: &str,
        _db_name: &str,
        _org: &str,
    ) -> Result<DatabaseDetails> {
        Ok(self.details.clone())
    }
}
