use async_trait::async_trait;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum CloudAuthError {
    #[error("Response from GlareDB Cloud: {0}")]
    CloudResponse(String),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ReqwestHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}

type Result<T, E = CloudAuthError> = std::result::Result<T, E>;

/// Connection details for a database. Returned by the connection authenticator.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
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
            return Err(CloudAuthError::CloudResponse(text));
        }

        let db_details: DatabaseDetails = res.json().await?;
        Ok(db_details)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_response() {
        let resp = r#"
            {
              "user_id": "b3e5b1ff-6c92-470b-b96d-103dd18a85db",
              "database_id": "6df36b37-21f1-45b1-aadb-4d65c1a50c32",
              "credential_type": "system",
              "ip": "1.2.3.4",
              "port": "5432",
              "memory_limit_bytes": 268435456,
              "gcs_storage_bucket": "",
              "storage_size_bytes": 0,
              "max_storage_bytes": 0
            }
            "#;

        let out: DatabaseDetails = serde_json::from_str(&resp).unwrap();
        let expected = DatabaseDetails {
            user_id: "b3e5b1ff-6c92-470b-b96d-103dd18a85db".to_string(),
            database_id: "6df36b37-21f1-45b1-aadb-4d65c1a50c32".to_string(),
            gcs_storage_bucket: String::new(),
            ip: "1.2.3.4".to_string(),
            port: "5432".to_string(),
            memory_limit_bytes: 268435456,
        };

        assert_eq!(expected, out)
    }
}
