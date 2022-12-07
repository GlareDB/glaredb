use crate::errors::{PgSrvError, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::fmt;

/// Connection details for a database. Returned by the connection authenticator.
#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseDetails {
    pub ip: String,
    pub port: String,
}

#[async_trait]
pub trait ConnectionAuthenticator: Sync + Send + fmt::Debug {
    /// Authenticate a database connection.
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org_id: &str,
    ) -> Result<DatabaseDetails>;
}

/// Authentice connections using the Cloud service.
#[derive(Debug)]
pub struct CloudAuthenticator {
    api_url: String, // TODO: Use CloudClient.
}

impl CloudAuthenticator {
    pub fn new(api_url: String) -> Self {
        CloudAuthenticator { api_url }
    }
}

#[async_trait]
impl ConnectionAuthenticator for CloudAuthenticator {
    async fn authenticate(
        &self,
        user: &str,
        password: &str,
        db_name: &str,
        org_id: &str,
    ) -> Result<DatabaseDetails> {
        let client = reqwest::Client::builder().build()?;

        let query = &[
            ("user", user),
            ("password", password),
            ("name", db_name),
            ("org", org_id),
        ];

        let res = client
            .get(format!(
                "{}/api/internal/databases/authenticate",
                &self.api_url
            ))
            .header("Authorization", "Basic 6tCvEVBkD91q4KhjGVtT")
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
        _org_id: &str,
    ) -> Result<DatabaseDetails> {
        Ok(self.details.clone())
    }
}
