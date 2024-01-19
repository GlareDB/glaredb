use chrono::{DateTime, Duration, Utc};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};

use crate::errors::{Result, SnowflakeError};
use crate::req::{EmptySerde, ExecMethod, RequestId, SnowflakeClient};

const SESSION_ENDPOINT: &str = "/session";
const AUTH_ENDPOINT: &str = "/session/v1/login-request";

#[derive(Debug)]
pub struct Token {
    value: String,
    validity: Duration,
    created_at: DateTime<Utc>,
}

impl Token {
    pub fn new(value: String, validity_in_seconds: i64, created_at: DateTime<Utc>) -> Self {
        Self {
            value,
            validity: Duration::seconds(validity_in_seconds),
            created_at,
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn is_valid(&self) -> bool {
        Utc::now().signed_duration_since(self.created_at) < self.validity
    }
}

#[derive(Debug)]
pub struct Session {
    pub token: Token,
    pub master_token: Token,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionParams {
    delete: bool,
}

impl Session {
    pub async fn close(&self, client: &SnowflakeClient) -> Result<()> {
        let _: EmptySerde = client
            .execute(
                ExecMethod::Post,
                SESSION_ENDPOINT,
                Some(&SessionParams { delete: true }),
                EmptySerde::new(),
                Some(&self.token),
            )
            .await?;
        Ok(())
    }
}

impl From<TokenResponse> for Session {
    fn from(value: TokenResponse) -> Self {
        let created_at = Utc::now();
        Self {
            token: Token::new(
                value.token.expect("token should exist"),
                value
                    .validity_in_seconds
                    .expect("token validity should exist"),
                created_at,
            ),
            master_token: Token::new(
                value.master_token.expect("master token should exist"),
                value
                    .master_validity_in_seconds
                    .expect("master token validity should exist"),
                created_at,
            ),
        }
    }
}

#[derive(Debug)]
pub enum Authenticator {
    Default(DefaultAuthenticator),
}

#[derive(Debug, Serialize)]
struct AuthRequest {
    data: AuthBodyData,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthParams {
    request_id: RequestId,

    #[serde(skip_serializing_if = "Option::is_none")]
    database_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    schema_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    warehouse: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    role_name: Option<String>,
}

impl From<AuthOptions> for AuthParams {
    fn from(value: AuthOptions) -> Self {
        Self {
            request_id: RequestId::new(),
            database_name: value.database_name,
            schema_name: value.schema_name,
            warehouse: value.warehouse,
            role_name: value.role_name,
        }
    }
}

#[derive(Debug, Default)]
pub struct AuthOptions {
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub warehouse: Option<String>,
    pub role_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthResponse {
    data: TokenResponse,
    message: Option<String>,
    code: Option<String>,
    success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenResponse {
    master_token: Option<String>,
    token: Option<String>,
    validity_in_seconds: Option<i64>,
    master_validity_in_seconds: Option<i64>,
}

impl Authenticator {
    pub(crate) async fn authenticate(
        self,
        client: &SnowflakeClient,
        opts: AuthOptions,
    ) -> Result<Session> {
        let res: AuthResponse = match self {
            Self::Default(req) => Self::execute_req(client, req, opts).await?,
        };

        if !res.success {
            return Err(SnowflakeError::AuthError {
                code: res.code.unwrap_or_default(),
                message: res.message.unwrap_or_default(),
            });
        }

        Ok(res.data.into())
    }

    async fn execute_req<R: Into<AuthBodyData>>(
        client: &SnowflakeClient,
        req: R,
        opts: AuthOptions,
    ) -> Result<AuthResponse> {
        let params: AuthParams = opts.into();
        client
            .execute(
                ExecMethod::Post,
                AUTH_ENDPOINT,
                Some(&params),
                &AuthRequest { data: req.into() },
                /* Token = */ None,
            )
            .await
    }
}

#[derive(Debug)]
pub struct DefaultAuthenticator {
    pub account_name: String,
    pub login_name: String,
    pub password: String,
}

const CLIENT_APP_ID: &str = "Go";
const CLIENT_APP_VERSION: &str = "1.6.18";
const CLIENT_APP_OS: &str = "darwin";
const CLIENT_APP_OS_VERSION: &str = "gc-arm64";

#[derive(Debug, Default)]
struct ClientAppId;

impl Serialize for ClientAppId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(CLIENT_APP_ID)
    }
}

#[derive(Debug, Default)]
struct ClientAppVersion;

impl Serialize for ClientAppVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(CLIENT_APP_VERSION)
    }
}

#[derive(Debug, Default)]
struct ClientEnvironment;

impl Serialize for ClientEnvironment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(/* len = */ None)?;
        s.serialize_entry("APPLICATION", CLIENT_APP_ID)?;
        s.serialize_entry("OS", CLIENT_APP_OS)?;
        s.serialize_entry("OS_VERSION", CLIENT_APP_OS_VERSION)?;
        s.end()
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct AuthBodyData {
    // Client information required so we can identify as a supported client for
    // snowflake servers. This helps us fetch results in "arrow" format which
    // they only support for their own clients. Their exposed (documented) rest
    // api doesn't support returning results in arrow format :/
    client_app_id: ClientAppId,
    client_app_version: ClientAppVersion,
    client_environment: ClientEnvironment,

    account_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    login_name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
}

impl From<DefaultAuthenticator> for AuthBodyData {
    fn from(value: DefaultAuthenticator) -> Self {
        Self {
            account_name: value.account_name,
            login_name: Some(value.login_name),
            password: Some(value.password),

            ..Default::default()
        }
    }
}
