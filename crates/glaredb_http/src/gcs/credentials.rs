use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, Utc};
use glaredb_error::{DbError, Result, ResultExt};
use reqwest::{Method, Request};
use ring::signature::RsaKeyPair;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::client::{HttpClient, HttpResponse, read_json_response, set_form_body};

// TODO: Different scopes depending the open flags used for the file.
const READ_ONLY_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_only";

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct ServiceAccount {
    project_id: String,
    private_key_id: String,
    private_key: String,
    client_email: String,
    auth_uri: String,
    token_uri: String,
    auth_provider_x509_cert_url: String,
    client_x509_cert_url: String,
    universe_domain: String,
}

#[derive(Serialize)]
struct JwtHeader {
    alg: &'static str,
    typ: &'static str,
}

#[derive(Serialize)]
struct JwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Deserialize)]
pub struct AccessToken {
    pub access_token: String,
    pub expires_in: u64,
}

impl ServiceAccount {
    pub fn try_from_str(input: &str) -> Result<Self> {
        serde_json::from_str(input).context("Failed to deserialize json service account key")
    }

    /// Fetch an access token using this service account.
    pub async fn fetch_access_token<C>(&self, client: &C) -> Result<AccessToken>
    where
        C: HttpClient,
    {
        // TODO: Check if this also works in wasm. Might also want to check the
        // s3 creds too.
        let now = Utc::now();
        let iat = now.timestamp() as u64;
        let exp = (now + Duration::hours(1)).timestamp() as u64;

        let claims = JwtClaims {
            iss: &self.client_email,
            scope: READ_ONLY_SCOPE,
            aud: &self.token_uri,
            iat,
            exp,
        };
        let header = JwtHeader {
            alg: "RS256",
            typ: "JWT",
        };

        let header_b64 = BASE64_URL_SAFE_NO_PAD
            .encode(serde_json::to_string(&header).context("Failed to encode jwt header")?);
        let claims_b64 = BASE64_URL_SAFE_NO_PAD
            .encode(serde_json::to_string(&claims).context("Failed to encode jwt claims")?);
        let signing_input = format!("{}.{}", header_b64, claims_b64);

        let mut reader = std::io::Cursor::new(self.private_key.as_bytes());
        let key = rustls_pemfile::read_one(&mut reader).context("invalid PEM private key")?;
        let key_pair = match key {
            Some(rustls_pemfile::Item::Pkcs8Key(der)) => {
                RsaKeyPair::from_pkcs8(der.secret_pkcs8_der())
                    .map_err(|_| DbError::new("Failed to create rsa key pair from pkcs8 key"))?
            }
            Some(rustls_pemfile::Item::Pkcs1Key(der)) => {
                RsaKeyPair::from_der(der.secret_pkcs1_der())
                    .map_err(|_| DbError::new("Failed to create rsa key pair from pkcs1 key"))?
            }
            _ => return Err(DbError::new("Missing key")),
        };

        // Sign with PKCS#1 v1.5 SHA-256 (RS256)
        let mut signature = vec![0; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                signing_input.as_bytes(),
                &mut signature,
            )
            .map_err(|_| DbError::new("Failed to sign payload"))?;

        let sig_b64 = BASE64_URL_SAFE_NO_PAD.encode(&signature);
        let jwt = format!("{}.{}", signing_input, sig_b64);

        // Exchange the JWT for an access token
        let params = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ];
        let url = Url::parse(&self.token_uri).context("Failed to parse token uri as url")?;
        let mut request = Request::new(Method::POST, url);
        set_form_body(&mut request, &params)?;

        let resp_stream = client.do_request(request).await?.into_bytes_stream();
        let tok_resp: AccessToken = read_json_response(resp_stream).await?;

        Ok(tok_resp)
    }
}
