use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use glaredb_error::{Result, not_implemented};
use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Request;
use reqwest::header::{AUTHORIZATION, HOST, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::util::hex;

const SIGN_ALG: &str = "AWS4-HMAC-SHA256";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub key_id: String,
    pub secret: String,
}

impl fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("key_id", &self.key_id)
            .field("secret_key", &"<secret>")
            .finish()
    }
}

#[derive(Debug)]
pub struct AwsRequestAuthorizer<'a> {
    pub date: DateTime<Utc>,
    pub credentials: &'a AwsCredentials,
    pub region: &'a str,
}

impl AwsRequestAuthorizer<'_> {
    /// Authorizes a request for s3 with a set of credentials.
    ///
    /// See <https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html>
    pub fn authorize(&self, mut request: Request) -> Result<Request> {
        let payload_hash = {
            let mut hasher = Sha256::new();
            match request.body() {
                Some(body) => match body.as_bytes() {
                    Some(bs) => hasher.update(bs),
                    None => not_implemented!("streaming body"),
                },
                None => hasher.update("".as_bytes()),
            }

            let result = hasher.finalize();
            hex::encode(result)
        };

        // Manually add host header here to ensure it's included as a signed header.
        let host_val =
            HeaderValue::from_str(request.url().host_str().expect("host to exist")).unwrap();
        request.headers_mut().insert(HOST, host_val);

        request.headers_mut().insert(
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_str(&payload_hash).unwrap(),
        );
        request.headers_mut().insert(
            HeaderName::from_static("x-amz-date"),
            HeaderValue::from_str(&self.date.format("%Y%m%dT%H%M%SZ").to_string()).unwrap(),
        );

        let (canonical_headers, signed_headers) = canonical_headers(request.headers());

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method(),
            request.url().path(),
            canonical_query_string(request.url()),
            canonical_headers,
            signed_headers,
            payload_hash,
        );

        let request_hash = {
            let mut hasher = Sha256::new();
            hasher.update(canonical_request.as_bytes());
            hex::encode(hasher.finalize())
        };

        let scope = self.credential_scope();
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            SIGN_ALG,
            self.date.format("%Y%m%dT%H%M%SZ"),
            scope,
            request_hash,
        );

        let sig = self.compute_signature(&string_to_sign);

        let auth_val = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            SIGN_ALG, self.credentials.key_id, scope, signed_headers, sig,
        );

        request.headers_mut().insert(
            AUTHORIZATION,
            auth_val.try_into().expect("valid header value"),
        );

        Ok(request)
    }

    fn compute_signature(&self, string_to_sign: &str) -> String {
        // DateKey = HMAC-SHA256("AWS4"+"<SecretAccessKey>", "<YYYYMMDD>")
        // DateRegionKey = HMAC-SHA256(<DateKey>, "<aws-region>")
        // DateRegionServiceKey = HMAC-SHA256(<DateRegionKey>, "<aws-service>")
        // SigningKey = HMAC-SHA256(<DateRegionServiceKey>, "aws4_request")

        // Note that we have to get the output of each hmac call and feed it
        // into the next to get the correct signature. It's not sufficient to
        // create a single hmac state and continually call `chain_update`.

        let date_key =
            HmacSha256::new_from_slice(format!("AWS4{}", self.credentials.secret).as_bytes())
                .unwrap()
                .chain_update(self.date.format("%Y%m%d").to_string())
                .finalize();

        let region_key = HmacSha256::new_from_slice(&date_key.into_bytes())
            .unwrap()
            .chain_update(self.region)
            .finalize();

        let service_key = HmacSha256::new_from_slice(&region_key.into_bytes())
            .unwrap()
            .chain_update("s3")
            .finalize();

        let signing_key = HmacSha256::new_from_slice(&service_key.into_bytes())
            .unwrap()
            .chain_update("aws4_request")
            .finalize();

        let sig = HmacSha256::new_from_slice(&signing_key.into_bytes())
            .unwrap()
            .chain_update(string_to_sign)
            .finalize();

        hex::encode(sig.into_bytes())
    }

    fn credential_scope(&self) -> String {
        format!(
            "{}/{}/s3/aws4_request",
            self.date.format("%Y%m%d"),
            self.region,
        )
    }
}

/// URI encodding set following aws rules.
///
/// See <https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html>
const S3_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn canonical_query_string(url: &Url) -> String {
    // TODO: Sort should happen on encoded keys.
    let params: BTreeMap<_, _> = url.query_pairs().collect();
    let mut buf = String::with_capacity(url.query().map(|q| q.len()).unwrap_or(0));

    for (idx, (key, val)) in params.into_iter().enumerate() {
        if idx > 0 {
            buf.push('&');
        }

        let key = utf8_percent_encode(key.as_ref(), S3_ENCODE_SET);
        let val = utf8_percent_encode(val.as_ref(), S3_ENCODE_SET);

        write!(buf, "{key}={val}").expect("writing to string not to fail");
    }

    buf
}

fn canonical_headers(header_map: &HeaderMap) -> (String, String) {
    let mut headers: BTreeMap<_, Vec<_>> = BTreeMap::new();

    for (key, val) in header_map {
        let key = key.as_str();
        let val = val.to_str().expect("value to be utf8");

        headers.entry(key).or_default().push(val);
    }

    let mut header_buf = String::new();
    let mut signed_buf = String::new();

    for (idx, (key, val)) in headers.into_iter().enumerate() {
        if idx > 0 {
            signed_buf.push(';');
        }

        header_buf.push_str(key);
        header_buf.push(':');
        signed_buf.push_str(key);

        for (val_idx, val) in val.into_iter().enumerate() {
            if val_idx > 0 {
                header_buf.push(',');
            }
            header_buf.push_str(val.trim());
        }
        header_buf.push('\n');
    }

    (header_buf, signed_buf)
}
