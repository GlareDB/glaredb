use crate::errors::{PgSrvError, Result};
use rustls::{Certificate, PrivateKey, ServerConfig};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

/// Configuration for creating encrypted connections using SSL/TLS.
#[derive(Debug)]
pub struct SslConfig {
    pub config: Arc<ServerConfig>,
}

impl SslConfig {
    /// Create a new ssl config using the provided cert and key files.
    pub async fn new<P: AsRef<Path>>(cert: P, key: P) -> Result<SslConfig> {
        let cert_bs = fs::read(cert).await?;
        let certs: Vec<_> = rustls_pemfile::certs(&mut cert_bs.as_slice())?
            .into_iter()
            .map(Certificate)
            .collect();

        let key_bs = fs::read(key).await?;
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_bs.as_slice())?;
        let key = match keys.len() {
            0 => return Err(PgSrvError::ReadCertsAndKeys("No keys found")),
            1 => PrivateKey(keys.pop().unwrap()),
            _ => return Err(PgSrvError::ReadCertsAndKeys("Expected exactly one key")),
        };

        let config = ServerConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        Ok(SslConfig {
            config: Arc::new(config),
        })
    }
}

/// A wrapper around a connection, optionally providing SSL encryption.
pub enum Connection<C> {
    Unencrypted(C),
    Encrypted(Box<TlsStream<C>>), // Boxed due to large size difference between variants (clippy)
}

impl<C> Connection<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn new_encrypted(conn: C, conf: Arc<ServerConfig>) -> Result<Self> {
        let stream = TlsAcceptor::from(conf).accept(conn).await?;
        Ok(Connection::Encrypted(Box::new(stream)))
    }

    pub fn new_unencrypted(conn: C) -> Self {
        Connection::Unencrypted(conn)
    }

    pub fn servername(&self) -> Option<String> {
        match self {
            Self::Unencrypted(_) => None,
            Self::Encrypted(stream) => stream.get_ref().1.server_name().map(|s| s.to_string()),
        }
    }
}

impl<C> AsyncRead for Connection<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Connection::Unencrypted(inner) => Pin::new(inner).poll_read(cx, buf),
            Connection::Encrypted(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<A> AsyncWrite for Connection<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Connection::Unencrypted(inner) => Pin::new(inner).poll_write(cx, buf),
            Connection::Encrypted(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Connection::Unencrypted(inner) => Pin::new(inner).poll_flush(cx),
            Connection::Encrypted(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Connection::Unencrypted(inner) => Pin::new(inner).poll_shutdown(cx),
            Connection::Encrypted(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const TEST_CERT: &str = r#"
-----BEGIN CERTIFICATE-----
MIIDDTCCAfWgAwIBAgIUBhtwC7ezA9RQTLP8EsHdcGB8X+cwDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLZ2xhcmVkYi5jb20wHhcNMjMwNTI1MDA0MDExWhcNMjQw
NTI0MDA0MDExWjAWMRQwEgYDVQQDDAtnbGFyZWRiLmNvbTCCASIwDQYJKoZIhvcN
AQEBBQADggEPADCCAQoCggEBALnqZnssy27imdBiD9IhVYengY0g5LEjR0RKDCbI
iyxicWZNHrVurjXlm0XCEi1TX4BJR0l6UIN0Hbw9HnFC69zZw7dxRCe+1XIvj49B
ZxhfR/u7gatAulKZo20I6Zs/TPAeu8wO8tSkKwFgvydCI8yw1WP1Sf/1zRYVH5R6
s9SOuJwLDhnBLIC6i0etnh+OCf16I5wFHkgGvBAx+Ec23vCN5R5ZOaaHXMkogx19
uPLm5nSt1Mj60HV7hTh30QTDR5urgcQ+OHbt+sYJnw+VyOJfP6XtlXOSAQhw/VL+
klokrmYS4DhlIHrSnJTfpPymPhqsSVHd97Jr20nbuBbXlRsCAwEAAaNTMFEwHQYD
VR0OBBYEFHMbgh0q1cxHVpR0UCWq92+0zQrsMB8GA1UdIwQYMBaAFHMbgh0q1cxH
VpR0UCWq92+0zQrsMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB
AEJ554uXpkgXLGECVueikjzGOyCb7aBmKYdssuutWpEGM3pZpyCKzz/tsanULH3Q
kNu5/eJYc0BBI2YXz1eIFrJR3fM+qbpIvP1j67CLhsjtvKSQTFEvqseCCZ1l34WK
dNzpwf0HMB5MAt6T8xeQ5gDRNo7C3HuKT/8WouaIdn9H2UEluQBDM5Yg+cpDWOlv
Dr/IzcLC/pyEmx7spkPfvxebtY64kdFh8qyl9iWZLO9wrpaWY1vV41sjZ1F41AgY
KaRWGJhHwhyDcYzelsn0Ew2LdB7WfFQZfZe9EHJTj7bd6y6AbkduZ31oebXYlOzo
1DkrU2K5aMlVF/vExWpfChk=
-----END CERTIFICATE-----
"#;

    const TEST_KEY: &str = r#"
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC56mZ7LMtu4pnQ
Yg/SIVWHp4GNIOSxI0dESgwmyIssYnFmTR61bq415ZtFwhItU1+ASUdJelCDdB28
PR5xQuvc2cO3cUQnvtVyL4+PQWcYX0f7u4GrQLpSmaNtCOmbP0zwHrvMDvLUpCsB
YL8nQiPMsNVj9Un/9c0WFR+UerPUjricCw4ZwSyAuotHrZ4fjgn9eiOcBR5IBrwQ
MfhHNt7wjeUeWTmmh1zJKIMdfbjy5uZ0rdTI+tB1e4U4d9EEw0ebq4HEPjh27frG
CZ8PlcjiXz+l7ZVzkgEIcP1S/pJaJK5mEuA4ZSB60pyU36T8pj4arElR3feya9tJ
27gW15UbAgMBAAECggEAPzt8E5TOtC4aBofzvZJc1sCgDXIMljrbeFx++Ynn2/a3
fwXn1emJEwe+4eD58bSTnLxPpKwXSj5qBqX6/qa2Ne1S5cd+WM5NJoMKnryt5doy
T2oc1jQzqvhaXzFS0tyavGiXkvXanXwgrF1NZnrWVj4mtdKtkoL1d/dDQGrjUv2L
A+9Y8luWiVxiwd5koTQh7Qv77bZu1L7hpon3N6a8/wTzdrryltSVIGxCCc5JbOAl
RwqXEoY7oEa7jInfuE8+vOnRPxUh9xi2bnmlxMaS4jWeXOhVJ5LetElCHpXwpm9O
gGnUfRwkc0h8Q+nKT+7As3dKmNPUPulkdxi0holMgQKBgQDqCYey/39W4V24fhu1
HTRJP3Px1wEWaDic1fiUzTt+L7UK/OL3RGI6O1hPVLQQz2ZblKcaOnA6zAxUSlZ5
fBV9/lMWRwXsIudhsZ7y0O5h4qpKbHt+uoxjWrHOvWKcCobLydewA/SediNGewOB
z9SSgUgslfNtHE/EMDO3tJvwOwKBgQDLXM4iyJ2oc+STCvmjAF3OPC0YI3ypo1Qf
wyzlTpecr0aS7XQJ/qYWtoAIKeahdsvKTfBgr4Ldb+P46jWBrn/QC11yzmQugrdD
5MDOq/yl0AnZJip3v7S2bVEJVDaPAshkTUG5cH+mbqhQDirmFAWwoIjozDJsyS0S
ekcA93iAoQKBgQCz8VHhoapxzWN215dMIMEz1FK8Thhq9wUYKTgwiL/GXL3xTdDg
VzbDGR/kFvc/uYc0wM3eT7I8suiD4ogsbehEcEfH6CG4lnma1qukfFnc7x8Ji/FD
1gDc3z24/EoWOc28YFuy3Me3EpQ6u5hNtdL2NeqdRUndyZfW+0y4YWhIQwKBgAoF
B48FLWdoVmG8T7JtSTHpGxuuW+0LxBBQkHy305Z/uE1y752yc0J5OXOthNR9wuqz
zl0lKKB6a9QOzhYWn+uDM9Z9Pshf3mG0+p70MF2HE4UkcUE3K9M1LaH1CT8glvkC
KXPWjCOKHjbbi9eMMv+R2U0HCpD7lIHtjmQIzWIhAoGAQTt6RCCn6W0ymimonSNR
6tODrgdT6qrzzWEkzvib4AtpCFrBmrceUXR3Oh8VN0yj/I158dLOpd0Gut3baCbV
9XBK3NBfS7FfBW1EjZB5Do6u1I1jenb2ShF88AeyWZVcFh5myyMYc23R99ZmHP6N
yISdkDEeNZpoI5xRIgPnTBQ=
-----END PRIVATE KEY-----
"#;

    fn create_file(contents: &str) -> NamedTempFile {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(contents.as_bytes()).unwrap();
        temp
    }

    #[tokio::test]
    async fn create_with_invalid_cert() {
        let cert = create_file("invalid");
        let key = create_file("invalid");

        let _ = SslConfig::new(cert.path(), key.path()).await.unwrap_err();
    }

    #[tokio::test]
    async fn create_with_valid_cert() {
        let cert = create_file(TEST_CERT);
        let key = create_file(TEST_KEY);

        let _ = SslConfig::new(cert.path(), key.path()).await.unwrap();
    }
}
