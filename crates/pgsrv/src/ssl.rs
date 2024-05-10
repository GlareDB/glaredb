use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{server, sign, ServerConfig};
use tokio::fs;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tracing::debug;

use crate::errors::{PgSrvError, Result};

/// Configuration for creating encrypted connections using SSL/TLS.
#[derive(Debug)]
pub struct SslConfig {
    pub config: Arc<ServerConfig>,
}

impl SslConfig {
    /// Create a new ssl config using the provided cert and key files.
    pub async fn new<P: AsRef<Path>>(cert: P, key: P) -> Result<SslConfig> {
        let cert_bs = fs::read(cert).await?;
        let mut chain = Vec::new();
        for cert in rustls_pemfile::certs(&mut cert_bs.as_slice()) {
            chain.push(CertificateDer::from(cert?.to_vec()))
        }

        let key_bs = fs::read(key).await?;
        let mut keys = Vec::new();
        for key in rustls_pemfile::pkcs8_private_keys(&mut key_bs.as_slice()) {
            keys.push(key?.secret_pkcs8_der().to_vec());
        }
        let key = match keys.len() {
            0 => return Err(PgSrvError::ReadCertsAndKeys("No keys found")),
            1 => PrivateKeyDer::try_from(keys.pop().unwrap())
                .map_err(|e| PgSrvError::InternalError(e.to_owned()))?,
            _ => return Err(PgSrvError::ReadCertsAndKeys("Expected exactly one key")),
        };

        let resolver = CertResolver::new(chain, &key)?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(resolver));

        Ok(SslConfig {
            config: Arc::new(config),
        })
    }
}

#[derive(Debug)]
struct CertResolver {
    cert: Arc<sign::CertifiedKey>,
}


impl CertResolver {
    fn new(chain: Vec<CertificateDer<'static>>, key: &PrivateKeyDer) -> Result<CertResolver> {
        let key = rustls::crypto::aws_lc_rs::sign::any_supported_type(key)?;
        Ok(CertResolver {
            cert: Arc::new(sign::CertifiedKey::new(chain, key.to_owned())),
        })
    }
}

impl server::ResolvesServerCert for CertResolver {
    fn resolve(&self, client_hello: server::ClientHello) -> Option<Arc<sign::CertifiedKey>> {
        let server_name = client_hello.server_name();
        debug!(?server_name, "sni server name");

        // TODO: Per host certs.

        Some(self.cert.clone())
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
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    const TEST_CERT: &str = r#"
-----BEGIN CERTIFICATE-----
MIIBkzCCAUUCFGipMcv8Oq6O89V+OkbybaF4q3XaMAUGAytlcDBsMQswCQYDVQQG
EwJVUzEOMAwGA1UECAwFVGV4YXMxDzANBgNVBAcMBkF1c3RpbjEQMA4GA1UECgwH
R2xhcmVEQjEUMBIGA1UECwwLRW5naW5lZXJpbmcxFDASBgNVBAMMC2dsYXJlZGIu
Y29tMB4XDTIzMTIxNTIxMTgwOFoXDTMzMTIxMjIxMTgwOFowbDELMAkGA1UEBhMC
VVMxDjAMBgNVBAgMBVRleGFzMQ8wDQYDVQQHDAZBdXN0aW4xEDAOBgNVBAoMB0ds
YXJlREIxFDASBgNVBAsMC0VuZ2luZWVyaW5nMRQwEgYDVQQDDAtnbGFyZWRiLmNv
bTAqMAUGAytlcAMhAIA9Nax1Q31xpAIyNT/qP3b65ER9EUcSxKJ7CBl0JTLRMAUG
AytlcANBACIk9nRqbndso7/NfIDHtoFI+yu5ezPvNjkqtl44o5akdz0YPg7k6D1F
zaVROyewlGxWQUwoIRcqgyw+cTF4LAY=
-----END CERTIFICATE-----
"#;

    const TEST_KEY: &str = r#"
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIDGe13glRciPej49XvEZqqq4oZ5yUuL9HD2Pw1rSue2j
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
