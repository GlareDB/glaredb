use crate::errors::Result;
use rustls::{Certificate, PrivateKey, ServerConfig};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

/// Configuration for creating encrypted connections using SSL/TLS.
pub struct SslConfig {
    pub config: Arc<ServerConfig>,
}

impl SslConfig {
    /// Create a new ssl config using the provided cert and key files.
    pub async fn new<P: AsRef<Path>>(cert: P, key: P) -> Result<SslConfig> {
        let cert = Certificate(fs::read(cert).await?);
        let key = PrivateKey(fs::read(key).await?);

        let config = ServerConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;

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
