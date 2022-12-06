use crate::errors::Result;
use openssl::ssl::{Ssl, SslAcceptor, SslContext, SslFiletype, SslMethod};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio_openssl::SslStream;

pub struct SslConfig {
    pub context: SslContext,
}

impl SslConfig {
    /// Create a new ssl config using the provided cert and key files.
    pub fn new<P: AsRef<Path>>(cert: P, key: P) -> Result<SslConfig> {
        // See <https://wiki.mozilla.org/Security/Server_Side_TLS>
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        builder.set_certificate_chain_file(cert)?;
        builder.set_private_key_file(key, SslFiletype::PEM)?;
        builder.check_private_key()?;

        let context = builder.build().into_context();
        Ok(SslConfig { context })
    }
}

/// A wrapper around a connection, optionally providing SSL encryption.
pub enum Connection<C> {
    Unencrypted(C),
    Encrypted(SslStream<C>),
}

impl<C> Connection<C>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn new_encrypted(conn: C, conf: &SslConfig) -> Result<Self> {
        let mut stream = SslStream::new(Ssl::new(&conf.context)?, conn)?;
        Pin::new(&mut stream).accept().await?;
        Ok(Connection::Encrypted(stream))
    }

    pub fn new_unencrypted(conn: C) -> Self {
        Connection::Unencrypted(conn)
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
