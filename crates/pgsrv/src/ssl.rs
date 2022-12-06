use crate::errors::{PgSrvError, Result};
use openssl::ssl::{SslContext, SslStream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

/// A wrapper around a connection, optionally providing SSL encryption.
pub enum Connection<C> {
    Unencrypted(C),
    Encrypted(C),
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
