use std::io::SeekFrom;
use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::{File, FileStat, FileSystem, OpenFlags};
use glaredb_error::{DbError, Result};

use crate::client::HttpClient;

#[derive(Debug)]
pub struct S3FileSystem<C: HttpClient> {
    client: C,
}

impl<C> S3FileSystem<C>
where
    C: HttpClient,
{
    pub fn new(client: C) -> Self {
        S3FileSystem { client }
    }
}

impl<C> FileSystem for S3FileSystem<C>
where
    C: HttpClient,
{
    type File = S3FileHandle<C>;

    async fn open(&self, flags: OpenFlags, path: &str) -> Result<Self::File> {
        unimplemented!()
    }

    async fn stat(&self, path: &str) -> Result<Option<FileStat>> {
        unimplemented!()
    }

    fn can_handle_path(&self, path: &str) -> bool {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct S3FileHandle<C: HttpClient> {
    client: C,
}

impl<C> File for S3FileHandle<C>
where
    C: HttpClient,
{
    fn path(&self) -> &str {
        unimplemented!()
    }

    fn size(&self) -> usize {
        unimplemented!()
    }

    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn poll_write(&mut self, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn poll_seek(&mut self, cx: &mut Context, seek: SeekFrom) -> Poll<Result<()>> {
        unimplemented!()
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        unimplemented!()
    }
}
