use crate::codec::{Decodeable, Encodeable};
use crate::errors::{LlamaError, Result};
use crate::page::{BasePage, DiskPtr, MemPtr, PageHeader, PageKind};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

// Note that this type definition requires that we synchronize reads/writes.
// Eventually it will make sense to use pwrite/pread (on unix) to allow for
// concurrent access.
#[async_trait]
pub trait File: AsyncRead + AsyncWrite + AsyncSeek + Unpin {}

#[async_trait]
pub trait FileSystem {
    type File: File;

    /// Opens a file at the given path. Creates the file if it doesn't exist.
    async fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<Self::File>;
}

pub enum FsRequest {
    ReadPage {
        disk: DiskPtr,
        buf: BytesMut,
        response: oneshot::Sender<(Result<MemPtr>, BytesMut)>,
    },
}

pub struct FileSystemClient {
    requests: mpsc::Sender<FsRequest>,
}

impl FileSystemClient {
    pub async fn read_file(&self, disk: DiskPtr, buf: BytesMut) -> Result<(MemPtr, BytesMut)> {
        let (tx, rx) = oneshot::channel();
        self.requests
            .send(FsRequest::ReadPage {
                disk,
                buf,
                response: tx,
            })
            .await
            .map_err(|_| LlamaError::BrokenChannel)?;
        let (result, buf) = rx.await?;
        result.map(|ptr| (ptr, buf))
    }
}

/// Serialize all file system access.
// TODO: Remove.
pub struct FileSystemWorker<F: FileSystem> {
    fs: F,
    requests: mpsc::Receiver<FsRequest>,
    open_files: HashMap<String, F::File>,
}

impl<F: FileSystem> FileSystemWorker<F> {
    pub async fn start(mut self) -> Result<()> {
        trace!("starting filesystem worker");
        while let Some(msg) = self.requests.recv().await {
            match msg {
                FsRequest::ReadPage {
                    disk,
                    mut buf,
                    response,
                } => {
                    let result = self.read_page(disk, &mut buf).await;
                    if let Err(_) = response.send((result, buf)) {
                        warn!("failed to send response for read page")
                    }
                }
            }
        }
        trace!("exiting filesystem worker");
        Ok(())
    }

    async fn read_page(&mut self, disk: DiskPtr, mut buf: &mut BytesMut) -> Result<MemPtr> {
        let file = self.get_or_open_file(disk.partition_num).await?;

        buf.reserve(PageHeader::min_decode_size());
        file.seek(SeekFrom::Start(disk.offset as u64)).await?;
        file.read_exact(&mut buf[0..PageHeader::min_decode_size()])
            .await?;

        let header = PageHeader::decode(&mut buf);
        match header.kind {
            PageKind::Base => {
                buf.clear();
                buf.reserve(header.size as usize);
                file.read_exact(&mut buf[0..header.size as usize]).await?;
                let data = buf.split_off(header.size as usize);
                let base = BasePage {
                    data: data.freeze(),
                };
                Ok(MemPtr::Base(base))
            }
            PageKind::Delta => unimplemented!(),
            PageKind::Unknown => Err(LlamaError::UnknownPageKind(disk)),
        }
    }

    async fn get_or_open_file(&mut self, pnum: usize) -> Result<&mut F::File> {
        use std::collections::hash_map::Entry;

        let fname = format!("log-{}", pnum);
        let file = match self.open_files.entry(fname.clone()) {
            Entry::Occupied(ent) => ent.into_mut(),
            Entry::Vacant(ent) => {
                let file = self.fs.open_file(&fname).await?;
                ent.insert(file)
            }
        };

        Ok(file)
    }
}
