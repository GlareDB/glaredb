use crate::errors::{LlamaError, Result};
use crate::fs::{File, FileSystem, FileSystemClient};
use crate::page::{PageAddr, PageId, PagePtr};
use crate::table::MappingTable;
use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::trace;

pub struct PagePool {
    page_map: MappingTable<PageAddr>,
    fs_client: FileSystemClient,
}

impl PagePool {
    /// Updates the page with a delta.
    pub async fn update_delta(
        &self,
        pid: &PageId,
        ptr: PagePtr,
        delta: Vec<u8>,
    ) -> Result<PagePtr> {
        unimplemented!()
    }

    pub async fn update_replace(&self) -> Result<PagePtr> {
        unimplemented!()
    }

    pub async fn read_page(&self, pid: &PageId) -> Result<PagePtr> {
        trace!(?pid, "reading page");
        let atomic = self.page_map.get(pid.pid);
        let orig = atomic.load(Ordering::Acquire);
        let addr = unsafe {
            orig.as_ref()
                .ok_or(LlamaError::MissingPagePtr(pid.clone()))?
        };

        match addr {
            PageAddr::Mem(_) => Ok(addr as PagePtr),
            PageAddr::Disk(disk) => {
                let buf = BytesMut::new();
                let (mem_ptr, _) = self.fs_client.read_file(disk.clone(), buf).await?;
                let addr = PageAddr::from(mem_ptr);

                let mut ptr = Box::into_raw(Box::new(addr));
                match atomic.compare_exchange(orig, ptr, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(orig) => unsafe {
                        Box::from_raw(orig);
                    },
                    Err(curr) => {
                        unsafe { Box::from_raw(ptr) };
                        ptr = curr;
                    }
                }

                Ok(ptr)
            }
        }
    }
}
