use crate::errors::{LlamaError, Result};
use crate::fs::FileSystem;
use crate::page::{PageAddr, PageId, PagePtr};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PagePool<F> {
    page_map: Arc<Mutex<HashMap<PageId, PageAddr>>>,
    f: F,
}

impl<F: FileSystem> PagePool<F> {
    /// Updates the page with a delta.
    pub async fn update_delta(&self) -> Result<PagePtr> {
        unimplemented!()
    }

    pub async fn update_replace(&self) -> Result<PagePtr> {
        unimplemented!()
    }

    pub async fn read_page(&self, pid: &PageId) -> Result<PagePtr> {
        unimplemented!()
    }
}
