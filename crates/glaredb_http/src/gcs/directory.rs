use std::task::{Context, Poll};

use glaredb_core::runtime::filesystem::directory::{DirEntry, ReadDirHandle};
use glaredb_error::Result;

use crate::client::HttpClient;

#[derive(Debug)]
pub struct GcsDirHandle<C: HttpClient> {
    client: C,
}

impl<C> GcsDirHandle<C> where C: HttpClient {}

impl<C> ReadDirHandle for GcsDirHandle<C>
where
    C: HttpClient,
{
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        unimplemented!()
    }

    fn change_dir(&mut self, relative: impl Into<String>) -> Result<Self> {
        unimplemented!()
    }
}
