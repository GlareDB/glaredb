use crate::codec::{Decodeable, Encodeable};
use bytes::{Buf, BufMut};
use std::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageId {
    pub pid: u64,
}

impl Encodeable for PageId {
    fn encode_size(&self) -> usize {
        std::mem::size_of::<u64>()
    }

    fn encode<B: BufMut>(&self, dst: &mut B) {
        dst.put_u64(self.pid);
    }
}

impl Decodeable for PageId {
    fn decode<B: Buf>(src: &mut B) -> Self {
        PageId { pid: src.get_u64() }
    }
}

pub enum PageAddr {
    Mem(PagePtr),
    Disk(DiskPtr),
}

/// A "pointer" to page on disk.
#[derive(Debug)]
pub struct DiskPtr {
    partition_num: usize,
    offset: usize,
}

pub enum PagePtr {
    Delta(DeltaPage),
    Base(BasePage),
}

pub struct DeltaPage {
    data: Box<[u8]>,
    ptr: Box<PagePtr>,
}

pub struct BasePage {
    buf: Box<[u8]>,
}
