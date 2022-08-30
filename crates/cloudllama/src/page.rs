use crate::codec::{Decodeable, Encodeable};
use bytes::{Buf, BufMut, Bytes};
use std::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

pub type PagePtr = *const PageAddr;

pub enum PageAddr {
    Mem(MemPtr),
    Disk(DiskPtr),
}

impl From<MemPtr> for PageAddr {
    fn from(ptr: MemPtr) -> Self {
        PageAddr::Mem(ptr)
    }
}

impl From<DiskPtr> for PageAddr {
    fn from(ptr: DiskPtr) -> Self {
        PageAddr::Disk(ptr)
    }
}

#[derive(Debug, Clone)]
pub struct DiskPtr {
    pub partition_num: usize,
    pub offset: usize,
}

pub enum MemPtr {
    Delta(DeltaPage),
    Base(BasePage),
}

pub struct DeltaPage {
    pub(crate) data: Bytes,
    pub(crate) ptr: PagePtr,
}

pub struct BasePage {
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum PageKind {
    Unknown = 0,
    Base = 1,
    Delta = 2,
}

impl PageKind {
    fn from_u8(b: u8) -> PageKind {
        match b {
            1 => PageKind::Base,
            2 => PageKind::Delta,
            _ => PageKind::Unknown,
        }
    }
}

/// Header at the beginning of every page.
///
/// The page size is the size of the page minus the header.
///
/// ```text
/// <page size> // 8 bytes
/// <page kind> // 1 byte
/// <empty>     // 7 bytes
/// ```
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub size: u64,
    pub kind: PageKind,
}

impl PageHeader {
    pub const fn min_decode_size() -> usize {
        16
    }
}

impl Encodeable for PageHeader {
    fn encode_size(&self) -> usize {
        Self::min_decode_size()
    }

    fn encode<B: BufMut>(&self, dst: &mut B) {
        dst.put_u64(self.size);
        dst.put_u8(self.kind as u8);
        dst.put(&[0; 7][..]);
    }
}

impl Decodeable for PageHeader {
    fn decode<B: Buf>(src: &mut B) -> Self {
        let hdr = PageHeader {
            size: src.get_u64(),
            kind: PageKind::from_u8(src.get_u8()),
        };
        src.advance(7);
        hdr
    }
}
