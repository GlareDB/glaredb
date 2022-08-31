use std::sync::atomic::{AtomicPtr, Ordering};

const FANOUT: usize = 1 << 16;

/// Map some u64 identifier to an atomic ptr.
pub struct MappingTable<T> {
    root: Box<L0<T>>,
}

impl<T> MappingTable<T> {
    /// Get an atomic ptr for some id.
    ///
    /// The atomic ptr will be null if nothing has been previously stored for
    /// this mapping table instance.
    ///
    /// It is the responsibility of the caller to ensure pointers are dropped
    /// appropriately.
    pub fn get(&self, id: u64) -> &AtomicPtr<T> {
        let bs = id.to_be_bytes();
        let k0 = u16::from_be_bytes([bs[0], bs[1]]);
        let k1 = u16::from_be_bytes([bs[2], bs[3]]);
        let k2 = u16::from_be_bytes([bs[4], bs[5]]);
        let k3 = u16::from_be_bytes([bs[6], bs[7]]);

        let l1 = traverse(&self.root.0, k0);
        let l2 = traverse(&l1.0, k1);
        let l3 = traverse(&l2.0, k2);

        &l3.0[k3 as usize]
    }

    pub fn iter_range(&self, start: u64, end: u64) -> MappingTableRangeIter<'_, T> {
        MappingTableRangeIter {
            idx: start,
            end,
            table: self,
        }
    }

    fn try_get(&self, id: u64) -> Option<&AtomicPtr<T>> {
        let bs = id.to_be_bytes();
        let k0 = u16::from_be_bytes([bs[0], bs[1]]);
        let k1 = u16::from_be_bytes([bs[2], bs[3]]);
        let k2 = u16::from_be_bytes([bs[4], bs[5]]);
        let k3 = u16::from_be_bytes([bs[6], bs[7]]);

        let l1 = traverse_no_install(&self.root.0, k0)?;
        let l2 = traverse_no_install(&l1.0, k1)?;
        let l3 = traverse_no_install(&l2.0, k2)?;

        Some(&l3.0[k3 as usize])
    }
}

impl<T> Default for MappingTable<T> {
    fn default() -> Self {
        MappingTable {
            root: Box::default(),
        }
    }
}

pub struct MappingTableRangeIter<'a, T> {
    idx: u64,
    end: u64, // Inclusive.
    table: &'a MappingTable<T>,
}

impl<'a, T> Iterator for MappingTableRangeIter<'a, T> {
    type Item = &'a AtomicPtr<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == u64::MAX || self.idx > self.end {
            return None;
        }
        let atomic = self.table.try_get(self.idx);
        if atomic.is_some() {
            self.idx += 1;
        }
        atomic
    }
}

struct L0<T>([AtomicPtr<L1<T>>; FANOUT]);
struct L1<T>([AtomicPtr<L2<T>>; FANOUT]);
struct L2<T>([AtomicPtr<L3<T>>; FANOUT]);
struct L3<T>([AtomicPtr<T>; FANOUT]);

macro_rules! impl_default {
    ($level:ty) => {
        impl<T> Default for $level {
            fn default() -> Self {
                Self(unsafe { std::mem::MaybeUninit::zeroed().assume_init() })
            }
        }
    };
}

impl_default!(L0<T>);
impl_default!(L1<T>);
impl_default!(L2<T>);
impl_default!(L3<T>);

macro_rules! impl_drop {
    ($level:ty) => {
        impl<T> Drop for $level {
            fn drop(&mut self) {
                drop_children(&self.0)
            }
        }
    };
}

impl_drop!(L0<T>);
impl_drop!(L1<T>);
impl_drop!(L2<T>);

fn traverse_no_install<U: Default>(level: &[AtomicPtr<U>], idx: u16) -> Option<&U> {
    let atomic = &level[idx as usize];
    let ptr = atomic.load(Ordering::Acquire);
    unsafe { ptr.as_ref() }
}

fn traverse<U: Default>(level: &[AtomicPtr<U>], idx: u16) -> &U {
    let atomic = &level[idx as usize];
    let mut ptr = atomic.load(Ordering::Acquire);

    if ptr.is_null() {
        let child_ptr = Box::into_raw(Box::default());
        match atomic.compare_exchange(
            std::ptr::null_mut(),
            child_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => ptr = child_ptr,
            Err(curr) => ptr = curr,
        }
    }

    unsafe { ptr.as_ref().expect("null pointer initialized with cas") }
}

fn drop_children<U>(level: &[AtomicPtr<U>]) {
    for atomic in level.iter() {
        let ptr = atomic.load(Ordering::Acquire);
        if !ptr.is_null() {
            std::mem::drop(unsafe { Box::from_raw(ptr) })
        }
    }
}

#[cfg(test)]
mod tests {
    // These tests leak memory since we're not cleaning up the items inserted
    // into the table. Not a huge issue.

    use super::*;

    #[derive(Debug, PartialEq)]
    struct Large {
        a: u64,
        b: u64,
        c: u64,
    }

    #[cfg(miri)]
    const N: u64 = 10;
    #[cfg(not(miri))]
    const N: u64 = 100_000;

    #[test]
    fn simple() {
        let mt = MappingTable::<Large>::default();

        for i in 0..N {
            let atomic = mt.get(i);
            let ptr = atomic.load(Ordering::Relaxed);
            assert!(ptr.is_null());
            let ptr = Box::into_raw(Box::new(Large { a: i, b: i, c: i }));
            atomic.store(ptr, Ordering::Relaxed);
        }

        for i in 0..N {
            let ptr = mt.get(i).load(Ordering::Relaxed);
            let val = unsafe { ptr.as_ref().unwrap() };
            assert_eq!(&Large { a: i, b: i, c: i }, val);
        }
    }

    #[cfg(not(miri))] // Slow
    #[test]
    fn simple_iter() {
        let mt = MappingTable::<Large>::default();

        // Nothing in table yet.
        let iter = mt.iter_range(0, u64::MAX);
        let ptrs: Vec<_> = iter.collect();
        assert!(ptrs.is_empty());

        for i in 0..N {
            let ptr = Box::into_raw(Box::new(Large { a: i, b: i, c: i }));
            mt.get(i).store(ptr, Ordering::Relaxed);
        }

        let iter = mt.iter_range(0, N / 2);
        let ptrs: Vec<_> = iter.collect();
        // Iter range is inclusive, otherwise we wouldn't be able to get the
        // item at u64::MAX.
        assert_eq!((N / 2) + 1, ptrs.len() as u64);

        let iter = mt.iter_range(0, u64::MAX);
        let ptrs: Vec<_> = iter.collect();

        // Check that we're only iterating over leaves that have been installed.
        //
        // In the case of N = 100_000, two leaves will have been installed,
        // making room for (1 << 16) * 2 pointers.
        let num_leaves = if N % FANOUT as u64 == 0 {
            N / FANOUT as u64
        } else {
            (N / FANOUT as u64) + 1
        };
        assert_eq!(FANOUT * num_leaves as usize, ptrs.len());
    }
}
