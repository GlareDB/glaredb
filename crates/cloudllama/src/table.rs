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
}

impl<T> Default for MappingTable<T> {
    fn default() -> Self {
        MappingTable {
            root: Box::default(),
        }
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

    unsafe { ptr.as_ref().unwrap() }
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
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Large {
        a: u64,
        b: u64,
        c: u64,
    }

    #[test]
    fn simple() {
        #[cfg(miri)]
        const N: u64 = 10;
        #[cfg(not(miri))]
        const N: u64 = 100_000;

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
}
