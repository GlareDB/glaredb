#![allow(dead_code)]

use std::ops::Deref;

use rayexec_error::{RayexecError, Result};

use super::array_buffer::ArrayBuffer;
use super::buffer_manager::BufferManager;

/// Abstraction layer for determining where an array's buffer resides.
///
/// An array may have and 'owned' buffer which it can freely modify, or it may
/// have a reference to a 'managed' buffer that's shared between multiple
/// arrays.
#[derive(Debug)]
pub struct ArrayData<B: BufferManager> {
    inner: ArrayDataInner<B>,
}

#[derive(Debug)]
enum ArrayDataInner<B: BufferManager> {
    /// Array buffer is being managed and is behind a shared pointer.
    ///
    /// If the array data previously contained and owned array buffer, we store
    /// it so that we can flip back to using it when we want the array to be
    /// writable.
    Managed(B::CowPtr<ArrayBuffer<B>>, Option<ArrayBuffer<B>>),
    /// Array buffer is owned by this array.
    Owned(ArrayBuffer<B>),
    /// Intermediate state for array data when switching to/from managed. Array
    /// data should never be in this state after any operation.
    Uninit,
}

impl<B> ArrayData<B>
where
    B: BufferManager,
{
    /// Create an owned variant of array data from a buffer.
    pub(crate) fn owned(buffer: ArrayBuffer<B>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Owned(buffer),
        }
    }

    pub(crate) fn managed(buffer: B::CowPtr<ArrayBuffer<B>>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Managed(buffer, None),
        }
    }

    /// Set this array data to point to a buffer that's being managed.
    ///
    /// If this array data was previously holding onto an owned buffer, we store
    /// that so we can quickly reset back to it as needed without needing to
    /// allocate an additional buffer.
    pub(crate) fn set_managed(&mut self, managed: B::CowPtr<ArrayBuffer<B>>) -> Result<()> {
        match std::mem::replace(&mut self.inner, ArrayDataInner::Uninit) {
            ArrayDataInner::Managed(_, cached) => {
                // Nothing fancy, just update the managed array.
                self.inner = ArrayDataInner::Managed(managed, cached);
            }
            ArrayDataInner::Owned(owned) => {
                // Cache our owned version so we can reset the data to a mutable
                // variant as needed.
                self.inner = ArrayDataInner::Managed(managed, Some(owned))
            }
            ArrayDataInner::Uninit => panic!("Array data in invalid state"),
        }

        Ok(())
    }

    pub fn is_managed(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Managed(_, _))
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Owned(_))
    }

    /// Try to reset the array data for writes.
    ///
    /// If the buffer is already owned, nothing is done. If the buffer is
    /// managed, but we have a cached owned buffer, we use the cached buffer to
    /// make this `Owned`.
    ///
    /// Returns `Ok(())` if the reset was successful, `Err(())` otherwise. If
    /// `Err(())` is returned, this remains unchanged.
    pub(crate) fn try_reset_for_write(&mut self) -> Result<(), ()> {
        match &mut self.inner {
            ArrayDataInner::Managed(_, cached) => {
                if let Some(cached) = cached.take() {
                    self.inner = ArrayDataInner::Owned(cached);
                    Ok(())
                } else {
                    // No cached buffer.
                    Err(())
                }
            }
            ArrayDataInner::Owned(_) => Ok(()), // Nothing to do, already writable.
            ArrayDataInner::Uninit => panic!("Array data in invalid state"),
        }
    }

    /// Try to make the array managed by the buffer manager.
    ///
    /// Does nothing if the array is already managed.
    ///
    /// Returns an error if the array cannot be made to be managed. The array is
    /// still valid (and remains in the 'owned' state).
    ///
    /// A cloned pointer to the newly managed array will be returned.
    pub(crate) fn make_managed(&mut self, manager: &B) -> Result<B::CowPtr<ArrayBuffer<B>>> {
        match &mut self.inner {
            ArrayDataInner::Managed(m, _) => Ok(m.clone()), // Already managed.
            ArrayDataInner::Owned(_) => {
                let orig = std::mem::replace(&mut self.inner, ArrayDataInner::Uninit);
                let array = match orig {
                    ArrayDataInner::Owned(array) => array,
                    _ => unreachable!("variant already checked"),
                };

                match manager.make_cow(array) {
                    Ok(managed) => {
                        self.inner = ArrayDataInner::Managed(managed, None); // Manager took ownership, nothing to cache.
                        match &self.inner {
                            ArrayDataInner::Managed(m, _) => Ok(m.clone()),
                            _ => unreachable!("variant just set"),
                        }
                    }
                    Err(orig) => {
                        // Manager rejected it, put it back as owned and return
                        // an error.
                        self.inner = ArrayDataInner::Owned(orig);
                        Err(RayexecError::new("Failed to make batch array managed"))
                    }
                }
            }
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }

    pub fn try_as_mut(&mut self) -> Result<&mut ArrayBuffer<B>> {
        match &mut self.inner {
            ArrayDataInner::Managed(_, _) => Err(RayexecError::new(
                "Mut references from managed arrays not yet supported",
            )),
            ArrayDataInner::Owned(array) => Ok(array),
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }
}

impl<B> AsRef<ArrayBuffer<B>> for ArrayData<B>
where
    B: BufferManager,
{
    fn as_ref(&self) -> &ArrayBuffer<B> {
        match &self.inner {
            ArrayDataInner::Managed(m, _) => m.as_ref(),
            ArrayDataInner::Owned(array) => array,
            ArrayDataInner::Uninit => panic!("array in uninit state"),
        }
    }
}

impl<B> Deref for ArrayData<B>
where
    B: BufferManager,
{
    type Target = ArrayBuffer<B>;

    fn deref(&self) -> &Self::Target {
        ArrayData::as_ref(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::PhysicalI32;

    #[test]
    fn make_owned_and_deref() {
        let mut buf =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
                .unwrap();

        let s = buf.try_as_slice_mut::<PhysicalI32>().unwrap();
        for i in 0..4 {
            s[i] = i as i32;
        }

        let data = ArrayData::owned(buf);
        assert_eq!(&[0, 1, 2, 3], data.try_as_slice::<PhysicalI32>().unwrap());
    }

    #[test]
    fn make_shared_and_deref() {
        let mut buf =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
                .unwrap();

        let s = buf.try_as_slice_mut::<PhysicalI32>().unwrap();
        for i in 0..4 {
            s[i] = i as i32;
        }

        let mut data = ArrayData::owned(
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
                .unwrap(),
        );
        data.set_managed(NopBufferManager.make_cow(buf).unwrap())
            .unwrap();

        assert_eq!(&[0, 1, 2, 3], data.try_as_slice::<PhysicalI32>().unwrap());
    }

    #[test]
    fn reset_for_write() {
        let buf = ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
            .unwrap();
        let mut data = ArrayData::owned(buf);

        let shared =
            ArrayBuffer::with_primary_capacity::<PhysicalI32>(&Arc::new(NopBufferManager), 4)
                .unwrap();
        data.set_managed(NopBufferManager.make_cow(shared).unwrap())
            .unwrap();

        assert!(data.is_managed());
        data.try_reset_for_write().unwrap();

        assert!(data.is_owned());
        let _s = data
            .try_as_mut()
            .unwrap()
            .try_as_slice_mut::<PhysicalI32>()
            .unwrap();
    }
}
