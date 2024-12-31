use std::ops::Deref;

use rayexec_error::{RayexecError, Result};

use crate::arrays::buffer::buffer_manager::{BufferManager, NopBufferManager};
use crate::arrays::buffer::ArrayBuffer;

/// Abstraction layer for determining where an array's buffer resides.
#[derive(Debug)]
pub struct ArrayData<B: BufferManager = NopBufferManager> {
    inner: ArrayDataInner<B>,
}

#[derive(Debug)]
enum ArrayDataInner<B: BufferManager> {
    /// Array buffer is being managed and is behind a shared pointer.
    Managed(B::CowPtr<ArrayBuffer<B>>, Option<ArrayBuffer<B>>),
    Owned(ArrayBuffer<B>),
    Uninit,
}

impl<B> ArrayData<B>
where
    B: BufferManager,
{
    pub(crate) fn owned(buffer: ArrayBuffer<B>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Owned(buffer),
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
    pub fn try_reset_for_write(&mut self) -> Result<(), ()> {
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
        ArrayData::as_ref(&self)
    }
}
