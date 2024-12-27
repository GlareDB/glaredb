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
    Managed(B::CowPtr<ArrayBuffer<B>>),
    Owned(ArrayBuffer<B>),
    Uninit,
}

impl<B> ArrayData<B>
where
    B: BufferManager,
{
    pub fn owned(buffer: ArrayBuffer<B>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Owned(buffer),
        }
    }

    pub fn managed(buffer: B::CowPtr<ArrayBuffer<B>>) -> Self {
        ArrayData {
            inner: ArrayDataInner::Managed(buffer),
        }
    }

    pub fn is_managed(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Managed(_))
    }

    pub fn is_owned(&self) -> bool {
        matches!(self.inner, ArrayDataInner::Owned(_))
    }

    /// Try to make the array managed by the buffer manager.
    ///
    /// Does nothing if the array is already managed.
    ///
    /// Returns an error if the array cannot be made to be managed. The array is
    /// still valid (and remains in the 'owned' state).
    ///
    /// A cloned pointer to the newly managed array will be returned.
    pub fn make_managed(&mut self, manager: &B) -> Result<B::CowPtr<ArrayBuffer<B>>> {
        match &mut self.inner {
            ArrayDataInner::Managed(m) => Ok(m.clone()), // Already managed.
            ArrayDataInner::Owned(_) => {
                let orig = std::mem::replace(&mut self.inner, ArrayDataInner::Uninit);
                let array = match orig {
                    ArrayDataInner::Owned(array) => array,
                    _ => unreachable!("variant already checked"),
                };

                match manager.make_cow(array) {
                    Ok(managed) => {
                        self.inner = ArrayDataInner::Managed(managed);
                        match &self.inner {
                            ArrayDataInner::Managed(m) => Ok(m.clone()),
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
            ArrayDataInner::Managed(_) => Err(RayexecError::new(
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
            ArrayDataInner::Managed(m) => m.as_ref(),
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
