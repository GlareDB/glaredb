use std::task::Waker;

/// Helper struct for storing at most one waker per partition.
#[derive(Debug)]
pub struct PartitionWakers {
    wakers: Vec<Option<Waker>>,
}

impl PartitionWakers {
    pub const fn empty() -> Self {
        PartitionWakers { wakers: Vec::new() }
    }

    /// Initialize for some number of partitions.
    ///
    /// Panics if previously initialized, or if number of partitions is zero.
    pub fn init_for_partitions(&mut self, num_partitions: usize) {
        assert_eq!(0, self.wakers.len());
        assert_ne!(0, num_partitions);

        self.wakers.resize(num_partitions, None);
    }

    /// Wakes a waker for the given partition.
    ///
    /// Removes the waker after waking.
    pub fn wake(&mut self, partition: usize) {
        if let Some(waker) = self.wakers[partition].take() {
            waker.wake();
        }
    }

    /// Wakes all wakers.
    ///
    /// Removes all wakers after waking.
    pub fn wake_all(&mut self) {
        for waker in &mut self.wakers {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
    }

    /// Stores a waker for the given partition.
    ///
    /// Overwrites the existing waker for the partition.
    pub fn store(&mut self, waker: &Waker, partition: usize) {
        self.wakers[partition] = Some(waker.clone())
    }

    /// Returns indices for empty wakers.
    #[allow(dead_code)]
    pub fn empty_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.wakers
            .iter()
            .enumerate()
            .filter_map(|(idx, waker)| if waker.is_some() { None } else { Some(idx) })
    }

    /// Returns indices for non-empty wakers.
    #[allow(dead_code)]
    pub fn non_empty_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.wakers
            .iter()
            .enumerate()
            .filter_map(|(idx, waker)| if waker.is_some() { Some(idx) } else { None })
    }
}
