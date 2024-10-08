use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

/// A barrier which hands out per-partition futures that resolve to Option<T>
/// only when the barrier is explicitly unblocked.
///
/// This allows for synchronizing multiple partitions on a single action before
/// proceeding.
#[derive(Debug)]
pub struct PartitionBarrier<T> {
    inner: Arc<Mutex<BarrierInner<T>>>,
}

impl<T> Clone for PartitionBarrier<T> {
    fn clone(&self) -> Self {
        PartitionBarrier {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
struct BarrierInner<T> {
    unblocked: bool,
    wakers: Vec<Option<Waker>>,
    items: Vec<Option<T>>,
}

impl<T> PartitionBarrier<T> {
    /// Create a new barrier corresponding to some number of partitions.
    pub fn new(num_partitions: usize) -> Self {
        PartitionBarrier {
            inner: Arc::new(Mutex::new(BarrierInner {
                unblocked: false,
                wakers: (0..num_partitions).map(|_| None).collect(),
                items: (0..num_partitions).map(|_| None).collect(),
            })),
        }
    }

    /// Try to take an item for a partitions.
    ///
    /// Returns a future which only resolves once the barrier is unblocked by
    /// calling the `unblock` method.
    pub fn item_for_partition(&self, idx: usize) -> PartitionBarrierFut<T> {
        PartitionBarrierFut {
            idx,
            inner: self.inner.clone(),
        }
    }

    /// Unblocks the barrier, emplacing `items` which futures will resolve to.
    ///
    /// `items` length much equal the number of partitions.
    pub fn unblock(&self, items: Vec<Option<T>>) {
        let mut inner = self.inner.lock();
        inner.unblocked = true;

        debug_assert_eq!(items.len(), inner.items.len());
        inner.items = items;

        for waker in &mut inner.wakers {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
    }
}

pub struct PartitionBarrierFut<T> {
    idx: usize,
    inner: Arc<Mutex<BarrierInner<T>>>,
}

impl<T> Future for PartitionBarrierFut<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.lock();
        if inner.unblocked {
            return Poll::Ready(inner.items[self.idx].take());
        }

        inner.wakers[self.idx] = Some(cx.waker().clone());

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use futures::FutureExt;

    use super::*;

    #[derive(Default)]
    struct TestWaker {
        wake_count: AtomicUsize,
    }

    impl TestWaker {
        fn load_count(&self) -> usize {
            self.wake_count.load(Ordering::SeqCst)
        }
    }

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn unblock_before_poll() {
        let t0 = Arc::new(TestWaker::default());
        let t1 = Arc::new(TestWaker::default());

        let w0 = Waker::from(t0.clone());
        let w1 = Waker::from(t1.clone());

        let mut c0 = Context::from_waker(&w0);
        let mut c1 = Context::from_waker(&w1);

        let barrier = PartitionBarrier::<i32>::new(2);

        barrier.unblock(vec![Some(8), Some(9)]);

        let poll = barrier.item_for_partition(0).poll_unpin(&mut c0);
        assert_eq!(Poll::Ready(Some(8)), poll);

        let poll = barrier.item_for_partition(1).poll_unpin(&mut c1);
        assert_eq!(Poll::Ready(Some(9)), poll);

        assert_eq!(0, t0.load_count());
        assert_eq!(0, t1.load_count());
    }

    #[test]
    fn single_poll_before_unblock() {
        let t0 = Arc::new(TestWaker::default());
        let t1 = Arc::new(TestWaker::default());

        let w0 = Waker::from(t0.clone());
        let w1 = Waker::from(t1.clone());

        let mut c0 = Context::from_waker(&w0);
        let mut c1 = Context::from_waker(&w1);

        let barrier = PartitionBarrier::<i32>::new(2);

        let mut fut = barrier.item_for_partition(0);
        let poll = fut.poll_unpin(&mut c0);
        assert_eq!(Poll::Pending, poll);

        barrier.unblock(vec![Some(4), Some(5)]);

        let poll = fut.poll_unpin(&mut c0);
        assert_eq!(Poll::Ready(Some(4)), poll);

        let mut fut = barrier.item_for_partition(1);
        let poll = fut.poll_unpin(&mut c1);
        assert_eq!(Poll::Ready(Some(5)), poll);

        assert_eq!(1, t0.load_count());
        assert_eq!(0, t1.load_count());
    }

    #[test]
    fn all_poll_before_unblock() {
        let t0 = Arc::new(TestWaker::default());
        let t1 = Arc::new(TestWaker::default());

        let w0 = Waker::from(t0.clone());
        let w1 = Waker::from(t1.clone());

        let mut c0 = Context::from_waker(&w0);
        let mut c1 = Context::from_waker(&w1);

        let barrier = PartitionBarrier::<i32>::new(2);

        let mut fut0 = barrier.item_for_partition(0);
        let poll = fut0.poll_unpin(&mut c0);
        assert_eq!(Poll::Pending, poll);

        let mut fut1 = barrier.item_for_partition(1);
        let poll = fut1.poll_unpin(&mut c1);
        assert_eq!(Poll::Pending, poll);

        barrier.unblock(vec![Some(2), Some(3)]);

        let poll = fut0.poll_unpin(&mut c0);
        assert_eq!(Poll::Ready(Some(2)), poll);

        let poll = fut1.poll_unpin(&mut c1);
        assert_eq!(Poll::Ready(Some(3)), poll);

        assert_eq!(1, t0.load_count());
        assert_eq!(1, t1.load_count());
    }
}
