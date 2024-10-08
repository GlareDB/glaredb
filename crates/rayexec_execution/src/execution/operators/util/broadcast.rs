use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;

#[derive(Debug)]
pub struct BroadcastChannel {
    state: Arc<Mutex<BroadcastState>>,
}

impl BroadcastChannel {
    pub fn new(num_recvs: usize) -> (Self, Vec<BroadcastReceiver>) {
        let state = Arc::new(Mutex::new(BroadcastState {
            num_receivers: num_recvs,
            batches: Vec::new(),
            recv_wakers: (0..num_recvs).map(|_| None).collect(),
            finished: false,
        }));

        let recvs = (0..num_recvs)
            .map(|idx| BroadcastReceiver {
                subscribe_idx: idx,
                batch_idx: 0,
                state: state.clone(),
            })
            .collect();

        let ch = BroadcastChannel { state };

        (ch, recvs)
    }

    pub fn send(&self, batch: Batch) {
        let mut state = self.state.lock();
        let idx = state.batches.len();

        let remaining_recv = state.num_receivers;

        state.batches.push(BatchState {
            remaining_recv,
            batch: Some(batch),
        });

        // Wake up any receivers waiting on this batch.
        for recv_waker in &mut state.recv_wakers {
            if let Some((batch_idx, waker)) = recv_waker.take() {
                if batch_idx == idx {
                    waker.wake();
                } else {
                    *recv_waker = Some((batch_idx, waker));
                }
            }
        }
    }

    pub fn finish(&self) {
        let mut state = self.state.lock();
        state.finished = true;

        for waker in &mut state.recv_wakers {
            // Just wake everyone up.
            if let Some((_, waker)) = waker.take() {
                waker.wake()
            }
        }
    }
}

#[derive(Debug)]
pub struct BroadcastReceiver {
    subscribe_idx: usize,
    batch_idx: usize,
    state: Arc<Mutex<BroadcastState>>,
}

impl BroadcastReceiver {
    pub fn recv(&mut self) -> RecvFut {
        let fut = RecvFut {
            subscribe_idx: self.subscribe_idx,
            batch_idx: self.batch_idx,
            state: self.state.clone(),
        };

        self.batch_idx += 1;

        fut
    }
}

#[derive(Debug)]
struct BroadcastState {
    num_receivers: usize,
    batches: Vec<BatchState>,
    recv_wakers: Vec<Option<(usize, Waker)>>,
    finished: bool,
}

#[derive(Debug)]
struct BatchState {
    remaining_recv: usize,
    batch: Option<Batch>,
}

#[derive(Debug)]
pub struct RecvFut {
    subscribe_idx: usize,
    batch_idx: usize,
    state: Arc<Mutex<BroadcastState>>,
}

impl Future for RecvFut {
    type Output = Option<Batch>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock();

        match state.batches.get_mut(self.batch_idx) {
            Some(state) => {
                state.remaining_recv -= 1;
                if state.remaining_recv == 0 {
                    // If we're the last receiver for this batch, just take it.
                    // This lets us not have to hold the batch in memory longer
                    // than necessary.
                    //
                    // Note that this doesn't shrink the vec, so there's still
                    // some amount of waste.
                    Poll::Ready(Some(state.batch.take().unwrap()))
                } else {
                    Poll::Ready(Some(state.batch.as_ref().unwrap().clone()))
                }
            }
            None => {
                if state.finished {
                    return Poll::Ready(None);
                }

                state.recv_wakers[self.subscribe_idx] = Some((self.batch_idx, cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Wake;

    use futures::FutureExt;
    use rayexec_bullet::array::Array;

    use super::*;

    struct NopWaker {}

    impl Wake for NopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_with_noop<T, F>(fut: &mut F) -> Poll<T>
    where
        F: Future<Output = T> + Unpin,
    {
        let waker = Waker::from(Arc::new(NopWaker {}));
        let mut cx = Context::from_waker(&waker);
        fut.poll_unpin(&mut cx)
    }

    /// Create a batch with a single int64 value.
    fn test_batch(n: i64) -> Batch {
        let col = Array::from_iter([n]);
        Batch::try_new([col]).unwrap()
    }

    #[test]
    fn single_send_recv_simple() {
        let (send, mut recvs) = BroadcastChannel::new(1);
        send.send(test_batch(1));

        let mut fut = recvs[0].recv();
        let poll = poll_with_noop(&mut fut);
        assert_eq!(Poll::Ready(Some(test_batch(1))), poll);

        send.finish();

        let mut fut = recvs[0].recv();
        let poll = poll_with_noop(&mut fut);
        assert_eq!(Poll::Ready(None), poll);
    }
}
