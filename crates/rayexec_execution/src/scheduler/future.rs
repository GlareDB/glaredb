use parking_lot::Mutex;
use rayon::ThreadPool;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::{Context, Wake, Waker};

// TODO: This is the 'easy' solution for handling futures on the scheduler. We
// can definitely be more efficient.
pub struct FutureTask<F>
where
    F: Future + Send,
    F::Output: Send,
{
    inner: Mutex<FutureTaskInner<F>>,
}

struct FutureTaskInner<F>
where
    F: Future + Send,
    F::Output: Send,
{
    /// Boolean for if the task was woken up whil
    was_woken_while_none: bool,

    /// The future, None while the task is executing.
    future: Option<F>,

    /// Output of the future, once it's available.
    output: Option<F::Output>,
}

impl<F> FutureTask<F>
where
    F: Future + Send + Unpin + 'static,
    F::Output: Send,
{
    pub fn new(future: F) -> Self {
        FutureTask {
            inner: Mutex::new(FutureTaskInner {
                was_woken_while_none: false,
                future: Some(future),
                output: None,
            }),
        }
    }

    pub(crate) fn execute(self: Arc<Self>, pool: Arc<ThreadPool>) {
        let mut fut = {
            let mut inner = self.inner.lock();
            match inner.future.take() {
                Some(fut) => fut,
                None => {
                    // Do something
                    return;
                }
            }
        };

        let waker: Waker = Arc::new(FutureWaker {
            pool,
            task: self.clone(),
        })
        .into();
        let mut cx = Context::from_waker(&waker);
        // This does as much work as possible. It may be reasonable to add in
        // parameters to make sure a future isn't using up too much of the
        // thread pool.
        loop {
            match Pin::new(&mut fut).poll(&mut cx) {
                Poll::Ready(output) => {
                    let mut inner = self.inner.lock();
                    inner.output = Some(output);
                    break;
                }
                Poll::Pending => {
                    let mut inner = self.inner.lock();
                    if inner.was_woken_while_none {
                        // We go woken up almost immediately after the pending,
                        // just keep polling.
                        inner.was_woken_while_none = false;
                        continue;
                    }
                    // Otherwise we're still pending, put our future back.
                    inner.future = Some(fut);
                    break;
                }
            }
        }
    }
}

struct FutureWaker<F>
where
    F: Future + Send,
    F::Output: Send,
{
    task: Arc<FutureTask<F>>,
    pool: Arc<ThreadPool>,
}

impl<F> Wake for FutureWaker<F>
where
    F: Future + Send + Unpin + 'static,
    F::Output: Send,
{
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let task = self.task.clone();
        let mut inner = task.inner.lock();
        if inner.future.is_some() {
            std::mem::drop(inner);
            let pool = self.pool.clone();
            self.pool.spawn(|| task.execute(pool));
        } else {
            inner.was_woken_while_none = true;
            // Task currently executing, `execute` will handle re-executing the
            // future.
        }
    }
}
