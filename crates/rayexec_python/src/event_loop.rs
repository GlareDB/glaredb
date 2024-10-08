use std::future::Future;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use pyo3::types::PyAnyMethods;
use pyo3::{pyclass, pymethods, Py, PyAny, Python};
use rayexec_error::RayexecError;

use crate::errors::Result;

/// If we should block on tokio instead of using an asyncio event loop.
///
/// Added because apparently you can't have nested event loops and some
/// notebooks (google collab) already have one running. I'm not sure if we care
/// to keep the asyncio code, or try to do some detection to run on the current
/// event loop. Right now let's just do the easy thing.
const BLOCK_ON_TOKIO: bool = true;

static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn tokio_handle() -> &'static tokio::runtime::Handle {
    let runtime = TOKIO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .thread_name("rayexec_python_tokio")
            .build()
            .expect("to be able to build tokio runtime")
    });

    runtime.handle()
}

/// Runs a future until completion.
pub(crate) fn run_until_complete<F, T>(py: Python<'_>, fut: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    if BLOCK_ON_TOKIO {
        return tokio_handle().block_on(fut);
    }

    // asyncio docs: https://docs.python.org/3/library/asyncio.html

    // Bind a new python future to the global event loop.
    let py_fut = get_event_loop(py).call_method0(py, "create_future")?;
    py_fut.call_method1(py, "add_done_callback", (PyDoneCallback,))?;

    // Output will contain the result of the (rust) future when it completes.
    //
    // TODO: Could be refcell.
    let output = Arc::new(Mutex::new(None));
    spawn_python_future(py_fut.clone_ref(py), fut, output.clone());

    // Wait for the future to complete on the event loop.
    // TODO: Idk if this keeps the GIL or not.
    get_event_loop(py).call_method1(py, "run_until_complete", (py_fut,))?;

    let mut output = output.lock();
    match output.take() {
        Some(output) => output,
        None => Err(RayexecError::new("Missing output").into()),
    }
}

static ASYNCIO_EVENT_LOOP: OnceLock<Py<PyAny>> = OnceLock::new();

fn get_event_loop(py: Python<'_>) -> &Py<PyAny> {
    ASYNCIO_EVENT_LOOP.get_or_init(|| {
        // TODO: Handle unwraps a bit better (`get_or_try_init` would be real
        // cool right now).
        // Use once_cell crate if this becomes an issue.
        py.import_bound("asyncio")
            .unwrap()
            .call_method0("new_event_loop")
            .unwrap()
            .unbind()
    })
}

// TODO: Output could possibly be refcell.
fn spawn_python_future<F, T>(py_fut: Py<PyAny>, fut: F, output: Arc<Mutex<Option<Result<T>>>>)
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    tokio_handle().spawn(async move {
        // Await the (rust) future and set the output.
        let result = fut.await;
        {
            let mut output = output.lock();
            output.replace(result);
        }

        Python::with_gil(move |py| {
            // Set a dummy result on the python future. Doesn't need an actual
            // value since we're just using it for signalling.
            py_fut.call_method1(py, "set_result", (true,)).unwrap();
            // Trigger the event loop to run. Passed a callback that does
            // nothing.
            get_event_loop(py)
                .call_method1(py, "call_soon_threadsafe", (PyCallSoonCallback,))
                .unwrap();
        });
    });
}

/// Callback for the done callback on the py future.
///
/// Doesn't do anything yet but should be used for cancellation.
#[pyclass]
#[derive(Debug, Clone, Copy)]
struct PyDoneCallback;

#[pymethods]
impl PyDoneCallback {
    fn __call__(&self, _py_fut: Py<PyAny>) -> Result<()> {
        Ok(())
    }
}

/// Callback for the `call_soon_threadsafe call`, doesn't do anything.
#[pyclass]
#[derive(Debug, Clone, Copy)]
struct PyCallSoonCallback;

#[pymethods]
impl PyCallSoonCallback {
    fn __call__(&self) -> Result<()> {
        Ok(())
    }
}
