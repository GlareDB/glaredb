use std::future::Future;

use pyo3::Python;

use crate::errors::Result;

/// Runs a future until completion.
pub(crate) fn run_until_complete<F, T>(_py: Python<'_>, fut: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    // TODO: While this works fine now, there's currently not a way to cancel
    // the future.
    //
    // What we should do is wrap the fut in some other future that also waits on
    // a cancellation signal. Not sure what that would actually look like, and
    // if it'd involve asyncio.
    glaredb_core::util::future::block_on(fut)
}
