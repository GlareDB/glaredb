pub mod errors;
pub mod runtime;
pub mod session;
pub mod shell;

mod http;
mod time;
mod tracing;

use ::tracing::trace;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn init_panic_handler() {
    trace!("init panic handler");
    console_error_panic_hook::set_once();
}

#[cfg(test)]
pub mod wasm_tests {
    // Note that these tests are required to be in the root, or a pub mod.
    //
    // To make things easy, all wasm bindgen tests should just go here.
    //
    // Usage: <https://rustwasm.github.io/wasm-bindgen/wasm-bindgen-test/usage.html>

    use glaredb_core::generate_batch;
    use glaredb_core::testutil::arrays::assert_batches_eq;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use crate::session::WasmSession;

    // Force running tests in the browser.
    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    #[allow(unused)]
    async fn basic_query() {
        let session = WasmSession::try_new().unwrap();
        let q_res = session.query("SELECT 4::INT").await.unwrap();
        let batch = &q_res.batches[0];

        let expected = generate_batch!([4]);
        assert_batches_eq(&expected, batch);
    }

    #[wasm_bindgen_test]
    #[allow(unused)]
    async fn remote_io() {
        let session = WasmSession::try_new().unwrap();
        // Well-known parquet file
        let q_res = session
            .query("SELECT count(*)::INT FROM 's3://glaredb-public/userdata0.parquet'")
            .await
            .unwrap();
        let batch = &q_res.batches[0];

        let expected = generate_batch!([1000]);
        assert_batches_eq(&expected, batch);
    }
}
