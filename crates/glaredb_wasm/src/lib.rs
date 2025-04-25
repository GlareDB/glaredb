pub mod errors;
pub mod runtime;
pub mod session;
pub mod shell;

mod http;
mod origin_filesystem;
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

    use crate::origin_filesystem::OriginFileSystem;
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

    #[wasm_bindgen_test]
    #[allow(unused)]
    async fn query_temp_table() {
        let session = WasmSession::try_new().unwrap();
        let _ = session
            .query("CREATE TEMP TABLE t1 (a INT, b TEXT)")
            .await
            .unwrap();
        let _ = session
            .query("INSERT INTO t1 VALUES (4, 'four'), (5, 'five')")
            .await
            .unwrap();

        let q_res = session.query("SELECT * FROM t1 ORDER BY a").await.unwrap();
        let batch = &q_res.batches[0];

        let expected = generate_batch!([4, 5], ["four", "five"]);
        assert_batches_eq(&expected, batch);
    }

    #[wasm_bindgen_test]
    #[allow(unused)]
    async fn simple_read_from_opfs() {
        // Write some simple data to the file first.
        let handle = OriginFileSystem {}
            .open_sync_access_handle("/data.csv", true)
            .await
            .expect("open to not error")
            .expect("file to exist (after creating it)");

        let content = [
            "number,float,name",
            "4,8.5,mario",
            "5,9.1,wario",
            "6,7.0,yoshi",
        ]
        .join("\n");
        handle.write_with_u8_array(content.as_bytes()).unwrap();
    }
}
