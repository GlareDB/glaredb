mod file;
mod markdown_table;
mod section;
mod session;

use std::io;

use file::DocFile;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use section::{AggregateFunctionWriter, ScalarFunctionWriter, TableFunctionWriter};
use session::DocsSession;
use tracing::info;

const FILES: &[DocFile] = &[DocFile {
    path: "docs/sql/functions.md",
    sections: &[
        ("scalar_functions", &ScalarFunctionWriter),
        ("aggregate_functions", &AggregateFunctionWriter),
        ("table_functions", &TableFunctionWriter),
    ],
}];

fn main() -> Result<()> {
    logutil::configure_global_logger(
        tracing::Level::INFO,
        logutil::LogFormat::HumanReadable,
        io::stderr,
    );

    info!("starting docs gen");

    let executor = ThreadedNativeExecutor::try_new().unwrap();
    let runtime = NativeRuntime::with_default_tokio().unwrap();

    let engine = SingleUserEngine::try_new(executor, runtime)?;
    let session = DocsSession { engine };

    for file in FILES {
        info!(%file.path, "handing file");
        file.overwrite(&session)?;
    }

    info!("completed all files");

    Ok(())
}
