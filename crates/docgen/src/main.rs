mod file;
mod markdown_table;
mod section;
mod session;

use std::io;

use file::DocFile;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::functions::documentation::Category;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use section::FunctionSectionWriter;
use session::DocsSession;
use tracing::info;

const FILES: &[DocFile] = &[
    DocFile {
        path: "docs/reference/functions/aggregate.md",
        sections: &[(
            "aggregate_functions",
            &FunctionSectionWriter {
                category: Category::Aggregate,
            },
        )],
    },
    DocFile {
        path: "docs/reference/functions/numeric.md",
        sections: &[(
            "numeric_functions",
            &FunctionSectionWriter {
                category: Category::Numeric,
            },
        )],
    },
    DocFile {
        path: "docs/reference/functions/string.md",
        sections: &[(
            "string_functions",
            &FunctionSectionWriter {
                category: Category::String,
            },
        )],
    },
    DocFile {
        path: "docs/reference/functions/regexp.md",
        sections: &[(
            "regexp_functions",
            &FunctionSectionWriter {
                category: Category::Regexp,
            },
        )],
    },
    DocFile {
        path: "docs/reference/functions/date_time.md",
        sections: &[(
            "date_time_functions",
            &FunctionSectionWriter {
                category: Category::DateTime,
            },
        )],
    },
    DocFile {
        path: "docs/reference/functions/general.md",
        sections: &[(
            "general_functions",
            &FunctionSectionWriter {
                category: Category::General,
            },
        )],
    },
];

fn main() -> Result<()> {
    logutil::configure_global_logger(
        tracing::Level::INFO,
        logutil::LogFormat::HumanReadable,
        io::stderr,
    );

    info!("starting docs gen");

    let tokio_rt = new_tokio_runtime_for_io()?;
    let executor = ThreadedNativeExecutor::try_new().unwrap();
    let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

    let engine = SingleUserEngine::try_new(executor, runtime)?;
    let session = DocsSession { tokio_rt, engine };

    for file in FILES {
        info!(%file.path, "handing file");
        file.overwrite(&session)?;
    }

    info!("completed all files");

    Ok(())
}
