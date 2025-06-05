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
        path: "docs/sql/functions/aggregate.md",
        sections: &[(
            "general_purpose_aggregate_functions",
            &FunctionSectionWriter::<2> {
                category: Category::GENERAL_PURPOSE_AGGREGATE,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/statistical-aggregate.md",
        sections: &[(
            "statistical_aggregate_functions",
            &FunctionSectionWriter::<2> {
                category: Category::STATISTICAL_AGGREGATE,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/approximate-aggregate.md",
        sections: &[(
            "approximate_aggregate_functions",
            &FunctionSectionWriter::<2> {
                category: Category::APPROXIMATE_AGGREGATE,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/numeric.md",
        sections: &[(
            "numeric_functions",
            &FunctionSectionWriter::<2> {
                category: Category::Numeric,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/string.md",
        sections: &[(
            "string_functions",
            &FunctionSectionWriter::<2> {
                category: Category::String,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/regexp.md",
        sections: &[(
            "regexp_functions",
            &FunctionSectionWriter::<2> {
                category: Category::Regexp,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/date-time.md",
        sections: &[(
            "date_time_functions",
            &FunctionSectionWriter::<2> {
                category: Category::DateTime,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/operator.md",
        sections: &[
            (
                "numeric_operator_functions",
                &FunctionSectionWriter::<3> {
                    category: Category::NUMERIC_OPERATOR,
                },
            ),
            (
                "comparison_operator_functions",
                &FunctionSectionWriter::<3> {
                    category: Category::COMPARISON_OPERATOR,
                },
            ),
            (
                "logical_operator_functions",
                &FunctionSectionWriter::<3> {
                    category: Category::LOGICAL_OPERATOR,
                },
            ),
        ],
    },
    DocFile {
        path: "docs/sql/functions/list.md",
        sections: &[(
            "list_functions",
            &FunctionSectionWriter::<2> {
                category: Category::List,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/system.md",
        sections: &[(
            "system_functions",
            &FunctionSectionWriter::<2> {
                category: Category::System,
            },
        )],
    },
    DocFile {
        path: "docs/sql/functions/table.md",
        sections: &[(
            "table_functions",
            &FunctionSectionWriter::<2> {
                category: Category::Table,
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
