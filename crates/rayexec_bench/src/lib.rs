mod benchmark;
mod runner;

use clap::Parser;
use rayexec_error::Result;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;

#[derive(Parser)]
#[clap(name = "rayexec_bin")]
struct Arguments {
    // /// Print the EXPLAIN output for queries prior to running them.
    // #[clap(long, env = "DEBUG_PRINT_EXPLAIN")]
    // print_explain: bool,
    // /// Print the profile data for a query after running it.
    // #[clap(long, env = "DEBUG_PRINT_PROFILE_DATA")]
    // print_profile_data: bool,
    /// Pattern to match benchmark files to run.
    #[clap()]
    pattern: Option<String>,
}

pub trait EngineBuilder {
    fn build() -> Result<SingleUserEngine<ThreadedNativeExecutor, NativeRuntime>>;
}

pub fn run<B>(builder: B) -> Result<()>
where
    B: EngineBuilder,
{
    unimplemented!()
}
