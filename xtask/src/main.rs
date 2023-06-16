use anyhow::Result;
use clap::{Parser, Subcommand};
use std::{ffi::OsStr, io::Cursor};
use target::Target;
use xshell::{cmd, Shell};
use zip::ZipArchive;

mod target;
mod util;

#[derive(Parser)]
#[clap(name = "xtask")]
#[clap(about = "Additional cargo tasks", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build glaredb.
    Build {
        /// Build with the release profile.
        #[clap(short, long)]
        release: bool,
    },

    /// Run unit tests.
    UnitTests,

    /// Run doc tests.
    DocTests,

    /// Run SQL Logic Tests.
    #[clap(alias = "slt")]
    SqlLogicTests { rest: Vec<String> },

    /// Run tests with arbitrary arguments.
    Test { rest: Vec<String> },

    /// Run clippy.
    Clippy,

    /// Check formatting.
    FmtCheck,

    /// Build the dist binary for release.
    ///
    /// A zip archive will be placed in `target/dist` containing the release
    /// binary.
    ///
    /// By default, the compilation target matches the host machine. This can be
    /// overridden via the `DIST_TARGET_TRIPLE` environment variable.
    Dist,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = &Shell::new()?;
    sh.change_dir(util::project_root());
    sh.set_var("CARGO_TERM_COLOR", "always"); // Always print output with color.

    let target = Target::from_cfg()?;
    ensure_protoc(sh, &target)?;

    match cli.command {
        Commands::Build { release } => run_build(sh, release)?,
        Commands::UnitTests => run_tests(sh, ["--lib", "--bins"])?,
        Commands::DocTests => run_tests(sh, ["--doc"])?,
        Commands::SqlLogicTests { rest } => {
            let args = ["--test", "sqllogictests", "--"]
                .into_iter()
                .chain(rest.iter().map(|s| s.as_str()));
            run_tests(sh, args)?
        }
        Commands::Test { rest } => run_tests(sh, rest)?,
        Commands::Clippy => run_clippy(sh)?,
        Commands::FmtCheck => run_fmt_check(sh)?,
        Commands::Dist => run_dist(sh, &target)?,
    }

    Ok(())
}

fn run_build(sh: &Shell, release: bool) -> Result<()> {
    let mut cmd = cmd!(sh, "cargo build --bin glaredb");
    if release {
        cmd = cmd.arg("--release");
    }
    cmd.run()?;
    Ok(())
}

fn run_tests<I>(sh: &Shell, rest: I) -> Result<()>
where
    I: IntoIterator,
    I::Item: AsRef<OsStr>,
{
    let cmd = sh.cmd("cargo").arg("test").args(rest);
    cmd.run()?;
    Ok(())
}

fn run_clippy(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo clippy --all-features -- --deny warnings").run()?;
    Ok(())
}

fn run_fmt_check(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo fmt --check").run()?;
    Ok(())
}

fn run_dist(sh: &Shell, target: &Target) -> Result<()> {
    let triple = target.dist_target_triple()?;
    cmd!(sh, "cargo build --release --bin glaredb --target {triple}").run()?;

    // TODO: Code signing goes here.

    sh.remove_path(util::project_root().join("target").join("dist"))?;
    sh.create_dir(util::project_root().join("target").join("dist"))?;

    let src_path = util::project_root()
        .join("target")
        .join(target.dist_target_triple()?)
        .join("release")
        .join("glaredb");
    let dest_path = util::project_root()
        .join("target")
        .join("dist")
        .join(target.dist_zip_name()?);

    util::zip(&src_path, &dest_path)?;

    println!("Dist zip: {:?}", dest_path);
    Ok(())
}

/// Check if protoc is in path. If not, download and set the PROTOC env var.
fn ensure_protoc(sh: &Shell, target: &Target) -> Result<()> {
    const PROTOC_OUT_DIR: &str = "deps/protoc";
    const PROTOC_PATH: &str = "deps/protoc/bin/protoc";

    if cmd!(sh, "protoc --version").run().is_err() {
        if sh.path_exists(util::project_root().join(PROTOC_PATH)) {
            println!("Downloaded protoc already exists");
        } else {
            println!("Missing protoc, downloading...");
            sh.remove_path(util::project_root().join(PROTOC_OUT_DIR))?;
            let res = reqwest::blocking::get(target.protoc_url()?)?;
            ZipArchive::new(Cursor::new(res.bytes()?))?
                .extract(util::project_root().join(PROTOC_OUT_DIR))?;
        }

        sh.set_var("PROTOC", util::project_root().join(PROTOC_PATH));
    }
    Ok(())
}
