use anyhow::Result;
use clap::{Parser, Subcommand};
use std::env;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use target::Target;
use xshell::{cmd, Shell};
use zip::ZipArchive;

mod target;

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

    /// Build test binaries.
    TestBins,

    /// Run unit tests.
    UnitTests,

    /// Run doc tests.
    DocTests,

    /// Run clippy.
    Clippy,

    /// Check formatting.
    FmtCheck,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sh = &Shell::new()?;
    sh.change_dir(project_root());

    let target = Target::from_cfg()?;
    ensure_protoc(sh, &target)?;

    match cli.command {
        Commands::Build { release } => build(sh, release)?,
        Commands::TestBins => build_test_bins(sh)?,
        Commands::UnitTests => unit_tests(sh)?,
        Commands::DocTests => doc_tests(sh)?,
        Commands::Clippy => clippy(sh)?,
        Commands::FmtCheck => fmt_check(sh)?,
    }

    Ok(())
}

fn build(sh: &Shell, release: bool) -> Result<()> {
    if release {
        cmd!(sh, "cargo build --release --bin glaredb").run()?;
    } else {
        cmd!(sh, "cargo build --bin glaredb").run()?;
    }
    Ok(())
}

fn build_test_bins(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo build --bin pgprototest").run()?;
    cmd!(sh, "cargo build --test sqllogictests").run()?;
    Ok(())
}

fn unit_tests(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo test --lib --bins").run()?;
    Ok(())
}

fn doc_tests(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo test --doc").run()?;
    Ok(())
}

fn clippy(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo clippy --all-features -- --deny warnings").run()?;
    Ok(())
}

fn fmt_check(sh: &Shell) -> Result<()> {
    cmd!(sh, "cargo fmt --check").run()?;
    Ok(())
}

/// Check if protoc is in path. If not, download and set the PROTOC env var.
fn ensure_protoc(sh: &Shell, target: &Target) -> Result<()> {
    const PROTOC_OUT_DIR: &str = "deps/protoc";
    const PROTOC_PATH: &str = "deps/protoc/bin/protoc";

    if cmd!(sh, "protoc --version").run().is_err() {
        if sh.path_exists(project_root().join(PROTOC_PATH)) {
            println!("Downloaded protoc already exists");
        } else {
            println!("Missing protoc, downloading...");
            sh.remove_path(project_root().join(PROTOC_OUT_DIR))?;
            let res = reqwest::blocking::get(target.protoc_url()?)?;
            ZipArchive::new(Cursor::new(res.bytes()?))?
                .extract(project_root().join(PROTOC_OUT_DIR))?;
        }

        sh.set_var("PROTOC", project_root().join(PROTOC_PATH));
    }
    Ok(())
}

fn project_root() -> PathBuf {
    Path::new(
        &env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}
